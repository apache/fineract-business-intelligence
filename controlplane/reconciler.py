# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from controlplane import secrets
from controlplane.connectors import (
    MANAGED_PREFIX,
    CdcSettings,
    ConnectClient,
    DesiredConnector,
    ObservedConnector,
    build_connector_config,
    config_hash,
    plan_actions,
)
from controlplane.contract import Contract
from controlplane.db import connect as pg_connect
from controlplane.db import source_pg_config
from controlplane.registry import RUNNING_STATES, SourceRecord, SourceRegistry, source_id_from_connector

log = logging.getLogger(__name__)


@dataclass
class ReconcileReport:
    desired: int = 0
    observed: int = 0
    actions: list[dict] = field(default_factory=list)
    deferred: list[str] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)

    def as_dict(self) -> dict:
        return self.__dict__


@dataclass
class DesiredState:
    connectors: list[DesiredConnector]
    shard_source_ids: set[str]
    errors: list[dict]
    unknown_sources: set[str]


def kafka_owned_sources(conn) -> set[str]:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT DISTINCT source_id FROM meta.ingestion_ownership WHERE engine = 'KAFKA' OR shadow_engine = 'KAFKA'"
    )
    return {r[0] for r in cursor.fetchall()}


def desired_connectors(
    conn, contract: Contract, settings: CdcSettings, shard_index: int | None = None, shard_count: int | None = None
) -> DesiredState:
    registry = SourceRegistry(conn)
    shard_sources = registry.list(shard_index=shard_index, shard_count=shard_count)
    kafka_sources = kafka_owned_sources(conn)
    desired, errors, unknown = [], [], set()
    for s in shard_sources:
        if s.source_id not in kafka_sources or s.labels.get("synthetic"):
            continue
        if s.lifecycle_state not in RUNNING_STATES and s.lifecycle_state != "PAUSED":
            continue
        try:
            config = build_connector_config(s, contract, settings)
        except ValueError as exc:
            errors.append({"source_id": s.source_id, "error": str(exc)})
            unknown.add(s.source_id)
            continue
        desired.append(
            DesiredConnector(
                source_id=s.source_id,
                name=s.connector_name,
                cluster=s.connect_cluster,
                config=config,
                paused=s.lifecycle_state == "PAUSED",
            )
        )
    return DesiredState(desired, {s.source_id for s in shard_sources}, errors, unknown)


def observe_cluster(
    client: ConnectClient, shard_source_ids: set[str], all_registered: set[str], shard_index: int | None
) -> dict[str, ObservedConnector]:
    result: dict[str, ObservedConnector] = {}
    for name in client.list_names():
        if not name.startswith(MANAGED_PREFIX):
            continue
        source_id = source_id_from_connector(name)
        mine = source_id in shard_source_ids or ((shard_index or 0) == 0 and source_id not in all_registered)
        if mine:
            result[name] = client.describe(name)
    return result


def reconcile(
    conn,
    contract: Contract,
    settings: CdcSettings,
    shard_index: int | None = None,
    shard_count: int | None = None,
    dry_run: bool = False,
    clients: dict[str, ConnectClient] | None = None,
    connect_source: Callable = pg_connect,
) -> ReconcileReport:
    report = ReconcileReport()
    state = desired_connectors(conn, contract, settings, shard_index, shard_count)
    desired = state.connectors
    report.errors.extend(state.errors)
    report.desired = len(desired)

    clients = clients or {
        name: ConnectClient(url, user=settings.connect_user, password=settings.connect_password)
        for name, url in settings.connect_clusters.items()
    }
    cursor = conn.cursor()
    cursor.execute("SELECT source_id FROM meta.source_registry")
    all_registered = {r[0] for r in cursor.fetchall()}

    observed: dict[str, dict[str, ObservedConnector]] = {}
    for cluster, client in clients.items():
        try:
            observed[cluster] = observe_cluster(client, state.shard_source_ids, all_registered, shard_index)
        except Exception as exc:  # noqa: BLE001
            report.errors.append({"cluster": cluster, "error": str(exc)})
    report.observed = sum(len(v) for v in observed.values())
    protected = frozenset(
        name
        for conns in observed.values()
        for name in conns
        if source_id_from_connector(name) in state.unknown_sources
    )
    reachable = [d for d in desired if d.cluster in observed]
    actions, deferred = plan_actions(reachable, observed, settings, protected)
    report.deferred = deferred

    for action in actions:
        entry = {"kind": action.kind.value, "name": action.name, "cluster": action.cluster}
        if not dry_run:
            try:
                clients[action.cluster].execute(action)
            except Exception as exc:  # noqa: BLE001
                entry["error"] = str(exc)
                report.errors.append(entry)
                continue
        report.actions.append(entry)

    if not dry_run:
        _record_observed(conn, desired, observed)
        if settings.drop_stale_slots:
            report.actions.extend(
                cleanup_stale_slots(
                    conn, state, observed, shard_index, shard_count, report, connect_source,
                    all_clusters_observed=set(observed) == set(clients),
                )
            )
    return report


def stale_slots(slots: list[tuple[str, bool]], source_id: str, keep: str | None) -> list[str]:
    prefix = f"fbi_{source_id.replace('-', '_')}_g"
    return sorted(
        name
        for name, active in slots
        if name.startswith(prefix) and name[len(prefix) :].isdigit() and name != keep and not active
    )


def slot_cleanup_decision(
    source: SourceRecord, kafka_sources: set[str], desired_slots: dict[str, str | None], unknown: set[str]
) -> tuple[bool, str | None]:
    if source.source_id in unknown:
        return False, None
    if source.source_id in desired_slots:
        return True, desired_slots[source.source_id]
    if source.lifecycle_state in ("RETIRING", "RETIRED") or source.source_id not in kafka_sources:
        return True, None
    return False, None


def slot_check_key(source: SourceRecord, kafka_sources: set[str]) -> str:
    owner = "kafka" if source.source_id in kafka_sources else "other"
    return f"{source.capture_generation}:{owner}:{source.lifecycle_state}"


def _slot_check_due(conn, source_id: str, key: str, interval_minutes: int) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT check_key IS DISTINCT FROM %s OR checked_at < NOW() - make_interval(mins => %s) "
        "FROM meta.slot_check WHERE source_id = %s",
        (key, interval_minutes, source_id),
    )
    row = cursor.fetchone()
    return True if row is None else bool(row[0])


def _mark_slot_checked(conn, source_id: str, key: str) -> None:
    conn.cursor().execute(
        "INSERT INTO meta.slot_check (source_id, check_key, checked_at) VALUES (%s, %s, NOW()) "
        "ON CONFLICT (source_id) DO UPDATE SET check_key = EXCLUDED.check_key, checked_at = EXCLUDED.checked_at",
        (source_id, key),
    )


@dataclass
class SlotCleanup:
    dropped: list[str] = field(default_factory=list)
    remaining: list[str] = field(default_factory=list)


def drop_slots(source: SourceRecord, keep: str | None, connect_source: Callable = pg_connect) -> SlotCleanup:
    src = connect_source(source_pg_config(source, secrets.resolve(source.secret_ref, source.source_id)))
    result = SlotCleanup()
    prefix = f"fbi_{source.source_id.replace('-', '_')}_g"
    try:
        cur = src.cursor()
        cur.execute("SELECT slot_name, active FROM pg_replication_slots WHERE database = current_database()")
        for slot in stale_slots([(r[0], bool(r[1])) for r in cur.fetchall()], source.source_id, keep):
            cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
            result.dropped.append(slot)
        src.commit()
        cur.execute(
            "SELECT slot_name FROM pg_replication_slots WHERE database = current_database() AND slot_name LIKE %s",
            (prefix + "%",),
        )
        result.remaining = sorted(
            r[0] for r in cur.fetchall() if r[0].startswith(prefix) and r[0][len(prefix):].isdigit() and r[0] != keep
        )
    finally:
        src.close()
    return result


def cleanup_stale_slots(
    conn,
    state: DesiredState,
    observed,
    shard_index,
    shard_count,
    report,
    connect_source: Callable = pg_connect,
    all_clusters_observed: bool = True,
) -> list[dict]:
    interval = int(os.getenv("FBI_SLOT_CLEANUP_INTERVAL_MINUTES", "1440"))
    tombstone_interval = int(os.getenv("FBI_TOMBSTONE_RETRY_MINUTES", "30"))
    desired_slots = {d.source_id: d.config.get("slot.name") for d in state.connectors}
    kafka_sources = kafka_owned_sources(conn)
    observed_names = {name for conns in observed.values() for name in conns}
    actions: list[dict] = []
    for s in SourceRegistry(conn).list(shard_index=shard_index, shard_count=shard_count):
        if not s.db_host or not s.secret_ref or s.labels.get("synthetic"):
            continue
        inspect, keep = slot_cleanup_decision(s, kafka_sources, desired_slots, state.unknown_sources)
        if not inspect:
            continue
        if s.capture_generation == 0 and s.source_id not in desired_slots and s.lifecycle_state != "RETIRED":
            continue
        tombstone = s.lifecycle_state == "RETIRED" and bool(s.labels.get("forget_after_cleanup"))
        check_key = slot_check_key(s, kafka_sources)
        if not _slot_check_due(conn, s.source_id, check_key, tombstone_interval if tombstone else interval):
            continue
        try:
            cleanup = drop_slots(s, keep, connect_source)
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            _mark_slot_checked(conn, s.source_id, check_key)
            conn.commit()
            report.errors.append({"source_id": s.source_id, "error": f"slot cleanup: {exc}"})
            continue
        for slot in cleanup.dropped:
            actions.append({"kind": "drop_slot", "name": slot, "source_id": s.source_id})
        _mark_slot_checked(conn, s.source_id, check_key)
        conn.commit()
        if not tombstone:
            continue
        connector_left = any(source_id_from_connector(n) == s.source_id for n in observed_names)
        if cleanup.remaining or connector_left or not all_clusters_observed:
            report.errors.append({
                "source_id": s.source_id,
                "error": "retired source kept as a tombstone: "
                + (f"slots still present {cleanup.remaining}" if cleanup.remaining else
                   "a connector still exists" if connector_left else "not every Connect cluster could be observed"),
            })
            continue
        conn.cursor().execute("DELETE FROM meta.source_registry WHERE source_id = %s", (s.source_id,))
        conn.commit()
        actions.append({"kind": "forget_source", "source_id": s.source_id})
    return actions


def retire_artifacts(
    conn,
    source: SourceRecord,
    settings: CdcSettings,
    clients: dict[str, ConnectClient] | None = None,
    connect_source: Callable = pg_connect,
    wait_seconds: float = 10.0,
) -> dict:
    result: dict = {"connectors_deleted": [], "slots_dropped": [], "slots_remaining": [], "errors": []}
    clients = clients or {
        name: ConnectClient(url, user=settings.connect_user, password=settings.connect_password)
        for name, url in settings.connect_clusters.items()
    }
    for cluster, client in clients.items():
        try:
            for name in client.list_names():
                if name.startswith(MANAGED_PREFIX) and source_id_from_connector(name) == source.source_id:
                    client.delete(name)
                    result["connectors_deleted"].append(name)
        except Exception as exc:  # noqa: BLE001
            result["errors"].append(f"connect {cluster}: {exc}")
    if source.db_host and source.secret_ref and not source.labels.get("synthetic"):
        deadline = time.monotonic() + wait_seconds
        while True:
            try:
                cleanup = drop_slots(source, None, connect_source)
            except Exception as exc:  # noqa: BLE001
                result["errors"].append(f"source slots: {exc}")
                break
            result["slots_dropped"].extend(cleanup.dropped)
            result["slots_remaining"] = cleanup.remaining
            if not cleanup.remaining or time.monotonic() >= deadline:
                break
            time.sleep(1)
    return result


def _record_observed(conn, desired: list[DesiredConnector], observed: dict[str, dict[str, ObservedConnector]]) -> None:
    cursor = conn.cursor()
    for d in desired:
        obs = observed.get(d.cluster, {}).get(d.name)
        cursor.execute(
            "INSERT INTO meta.connector_state (source_id, connector_name, connect_cluster, desired_state, "
            "observed_state, task_states, last_error, config_hash, observed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW()) "
            "ON CONFLICT (source_id) DO UPDATE SET connector_name = EXCLUDED.connector_name, "
            "connect_cluster = EXCLUDED.connect_cluster, desired_state = EXCLUDED.desired_state, "
            "observed_state = EXCLUDED.observed_state, task_states = EXCLUDED.task_states, "
            "last_error = EXCLUDED.last_error, config_hash = EXCLUDED.config_hash, observed_at = NOW()",
            (
                d.source_id,
                d.name,
                d.cluster,
                "PAUSED" if d.paused else "RUNNING",
                obs.state if obs else "MISSING",
                json.dumps(list(obs.task_states) if obs else []),
                (obs.trace or "")[:2000] if obs else None,
                config_hash(d.config),
            ),
        )
    conn.commit()
