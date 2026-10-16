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


from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

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
from controlplane.registry import RUNNING_STATES, SourceRegistry, source_id_from_connector

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


def kafka_owned_sources(conn) -> set[str]:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT DISTINCT source_id FROM meta.ingestion_ownership WHERE engine = 'KAFKA' OR shadow_engine = 'KAFKA'"
    )
    return {r[0] for r in cursor.fetchall()}


def desired_connectors(
    conn, contract: Contract, settings: CdcSettings, shard_index: int | None = None, shard_count: int | None = None
) -> tuple[list[DesiredConnector], set[str], list[dict]]:
    registry = SourceRegistry(conn)
    shard_sources = registry.list(shard_index=shard_index, shard_count=shard_count)
    kafka_sources = kafka_owned_sources(conn)
    desired, errors = [], []
    for s in shard_sources:
        if s.source_id not in kafka_sources or s.labels.get("synthetic"):
            continue
        if s.lifecycle_state not in RUNNING_STATES and s.lifecycle_state != "PAUSED":
            continue
        try:
            config = build_connector_config(s, contract, settings)
        except ValueError as exc:
            errors.append({"source_id": s.source_id, "error": str(exc)})
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
    return desired, {s.source_id for s in shard_sources}, errors


def reconcile(
    conn,
    contract: Contract,
    settings: CdcSettings,
    shard_index: int | None = None,
    shard_count: int | None = None,
    dry_run: bool = False,
    clients: dict[str, ConnectClient] | None = None,
) -> ReconcileReport:
    report = ReconcileReport()
    desired, shard_source_ids, errors = desired_connectors(conn, contract, settings, shard_index, shard_count)
    report.errors.extend(errors)
    report.desired = len(desired)

    clients = clients or {name: ConnectClient(url) for name, url in settings.connect_clusters.items()}
    cursor = conn.cursor()
    cursor.execute("SELECT source_id FROM meta.source_registry")
    all_registered = {r[0] for r in cursor.fetchall()}

    observed: dict[str, dict[str, ObservedConnector]] = {}
    for cluster, client in clients.items():
        try:
            conns = client.list_connectors()
        except Exception as exc:  # noqa: BLE001
            report.errors.append({"cluster": cluster, "error": str(exc)})
            continue
        observed[cluster] = {
            name: c
            for name, c in conns.items()
            if not name.startswith(MANAGED_PREFIX)
            or source_id_from_connector(name) in shard_source_ids
            or ((shard_index or 0) == 0 and source_id_from_connector(name) not in all_registered)
        }
    report.observed = sum(len(v) for v in observed.values())
    reachable = [d for d in desired if d.cluster in observed]
    actions, deferred = plan_actions(reachable, observed, settings)
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
            report.actions.extend(cleanup_stale_slots(conn, desired, shard_index, shard_count, report))
    return report


def stale_slots(slots: list[tuple[str, bool]], source_id: str, keep: str | None) -> list[str]:
    prefix = f"fbi_{source_id.replace('-', '_')}_g"
    return sorted(
        name
        for name, active in slots
        if name.startswith(prefix) and name[len(prefix) :].isdigit() and name != keep and not active
    )


def cleanup_stale_slots(conn, desired, shard_index, shard_count, report) -> list[dict]:
    from controlplane import secrets
    from controlplane.db import PgConfig
    from controlplane.db import connect as pg_connect

    keep = {d.source_id: d.config.get("slot.name") for d in desired}
    actions = []
    for s in SourceRegistry(conn).list(shard_index=shard_index, shard_count=shard_count):
        if not s.db_host or not s.secret_ref or s.labels.get("synthetic"):
            continue
        try:
            src = pg_connect(
                PgConfig(
                    s.db_host,
                    s.db_port,
                    s.db_name or "",
                    s.db_user or "",
                    secrets.resolve(s.secret_ref),
                    s.labels.get("sslmode", "disable"),
                )
            )
            try:
                cur = src.cursor()
                cur.execute("SELECT slot_name, active FROM pg_replication_slots WHERE database = current_database()")
                for slot in stale_slots(
                    [(r[0], bool(r[1])) for r in cur.fetchall()], s.source_id, keep.get(s.source_id)
                ):
                    cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
                    actions.append({"kind": "drop_slot", "name": slot, "source_id": s.source_id})
                src.commit()
            finally:
                src.close()
        except Exception as exc:  # noqa: BLE001
            report.errors.append({"source_id": s.source_id, "error": f"slot cleanup: {exc}"})
    return actions


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
