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

import hashlib
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from enum import StrEnum

from controlplane.contract import Contract
from controlplane.registry import SourceRecord, source_id_from_connector
from controlplane.secrets import connect_placeholder

log = logging.getLogger(__name__)

MANAGED_PREFIX = "fbi-src-"


@dataclass(frozen=True)
class CdcSettings:
    connect_clusters: dict[str, str]
    kafka_bootstrap: str = "kafka:9092"
    snapshot_mode: str = "initial"
    snapshot_threads: int = 1
    heartbeat_ms: int = 10_000
    max_batch_size: int = 4096
    max_queue_size: int = 16384
    poll_interval_ms: int = 200
    publication_mode: str = "filtered"
    publication_name: str = "fbi_cdc"
    signal_topic: str = "fineract.cdc.signals"
    max_creates_per_run: int = 5
    max_deletes_per_run: int = 10
    delete_orphans: bool = True
    drop_stale_slots: bool = True
    overrides: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> CdcSettings:
        return cls(
            connect_clusters=json.loads(os.getenv("FBI_CONNECT_CLUSTERS") or '{"default": "http://connect:8083"}'),
            kafka_bootstrap=os.getenv("FBI_KAFKA_BOOTSTRAP", "kafka:9092"),
            snapshot_mode=os.getenv("FBI_DEBEZIUM_SNAPSHOT_MODE", "initial"),
            snapshot_threads=int(os.getenv("FBI_DEBEZIUM_SNAPSHOT_THREADS", "1")),
            heartbeat_ms=int(os.getenv("FBI_DEBEZIUM_HEARTBEAT_MS", "10000")),
            max_batch_size=int(os.getenv("FBI_DEBEZIUM_MAX_BATCH_SIZE", "4096")),
            max_queue_size=int(os.getenv("FBI_DEBEZIUM_MAX_QUEUE_SIZE", "16384")),
            poll_interval_ms=int(os.getenv("FBI_DEBEZIUM_POLL_INTERVAL_MS", "200")),
            publication_mode=os.getenv("FBI_DEBEZIUM_PUBLICATION_MODE", "filtered"),
            publication_name=os.getenv("FBI_DEBEZIUM_PUBLICATION_NAME", "fbi_cdc"),
            signal_topic=os.getenv("FBI_DEBEZIUM_SIGNAL_TOPIC", "fineract.cdc.signals"),
            max_creates_per_run=int(os.getenv("FBI_RECONCILER_MAX_CREATES", "5")),
            max_deletes_per_run=int(os.getenv("FBI_RECONCILER_MAX_DELETES", "10")),
            delete_orphans=os.getenv("FBI_RECONCILER_DELETE_ORPHANS", "true").lower() == "true",
            drop_stale_slots=os.getenv("FBI_RECONCILER_DROP_STALE_SLOTS", "true").lower() == "true",
            overrides=json.loads(os.getenv("FBI_CONNECTOR_OVERRIDES") or "{}"),
        )


def build_connector_config(source: SourceRecord, contract: Contract, settings: CdcSettings) -> dict[str, str]:
    if not source.db_host or not source.db_name or not source.db_user:
        raise ValueError(f"{source.source_id}: db_host, db_name and db_user are required for CDC")
    schema = source.db_schema or contract.source_schema
    tables = [f"{schema}.{t.table_name}" for t in contract.tables]
    columns = sorted(
        {
            f"{schema}.{t.table_name}.{candidate}"
            for t in contract.tables
            for c in t.columns
            if not c.synthetic
            for candidate in c.cdc_candidates
        }
    )
    config: dict[str, str] = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": source.db_host,
        "database.port": str(source.db_port),
        "database.user": source.db_user,
        "database.password": connect_placeholder(source.secret_ref),
        "database.dbname": source.db_name,
        "database.sslmode": source.labels.get("sslmode", "prefer"),
        "topic.prefix": source.source_id,
        "plugin.name": "pgoutput",
        "slot.name": source.slot_name,
        "slot.drop.on.stop": "false",
        "publication.name": settings.publication_name,
        "publication.autocreate.mode": settings.publication_mode,
        "table.include.list": ",".join(tables),
        "column.include.list": ",".join(columns).replace(".", r"\."),
        "snapshot.mode": settings.snapshot_mode,
        "snapshot.max.threads": str(settings.snapshot_threads),
        "decimal.handling.mode": "string",
        "time.precision.mode": "adaptive_time_microseconds",
        "interval.handling.mode": "string",
        "tombstones.on.delete": "false",
        "heartbeat.interval.ms": str(settings.heartbeat_ms),
        "max.batch.size": str(settings.max_batch_size),
        "max.queue.size": str(settings.max_queue_size),
        "poll.interval.ms": str(settings.poll_interval_ms),
        "signal.enabled.channels": "kafka",
        "signal.kafka.topic": settings.signal_topic,
        "signal.kafka.bootstrap.servers": settings.kafka_bootstrap,
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "transforms": "route,heartbeat",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": r"[^.]+\.[^.]+\.(.*)",
        "transforms.route.replacement": f"{contract.topic_prefix}.$1",
        "transforms.heartbeat.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.heartbeat.regex": r"__debezium-heartbeat\..*",
        "transforms.heartbeat.replacement": f"{contract.topic_prefix}.heartbeats",
        "producer.override.compression.type": "zstd",
        "producer.override.linger.ms": "50",
        "producer.override.batch.size": "262144",
        "producer.override.acks": "all",
        "producer.override.enable.idempotence": "true",
        "errors.retry.timeout": "-1",
        "errors.tolerance": "none",
    }
    if source.labels.get("heartbeat_table"):
        config["heartbeat.action.query"] = (
            f"INSERT INTO {source.labels['heartbeat_table']} (id, ts) VALUES (1, NOW()) "
            "ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts"
        )
    if source.labels.get("signal_table"):
        config["signal.enabled.channels"] = "source,kafka"
        config["signal.data.collection"] = source.labels["signal_table"]
    config.update({k: str(v) for k, v in settings.overrides.items()})
    return config


def config_hash(config: dict[str, str]) -> str:
    return hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()[:16]


class ActionKind(StrEnum):
    CREATE = "create"
    UPDATE = "update"
    RESTART_FAILED = "restart_failed"
    PAUSE = "pause"
    RESUME = "resume"
    DELETE = "delete"


@dataclass(frozen=True)
class DesiredConnector:
    source_id: str
    name: str
    cluster: str
    config: dict[str, str]
    paused: bool = False


@dataclass(frozen=True)
class ObservedConnector:
    name: str
    config: dict[str, str]
    state: str
    task_states: tuple[str, ...]
    trace: str | None = None


@dataclass(frozen=True)
class ConnectorAction:
    kind: ActionKind
    name: str
    cluster: str
    source_id: str | None = None
    config: dict[str, str] | None = None


def plan_actions(
    desired: list[DesiredConnector],
    observed: dict[str, dict[str, ObservedConnector]],
    settings: CdcSettings,
) -> tuple[list[ConnectorAction], list[str]]:
    actions: list[ConnectorAction] = []
    deferred: list[str] = []
    creates = deletes = 0
    desired_by_cluster: dict[str, set[str]] = {}

    for d in sorted(desired, key=lambda x: x.name):
        desired_by_cluster.setdefault(d.cluster, set()).add(d.name)
        obs = observed.get(d.cluster, {}).get(d.name)
        if obs is None:
            if creates >= settings.max_creates_per_run:
                deferred.append(d.name)
                continue
            creates += 1
            actions.append(ConnectorAction(ActionKind.CREATE, d.name, d.cluster, d.source_id, d.config))
            if d.paused:
                actions.append(ConnectorAction(ActionKind.PAUSE, d.name, d.cluster, d.source_id))
            continue
        drift = {k: v for k, v in d.config.items() if obs.config.get(k) != v}
        if drift:
            actions.append(ConnectorAction(ActionKind.UPDATE, d.name, d.cluster, d.source_id, d.config))
        if d.paused and obs.state != "PAUSED":
            actions.append(ConnectorAction(ActionKind.PAUSE, d.name, d.cluster, d.source_id))
        elif not d.paused and obs.state == "PAUSED":
            actions.append(ConnectorAction(ActionKind.RESUME, d.name, d.cluster, d.source_id))
        if obs.state == "FAILED" or "FAILED" in obs.task_states:
            actions.append(ConnectorAction(ActionKind.RESTART_FAILED, d.name, d.cluster, d.source_id))

    if settings.delete_orphans:
        for cluster, conns in sorted(observed.items()):
            for name in sorted(conns):
                if not name.startswith(MANAGED_PREFIX) or name in desired_by_cluster.get(cluster, set()):
                    continue
                if deletes >= settings.max_deletes_per_run:
                    deferred.append(name)
                    continue
                deletes += 1
                actions.append(ConnectorAction(ActionKind.DELETE, name, cluster, source_id_from_connector(name)))
    return actions, deferred


class ConnectClient:

    def __init__(self, base_url: str, timeout: float = 30.0) -> None:
        self._base = base_url.rstrip("/")
        self._timeout = timeout

    def _request(self, method: str, path: str, body: dict | None = None) -> object:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self._base + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:  # noqa: S310
                payload = resp.read()
                return json.loads(payload) if payload else None
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:500]
            raise RuntimeError(f"Kafka Connect {method} {path} failed: {exc.code} {detail}") from exc

    def list_connectors(self) -> dict[str, ObservedConnector]:
        payload = self._request("GET", "/connectors?expand=status&expand=info") or {}
        result = {}
        for name, item in payload.items():
            status = item.get("status", {})
            tasks = status.get("tasks", [])
            result[name] = ObservedConnector(
                name=name,
                config=item.get("info", {}).get("config", {}),
                state=status.get("connector", {}).get("state", "UNKNOWN"),
                task_states=tuple(t.get("state", "UNKNOWN") for t in tasks),
                trace=next((t.get("trace") for t in tasks if t.get("trace")), None),
            )
        return result

    def put_config(self, name: str, config: dict[str, str]) -> None:
        self._request("PUT", f"/connectors/{urllib.parse.quote(name)}/config", config)

    def restart_failed(self, name: str) -> None:
        self._request("POST", f"/connectors/{urllib.parse.quote(name)}/restart?includeTasks=true&onlyFailed=true")

    def pause(self, name: str) -> None:
        self._request("PUT", f"/connectors/{urllib.parse.quote(name)}/pause")

    def resume(self, name: str) -> None:
        self._request("PUT", f"/connectors/{urllib.parse.quote(name)}/resume")

    def delete(self, name: str) -> None:
        self._request("DELETE", f"/connectors/{urllib.parse.quote(name)}")

    def execute(self, action: ConnectorAction) -> None:
        if action.kind in (ActionKind.CREATE, ActionKind.UPDATE):
            self.put_config(action.name, action.config or {})
        elif action.kind is ActionKind.RESTART_FAILED:
            self.restart_failed(action.name)
        elif action.kind is ActionKind.PAUSE:
            self.pause(action.name)
        elif action.kind is ActionKind.RESUME:
            self.resume(action.name)
        elif action.kind is ActionKind.DELETE:
            self.delete(action.name)
