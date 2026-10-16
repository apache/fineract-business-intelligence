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

import logging
import math
import os
from collections.abc import Iterable

from controlplane import reconcile as data_reconcile
from controlplane import secrets
from controlplane.connectors import CdcSettings
from controlplane.contract import load_contract
from controlplane.db import PgConfig, connect, rows_as_dicts, warehouse_config_from_env
from controlplane.ownership import Action, Engine, TransitionRequest
from controlplane.ownership_repo import OwnershipRepository
from controlplane.reconciler import reconcile
from controlplane.registry import RUNNING_STATES, SourceRecord, SourceRegistry

log = logging.getLogger(__name__)


def _wh():
    return connect(warehouse_config_from_env(application_name="fbi-airflow"))


def shard_plan(shard_size: int | None = None, max_shards: int | None = None) -> list[dict]:
    shard_size = shard_size or int(os.getenv("FBI_SHARD_SIZE", "200"))
    max_shards = max_shards or int(os.getenv("FBI_MAX_SHARDS", "32"))
    conn = _wh()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM meta.source_registry WHERE lifecycle_state <> 'RETIRED'")
        total = int(cur.fetchone()[0])
        conn.commit()
    finally:
        conn.close()
    count = max(1, min(max_shards, math.ceil(total / max(1, shard_size))))
    return [{"index": i, "count": count} for i in range(count)]


def reconcile_connectors_shard(index: int, count: int, dry_run: bool = False) -> dict:
    conn = _wh()
    try:
        report = reconcile(conn, load_contract(), CdcSettings.from_env(), index, count, dry_run=dry_run)
    finally:
        conn.close()
    result = report.as_dict()
    log.info("connector reconcile shard %s/%s: %s", index, count, {k: result[k] for k in ("desired", "observed")})
    if report.errors:
        log.warning("reconcile errors: %s", report.errors[:20])
    return result


def python_ingest(mode: str, index: int | None = None, count: int | None = None,
                  source_ids: list[str] | None = None) -> dict:
    from extractor.extractor import run_sources

    results = run_sources(mode, source_ids=source_ids, shard=(index, count) if count else None)
    summary = {
        "mode": mode,
        "sources": len(results),
        "succeeded": sum(1 for r in results if r["status"] in ("success", "skipped")),
        "partial": [r["source_id"] for r in results if r["status"] == "partial"],
        "failed": [{"source_id": r["source_id"], "error": r.get("error")} for r in results if r["status"] == "failed"],
        "rows_applied": sum(r.get("rows_applied", 0) for r in results),
    }
    if summary["failed"]:
        raise RuntimeError(f"Python ingestion failed for {len(summary['failed'])} source(s): {summary['failed'][:5]}")
    return summary


def freshness_breaches(slo_seconds: int | None = None) -> list[dict]:
    slo = slo_seconds or int(os.getenv("FBI_FRESHNESS_SLO_SECONDS", "900"))
    conn = _wh()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT o.source_id, o.table_name, o.engine, f.last_applied_at,
                   EXTRACT(EPOCH FROM (NOW() - f.last_applied_at))::bigint AS age_seconds
            FROM meta.ingestion_ownership o
            JOIN meta.source_registry r USING (source_id)
            LEFT JOIN meta.v_table_freshness f
              ON f.source_id = o.source_id AND f.table_name = o.table_name AND f.target = 'prod'
            WHERE r.lifecycle_state = 'ACTIVE'
              AND o.engine IN ('PYTHON', 'KAFKA')
              AND o.cutover_state <> 'PAUSED'
              AND COALESCE((r.labels ->> 'synthetic')::boolean, false) = false
              AND (f.last_applied_at IS NULL OR f.last_applied_at < NOW() - make_interval(secs => %s))
            ORDER BY age_seconds DESC NULLS FIRST
            LIMIT 500
            """,
            (slo,),
        )
        rows = rows_as_dicts(cur)
        conn.commit()
        return rows
    finally:
        conn.close()


def apply_ownership_action(
    source_id: str,
    tables: str | Iterable[str],
    action: str,
    actor: str,
    reason: str,
    engine: str | None = None,
    approved_by: str | None = None,
) -> list[dict]:
    contract = load_contract()
    table_list = list(contract.table_names) if tables in (None, "", "all") else (
        [t.strip() for t in tables.split(",")] if isinstance(tables, str) else list(tables)
    )
    conn = _wh()
    try:
        results = OwnershipRepository(conn).apply(
            source_id,
            table_list,
            TransitionRequest(
                Action(action), actor, reason, engine=Engine(engine) if engine else None, approved_by=approved_by
            ),
        )
    finally:
        conn.close()
    return [{"table": r.current.table_name, "from": r.previous.as_dict(), "to": r.current.as_dict(),
             "side_effects": [e.value for e in r.side_effects]} for r in results]


def cutover_gate(source_id: str, tables: str, actor: str, settle_seconds: int, max_discrepancies: int) -> dict:
    contract = load_contract()
    table_list = list(contract.table_names) if tables in ("", "all") else [t.strip() for t in tables.split(",")]
    conn = _wh()
    try:
        return data_reconcile.run_cutover_gate(conn, source_id, table_list, actor, settle_seconds, max_discrepancies)
    finally:
        conn.close()


def validate_source(source: SourceRecord, engine: str) -> dict:
    checks: dict[str, object] = {}
    cfg = PgConfig(source.db_host or "", source.db_port, source.db_name or "", source.db_user or "",
                   secrets.resolve(source.secret_ref), source.labels.get("sslmode", "disable"),
                   application_name="fbi-onboarding")
    conn = connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute("SELECT current_setting('server_version_num')::int")
        checks["server_version_num"] = int(cur.fetchone()[0])
        if engine == "PYTHON":
            cur.execute(
                "SELECT table_name FROM information_schema.views WHERE table_schema = %s",
                (source.compat_schema,),
            )
            present = {r[0] for r in cur.fetchall()}
            missing = sorted(set(load_contract().table_names) - present)
            checks["missing_compat_views"] = missing
            checks["ok"] = not missing
        else:
            cur.execute("SELECT current_setting('wal_level')")
            checks["wal_level"] = cur.fetchone()[0]
            cur.execute("SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user")
            checks["replication_privilege"] = bool(cur.fetchone()[0])
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (source.db_schema,)
            )
            present = {r[0] for r in cur.fetchall()}
            missing = sorted(set(load_contract().table_names) - present)
            checks["missing_tables"] = missing
            checks["ok"] = checks["wal_level"] == "logical" and checks["replication_privilege"] and not missing
        conn.commit()
    finally:
        conn.close()
    return checks


def onboard_source(params: dict, actor: str) -> dict:
    engine = params.get("engine", "PYTHON").upper()
    record = SourceRecord(
        source_id=params["source_id"],
        tenant_id=params.get("tenant_id") or params["source_id"],
        display_name=params.get("display_name"),
        region=params.get("region") or "default",
        db_host=params.get("db_host"),
        db_port=int(params.get("db_port") or 5432),
        db_name=params.get("db_name"),
        db_user=params.get("db_user"),
        secret_ref=params.get("secret_ref"),
        compat_schema=params.get("compat_schema") or "bi_connector_source",
        connect_cluster=params.get("connect_cluster") or "default",
        lifecycle_state="REQUESTED",
        labels=params.get("labels") or {},
    )
    conn = _wh()
    try:
        registry = SourceRegistry(conn)
        registry.upsert([record])
        conn.commit()
        registry.set_lifecycle(record.source_id, "VALIDATING")
        conn.commit()
        checks = validate_source(record, engine)
        if not checks.get("ok"):
            registry.set_lifecycle(record.source_id, "REQUESTED")
            conn.commit()
            raise RuntimeError(f"Source {record.source_id} failed validation: {checks}")
        registry.set_lifecycle(record.source_id, "PROVISIONING")
        repo = OwnershipRepository(conn)
        repo.ensure_rows(record.source_id, load_contract().table_names)
        conn.commit()
        unowned = [o.table_name for o in repo.list(source_id=record.source_id) if o.engine is Engine.NONE]
        conn.commit()
        if unowned:
            repo.apply(record.source_id, unowned,
                       TransitionRequest(Action.ASSIGN, actor, "onboarding", engine=Engine(engine)))
        registry.set_lifecycle(record.source_id, "ACTIVE")
        conn.commit()
    finally:
        conn.close()
    return {"source_id": record.source_id, "engine": engine, "checks": checks}


def source_reconcile_shard(index: int, count: int, max_discrepancies: int = 0) -> dict:
    bucket = int(os.getenv("RECONCILE_BUCKET_SIZE", "100000"))
    contract = load_contract()
    conn = _wh()
    results, failures = [], []
    try:
        registry = SourceRegistry(conn)
        sources = [s for s in registry.list(states=RUNNING_STATES, shard_index=index, shard_count=count)
                   if s.db_host and s.secret_ref and not s.labels.get("synthetic")]
        owned = {(o.source_id, o.table_name) for o in OwnershipRepository(conn).list()
                 if o.engine in (Engine.PYTHON, Engine.KAFKA)}
        conn.commit()
        for s in sources:
            src = connect(PgConfig(s.db_host or "", s.db_port, s.db_name or "", s.db_user or "",
                                   secrets.resolve(s.secret_ref), s.labels.get("sslmode", "disable"),
                                   application_name="fbi-reconcile"))
            try:
                for t in contract.tables:
                    if (s.source_id, t.table_name) not in owned:
                        continue
                    r = data_reconcile.source_vs_warehouse(src, conn, s.source_id, s.db_schema, t, bucket_size=bucket)
                    passed = r.passed(max_discrepancies)
                    rid = data_reconcile.record_run(conn, r, "PASSED" if passed else "FAILED")
                    entry = {"source_id": s.source_id, "table": t.table_name, "reconciliation_id": rid,
                             "compared_rows": r.compared_rows, "discrepancies": r.discrepancies}
                    results.append(entry)
                    if not passed:
                        failures.append(entry)
            finally:
                src.close()
    finally:
        conn.close()
    if failures:
        raise RuntimeError(f"{len(failures)} table(s) failed source reconciliation: {failures[:10]}")
    return {"checked": len(results)}


def maintenance() -> dict:
    conn = _wh()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM meta.entity_change_log
            WHERE change_id <= COALESCE((SELECT MIN(last_change_id) FROM meta.dbt_change_cursor), 0)
              AND changed_at < NOW() - make_interval(days => meta.setting('change_log_retention_days', '14')::int)
            """
        )
        purged_changes = cur.rowcount
        cur.execute("DELETE FROM meta.ingestion_audit WHERE minute_bucket < NOW() - INTERVAL '30 days'")
        purged_audit = cur.rowcount
        cur.execute("DELETE FROM meta.reconciliation_run WHERE started_at < NOW() - INTERVAL '180 days'")
        purged_recon = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return {"change_log": purged_changes, "audit": purged_audit, "reconciliation": purged_recon}
