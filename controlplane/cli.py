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

import argparse
import csv
import json
import logging
import os
import sys

from controlplane import reconcile as data_reconcile
from controlplane.connectors import CdcSettings, build_connector_config
from controlplane.contract import load_contract
from controlplane.db import connect, rows_as_dicts, warehouse_config_from_env
from controlplane.ownership import Action, Engine, TransitionRequest
from controlplane.ownership_repo import OwnershipRepository
from controlplane.reconciler import reconcile
from controlplane.registry import SourceRecord, SourceRegistry


def _tables(value: str | None) -> list[str]:
    contract = load_contract()
    if not value or value == "all":
        return list(contract.table_names)
    tables = [t.strip() for t in value.split(",") if t.strip()]
    for t in tables:
        contract.table(t)
    return tables


def _shard(value: str | None) -> tuple[int | None, int | None]:
    if not value:
        return None, None
    i, n = (int(p) for p in value.split("/", 1))
    return i, n


def register_and_assign(conn, records: list[SourceRecord], engine: str, actor: str) -> dict:
    contract = load_contract()
    registry = SourceRegistry(conn)
    repo = OwnershipRepository(conn)
    registry.upsert(records)
    for r in records:
        repo.ensure_rows(r.source_id, contract.table_names)
    conn.commit()
    assigned = 0
    if engine != "NONE":
        for r in records:
            unowned = [o.table_name for o in repo.list(source_id=r.source_id) if o.engine is Engine.NONE]
            conn.commit()
            if unowned:
                repo.apply(
                    r.source_id,
                    unowned,
                    TransitionRequest(Action.ASSIGN, actor, "registered", engine=Engine(engine)),
                )
                assigned += len(unowned)
    return {"registered": len(records), "ownership_rows_assigned": assigned}


def _read_inventory(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as handle:
        if path.endswith(".json"):
            return json.load(handle)
        return list(csv.DictReader(handle))


def cmd_register_source(conn, args) -> dict:
    rec = SourceRecord(
        source_id=args.source_id,
        tenant_id=args.tenant_id,
        display_name=args.display_name,
        region=args.region,
        db_host=args.host,
        db_port=args.port,
        db_name=args.db,
        db_user=args.user,
        secret_ref=args.secret_ref,
        compat_schema=args.compat_schema,
        connect_cluster=args.connect_cluster,
        lifecycle_state=args.lifecycle,
        labels=json.loads(args.labels),
    )
    return register_and_assign(conn, [rec], args.engine, args.actor)


def cmd_register_bulk(conn, args) -> dict:
    records = []
    for row in _read_inventory(args.file):
        records.append(
            SourceRecord(
                source_id=row["source_id"],
                tenant_id=row.get("tenant_id") or row["source_id"],
                display_name=row.get("display_name"),
                region=row.get("region") or "default",
                tier=row.get("tier") or "standard",
                db_host=row.get("db_host"),
                db_port=int(row.get("db_port") or 5432),
                db_name=row.get("db_name"),
                db_user=row.get("db_user"),
                secret_ref=row.get("secret_ref"),
                compat_schema=row.get("compat_schema") or "bi_connector_source",
                connect_cluster=row.get("connect_cluster") or "default",
                lifecycle_state=row.get("lifecycle_state") or "ACTIVE",
            )
        )
    result = {"registered": 0, "ownership_rows_assigned": 0}
    for i in range(0, len(records), args.batch_size):
        part = register_and_assign(conn, records[i : i + args.batch_size], args.engine, args.actor)
        result["registered"] += part["registered"]
        result["ownership_rows_assigned"] += part["ownership_rows_assigned"]
    return result


def cmd_register_synthetic(conn, args) -> dict:
    records = [
        SourceRecord(
            source_id=f"{args.prefix}-{i:05d}",
            tenant_id=f"tenant-{i:05d}",
            display_name=f"Synthetic branch database {i}",
            region=f"region-{i % args.regions:02d}",
            lifecycle_state="ACTIVE",
            labels={"synthetic": True},
        )
        for i in range(args.count)
    ]
    result = {"registered": 0, "ownership_rows_assigned": 0}
    for i in range(0, len(records), 200):
        part = register_and_assign(conn, records[i : i + 200], args.engine, args.actor)
        result["registered"] += part["registered"]
        result["ownership_rows_assigned"] += part["ownership_rows_assigned"]
    return result


def cmd_ownership_list(conn, args) -> list[dict]:
    return [o.as_dict() for o in OwnershipRepository(conn).list(source_id=args.source_id)]


def cmd_ownership_apply(conn, args) -> list[dict]:
    request = TransitionRequest(
        action=Action(args.action),
        actor=args.actor,
        reason=args.reason,
        engine=Engine(args.engine) if args.engine else None,
        approved_by=args.approved_by,
    )
    results = OwnershipRepository(conn).apply(args.source_id, _tables(args.tables), request)
    return [
        {
            "previous": r.previous.as_dict(),
            "current": r.current.as_dict(),
            "side_effects": [e.value for e in r.side_effects],
        }
        for r in results
    ]


def cmd_cutover_gate(conn, args) -> dict:
    return data_reconcile.run_cutover_gate(
        conn, args.source_id, _tables(args.tables), args.actor, args.settle_seconds, args.max_discrepancies
    )


def cmd_reconcile_connectors(conn, args) -> dict:
    i, n = _shard(args.shard)
    return reconcile(conn, load_contract(), CdcSettings.from_env(), i, n, dry_run=args.dry_run).as_dict()


def cmd_connector_config(conn, args) -> dict:
    source = SourceRegistry(conn).get(args.source_id)
    return build_connector_config(source, load_contract(), CdcSettings.from_env())


def cmd_reconcile_source(conn, args) -> dict:
    from controlplane import secrets
    from controlplane.db import source_pg_config

    source = SourceRegistry(conn).get(args.source_id)
    conn.commit()
    src = connect(source_pg_config(source, secrets.resolve(source.secret_ref), "fbi-reconcile"))
    contract = load_contract()
    out = {}
    try:
        for table in _tables(args.tables):
            r = data_reconcile.source_vs_warehouse(
                src, conn, source.source_id, source.db_schema, contract.table(table), bucket_size=args.bucket_size
            )
            status = "PASSED" if r.passed(args.max_discrepancies) else "FAILED"
            rid = data_reconcile.record_run(conn, r, status)
            out[table] = {"status": status, "reconciliation_id": rid, "compared_rows": r.compared_rows,
                          "mismatched": r.mismatched_rows, "missing_in_warehouse": r.missing_left,
                          "missing_in_source": r.missing_right, "sample_keys": r.sample_keys[:5]}
    finally:
        src.close()
    return out


def cmd_lifecycle(conn, args) -> dict:
    SourceRegistry(conn).set_lifecycle(args.source_id, args.state)
    conn.commit()
    return {"source_id": args.source_id, "lifecycle_state": args.state}


def cmd_retire_source(conn, args) -> dict:
    from controlplane.connectors import CdcSettings
    from controlplane.registry import RegistryError
    from controlplane.retire import retire_source

    try:
        SourceRegistry(conn).get(args.source_id)
        registered = True
    except RegistryError:
        registered = False
    conn.rollback()
    if not registered and not args.allow_unregistered:
        raise SystemExit(
            f"{args.source_id} is not registered. Pass --allow-unregistered to purge leftover data of a removed source."
        )
    if not args.dry_run and OwnershipRepository(conn).dual_control_required():
        if not args.approved_by:
            raise SystemExit("retire-source purges data in every layer and needs --approved-by (dual control)")
        if args.approved_by == args.actor:
            raise SystemExit("--approved-by must differ from --actor")
    return retire_source(
        conn,
        args.source_id,
        forget=args.forget,
        dry_run=args.dry_run,
        settings=CdcSettings.from_env(),
        actor=args.actor,
        approved_by=args.approved_by,
    )


def cmd_approve_cutover(conn, args) -> dict:
    tables = _tables(args.tables)
    ids = OwnershipRepository(conn).approve_cutover(args.source_id, tables, args.label)
    return {"source_id": args.source_id, "approvals_recorded": len(ids), "approved_by_db_login": "session_user"}


def cmd_bump_generation(conn, args) -> dict:
    gen = SourceRegistry(conn).bump_capture_generation(args.source_id)
    conn.commit()
    return {"source_id": args.source_id, "capture_generation": gen}


def cmd_status(conn, args) -> dict:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM meta.v_ownership_overview ORDER BY region, engine")
    ownership = rows_as_dicts(cursor)
    cursor.execute(
        "SELECT source_id, table_name, target, engine, rows_applied, last_source_ts, seconds_since_last_apply "
        "FROM meta.v_table_freshness ORDER BY seconds_since_last_apply DESC NULLS FIRST LIMIT %s",
        (args.limit,),
    )
    freshness = rows_as_dicts(cursor)
    cursor.execute("SELECT lifecycle_state, COUNT(*) AS sources FROM meta.source_registry GROUP BY 1 ORDER BY 1")
    lifecycle = rows_as_dicts(cursor)
    conn.commit()
    return {"lifecycle": lifecycle, "ownership": ownership, "stalest_tables": freshness}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="controlplane", description="Fineract BI ingestion control plane")
    p.add_argument("--actor", default=os.getenv("FBI_ACTOR", os.getenv("USER", "operator")))
    sub = p.add_subparsers(dest="command", required=True)

    r = sub.add_parser("register-source")
    r.add_argument("--source-id", required=True)
    r.add_argument("--tenant-id", required=True)
    r.add_argument("--display-name")
    r.add_argument("--region", default="default")
    r.add_argument("--host")
    r.add_argument("--port", type=int, default=5432)
    r.add_argument("--db")
    r.add_argument("--user")
    r.add_argument("--secret-ref")
    r.add_argument("--compat-schema", default="bi_connector_source")
    r.add_argument("--connect-cluster", default="default")
    r.add_argument("--lifecycle", default="ACTIVE")
    r.add_argument("--labels", default="{}")
    r.add_argument(
        "--engine", choices=["PYTHON", "KAFKA", "NONE"], default=os.getenv("INGESTION_ENGINE_DEFAULT", "PYTHON")
    )
    r.set_defaults(func=cmd_register_source)

    b = sub.add_parser("register-bulk")
    b.add_argument("--file", required=True)
    b.add_argument("--engine", choices=["PYTHON", "KAFKA", "NONE"], default="NONE")
    b.add_argument("--batch-size", type=int, default=200)
    b.set_defaults(func=cmd_register_bulk)

    s = sub.add_parser("register-synthetic")
    s.add_argument("--count", type=int, required=True)
    s.add_argument("--prefix", default="loadsrc")
    s.add_argument("--regions", type=int, default=4)
    s.add_argument("--engine", choices=["PYTHON", "KAFKA"], default="KAFKA")
    s.set_defaults(func=cmd_register_synthetic)

    ol = sub.add_parser("ownership-list")
    ol.add_argument("--source-id")
    ol.set_defaults(func=cmd_ownership_list)

    oa = sub.add_parser("ownership-apply", help="Feature-flag transition (assign/start_shadow/cutover/...)")
    oa.add_argument("--source-id", required=True)
    oa.add_argument("--tables", default="all")
    oa.add_argument("--action", required=True, choices=[a.value for a in Action])
    oa.add_argument("--engine", choices=["PYTHON", "KAFKA", "NONE"])
    oa.add_argument("--reason", required=True)
    oa.add_argument("--approved-by")
    oa.set_defaults(func=cmd_ownership_apply)

    g = sub.add_parser("cutover-gate", help="Reconcile shadow vs production and mark CUTOVER_READY")
    g.add_argument("--source-id", required=True)
    g.add_argument("--tables", default="all")
    g.add_argument("--settle-seconds", type=int, default=int(os.getenv("FBI_CUTOVER_SETTLE_SECONDS", "900")))
    g.add_argument("--max-discrepancies", type=int, default=0)
    g.set_defaults(func=cmd_cutover_gate)

    rc = sub.add_parser("reconcile-connectors")
    rc.add_argument("--dry-run", action="store_true")
    rc.add_argument("--shard")
    rc.set_defaults(func=cmd_reconcile_connectors)

    cc = sub.add_parser("connector-config")
    cc.add_argument("--source-id", required=True)
    cc.set_defaults(func=cmd_connector_config)

    rs = sub.add_parser("reconcile-source", help="Source vs warehouse bucketed checksum")
    rs.add_argument("--source-id", required=True)
    rs.add_argument("--tables", default="all")
    rs.add_argument("--bucket-size", type=int, default=100_000)
    rs.add_argument("--max-discrepancies", type=int, default=0)
    rs.set_defaults(func=cmd_reconcile_source)

    lc = sub.add_parser("lifecycle")
    lc.add_argument("--source-id", required=True)
    lc.add_argument("--state", required=True)
    lc.set_defaults(func=cmd_lifecycle)

    rt = sub.add_parser(
        "retire-source",
        help="Retire a source and purge everything derived from it (raw, shadow, facts, marts)",
    )
    rt.add_argument("--source-id", required=True)
    rt.add_argument(
        "--forget",
        action="store_true",
        help="Also delete the registry row once the replication slot is confirmed gone (default keeps it RETIRED)",
    )
    rt.add_argument("--dry-run", action="store_true", help="Show what would be purged and change nothing")
    rt.add_argument("--approved-by", help="Second person approving the purge (required with dual control)")
    rt.add_argument("--allow-unregistered", action="store_true", help="Purge leftover data of an unregistered id")
    rt.set_defaults(func=cmd_retire_source)

    ap = sub.add_parser(
        "approve-cutover",
        help="Record a cutover approval under THIS database login (dual_control_mode=database)",
    )
    ap.add_argument("--source-id", required=True)
    ap.add_argument("--tables", default="all")
    ap.add_argument("--label", help="Free-text approver name for the audit trail")
    ap.set_defaults(func=cmd_approve_cutover)

    bg = sub.add_parser("bump-generation", help="After a source restore/re-provision")
    bg.add_argument("--source-id", required=True)
    bg.set_defaults(func=cmd_bump_generation)

    st = sub.add_parser("status")
    st.add_argument("--limit", type=int, default=20)
    st.set_defaults(func=cmd_status)
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = build_parser().parse_args(argv)
    conn = connect(warehouse_config_from_env())
    try:
        result = args.func(conn, args)
    finally:
        conn.close()
    print(json.dumps(result, indent=2, default=str))
    failed = isinstance(result, dict) and any(
        isinstance(v, dict) and v.get("status") == "FAILED" for v in result.values()
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
