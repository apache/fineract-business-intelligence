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
from dataclasses import dataclass, field

from controlplane.contract import TableContract
from controlplane.db import quote_ident
from controlplane.ownership import Action, TransitionRequest
from controlplane.ownership_repo import OwnershipRepository

log = logging.getLogger(__name__)


@dataclass
class ReconciliationResult:
    source_id: str
    table_name: str
    comparison: str
    compared_rows: int = 0
    mismatched_rows: int = 0
    missing_left: int = 0
    missing_right: int = 0
    sample_keys: list = field(default_factory=list)
    details: dict = field(default_factory=dict)

    @property
    def discrepancies(self) -> int:
        return self.mismatched_rows + self.missing_left + self.missing_right

    def passed(self, max_discrepancies: int = 0) -> bool:
        return self.discrepancies <= max_discrepancies


def record_run(conn, result: ReconciliationResult, status: str) -> int:
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO meta.reconciliation_run (source_id, table_name, comparison, status, compared_rows, "
        "mismatched_rows, missing_left, missing_right, details, finished_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()) RETURNING reconciliation_id",
        (
            result.source_id,
            result.table_name,
            result.comparison,
            status,
            result.compared_rows,
            result.mismatched_rows,
            result.missing_left,
            result.missing_right,
            json.dumps({**result.details, "sample_keys": result.sample_keys}, default=str),
        ),
    )
    rid = int(cursor.fetchone()[0])
    conn.commit()
    return rid


def reconcile_shadow(conn, source_id: str, table_name: str, settle_seconds: int) -> ReconciliationResult:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT compared_rows, mismatched_rows, missing_in_prod, missing_in_shadow, sample_keys "
        "FROM meta.reconcile_shadow(%s, %s, make_interval(secs => %s))",
        (source_id, table_name, settle_seconds),
    )
    compared, mismatched, missing_prod, missing_shadow, sample = cursor.fetchone()
    conn.commit()
    return ReconciliationResult(
        source_id=source_id,
        table_name=table_name,
        comparison="shadow_vs_prod",
        compared_rows=int(compared or 0),
        mismatched_rows=int(mismatched or 0),
        missing_left=int(missing_prod or 0),
        missing_right=int(missing_shadow or 0),
        sample_keys=sample if isinstance(sample, list) else json.loads(sample or "[]"),
        details={"settle_seconds": settle_seconds},
    )


def run_cutover_gate(
    conn,
    source_id: str,
    tables: list[str],
    actor: str,
    settle_seconds: int,
    max_discrepancies: int = 0,
) -> dict[str, dict]:
    repo = OwnershipRepository(conn)
    repo.apply(source_id, tables, TransitionRequest(Action.BEGIN_RECONCILIATION, actor, "cutover gate started"))
    outcome: dict[str, dict] = {}
    for table in sorted(tables):
        result = reconcile_shadow(conn, source_id, table, settle_seconds)
        passed = result.passed(max_discrepancies)
        rid = record_run(conn, result, "PASSED" if passed else "FAILED")
        repo.apply(
            source_id,
            [table],
            TransitionRequest(
                Action.RECORD_RECONCILIATION,
                actor,
                f"reconciliation {rid}: {result.discrepancies} discrepancies over {result.compared_rows} rows",
                reconciliation_passed=passed,
                reconciliation_id=rid,
            ),
        )
        outcome[table] = {
            "reconciliation_id": rid,
            "passed": passed,
            "compared_rows": result.compared_rows,
            "discrepancies": result.discrepancies,
            "sample_keys": result.sample_keys[:10],
        }
    return outcome




def _source_columns(source_conn, schema: str, table: str) -> set[str]:
    cursor = source_conn.cursor()
    cursor.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return {r[0] for r in cursor.fetchall()}


def comparable_columns(contract: TableContract) -> tuple:
    exclude = os.getenv("FBI_EXCLUDE_PII", "true").strip().lower() in ("1", "true", "yes", "on")
    return tuple(c for c in contract.business_columns if not (exclude and c.pii))


def source_row_expression(contract: TableContract, available: set[str]) -> str:
    parts = []
    for c in comparable_columns(contract):
        src = next((cand for cand in c.cdc_candidates if cand in available), None)
        parts.append(f"CAST({quote_ident(src)} AS {c.type})" if src else f"CAST(NULL AS {c.type})")
    return "ROW(" + ", ".join(parts) + ")::text"


def warehouse_row_expression(contract: TableContract) -> str:
    parts = [f"CAST({quote_ident(c.name)} AS {c.type})" for c in comparable_columns(contract)]
    return "ROW(" + ", ".join(parts) + ")::text"


def _bucket_aggregates(conn, sql: str, params: tuple) -> dict[int, tuple[int, int]]:
    cursor = conn.cursor()
    cursor.execute(sql, params)
    return {int(b): (int(n), int(h)) for b, n, h in cursor.fetchall()}


def _bucket_rows(conn, sql: str, params: tuple) -> dict[int, int]:
    cursor = conn.cursor()
    cursor.execute(sql, params)
    return {int(k): int(h) for k, h in cursor.fetchall()}


def source_vs_warehouse(
    source_conn,
    warehouse_conn,
    source_id: str,
    source_schema: str,
    contract: TableContract,
    bucket_size: int = 100_000,
    max_drilldown_buckets: int = 50,
    keys_only: bool = False,
    warehouse_schema: str = "raw",
) -> ReconciliationResult:
    for c in (source_conn, warehouse_conn):
        c.cursor().execute("SET TIME ZONE 'UTC'")
    pk = quote_ident(contract.primary_key)
    available = _source_columns(source_conn, source_schema, contract.table_name)
    src_expr = "0" if keys_only else f"hashtextextended({source_row_expression(contract, available)}, 0)"
    wh_expr = "0" if keys_only else f"hashtextextended({warehouse_row_expression(contract)}, 0)"
    src_from = f"{quote_ident(source_schema)}.{quote_ident(contract.table_name)}"
    if warehouse_schema not in ("raw", "raw_shadow"):
        raise ValueError("warehouse_schema must be raw or raw_shadow")
    wh_from = f"{warehouse_schema}.{quote_ident(contract.raw_table)}"
    wh_where = "source_id = %s AND NOT _is_deleted"

    src_buckets = _bucket_aggregates(
        source_conn,
        f"SELECT {pk} / %s, COUNT(*), COALESCE(SUM({src_expr}::numeric), 0) FROM {src_from} GROUP BY 1",
        (bucket_size,),
    )
    wh_buckets = _bucket_aggregates(
        warehouse_conn,
        f"SELECT {pk} / %s, COUNT(*), COALESCE(SUM({wh_expr}::numeric), 0) FROM {wh_from} WHERE {wh_where} GROUP BY 1",
        (bucket_size, source_id),
    )
    warehouse_conn.commit()
    mismatched_buckets = sorted(
        b for b in set(src_buckets) | set(wh_buckets) if src_buckets.get(b) != wh_buckets.get(b)
    )
    result = ReconciliationResult(
        source_id=source_id,
        table_name=contract.table_name,
        comparison="source_vs_warehouse",
        compared_rows=sum(n for n, _ in src_buckets.values()),
        details={
            "bucket_size": bucket_size,
            "buckets": len(set(src_buckets) | set(wh_buckets)),
            "mismatched_buckets": len(mismatched_buckets),
            "keys_only": keys_only,
        },
    )
    missing_in_wh: list[int] = []
    missing_in_src: list[int] = []
    for b in mismatched_buckets[:max_drilldown_buckets]:
        lo, hi = b * bucket_size, (b + 1) * bucket_size
        src_rows = _bucket_rows(
            source_conn, f"SELECT {pk}, {src_expr} FROM {src_from} WHERE {pk} >= %s AND {pk} < %s", (lo, hi)
        )
        wh_rows = _bucket_rows(
            warehouse_conn,
            f"SELECT {pk}, {wh_expr} FROM {wh_from} WHERE {wh_where} AND {pk} >= %s AND {pk} < %s",
            (source_id, lo, hi),
        )
        missing_in_wh.extend(sorted(set(src_rows) - set(wh_rows)))
        missing_in_src.extend(sorted(set(wh_rows) - set(src_rows)))
        differing = sorted(k for k in set(src_rows) & set(wh_rows) if src_rows[k] != wh_rows[k])
        result.mismatched_rows += len(differing)
        result.sample_keys.extend(differing[: max(0, 20 - len(result.sample_keys))])
    warehouse_conn.commit()
    result.missing_left = len(missing_in_wh)
    result.missing_right = len(missing_in_src)
    result.details["missing_in_warehouse_sample"] = missing_in_wh[:20]
    result.details["missing_in_source_keys"] = missing_in_src
    if len(mismatched_buckets) > max_drilldown_buckets:
        result.details["drilldown_truncated"] = True
    return result
