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

import io
import logging
import uuid
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pg8000.dbapi

from controlplane import secrets
from controlplane.contract import Contract, TableContract, load_contract
from controlplane.db import PgConfig, connect, quote_ident, source_pg_config, warehouse_config_from_env
from controlplane.ownership import Action, CutoverState, Engine, Ownership, TransitionRequest
from controlplane.ownership_repo import OwnershipRepository
from controlplane.reconcile import source_vs_warehouse
from controlplane.registry import SourceRecord, SourceRegistry
from extractor.config import EnvSourceBootstrap, ExtractSettings, warehouse_config
from extractor.replica_lag_check import ensure_replica_safe
from extractor.watermark_manager import Watermark, WatermarkManager

logger = logging.getLogger(__name__)

FENCED_SQLSTATE = "FBI01"
MODES = ("backfill", "incremental", "reconcile")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
STAGE_META_COLUMNS = (
    "_source_generation",
    "_source_lsn",
    "_source_ts",
    "_op",
    "_unavailable",
    "_owner_epoch",
    "_target",
)


class FencedError(RuntimeError):
    pass


def as_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def sqlstate(exc: BaseException) -> str | None:
    payload = exc.args[0] if exc.args else None
    return payload.get("C") if isinstance(payload, dict) else None


def csv_field(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, datetime):
        text = as_utc_datetime(value).isoformat()
    elif isinstance(value, (date, Decimal, int, float)):
        text = str(value)
    else:
        text = str(value)
    return '"' + text.replace('"', '""') + '"'


def build_copy_payload(rows: Iterable[Sequence]) -> io.StringIO:
    buf = io.StringIO()
    for row in rows:
        buf.write(",".join(csv_field(v) for v in row))
        buf.write("\n")
    buf.seek(0)
    return buf


@dataclass(frozen=True)
class Assignment:
    table: TableContract
    target: str
    epoch: int


def plan_assignments(
    ownerships: Iterable[Ownership], contract: Contract, tables: set[str] | None = None
) -> list[Assignment]:
    plan = []
    for o in ownerships:
        if o.cutover_state is CutoverState.PAUSED or (tables and o.table_name not in tables):
            continue
        if o.table_name not in contract.table_names:
            continue
        t = contract.table(o.table_name)
        if o.engine is Engine.PYTHON:
            plan.append(Assignment(t, "prod", o.epoch))
        if o.shadow_engine is Engine.PYTHON:
            plan.append(Assignment(t, "shadow", o.epoch))
    return sorted(plan, key=lambda a: (a.table.table_name, a.target))


def in_null_cursor_phase(table: TableContract, start: Watermark | None) -> bool:
    return table.cursor_is_usable and (start is None or start.cursor_value is None)


def build_page_query(
    compat_schema: str,
    table: TableContract,
    start: Watermark | None,
    page_size: int,
    exclude_pii: bool = False,
) -> tuple[str, tuple]:
    pk = quote_ident(table.primary_key)
    select = []
    for c in table.columns:
        if c.synthetic or (exclude_pii and c.pii):
            select.append(f"CAST(NULL AS {c.type}) AS {quote_ident(c.name)}")
        else:
            select.append(quote_ident(c.name))
    base = f"SELECT {', '.join(select)} FROM {quote_ident(compat_schema)}.{quote_ident(table.table_name)}"
    last_pk = start.last_pk if start and start.last_pk is not None else -(2**63)
    if table.cursor_is_usable:
        cur = quote_ident(table.python_cursor)
        if in_null_cursor_phase(table, start):
            return f"{base} WHERE {cur} IS NULL AND {pk} > %s ORDER BY {pk} LIMIT %s", (last_pk, page_size)
        return (
            f"{base} WHERE ({cur}, {pk}) > (%s, %s) ORDER BY {cur}, {pk} LIMIT %s",
            (start.cursor_value, last_pk, page_size),
        )
    return f"{base} WHERE {pk} > %s ORDER BY {pk} LIMIT %s", (last_pk, page_size)


@dataclass
class TableOutcome:
    table: str
    target: str
    rows_read: int = 0
    rows_applied: int = 0
    rows_deleted: int = 0
    status: str = "ok"
    detail: str | None = None


@dataclass
class SourceOutcome:
    source_id: str
    mode: str
    status: str = "success"
    tables: list[TableOutcome] = field(default_factory=list)
    error: str | None = None

    def as_dict(self) -> dict:
        return {
            "source_id": self.source_id,
            "mode": self.mode,
            "status": self.status,
            "error": self.error,
            "rows_read": sum(t.rows_read for t in self.tables),
            "rows_applied": sum(t.rows_applied for t in self.tables),
            "rows_deleted": sum(t.rows_deleted for t in self.tables),
            "tables": [t.__dict__ for t in self.tables],
        }


class SourceExtractor:
    def __init__(
        self,
        source: SourceRecord,
        warehouse: PgConfig,
        settings: ExtractSettings,
        contract: Contract | None = None,
        tables: set[str] | None = None,
    ) -> None:
        self.source = source
        self.warehouse = warehouse
        self.settings = settings
        self.contract = contract or load_contract()
        self.tables = tables

    def _source_conn(self):
        cfg = source_pg_config(self.source, secrets.resolve(self.source.secret_ref), "fbi-python-engine")
        conn = pg8000.dbapi.connect(**cfg.connect_kwargs())
        conn.autocommit = True
        return conn

    def run(self, mode: str) -> SourceOutcome:
        if mode not in MODES:
            raise ValueError(f"Unsupported mode: {mode}")
        outcome = SourceOutcome(self.source.source_id, mode)
        wh = connect(self.warehouse)
        run_id = uuid.uuid4()
        try:
            assignments = plan_assignments(
                OwnershipRepository(wh).list(source_id=self.source.source_id, engine=Engine.PYTHON),
                self.contract,
                self.tables,
            )
            wh.commit()
            if not assignments:
                outcome.status = "skipped"
                outcome.error = "no tables owned by the PYTHON engine"
                return outcome
            self._pipeline_state(wh, run_id, mode, "running")
            src = self._source_conn()
            try:
                lag = ensure_replica_safe(src, self.settings.replica_lag_threshold_seconds)
                logger.info("[%s] replica lag %ss within threshold", self.source.source_id, lag)
                if mode != "reconcile" and self.settings.cob_gate_enabled:
                    self._ensure_cob_completed(src)
                watermarks = WatermarkManager(wh, self.source.source_id)
                for a in assignments:
                    outcome.tables.append(self._process(src, wh, watermarks, a, mode))
            finally:
                src.close()
            if any(t.status == "fenced" for t in outcome.tables):
                outcome.status = "partial"
            self._pipeline_state(wh, run_id, mode, outcome.status, outcome)
            return outcome
        except Exception as exc:
            wh.rollback()
            outcome.status = "failed"
            outcome.error = str(exc)
            logger.exception("[%s] %s run failed", self.source.source_id, mode)
            try:
                self._pipeline_state(wh, run_id, mode, "failed", outcome)
            except Exception:  # noqa: BLE001
                wh.rollback()
            raise
        finally:
            wh.close()

    def _lock_key(self, a: Assignment) -> str:
        return f"{self.source.source_id}:{a.table.table_name}:{a.target}"

    def _process(self, src, wh, watermarks: WatermarkManager, a: Assignment, mode: str) -> TableOutcome:
        out = TableOutcome(a.table.table_name, a.target)
        cursor = wh.cursor()
        cursor.execute("SELECT pg_advisory_lock(hashtextextended(%s, 11))", (self._lock_key(a),))
        try:
            return self._process_locked(src, wh, watermarks, a, mode, out)
        finally:
            wh.rollback()
            wh.cursor().execute("SELECT pg_advisory_unlock(hashtextextended(%s, 11))", (self._lock_key(a),))
            wh.commit()

    def _process_locked(
        self, src, wh, watermarks: WatermarkManager, a: Assignment, mode: str, out: TableOutcome
    ) -> TableOutcome:
        try:
            if mode == "backfill":
                watermarks.reset(a.table.table_name, a.target)
                wh.commit()
            if mode in ("backfill", "incremental"):
                self._extract(src, wh, watermarks, a, out)
            if mode in ("backfill", "reconcile") or not a.table.cursor_is_usable:
                self._reconcile_deletes(src, wh, a, out)
            self._record_progress(wh, a, out)
        except FencedError as exc:
            wh.rollback()
            out.status = "fenced"
            out.detail = str(exc)
            logger.warning("[%s] %s/%s fenced: %s", self.source.source_id, a.table.table_name, a.target, exc)
        return out

    def _extract(self, src, wh, watermarks: WatermarkManager, a: Assignment, out: TableOutcome) -> None:
        t = a.table
        stored = watermarks.get(t.table_name, a.target)
        start = stored
        if stored and stored.cursor_value and t.cursor_is_usable and self.settings.lookback_seconds > 0:
            start = Watermark(stored.cursor_value - timedelta(seconds=self.settings.lookback_seconds), None)
        if not t.cursor_is_usable:
            start = None
        cursor_idx = t.column_names.index(t.python_cursor) if t.cursor_is_usable else None
        pk_idx = t.column_names.index(t.primary_key)
        null_phase = in_null_cursor_phase(t, start)
        while True:
            sql, params = build_page_query(
                self.source.compat_schema, t, start, self.settings.page_size, self.settings.exclude_pii
            )
            cur = src.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            if rows:
                last = rows[-1]
                cursor_value = None if null_phase or cursor_idx is None else as_utc_datetime(last[cursor_idx])
                next_wm = Watermark(cursor_value, last[pk_idx])
                applied, _ = self._write_page(wh, a, rows, cursor_idx, op="u")
                if t.cursor_is_usable:
                    watermarks.update(t.table_name, a.target, next_wm.cursor_value, next_wm.last_pk)
                wh.commit()
                out.rows_read += len(rows)
                out.rows_applied += applied
                start = next_wm
            if len(rows) < self.settings.page_size:
                if not null_phase:
                    break
                null_phase = False
                start = Watermark(_EPOCH, None)
                watermarks.update(t.table_name, a.target, _EPOCH, None)
                wh.commit()
        logger.info(
            "[%s] %s -> %s: read=%s applied=%s",
            self.source.source_id,
            t.table_name,
            a.target,
            out.rows_read,
            out.rows_applied,
        )

    def _write_page(
        self, wh, a: Assignment, rows: Sequence[Sequence], cursor_idx: int | None, op: str
    ) -> tuple[int, int]:
        t = a.table
        now = datetime.now(UTC)
        staged = []
        for row in rows:
            source_ts = as_utc_datetime(row[cursor_idx]) if cursor_idx is not None and row[cursor_idx] else now
            staged.append(
                (
                    self.source.source_id,
                    self.source.tenant_id,
                    *row,
                    self.source.capture_generation,
                    None,
                    source_ts,
                    op,
                    None,
                    a.epoch,
                    a.target,
                )
            )
        cursor = wh.cursor()
        try:
            cursor.execute("SELECT meta.create_stage_table(%s)", (t.table_name,))
            stage = cursor.fetchone()[0]
            columns = ", ".join(
                quote_ident(c) for c in ("source_id", "tenant_id", *t.column_names, *STAGE_META_COLUMNS)
            )
            cursor.execute(
                f"COPY pg_temp.{quote_ident(stage)} ({columns}) FROM STDIN WITH (FORMAT csv)",
                stream=build_copy_payload(staged),
            )
            cursor.execute(
                "SELECT target, rows_applied, rows_deleted FROM meta.apply_staged_batch(%s, 'PYTHON')", (t.table_name,)
            )
            results = cursor.fetchall()
        except pg8000.dbapi.DatabaseError as exc:
            wh.rollback()
            if sqlstate(exc) == FENCED_SQLSTATE:
                raise FencedError(str(exc.args[0].get("M", exc))) from exc
            raise
        applied = sum(int(r[1]) for r in results)
        deleted = sum(int(r[2]) for r in results)
        return applied, deleted

    def _reconcile_deletes(self, src, wh, a: Assignment, out: TableOutcome) -> None:
        t = a.table
        result = source_vs_warehouse(
            src,
            wh,
            self.source.source_id,
            self.source.compat_schema,
            t,
            bucket_size=self.settings.reconcile_bucket_size,
            max_drilldown_buckets=10_000,
            keys_only=True,
            warehouse_schema="raw" if a.target == "prod" else "raw_shadow",
        )
        missing = self._still_missing(src, t, result.details.get("missing_in_source_keys", []))
        if not missing:
            return
        pk_idx = t.column_names.index(t.primary_key)
        for i in range(0, len(missing), self.settings.page_size):
            chunk = missing[i : i + self.settings.page_size]
            rows = []
            for key in chunk:
                row = [None] * len(t.column_names)
                row[pk_idx] = key
                rows.append(row)
            _, deleted = self._write_page(wh, a, rows, None, op="d")
            wh.commit()
            out.rows_deleted += deleted
        logger.info(
            "[%s] %s -> %s: soft-deleted %s rows missing at source",
            self.source.source_id,
            t.table_name,
            a.target,
            out.rows_deleted,
        )

    def _still_missing(self, src, t: TableContract, keys: Sequence[int]) -> list[int]:
        if not keys:
            return []
        present: set[int] = set()
        pk = quote_ident(t.primary_key)
        for i in range(0, len(keys), 5000):
            chunk = list(keys[i : i + 5000])
            cur = src.cursor()
            cur.execute(
                f"SELECT {pk} FROM {quote_ident(self.source.compat_schema)}.{quote_ident(t.table_name)} "
                f"WHERE {pk} = ANY(%s)",
                (chunk,),
            )
            present.update(int(r[0]) for r in cur.fetchall())
        return [k for k in keys if int(k) not in present]

    def _ensure_cob_completed(self, src) -> None:
        cursor = src.cursor()
        cursor.execute(
            f"SELECT MAX(end_time) FROM {quote_ident(self.source.compat_schema)}.batch_job_execution "
            "WHERE status = %s AND job_name = %s",
            ("COMPLETED", self.source.cob_job_name),
        )
        last_completed = as_utc_datetime(cursor.fetchone()[0])
        if last_completed is None:
            raise RuntimeError(f"No completed '{self.source.cob_job_name}' execution found in batch_job_execution.")
        cutoff = datetime.now(UTC) - timedelta(hours=self.settings.cob_lookback_hours)
        if last_completed < cutoff:
            raise RuntimeError(
                f"Latest '{self.source.cob_job_name}' completion {last_completed.isoformat()} "
                f"is older than {self.settings.cob_lookback_hours} hours."
            )

    def _record_progress(self, wh, a: Assignment, out: TableOutcome) -> None:
        cursor = wh.cursor()
        cursor.execute(
            "INSERT INTO meta.ingestion_progress (source_id, table_name, target, writer_id, engine, rows_applied, "
            "last_source_ts, last_applied_at) VALUES (%s, %s, %s, %s, 'PYTHON', %s, "
            "(SELECT cursor_value FROM meta.extract_watermarks "
            "WHERE source_id = %s AND table_name = %s AND target = %s), "
            "NOW()) ON CONFLICT (source_id, table_name, target, writer_id) DO UPDATE SET "
            "rows_applied = meta.ingestion_progress.rows_applied + EXCLUDED.rows_applied, "
            "last_source_ts = COALESCE(EXCLUDED.last_source_ts, meta.ingestion_progress.last_source_ts), "
            "last_applied_at = NOW()",
            (
                self.source.source_id,
                a.table.table_name,
                a.target,
                self.settings.writer_id,
                out.rows_applied + out.rows_deleted,
                self.source.source_id,
                a.table.table_name,
                a.target,
            ),
        )
        wh.commit()

    def _pipeline_state(self, wh, run_id, mode: str, status: str, outcome: SourceOutcome | None = None) -> None:
        cursor = wh.cursor()
        if outcome is None:
            cursor.execute(
                "INSERT INTO meta.pipeline_state (run_id, source_id, tenant_id, run_mode, started_at, status) "
                "VALUES (%s, %s, %s, %s, NOW(), %s)",
                (str(run_id), self.source.source_id, self.source.tenant_id, mode, status),
            )
        else:
            d = outcome.as_dict()
            cursor.execute(
                "UPDATE meta.pipeline_state SET completed_at = NOW(), status = %s, rows_extracted = %s, "
                "rows_loaded = %s, error_message = %s WHERE run_id = %s",
                (status, d["rows_read"], d["rows_applied"], outcome.error, str(run_id)),
            )
        wh.commit()




def ensure_env_source(conn, boot: EnvSourceBootstrap, settings: ExtractSettings, contract: Contract) -> SourceRecord:
    registry = SourceRegistry(conn)
    try:
        existing = registry.get(boot.source_id)
        lifecycle, generation, labels = existing.lifecycle_state, existing.capture_generation, existing.labels
    except Exception:  # noqa: BLE001
        conn.rollback()
        lifecycle, generation, labels = "ACTIVE", 0, {}
    record = SourceRecord(
        source_id=boot.source_id,
        tenant_id=boot.tenant_id,
        display_name=f"{boot.dbname}@{boot.host}",
        db_host=boot.host,
        db_port=boot.port,
        db_name=boot.dbname,
        db_user=boot.user,
        compat_schema=boot.compat_schema,
        secret_ref=boot.secret_ref,
        lifecycle_state=lifecycle,
        capture_generation=generation,
        cob_job_name=boot.cob_job_name,
        labels={**labels, "sslmode": boot.ssl_mode},
    )
    registry.upsert([record])
    repo = OwnershipRepository(conn)
    repo.ensure_rows(boot.source_id, contract.table_names)
    conn.commit()
    if settings.default_engine != "NONE":
        unowned = [o.table_name for o in repo.list(source_id=boot.source_id) if o.engine is Engine.NONE]
        conn.commit()
        if unowned:
            repo.apply(
                boot.source_id,
                unowned,
                TransitionRequest(
                    Action.ASSIGN,
                    "bootstrap",
                    "initial engine from INGESTION_ENGINE_DEFAULT",
                    engine=Engine(settings.default_engine),
                ),
            )
    return registry.get(boot.source_id)


def run_sources(
    mode: str,
    source_ids: list[str] | None = None,
    shard: tuple[int, int] | None = None,
    tables: set[str] | None = None,
    settings: ExtractSettings | None = None,
) -> list[dict]:
    settings = settings or ExtractSettings.from_env()
    contract = load_contract()
    wh_cfg = warehouse_config()
    boot = EnvSourceBootstrap.from_env()
    if boot and (not source_ids or boot.source_id in source_ids) and (shard is None or shard[0] == 0):
        control = connect(warehouse_config_from_env("fbi-python-engine-bootstrap"))
        try:
            ensure_env_source(control, boot, settings, contract)
        finally:
            control.close()
    conn = connect(wh_cfg)
    try:
        registry = SourceRegistry(conn)
        if source_ids:
            sources = [registry.get(s) for s in source_ids]
        else:
            sources = registry.list(
                states=("ACTIVE", "DEGRADED", "PROVISIONING"),
                shard_index=shard[0] if shard else None,
                shard_count=shard[1] if shard else None,
            )
        conn.commit()
    finally:
        conn.close()

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=max(1, settings.parallelism)) as pool:
        futures = {
            pool.submit(SourceExtractor(s, wh_cfg, settings, contract, tables).run, mode): s.source_id for s in sources
        }
        for fut in as_completed(futures):
            try:
                results.append(fut.result().as_dict())
            except Exception as exc:  # noqa: BLE001
                results.append({"source_id": futures[fut], "mode": mode, "status": "failed", "error": str(exc)})
    return sorted(results, key=lambda r: r["source_id"])
