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

import os
import threading
import time
import uuid

import pg8000.dbapi
import pytest

from controlplane.db import PgConfig, connect
from controlplane.ownership import Action, CutoverState, Engine, TransitionError, TransitionRequest
from controlplane.ownership_repo import OwnershipRepository
from controlplane.reconcile import run_cutover_gate
from controlplane.registry import SourceRecord, SourceRegistry

TABLE = "batch_job_instance"
STAGE = "stage_raw_batch_job_instance"


def _cfg() -> PgConfig:
    return PgConfig(
        host=os.getenv("WAREHOUSE_DB_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_DB_PORT", "5434")),
        database=os.getenv("WAREHOUSE_DB_NAME", "analytics"),
        user=os.getenv("WAREHOUSE_LOADER_USER", "analytics_loader"),
        password=os.getenv("WAREHOUSE_LOADER_PASSWORD", "analytics_loader_dev_only"),
    )


def _available() -> bool:
    try:
        conn = connect(_cfg())
        conn.close()
        return True
    except Exception:  # noqa: BLE001
        return False


pytestmark = pytest.mark.skipif(not _available(), reason="warehouse database not reachable")


def _admin_cfg() -> PgConfig:
    return PgConfig(
        host=os.getenv("WAREHOUSE_DB_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_DB_PORT", "5434")),
        database=os.getenv("WAREHOUSE_DB_NAME", "analytics"),
        user=os.getenv("WAREHOUSE_ADMIN_USER", "analytics_admin"),
        password=os.getenv("WAREHOUSE_ADMIN_PASSWORD", "analytics_admin_dev_only"),
    )


def purge_audit(sid):
    admin = connect(_admin_cfg())
    try:
        cur = admin.cursor()
        cur.execute("SET fbi.audit_maintenance = 'on'")
        cur.execute("DELETE FROM meta.ingestion_ownership_audit WHERE source_id = %s", (sid,))
        admin.commit()
    finally:
        admin.close()


@pytest.fixture
def conn():
    c = connect(_cfg())
    yield c
    c.rollback()
    c.close()


@pytest.fixture
def source(conn):
    sid = f"it-{uuid.uuid4().hex[:10]}"
    SourceRegistry(conn).upsert([SourceRecord(sid, f"tenant-{sid}", lifecycle_state="ACTIVE")])
    OwnershipRepository(conn).ensure_rows(sid, [TABLE])
    conn.commit()
    yield sid
    cur = conn.cursor()
    conn.rollback()
    for schema in ("raw", "raw_shadow"):
        cur.execute(f"DELETE FROM {schema}.raw_batch_job_instance WHERE source_id = %s", (sid,))
    cur.execute("DELETE FROM meta.entity_change_log WHERE source_id = %s", (sid,))
    cur.execute("DELETE FROM meta.source_registry WHERE source_id = %s", (sid,))
    conn.commit()
    purge_audit(sid)


def passed_run(conn, sid, table=TABLE, status="PASSED", age="0 seconds"):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO meta.reconciliation_run (source_id, table_name, comparison, status, compared_rows, "
        "mismatched_rows, missing_left, missing_right, started_at, finished_at) "
        "VALUES (%s, %s, 'shadow_vs_prod', %s, 0, 0, 0, 0, NOW() - %s::interval, NOW() - %s::interval) "
        "RETURNING reconciliation_id",
        (sid, table, status, age, age),
    )
    rid = int(cur.fetchone()[0])
    conn.commit()
    return rid


def assign(conn, sid, engine):
    OwnershipRepository(conn).apply(sid, [TABLE], TransitionRequest(Action.ASSIGN, "it", "test", engine=engine))


def epoch(conn, sid) -> int:
    o = OwnershipRepository(conn).get(sid, TABLE)
    conn.commit()
    return o.epoch


def stage(conn, sid, rows, engine="KAFKA", target="prod", owner_epoch=None, commit=True):
    cur = conn.cursor()
    cur.execute("SELECT meta.create_stage_table(%s)", (TABLE,))
    ep = owner_epoch if owner_epoch is not None else epoch_in_tx(cur, sid)
    for r in rows:
        cur.execute(
            f"INSERT INTO pg_temp.{STAGE} (source_id, tenant_id, job_instance_id, job_name, _source_lsn, _op, "
            "_unavailable, _owner_epoch, _target) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (sid, f"tenant-{sid}", r[0], r[1], r[2], r[3], r[4] if len(r) > 4 else None, ep, target),
        )
    cur.execute("SELECT target, rows_applied, rows_deleted FROM meta.apply_staged_batch(%s, %s)", (TABLE, engine))
    result = cur.fetchall()
    if commit:
        conn.commit()
    return result


def epoch_in_tx(cur, sid) -> int:
    cur.execute("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = %s AND table_name = %s", (sid, TABLE))
    return int(cur.fetchone()[0])


def rows(conn, sid, schema="raw"):
    cur = conn.cursor()
    cur.execute(
        "SELECT job_instance_id, job_name, _source_lsn, _is_deleted, _ingest_engine "
        f"FROM {schema}.raw_batch_job_instance "
        "WHERE source_id = %s ORDER BY job_instance_id",
        (sid,),
    )
    out = [tuple(r) for r in cur.fetchall()]
    conn.commit()
    return out


def sqlstate(exc) -> str | None:
    payload = exc.args[0] if exc.args else None
    return payload.get("C") if isinstance(payload, dict) else None


def test_cdc_ordering_guard_dedup_and_soft_deletes(conn, source):
    assign(conn, source, Engine.KAFKA)
    stage(conn, source, [(1, "v200", 200, "u"), (1, "v100", 100, "u"), (1, "v200", 200, "u"), (2, None, 150, "d")])
    assert rows(conn, source) == [(1, "v200", 200, False, "KAFKA"), (2, None, 150, True, "KAFKA")]
    stage(conn, source, [(1, "stale", 50, "u")])
    stage(conn, source, [(1, None, 300, "d")])
    stage(conn, source, [(1, "late", 250, "u")])
    assert rows(conn, source)[0] == (1, "v200", 300, True, "KAFKA")


def test_unchanged_toast_columns_keep_stored_values(conn, source):
    assign(conn, source, Engine.KAFKA)
    stage(conn, source, [(5, "original", 10, "u")])
    stage(conn, source, [(5, None, 20, "u", ["job_name"])])
    assert rows(conn, source) == [(5, "original", 20, False, "KAFKA")]


@pytest.mark.parametrize("case", ["stale_epoch", "wrong_engine", "unknown_source", "paused"])
def test_fencing_rejects_writers_without_current_ownership(conn, source, case):
    assign(conn, source, Engine.KAFKA)
    sid, engine, ep = source, "KAFKA", None
    if case == "stale_epoch":
        ep = epoch(conn, source) - 1
    elif case == "wrong_engine":
        engine = "PYTHON"
    elif case == "unknown_source":
        sid, ep = "not-registered", 1
    else:
        OwnershipRepository(conn).apply(source, [TABLE], TransitionRequest(Action.PAUSE, "it", "test"))
    with pytest.raises(pg8000.dbapi.DatabaseError) as err:
        stage(conn, sid, [(1, "x", 1, "u")], engine=engine, owner_epoch=ep)
    conn.rollback()
    assert sqlstate(err.value) == "FBI01"


def test_python_to_kafka_cutover_end_to_end(conn, source):
    repo = OwnershipRepository(conn)
    assign(conn, source, Engine.PYTHON)
    stage(conn, source, [(1, "py-1", None, "u"), (2, "py-2", None, "u")], engine="PYTHON")

    gen_before = SourceRegistry(conn).get(source).capture_generation
    repo.apply(source, [TABLE], TransitionRequest(Action.START_SHADOW, "alice", "migrate", engine=Engine.KAFKA))
    assert SourceRegistry(conn).get(source).capture_generation == gen_before + 1, "fresh connector/slot"
    conn.commit()

    stage(conn, source, [(1, "py-1", 10, "u"), (2, "py-2", 11, "u")], target="shadow")
    stage(conn, source, [(3, "only-in-shadow", 12, "u")], target="shadow")
    gate = run_cutover_gate(conn, source, [TABLE], "alice", settle_seconds=0, max_discrepancies=0)
    assert gate[TABLE]["passed"] is False, "row 3 missing in production must block cutover"
    stage(conn, source, [(3, "only-in-shadow", None, "u")], engine="PYTHON")
    gate = run_cutover_gate(conn, source, [TABLE], "alice", settle_seconds=0, max_discrepancies=0)
    assert gate[TABLE]["passed"] is True

    with pytest.raises(TransitionError, match="approver"):
        repo.apply(source, [TABLE], TransitionRequest(Action.CUTOVER, "alice", "go", approved_by="alice"))
    repo.apply(source, [TABLE], TransitionRequest(Action.CUTOVER, "alice", "go", approved_by="bob"))

    o = repo.get(source, TABLE)
    conn.commit()
    assert (o.engine, o.shadow_engine, o.cutover_state) == (Engine.KAFKA, None, CutoverState.STEADY)
    assert [r[4] for r in rows(conn, source)] == ["KAFKA"] * 3, "production replaced by the CDC copy"
    assert rows(conn, source, "raw_shadow") == []

    with pytest.raises(pg8000.dbapi.DatabaseError) as err:
        stage(conn, source, [(9, "late-python", None, "u")], engine="PYTHON")
    conn.rollback()
    assert sqlstate(err.value) == "FBI01"
    stage(conn, source, [(4, "kafka-4", 20, "u")])
    cur = conn.cursor()
    cur.execute("SELECT action FROM meta.ingestion_ownership_audit WHERE source_id = %s ORDER BY audit_id", (source,))
    actions = [r[0] for r in cur.fetchall()]
    conn.commit()
    assert actions[:2] == ["assign", "start_shadow"] and actions[-1] == "cutover"


def test_in_flight_writer_batch_cannot_straddle_a_cutover(conn, source):
    repo = OwnershipRepository(conn)
    assign(conn, source, Engine.PYTHON)
    repo.apply(source, [TABLE], TransitionRequest(Action.START_SHADOW, "alice", "m", engine=Engine.KAFKA))
    repo.apply(source, [TABLE], TransitionRequest(Action.BEGIN_RECONCILIATION, "alice", "m"))
    repo.apply(source, [TABLE], TransitionRequest(Action.RECORD_RECONCILIATION, "alice", "m",
                                                  reconciliation_passed=True,
                                                  reconciliation_id=passed_run(conn, source)))

    writer = connect(_cfg())
    try:
        stage(writer, source, [(7, "in-flight", 99, "u")], target="shadow", commit=False)
        done = threading.Event()

        def cut():
            c = connect(_cfg())
            try:
                OwnershipRepository(c).apply(source, [TABLE],
                                             TransitionRequest(Action.CUTOVER, "alice", "go", approved_by="bob"))
            finally:
                c.close()
                done.set()

        t = threading.Thread(target=cut)
        t.start()
        time.sleep(1.5)
        assert not done.is_set(), "cutover must wait for the in-flight writer transaction"
        writer.commit()
        t.join(timeout=30)
        assert done.is_set()
    finally:
        writer.close()
    assert rows(conn, source) == [(7, "in-flight", 99, False, "KAFKA")]


def test_reconcile_shadow_reports_value_mismatches(conn, source):
    repo = OwnershipRepository(conn)
    assign(conn, source, Engine.PYTHON)
    stage(conn, source, [(1, "a", None, "u"), (2, "b", None, "u")], engine="PYTHON")
    repo.apply(source, [TABLE], TransitionRequest(Action.START_SHADOW, "alice", "m", engine=Engine.KAFKA))
    stage(conn, source, [(1, "a", 1, "u"), (2, "DIFFERENT", 2, "u")], target="shadow")
    cur = conn.cursor()
    cur.execute("SELECT * FROM meta.reconcile_shadow(%s, %s, interval '0 seconds')", (source, TABLE))
    compared, mismatched, missing_prod, missing_shadow, sample = cur.fetchone()
    conn.commit()
    assert (compared, mismatched, missing_prod, missing_shadow) == (2, 1, 0, 0)
    assert 2 in (sample if isinstance(sample, list) else [])


def test_change_log_records_affected_loans(conn, source):
    OwnershipRepository(conn).ensure_rows(source, ["m_loan_transaction"])
    conn.commit()
    OwnershipRepository(conn).apply(source, ["m_loan_transaction"],
                                    TransitionRequest(Action.ASSIGN, "it", "t", engine=Engine.KAFKA))
    cur = conn.cursor()
    cur.execute("SELECT meta.create_stage_table('m_loan_transaction')")
    cur.execute("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = %s AND table_name = 'm_loan_transaction'",
                (source,))
    ep = cur.fetchone()[0]
    for txn, loan, day in ((1, 10, "2026-03-05"), (2, 10, "2026-01-02"), (3, 11, "2026-02-01")):
        cur.execute(
            "INSERT INTO pg_temp.stage_raw_m_loan_transaction (source_id, tenant_id, id, loan_id, transaction_date, "
            "_source_lsn, _owner_epoch, _target) VALUES (%s, %s, %s, %s, %s, %s, %s, 'prod')",
            (source, f"tenant-{source}", txn, loan, day, txn, ep),
        )
    cur.execute("SELECT * FROM meta.apply_staged_batch('m_loan_transaction', 'KAFKA')")
    cur.execute(
        "SELECT entity_id, min_affected_date::text FROM meta.entity_change_log WHERE source_id = %s ORDER BY 1",
        (source,),
    )
    changes = [tuple(r) for r in cur.fetchall()]
    cur.execute("DELETE FROM raw.raw_m_loan_transaction WHERE source_id = %s", (source,))
    conn.commit()
    assert changes == [(10, "2026-01-02"), (11, "2026-02-01")]


def test_retire_source_purges_every_layer_and_is_idempotent(conn, source):
    from controlplane.retire import DERIVED_TABLES, retire_source

    assign(conn, source, Engine.KAFKA)
    stage(conn, source, [(1, "a", 10, "u"), (2, "b", 11, "u")])
    cur = conn.cursor()
    present = []
    for name in DERIVED_TABLES:
        cur.execute("SELECT to_regclass(%s)", (f"analytics.{name}",))
        if cur.fetchone()[0] is not None:
            present.append(name)
    if "fact_loan_snapshot" in present:
        cur.execute(
            "INSERT INTO analytics.fact_loan_snapshot (snapshot_key, source_id, tenant_id, loan_id, snapshot_date) "
            "VALUES (%s, %s, 'tenant-x', 1, DATE '2026-01-01')",
            (f"k-{source}", source),
        )
    conn.commit()

    first = retire_source(conn, source)
    assert first["raw_rows"] == 2
    assert rows(conn, source) == []
    assert SourceRegistry(conn).get(source).lifecycle_state == "RETIRED"
    assert OwnershipRepository(conn).get(source, TABLE).engine is Engine.NONE
    conn.commit()
    if "fact_loan_snapshot" in present:
        assert first["derived_rows"]["fact_loan_snapshot"] == 1
        cur.execute("SELECT count(*) FROM analytics.fact_loan_snapshot WHERE source_id = %s", (source,))
        assert cur.fetchone()[0] == 0
        conn.commit()

    with pytest.raises(pg8000.dbapi.DatabaseError) as err:
        stage(conn, source, [(3, "late", 12, "u")], owner_epoch=epoch(conn, source))
    conn.rollback()
    assert sqlstate(err.value) == "FBI01"

    again = retire_source(conn, source, forget=True)
    assert again["raw_rows"] == 0 and again["lifecycle"] == "removed from registry"
    again2 = retire_source(conn, source)
    assert again2["lifecycle"] == "not registered"


def test_reregistered_source_never_reuses_a_connector_name(conn, source):
    from controlplane.retire import retire_source

    registry = SourceRegistry(conn)
    first = registry.get(source)
    assert first.capture_generation == 0 and first.connector_name.endswith("-g0")
    retire_source(conn, source, forget=True)
    registry.upsert([SourceRecord(source, f"tenant-{source}", lifecycle_state="ACTIVE")])
    conn.commit()
    again = registry.get(source)
    assert again.capture_generation == 1
    assert again.connector_name != first.connector_name and again.slot_name != first.slot_name
    cur = conn.cursor()
    cur.execute("DELETE FROM meta.source_generation_floor WHERE source_id = %s", (source,))
    conn.commit()
