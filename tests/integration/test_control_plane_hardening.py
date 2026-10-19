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
import uuid
from datetime import date

import pg8000.dbapi
import pytest

from controlplane.db import PgConfig, connect
from controlplane.ownership import Action, Engine, TransitionError, TransitionRequest
from controlplane.ownership_repo import OwnershipRepository
from controlplane.registry import SourceRecord, SourceRegistry
from tests.integration.test_platform_write_path import _admin_cfg, _cfg, passed_run, purge_audit, sqlstate


def _reachable() -> bool:
    try:
        connect(_cfg()).close()
        return True
    except Exception:  # noqa: BLE001
        return False


pytestmark = pytest.mark.skipif(not _reachable(), reason="warehouse database not reachable")

KEY_TABLES = ["batch_job_instance", "m_loan", "m_loan_transaction", "m_client"]


@pytest.fixture
def conn():
    c = connect(_cfg())
    yield c
    c.rollback()
    c.close()


@pytest.fixture
def admin():
    c = connect(_admin_cfg())
    yield c
    c.rollback()
    c.close()


@pytest.fixture
def source(conn):
    sid = f"it-{uuid.uuid4().hex[:10]}"
    SourceRegistry(conn).upsert([SourceRecord(sid, f"tenant-{sid}", lifecycle_state="ACTIVE")])
    OwnershipRepository(conn).ensure_rows(sid, KEY_TABLES)
    conn.commit()
    yield sid
    cur = conn.cursor()
    conn.rollback()
    for table in ("raw_batch_job_instance", "raw_m_loan", "raw_m_loan_transaction", "raw_m_client"):
        for schema in ("raw", "raw_shadow"):
            cur.execute(f"DELETE FROM {schema}.{table} WHERE source_id = %s", (sid,))
    cur.execute("DELETE FROM meta.entity_change_log WHERE source_id = %s", (sid,))
    cur.execute("DELETE FROM meta.reconciliation_run WHERE source_id = %s", (sid,))
    cur.execute("DELETE FROM meta.cutover_approval WHERE source_id = %s", (sid,))
    cur.execute("DELETE FROM meta.source_registry WHERE source_id = %s", (sid,))
    conn.commit()
    purge_audit(sid)


def assign(conn, sid, table, engine=Engine.KAFKA):
    OwnershipRepository(conn).apply(sid, [table], TransitionRequest(Action.ASSIGN, "it", "test", engine=engine))


def epoch(conn, sid, table) -> int:
    cur = conn.cursor()
    cur.execute("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = %s AND table_name = %s", (sid, table))
    value = int(cur.fetchone()[0])
    conn.commit()
    return value


def apply_events(conn, sid, table, columns, events, engine="KAFKA", target="prod"):
    cur = conn.cursor()
    cur.execute("SELECT meta.create_stage_table(%s)", (table,))
    ep = epoch(conn, sid, table)
    for e in events:
        cols = ["source_id", "tenant_id", *columns, "_source_lsn", "_op", "_unavailable", "_owner_epoch", "_target"]
        values = [sid, f"tenant-{sid}", *[e.get(c) for c in columns], e["lsn"], e.get("op", "u"),
                  e.get("unavailable"), ep, target]
        cur.execute(
            f"INSERT INTO pg_temp.stage_raw_{table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
            tuple(values),
        )
    cur.execute("SELECT target, rows_applied, rows_deleted FROM meta.apply_staged_batch(%s, %s)", (table, engine))
    result = [tuple(r) for r in cur.fetchall()]
    conn.commit()
    return result


def change_log(conn, sid):
    cur = conn.cursor()
    cur.execute(
        "SELECT entity, entity_id, min_affected_date FROM meta.entity_change_log WHERE source_id = %s "
        "ORDER BY change_id",
        (sid,),
    )
    out = [(r[0], r[1], r[2]) for r in cur.fetchall()]
    conn.commit()
    return out


def test_change_cursor_never_skips_rows_of_a_slow_writer(conn, source):
    slow, fast, reader = connect(_cfg()), connect(_cfg()), connect(_cfg())
    try:
        slow_cur = slow.cursor()
        slow_cur.execute(
            "INSERT INTO meta.entity_change_log (source_id, tenant_id, entity, entity_id, min_affected_date) "
            "VALUES (%s, 't', 'loan', 100, DATE '2026-01-01') RETURNING change_id",
            (source,),
        )
        slow_id = slow_cur.fetchone()[0]
        fast_cur = fast.cursor()
        fast_cur.execute(
            "INSERT INTO meta.entity_change_log (source_id, tenant_id, entity, entity_id, min_affected_date) "
            "VALUES (%s, 't', 'loan', 101, DATE '2026-01-01') RETURNING change_id",
            (source,),
        )
        fast_id = fast_cur.fetchone()[0]
        fast.commit()
        assert fast_id > slow_id

        def window():
            cur = reader.cursor()
            cur.execute(
                "SELECT entity_id FROM meta.entity_change_log WHERE source_id = %s AND txid >= '0'::xid8 "
                "AND txid < pg_snapshot_xmin(pg_current_snapshot()) ORDER BY entity_id",
                (source,),
            )
            rows = [r[0] for r in cur.fetchall()]
            reader.commit()
            return rows

        assert window() == [], "the fast writer's row is newer than an open transaction, so it is not safe to read"
        slow.commit()
        assert window() == [100, 101], "once the slow writer commits, both are inside the window"
    finally:
        slow.close()
        fast.close()
        reader.close()


def test_toast_unavailable_column_keeps_the_value_from_an_earlier_event_in_the_same_batch(conn, source):
    assign(conn, source, "batch_job_instance")
    cols = ["job_instance_id", "job_name", "job_key"]
    apply_events(conn, source, "batch_job_instance", cols, [
        {"job_instance_id": 1, "job_name": "B", "job_key": "k1", "lsn": 10},
        {"job_instance_id": 1, "job_key": "k2", "lsn": 20, "unavailable": ["job_name"]},
    ])
    cur = conn.cursor()
    cur.execute(
        "SELECT job_name, job_key, _source_lsn FROM raw.raw_batch_job_instance WHERE source_id = %s", (source,)
    )
    assert [tuple(r) for r in cur.fetchall()] == [("B", "k2", 20)]
    conn.commit()
    apply_events(conn, source, "batch_job_instance", cols, [
        {"job_instance_id": 1, "job_key": "k3", "lsn": 30, "unavailable": ["job_name"]},
    ])
    cur.execute("SELECT job_name, job_key FROM raw.raw_batch_job_instance WHERE source_id = %s", (source,))
    assert [tuple(r) for r in cur.fetchall()] == [("B", "k3")], "an update with no earlier event keeps the stored value"
    conn.commit()


def test_toast_path_feeds_the_change_log_and_counts_deletes(conn, source):
    assign(conn, source, "m_loan_transaction")
    cols = ["id", "loan_id", "transaction_date", "amount"]
    apply_events(conn, source, "m_loan_transaction", cols, [
        {"id": 1, "loan_id": 10, "transaction_date": date(2026, 3, 1), "amount": 50, "lsn": 1},
    ])
    before = len(change_log(conn, source))
    result = apply_events(conn, source, "m_loan_transaction", cols, [
        {"id": 1, "loan_id": 10, "transaction_date": date(2026, 3, 1), "lsn": 2, "unavailable": ["amount"]},
    ])
    assert result and result[0][1] == 1
    assert len(change_log(conn, source)) == before + 1, "the TOAST path must feed the change log too"
    deleted = apply_events(conn, source, "m_loan_transaction", cols, [
        {"id": 1, "loan_id": 10, "transaction_date": date(2026, 3, 1), "lsn": 3, "op": "d",
         "unavailable": ["amount"]},
    ])
    assert deleted[0][1] == 1 and deleted[0][2] == 1, "a delete on the TOAST path is applied and counted"
    cur = conn.cursor()
    cur.execute("SELECT amount::text, _is_deleted FROM raw.raw_m_loan_transaction WHERE source_id = %s", (source,))
    assert [tuple(r) for r in cur.fetchall()] == [("50.000000", True)], "the earlier value is kept, marked deleted"
    conn.commit()
    assert len(change_log(conn, source)) == before + 2
    assert all(entry[0] == "loan" and entry[1] == 10 for entry in change_log(conn, source))


def test_change_log_uses_the_pre_image_when_a_loan_date_moves_later(conn, source):
    assign(conn, source, "m_loan")
    cols = ["id", "disbursedon_date", "principal_repaid_derived"]
    apply_events(conn, source, "m_loan", cols, [
        {"id": 1, "disbursedon_date": date(2026, 1, 10), "principal_repaid_derived": 0, "lsn": 1},
    ])
    apply_events(conn, source, "m_loan", cols, [
        {"id": 1, "disbursedon_date": date(2026, 2, 1), "principal_repaid_derived": 0, "lsn": 2},
    ])
    log = change_log(conn, source)
    assert log[-1] == ("loan", 1, date(2026, 1, 10)), "the January snapshots must be rebuilt, not only February on"


def test_a_repayment_update_does_not_rebuild_history_from_disbursement(conn, source):
    assign(conn, source, "m_loan")
    cols = ["id", "disbursedon_date", "principal_repaid_derived"]
    apply_events(conn, source, "m_loan", cols, [
        {"id": 1, "disbursedon_date": date(2026, 1, 10), "principal_repaid_derived": 0, "lsn": 1},
    ])
    first = len(change_log(conn, source))
    apply_events(conn, source, "m_loan", cols, [
        {"id": 1, "disbursedon_date": date(2026, 1, 10), "principal_repaid_derived": 25, "lsn": 2},
    ])
    log = change_log(conn, source)
    assert len(log) == first + 1
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_DATE")
    today = cur.fetchone()[0]
    conn.commit()
    assert log[-1][2] == today, "balance-only changes refresh only the current snapshot"


def test_client_edits_that_do_not_touch_history_columns_are_not_logged(conn, source):
    assign(conn, source, "m_client")
    cols = ["id", "office_id", "gender_cv_id"]
    apply_events(conn, source, "m_client", cols, [{"id": 1, "office_id": 101, "gender_cv_id": 1, "lsn": 1}])
    base = len(change_log(conn, source))
    apply_events(conn, source, "m_client", cols, [{"id": 1, "office_id": 101, "gender_cv_id": 2, "lsn": 2}])
    assert len(change_log(conn, source)) == base
    apply_events(conn, source, "m_client", cols, [{"id": 1, "office_id": 102, "gender_cv_id": 2, "lsn": 3}])
    assert change_log(conn, source)[-1] == ("client", 1, date(1900, 1, 1))


def _ready_for_cutover(conn, source, table="batch_job_instance", actor="alice", repo=None):
    repo = repo or OwnershipRepository(conn)
    repo.apply(source, [table], TransitionRequest(Action.ASSIGN, actor, "t", engine=Engine.PYTHON))
    repo.apply(source, [table], TransitionRequest(Action.START_SHADOW, actor, "t", engine=Engine.KAFKA))
    repo.apply(source, [table], TransitionRequest(Action.BEGIN_RECONCILIATION, actor, "t"))
    return repo


def test_cutover_needs_a_recent_passed_run_for_the_same_table(conn, source):
    repo = _ready_for_cutover(conn, source)
    stale = passed_run(conn, source, age="3 hours")
    repo.apply(source, ["batch_job_instance"], TransitionRequest(
        Action.RECORD_RECONCILIATION, "alice", "t", reconciliation_passed=True, reconciliation_id=stale))
    with pytest.raises(TransitionError, match="recent PASSED"):
        repo.apply(source, ["batch_job_instance"],
                   TransitionRequest(Action.CUTOVER, "alice", "go", approved_by="bob"))
    conn.rollback()
    cur = conn.cursor()
    fresh = passed_run(conn, source)
    cur.execute("UPDATE meta.ingestion_ownership SET last_reconciliation_id = %s WHERE source_id = %s", (fresh, source))
    conn.commit()
    repo.apply(source, ["batch_job_instance"], TransitionRequest(Action.CUTOVER, "alice", "go", approved_by="bob"))


def test_a_run_for_another_table_cannot_authorise_a_cutover(conn, source):
    repo = _ready_for_cutover(conn, source)
    wrong = passed_run(conn, source, table="m_loan")
    repo.apply(source, ["batch_job_instance"], TransitionRequest(
        Action.RECORD_RECONCILIATION, "alice", "t", reconciliation_passed=True, reconciliation_id=wrong))
    with pytest.raises(TransitionError, match="recent PASSED"):
        repo.apply(source, ["batch_job_instance"],
                   TransitionRequest(Action.CUTOVER, "alice", "go", approved_by="bob"))


def test_the_database_rejects_an_engine_flip_that_skipped_the_gate(conn, source):
    assign(conn, source, "batch_job_instance", Engine.PYTHON)
    cur = conn.cursor()
    with pytest.raises(pg8000.dbapi.DatabaseError) as err:
        cur.execute(
            "UPDATE meta.ingestion_ownership SET engine = 'KAFKA', epoch = epoch + 1 "
            "WHERE source_id = %s AND table_name = 'batch_job_instance'",
            (source,),
        )
    conn.rollback()
    assert sqlstate(err.value) == "FBI03"
    with pytest.raises(pg8000.dbapi.DatabaseError) as err:
        cur.execute(
            "UPDATE meta.ingestion_ownership SET engine = 'NONE' WHERE source_id = %s AND table_name = 'm_loan'",
            (source,),
        )
        cur.execute(
            "UPDATE meta.ingestion_ownership SET engine = 'PYTHON' WHERE source_id = %s AND table_name = 'm_loan'",
            (source,),
        )
        cur.execute(
            "UPDATE meta.ingestion_ownership SET engine = 'KAFKA' WHERE source_id = %s AND table_name = 'm_loan'",
            (source,),
        )
    conn.rollback()
    assert sqlstate(err.value) == "FBI03"


def test_the_ownership_audit_trail_is_append_only(conn, admin, source):
    assign(conn, source, "batch_job_instance")
    cur = conn.cursor()
    with pytest.raises(pg8000.dbapi.DatabaseError) as err:
        cur.execute("DELETE FROM meta.ingestion_ownership_audit WHERE source_id = %s", (source,))
    conn.rollback()
    assert sqlstate(err.value) == "42501", "the loader role has no DELETE privilege on the audit trail"
    acur = admin.cursor()
    with pytest.raises(pg8000.dbapi.DatabaseError) as err:
        acur.execute("UPDATE meta.ingestion_ownership_audit SET actor = 'mallory' WHERE source_id = %s", (source,))
    admin.rollback()
    assert sqlstate(err.value) == "FBI03", "even an owner cannot rewrite history without the maintenance switch"
    acur.execute("SELECT db_user FROM meta.ingestion_ownership_audit WHERE source_id = %s LIMIT 1", (source,))
    assert acur.fetchone()[0] == _cfg().user
    admin.rollback()


def test_the_loader_cannot_change_platform_policy(conn):
    cur = conn.cursor()
    with pytest.raises(pg8000.dbapi.DatabaseError) as err:
        cur.execute(
            "UPDATE meta.platform_settings SET setting_value = 'false' WHERE setting_key = 'require_dual_control'"
        )
    conn.rollback()
    assert sqlstate(err.value) == "42501"


def test_a_cutover_lock_blocks_only_its_own_source_and_table():
    holder, other = connect(_cfg()), connect(_cfg())
    try:
        holder.cursor().execute("SELECT meta.lock_ownership_exclusive('lock-src-a', 'm_loan')")
        cur = other.cursor()
        cur.execute("SELECT pg_try_advisory_xact_lock_shared(meta.ownership_lock_key('lock-src-b', 'm_loan'))")
        assert cur.fetchone()[0] is True, "another source is not held up by the copy"
        cur.execute("SELECT pg_try_advisory_xact_lock_shared(meta.ownership_lock_key('lock-src-a', 'm_client'))")
        assert cur.fetchone()[0] is True, "another table of the same source is not held up either"
        cur.execute("SELECT pg_try_advisory_xact_lock_shared(meta.ownership_lock_key('lock-src-a', 'm_loan'))")
        assert cur.fetchone()[0] is False, "writers to the table being cut over wait for it"
    finally:
        holder.rollback()
        other.rollback()
        holder.close()
        other.close()


def _engine_cfg() -> PgConfig:
    base = _cfg()
    return PgConfig(
        base.host,
        base.port,
        base.database,
        os.getenv("WAREHOUSE_ENGINE_USER", "analytics_engine"),
        os.getenv("WAREHOUSE_ENGINE_PASSWORD", "analytics_engine_dev_only"),
    )


def test_the_engine_role_writes_rows_but_cannot_change_control_tables(conn, source):
    assign(conn, source, "m_loan_transaction")
    engine = connect(_engine_cfg())
    try:
        cols = ["id", "loan_id", "transaction_date", "amount"]
        result = apply_events(engine, source, "m_loan_transaction", cols, [
            {"id": 1, "loan_id": 10, "transaction_date": date(2026, 3, 1), "amount": 50, "lsn": 1},
        ])
        assert result and result[0][1] == 1, "the engine role can apply a batch through the single write path"
        cur = engine.cursor()
        for statement in (
            "UPDATE meta.ingestion_ownership SET epoch = epoch + 1",
            "INSERT INTO meta.ingestion_ownership_audit (source_id, table_name, action, actor) "
            "VALUES ('x', 'm_loan', 'assign', 'mallory')",
            "DELETE FROM meta.ingestion_ownership_audit",
            "UPDATE meta.platform_settings SET setting_value = 'false'",
            "UPDATE meta.source_registry SET region = 'x'",
            "DELETE FROM meta.cutover_approval",
            "DELETE FROM meta.reconciliation_run",
        ):
            with pytest.raises(pg8000.dbapi.DatabaseError) as err:
                cur.execute(statement)
            engine.rollback()
            assert sqlstate(err.value) == "42501", statement
    finally:
        engine.close()


def _login(admin, name):
    cur = admin.cursor()
    cur.execute(f"DROP ROLE IF EXISTS {name}")
    cur.execute(f"CREATE ROLE {name} LOGIN PASSWORD 'it_{name}'")
    cur.execute(f"GRANT {_cfg().user} TO {name}")
    admin.commit()


def test_database_mode_needs_an_approval_from_a_different_database_login(conn, admin, source):
    suffix = uuid.uuid4().hex[:6]
    a_name, b_name = f"fbi_it_a_{suffix}", f"fbi_it_b_{suffix}"
    _login(admin, a_name)
    _login(admin, b_name)
    cur = admin.cursor()
    cur.execute("SELECT setting_value FROM meta.platform_settings WHERE setting_key = 'dual_control_mode'")
    previous = cur.fetchone()[0]
    cur.execute("UPDATE meta.platform_settings SET setting_value = 'database' WHERE setting_key = 'dual_control_mode'")
    admin.commit()

    def login(name):
        return connect(PgConfig(_cfg().host, _cfg().port, _cfg().database, name, f"it_{name}"))

    a, b = login(a_name), login(b_name)
    try:
        repo_a = _ready_for_cutover(a, source, actor="alice", repo=OwnershipRepository(a))
        run = passed_run(a, source)
        repo_a.apply(source, ["batch_job_instance"], TransitionRequest(
            Action.RECORD_RECONCILIATION, "alice", "t", reconciliation_passed=True, reconciliation_id=run))

        with pytest.raises(TransitionError, match="different database login"):
            repo_a.apply(source, ["batch_job_instance"],
                         TransitionRequest(Action.CUTOVER, "alice", "go", approved_by="bob"))
        a.rollback()
        with pytest.raises(pg8000.dbapi.DatabaseError) as err:
            repo_a.approve_cutover(source, ["batch_job_instance"], "alice approving herself")
        assert sqlstate(err.value) == "FBI03", "the login that requested the shadow cannot approve"
        a.rollback()

        OwnershipRepository(b).approve_cutover(source, ["batch_job_instance"], "bob")
        repo_a.apply(source, ["batch_job_instance"],
                     TransitionRequest(Action.CUTOVER, "alice", "go", approved_by="ignored"))
        o = repo_a.get(source, "batch_job_instance")
        a.commit()
        assert o.engine is Engine.KAFKA
        cur.execute(
            "SELECT approved_by FROM meta.ingestion_ownership_audit WHERE source_id = %s AND action = 'cutover'",
            (source,),
        )
        assert cur.fetchone()[0] == b_name, "the audit trail names the database login that approved"
        admin.rollback()
    finally:
        a.close()
        b.close()
        cur.execute("UPDATE meta.platform_settings SET setting_value = %s WHERE setting_key = 'dual_control_mode'",
                    (previous,))
        admin.commit()
        for name in (a_name, b_name):
            cur.execute(f"DROP ROLE IF EXISTS {name}")
        admin.commit()


def test_change_log_partitions_are_dropped_whole_and_stale_cursors_do_not_pin_them(conn, admin, source):
    cur = admin.cursor()
    cur.execute("SELECT meta.ensure_change_log_partitions(40, 1)")
    cur.execute(
        "INSERT INTO meta.entity_change_log (source_id, tenant_id, entity, entity_id, min_affected_date, changed_at) "
        "VALUES (%s, 't', 'loan', 1, DATE '2026-01-01', NOW() - INTERVAL '30 days')",
        (source,),
    )
    admin.commit()
    cur.execute("SELECT model_name, last_txid::text, updated_at FROM meta.dbt_change_cursor")
    saved = [tuple(r) for r in cur.fetchall()]
    cur.execute("DELETE FROM meta.dbt_change_cursor")
    cur.execute(
        "INSERT INTO meta.dbt_change_cursor (model_name, last_txid, updated_at) VALUES "
        "('it_current', pg_snapshot_xmin(pg_current_snapshot()), NOW()), "
        "('it_abandoned', '0'::xid8, NOW() - INTERVAL '60 days')"
    )
    admin.commit()
    try:
        cur.execute("SELECT to_char(NOW() - INTERVAL '30 days', 'YYYYMMDD')")
        part = f"entity_change_log_{cur.fetchone()[0]}"
        cur.execute("SELECT to_regclass(%s)", (f"meta.{part}",))
        assert cur.fetchone()[0] is not None
        dropped_by_loader = conn.cursor()
        dropped_by_loader.execute("SELECT meta.purge_change_log(14)")
        dropped = dropped_by_loader.fetchone()[0]
        conn.commit()
        assert dropped >= 1
        cur.execute("SELECT to_regclass(%s)", (f"meta.{part}",))
        assert cur.fetchone()[0] is None, "an old partition is dropped as a whole instead of row by row"
        admin.rollback()
    finally:
        cur.execute("DELETE FROM meta.dbt_change_cursor")
        for model, txid, updated in saved:
            cur.execute(
                "INSERT INTO meta.dbt_change_cursor (model_name, last_txid, updated_at) VALUES (%s, %s::xid8, %s)",
                (model, txid, updated),
            )
        admin.commit()


def test_retention_covers_progress_audit_and_ownership_audit_tables(conn):
    cur = conn.cursor()
    cur.execute("SELECT meta.purge_progress(36500)")
    assert cur.fetchone()[0] == 0
    cur.execute("SELECT meta.purge_ownership_audit(36500)")
    assert cur.fetchone()[0] == 0
    cur.execute("SELECT meta.purge_progress(0)")
    assert cur.fetchone()[0] >= 0
    conn.rollback()


def test_retire_dry_run_changes_nothing_and_forget_waits_for_the_replication_slot(conn, source, monkeypatch):
    from controlplane import reconciler
    from controlplane.connectors import CdcSettings
    from controlplane.retire import retire_source

    monkeypatch.setenv("FBI_SRC_IT_PASSWORD", "pw")
    monkeypatch.setenv("FBI_DEFAULT_SSLMODE", "disable")
    SourceRegistry(conn).upsert([SourceRecord(
        source, f"tenant-{source}", db_host="db.example", db_name="fineract", db_user="fbi_cdc",
        secret_ref="env:FBI_SRC_IT_PASSWORD", lifecycle_state="ACTIVE", capture_generation=1)])
    conn.commit()
    assign(conn, source, "batch_job_instance")
    apply_events(conn, source, "batch_job_instance", ["job_instance_id", "job_name"],
                 [{"job_instance_id": 1, "job_name": "a", "lsn": 1}])

    dry = retire_source(conn, source, dry_run=True)
    assert dry["dry_run"] is True and dry["raw_rows"] == 1 and dry["registered"] is True
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM raw.raw_batch_job_instance WHERE source_id = %s", (source,))
    assert cur.fetchone()[0] == 1, "a dry run must not delete anything"
    conn.commit()

    class Connect:
        def list_names(self):
            return []

        def delete(self, name):
            pass

    class Source:
        def __init__(self, slots):
            self.slots = slots

        def cursor(self):
            outer = self

            class Cur:
                last = ""

                def execute(self, sql, params=None):
                    Cur.last = sql

                def fetchall(self):
                    if "LIKE" in Cur.last:
                        return [(n,) for n, _ in outer.slots]
                    return [(n, a) for n, a in outer.slots]

            return Cur()

        def commit(self):
            pass

        def close(self):
            pass

    stuck = Source([("fbi_" + source.replace("-", "_") + "_g0", True)])
    settings = CdcSettings(connect_clusters={"default": "http://unused"})
    original = reconciler.retire_artifacts

    def with_fakes(c, src, st, clients=None, connect_source=None, wait_seconds=0.0):
        return original(c, src, st, {"default": Connect()}, lambda cfg: stuck, wait_seconds=0.0)

    monkeypatch.setattr("controlplane.retire.retire_artifacts", with_fakes)
    out = retire_source(conn, source, forget=True, settings=settings)
    assert "tombstone" in out["lifecycle"]
    assert SourceRegistry(conn).get(source).labels.get("forget_after_cleanup") is True
    conn.commit()
    assert SourceRegistry(conn).get(source).lifecycle_state == "RETIRED"

    stuck.slots.clear()
    state = reconciler.DesiredState([], set(), [], set())
    monkeypatch.setattr(reconciler, "kafka_owned_sources", lambda c: set())
    actions = reconciler.cleanup_stale_slots(
        conn, state, {}, None, None, reconciler.ReconcileReport(), lambda cfg: stuck
    )
    conn.commit()
    assert {"kind": "forget_source", "source_id": source} in actions
    with pytest.raises(Exception, match="Unknown source"):
        SourceRegistry(conn).get(source)
    conn.rollback()
    os.environ.pop("FBI_SRC_IT_PASSWORD", None)
