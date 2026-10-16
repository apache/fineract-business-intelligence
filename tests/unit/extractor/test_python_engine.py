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

import io
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pg8000.dbapi
import pytest
from freezegun import freeze_time

from controlplane.ownership import CutoverState, Engine, Ownership
from extractor.config import EnvSourceBootstrap, ExtractSettings
from extractor.extractor import (
    Assignment,
    FencedError,
    SourceExtractor,
    TableOutcome,
    build_copy_payload,
    build_page_query,
    csv_field,
    plan_assignments,
    sqlstate,
)
from extractor.watermark_manager import Watermark
from tests.conftest import FakeConnection, FakeCursor


def own(table, engine, shadow=None, state=CutoverState.STEADY, epoch=5):
    return Ownership("branch-001", table, engine, shadow, state, epoch)


def test_plan_assignments_follows_the_feature_flag(contract):
    plan = plan_assignments(
        [
            own("m_loan", Engine.PYTHON),
            own("m_client", Engine.KAFKA, Engine.PYTHON, CutoverState.SHADOWING, 9),
            own("m_office", Engine.KAFKA),
            own("m_currency", Engine.PYTHON, state=CutoverState.PAUSED),
        ],
        contract,
    )
    assert [(a.table.table_name, a.target, a.epoch) for a in plan] == [("m_client", "shadow", 9), ("m_loan", "prod", 5)]


def test_plan_assignments_can_be_limited_to_tables(contract):
    plan = plan_assignments([own("m_loan", Engine.PYTHON), own("m_client", Engine.PYTHON)], contract, {"m_client"})
    assert [a.table.table_name for a in plan] == ["m_client"]


def test_keyset_page_query_is_bounded_and_resumable(contract):
    t = contract.table("m_loan_transaction")
    sql, params = build_page_query("bi_connector_source", t, None, 5000)
    assert 'FROM "bi_connector_source"."m_loan_transaction"' in sql
    assert "LIMIT %s" in sql and params[-1] == 5000
    assert '("last_modified_on_utc"' in sql.replace("COALESCE(", "(")
    resume = Watermark(datetime(2026, 1, 1, tzinfo=UTC), 77)
    _, params = build_page_query("bi_connector_source", t, resume, 10)
    assert params == (resume.cursor_value, 77, 10)


def test_reference_tables_without_usable_cursor_page_by_primary_key(contract):
    t = contract.table("m_office")
    assert not t.cursor_is_usable
    sql, params = build_page_query("bi_connector_source", t, Watermark(None, 12), 100)
    assert 'WHERE "id" > %s ORDER BY "id" LIMIT %s' in sql
    assert params == (12, 100)
    assert 'CAST(NULL AS timestamptz) AS "created_on_utc"' in sql, "synthetic columns are written as NULL"


def test_csv_encoding_distinguishes_null_from_empty_and_escapes_quotes():
    assert csv_field(None) == ""
    assert csv_field("") == '""'
    assert csv_field('say "hi"') == '"say ""hi"""'
    assert csv_field(True) == "t"
    assert csv_field(Decimal("10.500000")) == '"10.500000"'
    assert csv_field(date(2026, 1, 2)) == '"2026-01-02"'
    assert csv_field(datetime(2026, 1, 2, 3, 4, 5)) == '"2026-01-02T03:04:05+00:00"'
    buf = build_copy_payload([(1, None, "x,y"), (2, "", "z")])
    assert buf.read() == '"1",,"x,y"\n"2","","z"\n'


def test_write_page_stages_rows_with_owner_epoch_and_target(source_record, extract_settings, contract):
    cursor = FakeCursor(fetchone_results=[("stage_raw_m_office",)], fetchall_results=[[("shadow", 2, 0)]])
    wh = FakeConnection(cursor)
    extractor = SourceExtractor(source_record, None, extract_settings, contract)
    a = Assignment(contract.table("m_office"), "shadow", 11)
    rows = [(1, None, ".1.", None, "Head Office", date(2015, 1, 1), None, None),
            (2, 1, ".1.2.", "X", "Branch", date(2016, 1, 1), None, None)]
    applied, deleted = extractor._write_page(wh, a, rows, None, op="u")
    assert (applied, deleted) == (2, 0)
    copy_sql = next(sql for sql, _ in cursor.executed if sql.startswith("COPY"))
    assert copy_sql.startswith('COPY pg_temp."stage_raw_m_office" ("source_id", "tenant_id", "id"')
    assert copy_sql.endswith('"_owner_epoch", "_target") FROM STDIN WITH (FORMAT csv)')
    staged = cursor.streams[0].splitlines()
    assert staged[0].startswith('"branch-001","default","1",,".1."')
    assert staged[0].endswith(',"u",,"11","shadow"'), "op, no TOAST markers, owner epoch, target"
    assert ',"3",,"' in staged[0], "capture generation 3, no LSN for the Python engine, then source ts"
    assert any("apply_staged_batch(%s, 'PYTHON')" in sql for sql, _ in cursor.executed)


def test_fencing_error_is_surfaced_and_rolled_back(source_record, extract_settings, contract):
    class FencingCursor(FakeCursor):
        def execute(self, sql, params=None, stream=None):
            super().execute(sql, params, stream)
            if "apply_staged_batch" in sql:
                raise pg8000.dbapi.DatabaseError({"C": "FBI01", "M": "Writer PYTHON fenced"})

    cursor = FencingCursor(fetchone_results=[("stage_raw_m_office",)])
    wh = FakeConnection(cursor)
    extractor = SourceExtractor(source_record, None, extract_settings, contract)
    with pytest.raises(FencedError, match="fenced"):
        extractor._write_page(wh, Assignment(contract.table("m_office"), "prod", 1), [(1,) + (None,) * 7], None, "u")
    assert wh.rolled_back == 1
    assert sqlstate(pg8000.dbapi.DatabaseError({"C": "23505"})) == "23505"
    assert sqlstate(RuntimeError("x")) is None


def test_extract_pages_until_short_page_and_advances_watermark(source_record, extract_settings, contract,
                                                               monkeypatch):
    t = contract.table("m_delinquency_bucket")
    ts = [datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=i) for i in range(3)]
    src_cursor = FakeCursor(fetchall_results=[
        [(1, "a", ts[0], ts[0]), (2, "b", ts[1], ts[1])],
        [(3, "c", ts[2], ts[2])],
    ])
    src = FakeConnection(src_cursor)
    extractor = SourceExtractor(source_record, None, extract_settings, contract)
    pages = []
    monkeypatch.setattr(extractor, "_write_page", lambda wh, a, rows, idx, op: pages.append(rows) or (len(rows), 0))

    class Marks:
        def __init__(self):
            self.updates = []

        def get(self, table, target):
            return Watermark(ts[0], 1)

        def update(self, table, target, cursor_value, last_pk):
            self.updates.append((cursor_value, last_pk))

    marks = Marks()
    out = TableOutcome("m_delinquency_bucket", "prod")
    extractor._extract(src, FakeConnection(), marks, Assignment(t, "prod", 1), out)
    assert [len(p) for p in pages] == [2, 1]
    assert out.rows_read == 3 and out.rows_applied == 3
    assert marks.updates == [(ts[1], 2), (ts[2], 3)]
    first_params = src_cursor.executed[0][1]
    assert first_params[0] == ts[0] - timedelta(seconds=600), "lookback window re-reads recent commits"


@freeze_time("2026-08-02 12:00:00")
def test_cob_gate(source_record, extract_settings, contract):
    extractor = SourceExtractor(source_record, None, extract_settings, contract)
    with pytest.raises(RuntimeError, match="No completed 'LOAN_COB'"):
        extractor._ensure_cob_completed(FakeConnection(FakeCursor(fetchone_results=[(None,)])))
    with pytest.raises(RuntimeError, match="older than 48 hours"):
        extractor._ensure_cob_completed(
            FakeConnection(FakeCursor(fetchone_results=[(datetime(2026, 7, 30, tzinfo=UTC),)])))
    extractor._ensure_cob_completed(FakeConnection(FakeCursor(fetchone_results=[(datetime(2026, 8, 2, 6),)])))


def test_settings_and_legacy_bootstrap_from_env(monkeypatch):
    monkeypatch.setenv("INGESTION_ENGINE_DEFAULT", "kafka")
    monkeypatch.setenv("EXTRACT_PAGE_SIZE", "250")
    s = ExtractSettings.from_env()
    assert s.default_engine == "KAFKA" and s.page_size == 250
    monkeypatch.setenv("INGESTION_ENGINE_DEFAULT", "spark")
    with pytest.raises(ValueError, match="INGESTION_ENGINE_DEFAULT"):
        ExtractSettings.from_env()
    monkeypatch.delenv("SOURCE_DB_USER", raising=False)
    assert EnvSourceBootstrap.from_env() is None
    monkeypatch.setenv("SOURCE_DB_USER", "reader")
    monkeypatch.setenv("SOURCE_DB_SCHEMA", "bi_connector_source")
    boot = EnvSourceBootstrap.from_env()
    assert boot.source_id == "default" and boot.compat_schema == "bi_connector_source"


def test_copy_payload_is_streamable():
    assert isinstance(build_copy_payload([]), io.StringIO)
