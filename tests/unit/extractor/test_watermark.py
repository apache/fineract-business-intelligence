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

from datetime import UTC, datetime

from extractor.extractor import TABLE_SPECS, FineractExtractor
from extractor.watermark_manager import WatermarkManager
from tests.conftest import FakeConnection, FakeCursor

TENANT = "default"
OTHER_TENANT = "acme"

SPEC = next(spec for spec in TABLE_SPECS if spec.source_table == "m_loan")


def test_get_returns_none_when_no_watermark_row_exists():
    connection = FakeConnection(FakeCursor(fetchone_results=[None]))
    manager = WatermarkManager(connection, TENANT)

    assert manager.get("m_loan") is None


def test_get_returns_stored_cursor_value():
    stored = datetime(2026, 8, 1, 10, 0, tzinfo=UTC)
    connection = FakeConnection(FakeCursor(fetchone_results=[(stored,)]))
    manager = WatermarkManager(connection, TENANT)

    assert manager.get("m_loan") == stored


def test_get_scopes_the_query_by_tenant_id():
    connection = FakeConnection(FakeCursor(fetchone_results=[None]))
    manager = WatermarkManager(connection, OTHER_TENANT)

    manager.get("m_loan")

    sql, params = connection.cursor().executed[0]
    assert "tenant_id = %s" in sql
    assert params == (OTHER_TENANT, "m_loan")


def test_update_upserts_the_watermark_row():
    connection = FakeConnection()
    manager = WatermarkManager(connection, TENANT)
    value = datetime(2026, 8, 2, 9, 30, tzinfo=UTC)

    manager.update("m_loan", "last_modified_on_utc", value)

    sql, params = connection.cursor().executed[0]
    assert "INSERT INTO meta.watermarks" in sql
    assert "ON CONFLICT (tenant_id, table_name)" in sql
    assert "DO UPDATE SET" in sql
    assert params == (TENANT, "m_loan", "last_modified_on_utc", value)


def test_update_does_not_commit_leaving_transaction_boundary_to_the_caller():
    connection = FakeConnection()
    manager = WatermarkManager(connection, TENANT)
    value = datetime(2026, 8, 2, 9, 30, tzinfo=UTC)

    manager.update("m_loan", "last_modified_on_utc", value)

    assert connection.committed == 0


def test_update_never_writes_outside_its_own_tenant():
    connection = FakeConnection()
    manager = WatermarkManager(connection, OTHER_TENANT)

    manager.update("m_loan", "last_modified_on_utc", datetime.now(UTC))

    _, params = connection.cursor().executed[0]
    assert params[0] == OTHER_TENANT


def test_reset_all_deletes_only_the_current_tenants_watermarks():
    connection = FakeConnection()
    manager = WatermarkManager(connection, TENANT)

    manager.reset_all()

    sql, params = connection.cursor().executed[0]
    assert "DELETE FROM meta.watermarks" in sql
    assert "tenant_id = %s" in sql
    assert params == (TENANT,)


def test_reset_all_does_not_commit_leaving_transaction_boundary_to_the_caller():
    connection = FakeConnection()
    manager = WatermarkManager(connection, TENANT)

    manager.reset_all()

    assert connection.committed == 0


class _RecordingWatermarkManager:
    def __init__(self, stored):
        self._stored = stored
        self.updates: list[tuple] = []

    def get(self, table_name):
        return self._stored

    def update(self, table_name, cursor_column, last_cursor_value):
        self.updates.append((table_name, cursor_column, last_cursor_value))


def _run_extract(app_config, mode, stored_watermark, batches):
    extractor = FineractExtractor(app_config)
    source = FakeConnection(FakeCursor(fetchmany_batches=batches))
    warehouse = FakeConnection()
    manager = _RecordingWatermarkManager(stored_watermark)

    rows = extractor._extract_table(source, warehouse, manager, SPEC, mode)
    return rows, source.cursor(), manager


def _row_with_cursor(timestamp):
    values = [None] * len(SPEC.columns)
    values[SPEC.columns.index(SPEC.cursor_column)] = timestamp
    return tuple(values)


def test_incremental_subtracts_the_lookback_window_from_the_watermark(app_config):
    stored = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)

    _, cursor, _ = _run_extract(app_config, "incremental", stored, batches=[])

    _, params = cursor.executed[0]
    assert params == (datetime(2026, 8, 2, 11, 50, tzinfo=UTC),)


def test_backfill_ignores_any_stored_watermark(app_config):
    stored = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)

    _, cursor, _ = _run_extract(app_config, "backfill", stored, batches=[])

    sql, params = cursor.executed[0]
    assert "WHERE" not in sql
    assert params == ()


def test_no_lookback_applied_when_there_is_no_previous_watermark(app_config):
    _, cursor, _ = _run_extract(app_config, "incremental", None, batches=[])

    sql, params = cursor.executed[0]
    assert "WHERE" not in sql
    assert params == ()


def test_watermark_advances_to_the_max_cursor_value_in_each_batch(app_config):
    older = datetime(2026, 8, 1, 8, 0, tzinfo=UTC)
    newer = datetime(2026, 8, 1, 9, 0, tzinfo=UTC)

    batch = [_row_with_cursor(newer), _row_with_cursor(older)]
    rows, _, manager = _run_extract(app_config, "incremental", None, batches=[batch, []])

    assert rows == 2
    assert manager.updates[-1][2] == newer


def test_row_count_accumulates_across_multiple_batches(app_config):
    row = _row_with_cursor(datetime(2026, 8, 1, 8, 0, tzinfo=UTC))
    batches = [[row] * 3, [row] * 2, []]

    rows, _, _ = _run_extract(app_config, "incremental", None, batches=batches)

    assert rows == 5


class _RecordingResetManager:
    def __init__(self):
        self.reset_calls = 0

    def reset_all(self):
        self.reset_calls += 1


def test_reset_backfill_state_deletes_every_raw_table_for_the_tenant(app_config):
    extractor = FineractExtractor(app_config)
    warehouse = FakeConnection()
    manager = _RecordingResetManager()

    extractor._reset_backfill_state(warehouse, manager)

    deleted_tables = {sql for sql, _ in warehouse.cursor().executed}
    assert len(deleted_tables) == len(TABLE_SPECS)
    for spec in TABLE_SPECS:
        assert any(spec.raw_table in sql for sql in deleted_tables)


def test_reset_backfill_state_delegates_watermark_reset_to_the_manager(app_config):
    extractor = FineractExtractor(app_config)
    warehouse = FakeConnection()
    manager = _RecordingResetManager()

    extractor._reset_backfill_state(warehouse, manager)

    assert manager.reset_calls == 1
