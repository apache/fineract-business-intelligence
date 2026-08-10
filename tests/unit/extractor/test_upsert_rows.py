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

import pytest

from extractor.extractor import TABLE_SPECS, FineractExtractor
from tests.conftest import FakeConnection

SPEC = next(spec for spec in TABLE_SPECS if spec.source_table == "m_office")


@pytest.fixture
def extractor(app_config) -> FineractExtractor:
    return FineractExtractor(app_config)


def _row(spec, marker=1) -> tuple:
    return tuple([marker] * len(spec.columns))


def test_upsert_targets_the_raw_table_with_conflict_handling(extractor):
    connection = FakeConnection()

    extractor._upsert_rows(connection, SPEC, [_row(SPEC)])

    sql = connection.cursor().last_sql
    assert 'INSERT INTO raw."raw_m_office"' in sql
    assert 'ON CONFLICT ("tenant_id", "id")' in sql
    assert "DO UPDATE SET" in sql


def test_tenant_id_is_prepended_to_every_row(extractor):
    connection = FakeConnection()

    extractor._upsert_rows(connection, SPEC, [_row(SPEC)])

    _, values = connection.cursor().executed[0]
    assert values[0] == "default"
    assert len(values) == len(SPEC.columns) + 1


def test_placeholder_count_matches_the_bound_value_count(extractor):
    connection = FakeConnection()
    rows = [_row(SPEC, 1), _row(SPEC, 2), _row(SPEC, 3)]

    extractor._upsert_rows(connection, SPEC, rows)

    sql, values = connection.cursor().executed[0]
    assert sql.count("%s") == len(values)
    assert len(values) == len(rows) * (len(SPEC.columns) + 1)


def test_multiple_rows_are_sent_as_a_single_multi_value_insert(extractor):
    connection = FakeConnection()

    extractor._upsert_rows(connection, SPEC, [_row(SPEC, 1), _row(SPEC, 2)])

    assert len(connection.cursor().executed) == 1


def test_primary_key_and_tenant_are_excluded_from_the_update_clause(extractor):
    connection = FakeConnection()

    extractor._upsert_rows(connection, SPEC, [_row(SPEC)])

    update_clause = connection.cursor().last_sql.split("DO UPDATE SET")[1]
    assert '"id" = EXCLUDED."id"' not in update_clause
    assert '"tenant_id" = EXCLUDED."tenant_id"' not in update_clause
    assert '"name" = EXCLUDED."name"' in update_clause


def test_every_non_key_column_is_refreshed_on_conflict(extractor):
    connection = FakeConnection()

    extractor._upsert_rows(connection, SPEC, [_row(SPEC)])

    update_clause = connection.cursor().last_sql.split("DO UPDATE SET")[1]
    for column in SPEC.columns:
        if column == SPEC.primary_key:
            continue
        assert f'"{column}" = EXCLUDED."{column}"' in update_clause


def test_reversed_transactions_are_loaded_rather_than_filtered(extractor):
    spec = next(s for s in TABLE_SPECS if s.source_table == "m_loan_transaction")
    connection = FakeConnection()

    extractor._upsert_rows(connection, spec, [_row(spec)])

    sql = connection.cursor().last_sql
    assert '"is_reversed"' in sql
    assert "WHERE" not in sql


def test_empty_rows_executes_nothing(extractor):
    connection = FakeConnection()

    extractor._upsert_rows(connection, SPEC, [])

    assert connection.cursor().executed == []


def test_upsert_splits_into_multiple_statements_when_over_the_parameter_limit(extractor, monkeypatch):
    monkeypatch.setattr(FineractExtractor, "_POSTGRES_MAX_BIND_PARAMETERS", len(SPEC.columns) + 1)
    connection = FakeConnection()
    rows = [_row(SPEC, marker) for marker in range(3)]

    extractor._upsert_rows(connection, SPEC, rows)

    assert len(connection.cursor().executed) == 3


def test_upsert_never_exceeds_the_configured_parameter_limit_per_statement(extractor, monkeypatch):
    limit = (len(SPEC.columns) + 1) * 2
    monkeypatch.setattr(FineractExtractor, "_POSTGRES_MAX_BIND_PARAMETERS", limit)
    connection = FakeConnection()
    rows = [_row(SPEC, marker) for marker in range(5)]

    extractor._upsert_rows(connection, SPEC, rows)

    for _, values in connection.cursor().executed:
        assert len(values) <= limit


def test_all_rows_are_represented_exactly_once_across_chunked_statements(extractor, monkeypatch):
    monkeypatch.setattr(FineractExtractor, "_POSTGRES_MAX_BIND_PARAMETERS", len(SPEC.columns) + 1)
    connection = FakeConnection()
    rows = [_row(SPEC, marker) for marker in range(4)]

    extractor._upsert_rows(connection, SPEC, rows)

    all_bound_values = [values for _, values in connection.cursor().executed]
    tenant_and_row_tuples = [tuple(v) for v in all_bound_values]
    expected = [("default", *row) for row in rows]
    assert tenant_and_row_tuples == expected
