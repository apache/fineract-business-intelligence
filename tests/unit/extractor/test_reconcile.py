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
from tests.conftest import FakeConnection, FakeCursor

SPEC = next(spec for spec in TABLE_SPECS if spec.source_table == "m_office")


@pytest.fixture
def extractor(app_config) -> FineractExtractor:
    return FineractExtractor(app_config)


def test_fetch_source_primary_keys_paginates_via_fetchmany(extractor):
    cursor = FakeCursor(fetchmany_batches=[[(1,), (2,)], [(3,)], []])
    connection = FakeConnection(cursor)

    result = extractor._fetch_source_primary_keys(connection, SPEC)

    assert result == {1, 2, 3}


def test_fetch_source_primary_keys_scopes_to_the_configured_schema_and_table(extractor):
    connection = FakeConnection(FakeCursor(fetchmany_batches=[[]]))

    extractor._fetch_source_primary_keys(connection, SPEC)

    sql, _ = connection.cursor().executed[0]
    assert 'FROM "bi_connector_source"."m_office"' in sql
    assert '"id"' in sql


def test_delete_orphaned_rows_deletes_nothing_when_warehouse_is_a_subset_of_source(extractor):
    cursor = FakeCursor(fetchmany_batches=[[(1,), (2,)], []])
    connection = FakeConnection(cursor)

    deleted = extractor._delete_orphaned_rows(connection, SPEC, source_pks={1, 2, 3})

    assert deleted == 0
    assert len(connection.cursor().executed) == 1


def test_delete_orphaned_rows_deletes_rows_missing_from_source(extractor):
    cursor = FakeCursor(fetchmany_batches=[[(1,), (2,), (3,)], []])
    connection = FakeConnection(cursor)

    deleted = extractor._delete_orphaned_rows(connection, SPEC, source_pks={1})

    assert deleted == 2
    delete_sql, delete_params = connection.cursor().executed[-1]
    assert "DELETE FROM raw." in delete_sql
    assert delete_params[0] == "default"
    assert set(delete_params[1:]) == {2, 3}


def test_delete_orphaned_rows_scopes_the_delete_to_the_current_tenant(extractor):
    cursor = FakeCursor(fetchmany_batches=[[(1,)], []])
    connection = FakeConnection(cursor)

    extractor._delete_orphaned_rows(connection, SPEC, source_pks=set())

    delete_sql, _ = connection.cursor().executed[-1]
    assert "tenant_id = %s" in delete_sql


def test_delete_orphaned_rows_chunks_when_over_the_parameter_limit(extractor, monkeypatch):
    monkeypatch.setattr(FineractExtractor, "_POSTGRES_MAX_BIND_PARAMETERS", 2)
    cursor = FakeCursor(fetchmany_batches=[[(1,), (2,), (3,)], []])
    connection = FakeConnection(cursor)

    deleted = extractor._delete_orphaned_rows(connection, SPEC, source_pks=set())

    assert deleted == 3
    delete_statements = connection.cursor().executed[1:]
    assert len(delete_statements) == 3


def test_reconcile_deletes_aggregates_counts_across_every_table_spec(extractor, mocker):
    mocker.patch.object(extractor, "_fetch_source_primary_keys", return_value=set())
    mocker.patch.object(extractor, "_delete_orphaned_rows", return_value=5)
    source_connection = FakeConnection()
    warehouse_connection = FakeConnection()

    result = extractor._reconcile_deletes(source_connection, warehouse_connection)

    assert len(result) == len(TABLE_SPECS)
    assert all(count == 5 for count in result.values())
    assert extractor._fetch_source_primary_keys.call_count == len(TABLE_SPECS)
    assert extractor._delete_orphaned_rows.call_count == len(TABLE_SPECS)
