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

import pytest

from extractor.extractor import TABLE_SPECS, FineractExtractor

WATERMARK = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def extractor(app_config) -> FineractExtractor:
    return FineractExtractor(app_config)


def _spec(name: str):
    return next(spec for spec in TABLE_SPECS if spec.source_table == name)


def test_backfill_query_has_no_where_clause_and_no_params(extractor):
    sql, params = extractor._build_source_query(_spec("m_loan"), None)

    assert "WHERE" not in sql
    assert params == ()


def test_incremental_query_binds_the_watermark_as_a_parameter(extractor):
    sql, params = extractor._build_source_query(_spec("m_loan"), WATERMARK)

    assert 'WHERE "last_modified_on_utc" >= %s' in sql
    assert params == (WATERMARK,)
    assert "2026-08-01" not in sql


@pytest.mark.parametrize("watermark", [None, WATERMARK])
def test_query_is_always_totally_ordered_for_stable_pagination(extractor, watermark):
    sql, _ = extractor._build_source_query(_spec("m_loan"), watermark)

    assert 'ORDER BY "last_modified_on_utc", "id"' in sql


def test_schema_table_and_columns_are_all_quoted(extractor):
    sql, _ = extractor._build_source_query(_spec("m_loan"), None)

    assert 'FROM "bi_connector_source"."m_loan"' in sql
    assert '"principal_outstanding_derived"' in sql


def test_only_the_declared_columns_are_selected(extractor):
    spec = _spec("m_client")
    sql, _ = extractor._build_source_query(spec, None)

    projection = sql.split(" FROM ")[0]
    for column in spec.columns:
        assert f'"{column}"' in projection
    assert "display_name" not in projection
    assert "mobile_no" not in projection


def test_repayment_schedule_uses_its_own_cursor_column(extractor):
    spec = _spec("m_loan_repayment_schedule")
    assert spec.cursor_column == "lastmodified_date"

    sql, params = extractor._build_source_query(spec, WATERMARK)

    assert 'WHERE "lastmodified_date" >= %s' in sql
    assert 'ORDER BY "lastmodified_date", "id"' in sql


def test_every_table_spec_declares_its_cursor_column_in_its_projection():
    for spec in TABLE_SPECS:
        assert spec.cursor_column in spec.columns, spec.source_table
        assert spec.primary_key in spec.columns, spec.source_table


def test_loan_transaction_spec_carries_the_fields_the_repayment_mart_needs():
    columns = _spec("m_loan_transaction").columns

    for required in (
        "is_reversed",
        "transaction_type_enum",
        "overpayment_portion_derived",
        "penalty_charges_portion_derived",
        "fee_charges_portion_derived",
    ):
        assert required in columns
