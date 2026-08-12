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

import dataclasses
from datetime import UTC, datetime, timedelta, timezone

import pytest

from extractor.extractor import (
    TABLE_SPECS,
    FineractExtractor,
    _quote_identifier,
    as_utc_datetime,
)


def test_none_passes_through():
    assert as_utc_datetime(None) is None


def test_naive_datetime_is_assumed_to_be_utc():
    result = as_utc_datetime(datetime(2026, 8, 2, 12, 0))

    assert result.tzinfo == UTC
    assert result.hour == 12


def test_aware_datetime_is_converted_rather_than_relabelled():
    ist = timezone(timedelta(hours=5, minutes=30))
    result = as_utc_datetime(datetime(2026, 8, 2, 17, 30, tzinfo=ist))

    assert result.tzinfo == UTC
    assert result.hour == 12


def test_utc_datetime_is_unchanged():
    value = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)
    assert as_utc_datetime(value) == value


def test_identifier_is_wrapped_in_double_quotes():
    assert _quote_identifier("m_loan") == '"m_loan"'


def test_embedded_double_quote_is_escaped_by_doubling():
    assert _quote_identifier('ab"c') == '"ab""c"'


def test_identifier_containing_a_quoted_injection_attempt_stays_inert():
    quoted = _quote_identifier('x" ; DROP TABLE raw.raw_m_loan; --')

    assert quoted.startswith('"') and quoted.endswith('"')
    assert quoted.count('"') % 2 == 0
    assert '";' not in quoted


def test_mixed_case_is_preserved():
    assert _quote_identifier("MixedCase") == '"MixedCase"'


@pytest.mark.parametrize("mode", ["", "full", "BACKFILL", "incremental ", "delete"])
def test_run_rejects_unsupported_modes_before_opening_connections(app_config, mode):
    with pytest.raises(ValueError, match="Unsupported mode"):
        FineractExtractor(app_config).run(mode)


def test_raw_table_names_are_prefixed_and_unique():
    raw_names = [spec.raw_table for spec in TABLE_SPECS]

    assert len(raw_names) == len(set(raw_names))
    assert all(name.startswith("raw_") for name in raw_names)


def test_source_table_names_are_unique():
    source_names = [spec.source_table for spec in TABLE_SPECS]
    assert len(source_names) == len(set(source_names))


def test_no_spec_declares_duplicate_columns():
    for spec in TABLE_SPECS:
        assert len(spec.columns) == len(set(spec.columns)), spec.source_table


def test_all_tables_required_by_the_marts_are_extracted():
    extracted = {spec.source_table for spec in TABLE_SPECS}

    required = {
        "m_office",
        "m_client",
        "m_loan",
        "m_loan_transaction",
        "m_product_loan",
        "m_currency",
        "m_loan_repayment_schedule",
        "m_loan_delinquency_tag_history",
        "batch_job_execution",
    }
    assert required <= extracted


def test_table_specs_are_immutable():
    with pytest.raises(dataclasses.FrozenInstanceError):
        TABLE_SPECS[0].source_table = "hacked"


def test_batch_job_execution_extracts_job_name_for_cob_gate_scoping():
    spec = next(s for s in TABLE_SPECS if s.source_table == "batch_job_execution")

    assert "job_name" in spec.columns
