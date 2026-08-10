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

from datetime import UTC, datetime, timedelta

import pytest
from freezegun import freeze_time

from extractor.extractor import FineractExtractor
from tests.conftest import FakeConnection, FakeCursor

FROZEN_NOW = "2026-08-02 12:00:00"


def _extractor_with_cob(app_config, last_completed):
    cursor = FakeCursor(fetchone_results=[(last_completed,)])
    connection = FakeConnection(cursor)
    return FineractExtractor(app_config), connection


@freeze_time(FROZEN_NOW)
def test_raises_when_no_completed_cob_execution_exists(app_config):
    extractor, connection = _extractor_with_cob(app_config, None)

    with pytest.raises(RuntimeError, match="No completed COB execution found"):
        extractor._ensure_cob_completed(connection)


@freeze_time(FROZEN_NOW)
def test_raises_when_latest_cob_is_older_than_lookback_window(app_config):
    stale = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
    extractor, connection = _extractor_with_cob(app_config, stale)

    with pytest.raises(RuntimeError, match="is older than 48 hours"):
        extractor._ensure_cob_completed(connection)


@freeze_time(FROZEN_NOW)
def test_passes_when_cob_completed_recently(app_config):
    recent = datetime(2026, 8, 2, 6, 0, tzinfo=UTC)
    extractor, connection = _extractor_with_cob(app_config, recent)

    extractor._ensure_cob_completed(connection)


@freeze_time(FROZEN_NOW)
def test_naive_timestamp_from_source_is_treated_as_utc(app_config):
    naive_recent = datetime(2026, 8, 2, 6, 0)
    extractor, connection = _extractor_with_cob(app_config, naive_recent)

    extractor._ensure_cob_completed(connection)


@freeze_time(FROZEN_NOW)
def test_gate_boundary_is_inclusive_at_exactly_the_lookback_limit(app_config):
    exactly_at_cutoff = datetime.now(UTC) - timedelta(hours=48)
    extractor, connection = _extractor_with_cob(app_config, exactly_at_cutoff)

    extractor._ensure_cob_completed(connection)


@freeze_time(FROZEN_NOW)
def test_query_filters_on_completed_status_and_configured_schema(app_config):
    recent = datetime(2026, 8, 2, 6, 0, tzinfo=UTC)
    extractor, connection = _extractor_with_cob(app_config, recent)

    extractor._ensure_cob_completed(connection)

    sql, params = connection.cursor().executed[0]
    assert "batch_job_execution" in sql
    assert "MAX(end_time)" in sql
    assert params == ("COMPLETED",)
    assert '"bi_connector_source"' in sql
