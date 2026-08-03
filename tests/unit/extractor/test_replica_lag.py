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

from extractor.replica_lag_check import (
    check_replica_connectivity,
    ensure_replica_safe,
    fetch_replica_lag_seconds,
)
from tests.conftest import FakeConnection, FakeCursor

THRESHOLD = 300


def _conn_with_lag(lag) -> FakeConnection:
    return FakeConnection(FakeCursor(fetchone_results=[(1,), (lag,)]))


def test_returns_lag_when_below_threshold():
    assert ensure_replica_safe(_conn_with_lag(120), THRESHOLD) == 120


def test_returns_zero_for_a_primary_that_is_not_in_recovery():
    assert ensure_replica_safe(_conn_with_lag(0), THRESHOLD) == 0


def test_raises_when_lag_exceeds_threshold():
    with pytest.raises(RuntimeError, match="Replica lag 301s exceeds"):
        ensure_replica_safe(_conn_with_lag(301), THRESHOLD)


def test_lag_exactly_at_threshold_is_allowed():
    assert ensure_replica_safe(_conn_with_lag(THRESHOLD), THRESHOLD) == THRESHOLD


def test_null_lag_is_coerced_to_zero():
    assert fetch_replica_lag_seconds(FakeConnection(FakeCursor([(None,)]))) == 0


def test_connectivity_check_issues_a_trivial_probe():
    connection = FakeConnection(FakeCursor([(1,)]))

    check_replica_connectivity(connection)

    assert connection.cursor().executed[0][0] == "SELECT 1"


def test_lag_query_uses_postgres_replication_functions():
    connection = FakeConnection(FakeCursor([(0,)]))

    fetch_replica_lag_seconds(connection)

    sql = connection.cursor().last_sql
    assert "pg_is_in_recovery()" in sql
    assert "pg_last_xact_replay_timestamp()" in sql


def test_connectivity_is_verified_before_lag_is_measured():
    connection = _conn_with_lag(10)

    ensure_replica_safe(connection, THRESHOLD)

    executed = connection.cursor().executed
    assert executed[0][0] == "SELECT 1"
    assert "pg_is_in_recovery()" in executed[1][0]
