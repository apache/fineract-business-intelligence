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

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class FakeCursor:
    def __init__(
        self,
        fetchone_results: list | None = None,
        fetchmany_batches: list[list[tuple]] | None = None,
        fetchall_results: list[list[tuple]] | None = None,
    ) -> None:
        self.executed: list[tuple[str, tuple | None]] = []
        self.streams: list[str] = []
        self.rowcount = 1
        self.description = None
        self._fetchone_results = list(fetchone_results or [])
        self._fetchmany_batches = list(fetchmany_batches or [])
        self._fetchall_results = list(fetchall_results or [])

    def execute(self, sql: str, params: tuple | None = None, stream=None) -> None:
        self.executed.append((sql, params))
        if stream is not None:
            self.streams.append(stream.read())

    def executemany(self, sql: str, rows) -> None:
        for row in rows:
            self.executed.append((sql, tuple(row)))

    def fetchall(self):
        if not self._fetchall_results:
            return []
        return self._fetchall_results.pop(0)

    def fetchone(self):
        if not self._fetchone_results:
            return None
        return self._fetchone_results.pop(0)

    def fetchmany(self, size: int):
        if not self._fetchmany_batches:
            return []
        return self._fetchmany_batches.pop(0)

    @property
    def last_sql(self) -> str:
        return self.executed[-1][0]

    @property
    def last_params(self):
        return self.executed[-1][1]


class FakeConnection:
    def __init__(self, cursor: FakeCursor | None = None) -> None:
        self._cursor = cursor or FakeCursor()
        self.committed = 0
        self.rolled_back = 0
        self.closed = False
        self.autocommit = False

    def cursor(self) -> FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        self.rolled_back += 1

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_cursor() -> FakeCursor:
    return FakeCursor()


@pytest.fixture
def fake_connection(fake_cursor: FakeCursor) -> FakeConnection:
    return FakeConnection(fake_cursor)


@pytest.fixture
def source_record():
    from controlplane.registry import SourceRecord

    return SourceRecord(
        source_id="branch-001",
        tenant_id="default",
        db_host="source-host",
        db_port=5432,
        db_name="fineract_default",
        db_user="reader",
        secret_ref="env:FBI_SRC_TEST_PASSWORD",
        compat_schema="bi_connector_source",
        lifecycle_state="ACTIVE",
        capture_generation=3,
    )


@pytest.fixture
def extract_settings():
    from extractor.config import ExtractSettings

    return ExtractSettings(page_size=2, lookback_seconds=600, cob_lookback_hours=48, writer_id="python-test")


@pytest.fixture
def contract():
    from controlplane.contract import load_contract

    return load_contract(PROJECT_ROOT / "contracts" / "fineract_tables.json")
