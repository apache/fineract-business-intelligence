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
from pathlib import Path

import jinja2
import pg8000.native
import pytest

DATASET_DIR = Path(__file__).resolve().parent.parent.parent / "superset" / "datasets"

DATASET_FILES = [
    "delinquency_par_secure_all_dates.sql",
    "delinquency_par_secure_latest.sql",
    "portfolio_health_secure_all_dates.sql",
    "portfolio_health_secure_latest.sql",
    "repayment_behavior_secure_all_dates.sql",
    "repayment_behavior_secure_latest.sql",
]

DATASET_FILES_WITH_GUARANTEED_MULTI_OFFICE_DATA = [
    f for f in DATASET_FILES if f != "repayment_behavior_secure_latest.sql"
]


def _render(dataset_file: str, username: str | None) -> str:
    template_source = (DATASET_DIR / dataset_file).read_text(encoding="utf-8")
    env = jinja2.Environment()
    env.globals["current_username"] = lambda: username
    return env.from_string(template_source).render()


def _connect():
    return pg8000.native.Connection(
        host=os.getenv("WAREHOUSE_DB_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_DB_PORT", "5434")),
        database=os.getenv("WAREHOUSE_DB_NAME", "analytics"),
        user=os.getenv("WAREHOUSE_READER_USER", "analytics_reader"),
        password=os.getenv("WAREHOUSE_READER_PASSWORD", "analytics_reader_dev_only"),
    )


@pytest.fixture(scope="module")
def conn():
    connection = _connect()
    yield connection
    connection.close()


def _office_ids(rows: list[list], office_id_index: int) -> set:
    return {row[office_id_index] for row in rows}


def _run(conn, dataset_file: str, username: str | None) -> tuple[list[str], list[list]]:
    sql = _render(dataset_file, username)
    rows = conn.run(sql)
    columns = [col["name"] for col in conn.columns]
    return columns, rows


@pytest.mark.parametrize("dataset_file", DATASET_FILES_WITH_GUARANTEED_MULTI_OFFICE_DATA)
def test_admin_sees_all_offices(conn, dataset_file):
    columns, rows = _run(conn, dataset_file, "admin")
    office_idx = columns.index("office_id")

    assert len(rows) > 0
    assert len(_office_ids(rows, office_idx)) > 1


@pytest.mark.parametrize("dataset_file", DATASET_FILES)
def test_north_manager_sees_only_north_branch(conn, dataset_file):
    columns, rows = _run(conn, dataset_file, "north_manager")
    office_idx = columns.index("office_id")

    assert len(rows) > 0
    assert _office_ids(rows, office_idx) == {101}


@pytest.mark.parametrize("dataset_file", DATASET_FILES_WITH_GUARANTEED_MULTI_OFFICE_DATA)
def test_south_manager_sees_only_south_branch(conn, dataset_file):
    columns, rows = _run(conn, dataset_file, "south_manager")
    office_idx = columns.index("office_id")

    assert len(rows) > 0
    assert _office_ids(rows, office_idx) == {102}


@pytest.mark.parametrize("dataset_file", DATASET_FILES)
def test_north_and_south_results_are_disjoint(conn, dataset_file):
    north_columns, north_rows = _run(conn, dataset_file, "north_manager")
    south_columns, south_rows = _run(conn, dataset_file, "south_manager")

    pk_idx_north = north_columns.index("office_id")
    pk_idx_south = south_columns.index("office_id")

    north_offices = _office_ids(north_rows, pk_idx_north)
    south_offices = _office_ids(south_rows, pk_idx_south)

    assert north_offices.isdisjoint(south_offices)


@pytest.mark.parametrize("dataset_file", DATASET_FILES)
def test_unknown_username_gets_zero_rows(conn, dataset_file):
    _, rows = _run(conn, dataset_file, "nonexistent_user_xyz")

    assert len(rows) == 0


@pytest.mark.parametrize("dataset_file", DATASET_FILES)
def test_empty_current_username_fails_closed_not_open_to_admin(conn, dataset_file):
    _, rows = _run(conn, dataset_file, "")

    assert len(rows) == 0


@pytest.mark.parametrize("dataset_file", DATASET_FILES)
def test_none_current_username_fails_closed_not_open_to_admin(conn, dataset_file):
    _, rows = _run(conn, dataset_file, None)

    assert len(rows) == 0


@pytest.mark.parametrize("dataset_file", DATASET_FILES)
def test_username_with_embedded_quote_does_not_break_out_of_the_predicate(conn, dataset_file):
    malicious_username = "x' OR '1'='1"

    _, rows = _run(conn, dataset_file, malicious_username)

    assert len(rows) == 0
