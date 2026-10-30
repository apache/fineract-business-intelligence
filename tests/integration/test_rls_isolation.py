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


OTHER_SOURCE = "it-other-bank"
MART_FOR_DATASET = {
    "delinquency_par": ("analytics.mart_delinquency_par", "snapshot_date"),
    "portfolio_health": ("analytics.mart_portfolio_health", "snapshot_date"),
    "repayment_behavior": ("analytics.mart_repayment_behavior", "reporting_date"),
}


def _admin():
    return pg8000.native.Connection(
        host=os.getenv("WAREHOUSE_DB_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_DB_PORT", "5434")),
        database=os.getenv("WAREHOUSE_DB_NAME", "analytics"),
        user=os.getenv("WAREHOUSE_ADMIN_USER", "analytics_admin"),
        password=os.getenv("WAREHOUSE_ADMIN_PASSWORD", "analytics_admin_dev_only"),
    )


@pytest.fixture(scope="module")
def second_bank():
    admin = _admin()
    admin.run("INSERT INTO meta.source_registry (source_id, tenant_id, region, lifecycle_state) "
              "VALUES (:s, 'other-tenant', 'it-region-west', 'ACTIVE') ON CONFLICT (source_id) DO NOTHING",
              s=OTHER_SOURCE)
    for table, _ in MART_FOR_DATASET.values():
        admin.run(f"CREATE TEMP TABLE clone AS SELECT * FROM {table} WHERE source_id = 'default'")
        admin.run("UPDATE clone SET source_id = :s, tenant_id = 'other-tenant'", s=OTHER_SOURCE)
        admin.run(f"INSERT INTO {table} SELECT * FROM clone")
        admin.run("DROP TABLE clone")
    admin.run("INSERT INTO meta.user_office_mapping (username, role_name, region) "
              "VALUES ('west_region', 'REGION_MANAGER', 'it-region-west') ON CONFLICT DO NOTHING")
    yield OTHER_SOURCE
    for table, _ in MART_FOR_DATASET.values():
        admin.run(f"DELETE FROM {table} WHERE source_id = :s", s=OTHER_SOURCE)
    admin.run("DELETE FROM meta.user_office_mapping WHERE username = 'west_region'")
    admin.run("DELETE FROM meta.source_registry WHERE source_id = :s", s=OTHER_SOURCE)
    admin.close()


@pytest.mark.parametrize("dataset_file", DATASET_FILES)
def test_same_office_id_in_another_bank_never_leaks(conn, second_bank, dataset_file):
    columns, rows = _run(conn, dataset_file, "north_manager")
    sources = {row[columns.index("source_id")] for row in rows}
    assert second_bank not in sources
    assert sources <= {"default"}


@pytest.mark.parametrize("dataset_file", DATASET_FILES_WITH_GUARANTEED_MULTI_OFFICE_DATA)
def test_region_scope_sees_only_its_region(conn, second_bank, dataset_file):
    columns, rows = _run(conn, dataset_file, "west_region")
    sources = {row[columns.index("source_id")] for row in rows}
    assert sources == {second_bank}


@pytest.mark.parametrize("dataset_file", DATASET_FILES_WITH_GUARANTEED_MULTI_OFFICE_DATA)
def test_admin_sees_every_bank(conn, second_bank, dataset_file):
    columns, rows = _run(conn, dataset_file, "admin")
    sources = {row[columns.index("source_id")] for row in rows}
    assert {"default", second_bank} <= sources


def test_a_non_admin_mapping_without_any_scope_is_rejected_by_the_database():
    admin = _admin()
    try:
        for role, region in (("ANALYST", None), ("REGION_MANAGER", None), ("BRANCH_MANAGER", "west")):
            with pytest.raises(Exception, match="scope_is_qualified"):
                admin.run(
                    "INSERT INTO meta.user_office_mapping (username, role_name, region) VALUES (:u, :r, :g)",
                    u="unscoped_" + role.lower(), r=role, g=region,
                )
    finally:
        admin.run("DELETE FROM meta.user_office_mapping WHERE username LIKE 'unscoped_%'")
        admin.close()


def test_an_unscoped_user_sees_nothing_in_every_dataset(conn):
    for dataset_file in DATASET_FILES:
        for username in ("unscoped_analyst", "", None):
            _, rows = _run(conn, dataset_file, username)
            assert rows == [], f"{dataset_file} leaked rows to {username!r}"


@pytest.mark.parametrize("dataset_file", DATASET_FILES_WITH_GUARANTEED_MULTI_OFFICE_DATA)
def test_an_analyst_scoped_to_one_source_sees_every_office_of_that_source_only(conn, second_bank, dataset_file):
    admin = _admin()
    admin.run(
        "INSERT INTO meta.user_office_mapping (username, role_name, source_id) "
        "VALUES ('it_analyst', 'ANALYST', 'default') ON CONFLICT DO NOTHING"
    )
    try:
        columns, rows = _run(conn, dataset_file, "it_analyst")
        sources = {row[columns.index("source_id")] for row in rows}
        offices = {row[columns.index("office_id")] for row in rows}
        assert sources == {"default"} and len(offices) > 1
    finally:
        admin.run("DELETE FROM meta.user_office_mapping WHERE username = 'it_analyst'")
        admin.close()


@pytest.mark.parametrize("table", ["meta.source_registry", "meta.user_office_mapping", "meta.connector_state",
                                   "meta.ingestion_ownership", "meta.ingestion_ownership_audit",
                                   "meta.v_user_access_scope"])
def test_the_dashboard_reader_cannot_read_control_plane_tables(conn, table):
    with pytest.raises(Exception, match="permission denied"):
        conn.run(f"SELECT * FROM {table} LIMIT 1")


def test_row_level_security_goes_through_functions_that_answer_only_for_the_given_user(conn):
    assert conn.run("SELECT meta.is_admin_user('admin'), meta.is_admin_user('north_manager'), "
                    "meta.is_admin_user(''), meta.is_admin_user(NULL)") == [[True, False, False, False]]
    assert conn.run("SELECT source_id, office_id FROM meta.user_scope('north_manager')") == [["default", 101]]
    assert conn.run("SELECT * FROM meta.user_scope('admin')") == []
    assert conn.run("SELECT * FROM meta.user_scope(NULL)") == []
