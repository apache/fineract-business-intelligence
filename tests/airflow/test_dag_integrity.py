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

import pytest

if os.getenv("FBI_REQUIRE_AIRFLOW", "").lower() != "true":
    pytest.importorskip("airflow")

from airflow.models.dagbag import DagBag  # noqa: E402

DAG_FOLDER = Path(__file__).resolve().parents[2] / "airflow" / "dags"

EXPECTED = {
    "fbi_connector_reconciler",
    "fbi_freshness_monitor",
    "fbi_maintenance",
    "fbi_python_ingestion",
    "fbi_python_delete_reconcile",
    "fbi_dbt_build",
    "fbi_dbt_full_refresh_presentations",
    "fbi_source_onboarding",
    "fbi_ingestion_feature_flag",
    "fbi_source_reconciliation",
}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_all_expected_dags_present(dagbag):
    assert set(dagbag.dag_ids) >= EXPECTED


def test_dags_are_tagged_owned_and_not_catching_up(dagbag):
    for dag_id in EXPECTED:
        dag = dagbag.dags[dag_id]
        assert "fbi" in dag.tags, dag_id
        assert dag.catchup is False, dag_id
        assert all(t.owner == "fbi-platform" for t in dag.tasks), dag_id


def test_source_facing_work_is_throttled_by_pools(dagbag):
    ingest = dagbag.dags["fbi_python_ingestion"].get_task("ingest_shard")
    assert ingest.pool == "fbi_source_extract"
    recon = dagbag.dags["fbi_source_reconciliation"].get_task("reconcile_shard")
    assert recon.pool == "fbi_source_reconcile"
    assert dagbag.dags["fbi_dbt_build"].get_task("dbt_build").pool == "fbi_dbt"


def test_operator_workflows_are_manual_and_parameterised(dagbag):
    flag = dagbag.dags["fbi_ingestion_feature_flag"]
    assert flag.schedule is None or getattr(flag, "timetable", None).summary in ("None", None, "")
    assert {"source_id", "action", "target_engine", "requested_by", "approved_by", "reason"} <= set(flag.params)
    onboarding = dagbag.dags["fbi_source_onboarding"]
    assert {"source_id", "engine", "secret_ref"} <= set(onboarding.params)


def test_reconciler_maps_over_shards(dagbag):
    dag = dagbag.dags["fbi_connector_reconciler"]
    assert {"plan_shards", "reconcile_shard"} <= set(dag.task_ids)
    assert dag.max_active_runs == 1
