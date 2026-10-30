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
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag, task
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

RAW_ASSET = Asset("fbi://warehouse/raw")
DBT_ENV = {
    "DBT_TARGET_PATH": "/tmp/dbt-target",
    "DBT_LOG_PATH": "/tmp/dbt-logs",
    "DBT_PACKAGES_INSTALL_PATH": "/tmp/dbt-packages",
}
MARTS_ASSET = Asset("fbi://warehouse/marts")
DEFAULT_ARGS = {"owner": "fbi-platform", "retries": 2, "retry_delay": timedelta(minutes=2)}


def _python_ingestion_dag(dag_id: str, mode: str, schedule):
    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["fbi", "ingestion", "python-engine"],
        doc_md=__doc__,
    )
    def _dag():
        @task
        def plan_shards() -> list[dict]:
            from controlplane.tasks import shard_plan

            return shard_plan()

        @task(pool="fbi_source_extract", outlets=[RAW_ASSET], execution_timeout=timedelta(hours=6))
        def ingest_shard(shard: dict) -> dict:
            from controlplane.tasks import python_ingest

            return python_ingest(mode, shard["index"], shard["count"])

        ingest_shard.expand(shard=plan_shards())

    return _dag()


fbi_python_ingestion = _python_ingestion_dag(
    "fbi_python_ingestion",
    "incremental",
    CronTriggerTimetable(os.getenv("FBI_PYTHON_INGESTION_CRON", "0 * * * *"), timezone="UTC"),
)
fbi_python_delete_reconcile = _python_ingestion_dag(
    "fbi_python_delete_reconcile",
    "reconcile",
    CronTriggerTimetable(os.getenv("FBI_PYTHON_RECONCILE_CRON", "15 1 * * *"), timezone="UTC"),
)


@dag(
    dag_id="fbi_dbt_build",
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable(os.getenv("FBI_DBT_CRON", "*/15 * * * *"), timezone="UTC"),
        assets=RAW_ASSET,
    ),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 1},
    tags=["fbi", "dbt"],
)
def fbi_dbt_build():
    dbt = os.getenv("FBI_DBT_BIN", "/opt/dbt-venv/bin/dbt")
    project = os.getenv("FBI_DBT_PROJECT_DIR", "/opt/fbi/dbt")
    common = f"--project-dir {project} --profiles-dir {project}"
    deps = BashOperator(
        task_id="dbt_deps", bash_command=f"{dbt} deps {common}", pool="fbi_dbt", env=DBT_ENV, append_env=True
    )
    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{dbt} build {common} --threads {os.getenv('DBT_THREADS', '4')}",
        pool="fbi_dbt",
        env=DBT_ENV,
        append_env=True,
        outlets=[MARTS_ASSET],
        execution_timeout=timedelta(hours=3),
    )
    deps >> build


@dag(
    dag_id="fbi_dbt_full_refresh_presentations",
    schedule=os.getenv("FBI_DBT_FULL_REFRESH_CRON", "0 3 * * 0"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fbi", "dbt"],
    doc_md="Weekly rebuild of presentation marts so renamed offices/products propagate to history.",
)
def fbi_dbt_full_refresh_presentations():
    dbt = os.getenv("FBI_DBT_BIN", "/opt/dbt-venv/bin/dbt")
    project = os.getenv("FBI_DBT_PROJECT_DIR", "/opt/fbi/dbt")
    common = f"--project-dir {project} --profiles-dir {project}"
    BashOperator(
        task_id="dbt_full_refresh_presentations",
        bash_command=f"{dbt} build {common} --full-refresh --select marts.presentations",
        pool="fbi_dbt",
        env=DBT_ENV,
        append_env=True,
        outlets=[MARTS_ASSET],
    )


fbi_dbt_build()
fbi_dbt_full_refresh_presentations()
