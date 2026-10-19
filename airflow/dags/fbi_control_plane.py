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

from airflow.sdk import dag, task

DEFAULT_ARGS = {"owner": "fbi-platform", "retries": 2, "retry_delay": timedelta(minutes=1)}


@dag(
    dag_id="fbi_connector_reconciler",
    schedule=timedelta(minutes=int(os.getenv("FBI_RECONCILE_INTERVAL_MINUTES", "2"))),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fbi", "control-plane", "cdc"],
    doc_md=__doc__,
)
def fbi_connector_reconciler():
    @task(execution_timeout=timedelta(minutes=2))
    def plan_shards() -> list[dict]:
        from controlplane.tasks import shard_plan

        return shard_plan()

    @task(
        max_active_tis_per_dagrun=8,
        execution_timeout=timedelta(minutes=int(os.getenv("FBI_RECONCILE_TIMEOUT_MINUTES", "10"))),
    )
    def reconcile_shard(shard: dict) -> dict:
        from controlplane.tasks import reconcile_connectors_shard

        return reconcile_connectors_shard(shard["index"], shard["count"])

    reconcile_shard.expand(shard=plan_shards())


@dag(
    dag_id="fbi_freshness_monitor",
    schedule=timedelta(minutes=5),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 0},
    tags=["fbi", "control-plane", "slo"],
)
def fbi_freshness_monitor():
    @task
    def check_freshness() -> int:
        from controlplane.tasks import freshness_breaches

        breaches = freshness_breaches()
        if breaches:
            raise RuntimeError(f"{len(breaches)} owned (source, table) pairs breach the freshness SLO: {breaches[:10]}")
        return 0

    check_freshness()


@dag(
    dag_id="fbi_maintenance",
    schedule="30 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fbi", "control-plane"],
)
def fbi_maintenance():
    @task
    def purge() -> dict:
        from controlplane.tasks import maintenance

        return maintenance()

    purge()


fbi_connector_reconciler()
fbi_freshness_monitor()
fbi_maintenance()
