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

from airflow.sdk import Param, dag, get_current_context, task

DEFAULT_ARGS = {"owner": "fbi-platform", "retries": 0}


def authenticated_actor(context, requested: str) -> str:
    user = getattr(context.get("dag_run"), "triggering_user_name", None)
    if user:
        return str(user)
    if os.getenv("FBI_REQUIRE_AUTHENTICATED_ACTOR", "false").lower() == "true":
        raise RuntimeError("No authenticated triggering user on this run; refusing to trust requested_by")
    return requested


ACTIONS = ["start_shadow", "cutover_gate", "cutover", "abort_shadow", "pause", "resume", "assign", "force_assign"]


@dag(
    dag_id="fbi_source_onboarding",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["fbi", "control-plane", "lifecycle"],
    params={
        "source_id": Param("", type="string", pattern="^[a-z0-9][a-z0-9_-]{0,62}$"),
        "tenant_id": Param("", type="string"),
        "display_name": Param("", type="string"),
        "region": Param("default", type="string"),
        "db_host": Param("", type="string"),
        "db_port": Param(5432, type="integer"),
        "db_name": Param("", type="string"),
        "db_user": Param("", type="string"),
        "secret_ref": Param("env:FBI_SRC_DEMO_PASSWORD", type="string", pattern="^(env|file):"),
        "compat_schema": Param("bi_connector_source", type="string"),
        "connect_cluster": Param("default", type="string"),
        "engine": Param("PYTHON", enum=["PYTHON", "KAFKA"]),
        "requested_by": Param("", type="string", minLength=1),
    },
    doc_md=__doc__,
)
def fbi_source_onboarding():
    @task
    def onboard() -> dict:
        from controlplane.tasks import onboard_source

        context = get_current_context()
        p = dict(context["params"])
        return onboard_source(p, actor=authenticated_actor(context, p["requested_by"]))

    @task
    def provision_connector(result: dict) -> dict:
        if result["engine"] != "KAFKA":
            return {"skipped": "python engine"}
        from controlplane.tasks import reconcile_connectors_shard

        return reconcile_connectors_shard(0, 1)

    provision_connector(onboard())


@dag(
    dag_id="fbi_ingestion_feature_flag",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fbi", "control-plane", "feature-flag"],
    params={
        "source_id": Param("", type="string", minLength=1),
        "tables": Param("all", type="string", description="'all' or comma-separated contract tables"),
        "action": Param("start_shadow", enum=ACTIONS),
        "target_engine": Param("KAFKA", enum=["KAFKA", "PYTHON", "NONE"]),
        "requested_by": Param("", type="string", minLength=1),
        "approved_by": Param("", type="string"),
        "reason": Param("", type="string", minLength=1),
        "settle_seconds": Param(900, type="integer", minimum=0),
        "max_discrepancies": Param(0, type="integer", minimum=0),
    },
    doc_md=__doc__,
)
def fbi_ingestion_feature_flag():
    @task
    def execute() -> dict:
        from controlplane import tasks

        context = get_current_context()
        p = context["params"]
        action = p["action"]
        actor = authenticated_actor(context, p["requested_by"])
        if action == "cutover_gate":
            return {"action": action, "gate": tasks.cutover_gate(
                p["source_id"], p["tables"], actor, int(p["settle_seconds"]), int(p["max_discrepancies"]))}
        engine = p["target_engine"] if action in ("start_shadow", "assign", "force_assign") else None
        changes = tasks.apply_ownership_action(
            p["source_id"], p["tables"], action, actor, p["reason"],
            engine=engine, approved_by=p["approved_by"] or None,
        )
        return {"action": action, "engine": engine, "changes": changes}

    @task
    def follow_up(result: dict) -> dict:
        from controlplane import tasks

        p = get_current_context()["params"]
        out = {"connectors": tasks.reconcile_connectors_shard(0, 1)}
        if result["action"] == "start_shadow" and result.get("engine") == "PYTHON":
            out["python_shadow_backfill"] = tasks.python_ingest("backfill", source_ids=[p["source_id"]])
        return out

    follow_up(execute())


@dag(
    dag_id="fbi_source_reconciliation",
    schedule="0 4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "fbi-platform", "retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["fbi", "data-quality"],
    doc_md=__doc__,
)
def fbi_source_reconciliation():
    @task
    def plan_shards() -> list[dict]:
        from controlplane.tasks import shard_plan

        return shard_plan()

    @task(pool="fbi_source_reconcile", execution_timeout=timedelta(hours=4))
    def reconcile_shard(shard: dict) -> dict:
        from controlplane.tasks import source_reconcile_shard

        return source_reconcile_shard(shard["index"], shard["count"])

    reconcile_shard.expand(shard=plan_shards())


fbi_source_onboarding()
fbi_ingestion_feature_flag()
fbi_source_reconciliation()
