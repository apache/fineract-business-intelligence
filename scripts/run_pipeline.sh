#!/usr/bin/env bash
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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

ensure_docker_prerequisites
load_environment

PIPELINE_MODE="${PIPELINE_MODE:-${1:-incremental}}"
LOOP_MODE="false"
if [[ "${PIPELINE_MODE}" == "--loop" ]]; then
    PIPELINE_MODE="${PIPELINE_MODE_ENV:-${PIPELINE_MODE_DEFAULT:-incremental}}"
    LOOP_MODE="true"
fi

PIPELINE_INTERVAL="${PIPELINE_INTERVAL_SECONDS:-3600}"
DBT_FULL_REFRESH="${DBT_FULL_REFRESH:-false}"

log()  { echo "[pipeline] $(date -u '+%Y-%m-%dT%H:%M:%SZ') $*"; }
fail() { echo "[pipeline] ERROR: $*" >&2; exit 1; }

run_once() {
    local mode="$1"
    local start
    start="$(date +%s)"

    log "=== Starting pipeline run (mode=${mode}) ==="

    log "Step 1/3 — Extractor (${mode})"
    if ! docker compose -f "${COMPOSE_FILE}" exec -T extractor \
            python -m extractor.cli "${mode}"; then
        fail "Extractor failed. dbt and Superset refresh skipped."
    fi
    log "Step 1/3 — Extractor OK"

    local dbt_args="dbt build"
    if [[ "${DBT_FULL_REFRESH}" == "true" || "${mode}" == "backfill" ]]; then
        dbt_args="dbt build --full-refresh"
    fi

    log "Step 2/3 — dbt (${dbt_args})"
    if ! docker compose -f "${COMPOSE_FILE}" exec -T dbt ${dbt_args}; then
        fail "dbt build failed. Superset refresh skipped to avoid showing broken data."
    fi
    log "Step 2/3 — dbt OK"

    log "Step 3/3 — Superset asset refresh"
    if ! docker compose -f "${COMPOSE_FILE}" exec -T superset \
            bash /workspace/docker/superset/refresh_superset_assets.sh; then
        log "WARNING: Superset refresh failed — dashboards may show stale data."
    else
        log "Step 3/3 — Superset OK"
    fi

    local elapsed=$(( $(date +%s) - start ))
    log "=== Pipeline run complete in ${elapsed}s ==="
}

if [[ "${LOOP_MODE}" == "true" ]]; then
    log "Starting pipeline loop (mode=${PIPELINE_MODE}, interval=${PIPELINE_INTERVAL}s)"
    while true; do
        run_once "${PIPELINE_MODE}" || log "Run failed — will retry after ${PIPELINE_INTERVAL}s"
        log "Sleeping ${PIPELINE_INTERVAL}s until next run..."
        sleep "${PIPELINE_INTERVAL}"
    done
else
    run_once "${PIPELINE_MODE}"
fi
