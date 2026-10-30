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
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"
SOURCES="${SOURCES:-100}"
KEYS="${KEYS:-5000}"
PREFIX="${PREFIX:-loadsrc}"
WH="${WH_CONTAINER:-fineract-bi-warehouse}"

loadgen() { docker compose --profile loadtest run --rm -T loadgen "$@"; }

register() {
    WAREHOUSE_DB_HOST=localhost WAREHOUSE_DB_PORT=5434 \
    WAREHOUSE_DB_USER="${WAREHOUSE_LOADER_USER:-analytics_loader}" \
    WAREHOUSE_DB_PASSWORD="${WAREHOUSE_LOADER_PASSWORD:-analytics_loader_dev_only}" LOG_LEVEL=WARNING \
    python -m controlplane.cli --actor loadtest register-synthetic --count "${SOURCES}" --prefix "${PREFIX}" --engine KAFKA >/dev/null
    sleep 6
}

verify() {
    loadgen verify --ground-truth="/results/$1-truth.json" --report="/results/$1-report.json" --timeout-sec=600
    python -c "import json; r=json.load(open('loadtest/results/$1-report.json')); \
print('$1', 'PASSED' if r['passed'] else 'FAILED', {k: v for k, v in r.items() if k != 'end_to_end_latency_seconds'})"
}

produce() {
    local name="$1"; shift
    loadgen produce --sources="${SOURCES}" --keys="${KEYS}" --source-prefix="${PREFIX}" \
        --ground-truth="/results/${name}-truth.json" "$@"
}

case "${1:-}" in
    1) register; produce stage1 --events=10000 --rate=2000; verify stage1 ;;
    2) register; produce stage2 --events=1000000 --rate=8000 --delete-ratio=0.03; verify stage2 ;;
    3) register; produce stage3 --events=1020000 --rate=3500 --burst-rate=10000 --burst-start-sec=60 \
           --burst-duration-sec=60; verify stage3 ;;
    4) register
       docker compose --profile cdc stop sink
       produce stage4 --events=1000000 --rate=0
       start=$(date +%s); docker compose --profile cdc start sink
       verify stage4; echo "drain wall-clock incl. JVM start: $(( $(date +%s) - start ))s" ;;
    5) register
       produce stage5 --events=600000 --rate=5000 & pid=$!
       sleep 20; docker compose --profile cdc kill -s KILL sink
       sleep 10; docker compose --profile cdc start sink
       sleep 30; docker stop "${WH}"; sleep 30; docker start "${WH}"
       wait "${pid}"
       until [ "$(docker inspect -f '{{.State.Health.Status}}' "${WH}")" = healthy ]; do sleep 2; done
       verify stage5 ;;
    cleanup)
       docker exec -i "${WH}" psql -v ON_ERROR_STOP=1 -U "${WAREHOUSE_ADMIN_USER:-analytics_admin}" -d analytics <<SQL
DO \$\$ DECLARE t text; BEGIN
  FOR t IN SELECT raw_table FROM meta.table_contract LOOP
    EXECUTE format('DELETE FROM raw.%I WHERE source_id LIKE %L', t, '${PREFIX}-%');
    EXECUTE format('DELETE FROM raw_shadow.%I WHERE source_id LIKE %L', t, '${PREFIX}-%');
  END LOOP; END \$\$;
DELETE FROM meta.entity_change_log WHERE source_id LIKE '${PREFIX}-%';
DELETE FROM meta.ingestion_progress WHERE source_id LIKE '${PREFIX}-%';
DELETE FROM meta.source_registry WHERE source_id LIKE '${PREFIX}-%';
SQL
       ;;
    *) sed -n '17,29p' "$0"; exit 2 ;;
esac
