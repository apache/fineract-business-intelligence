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

set -uo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}" || exit 1
if [[ -f .env ]]; then
    # shellcheck disable=SC1091
    set -a; source .env; set +a
fi

WH="${WH_CONTAINER:-fineract-bi-warehouse}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
GROUP="${SINK_GROUP_ID:-fbi-warehouse-sink}"

section() { printf '\n== %s ==\n' "$1"; }

sql() {
    docker exec -i "${WH}" psql -U "${WAREHOUSE_ADMIN_USER:-analytics_admin}" \
        -d "${WAREHOUSE_DB_NAME:-analytics}" -P pager=off -c "$1" 2>&1
}

kafka() {
    local tool="$1"; shift
    docker compose --profile cdc exec -T kafka "/opt/kafka/bin/${tool}" "$@" 2>/dev/null
}

show() {
    section "Containers"
    docker compose --profile cdc --profile airflow ps --format 'table {{.Service}}\t{{.State}}\t{{.Status}}' 2>&1 \
        | head -25

    section "Debezium connectors (one per CDC source)"
    if out="$(curl -sf "${CONNECT_URL}/connectors?expand=status")"; then
        printf '%s' "${out}" | python -c "
import json, sys
data = json.load(sys.stdin)
if not data:
    print('  none registered (sources owned by PYTHON need no connector)')
for name, v in sorted(data.items()):
    tasks = [t['state'] for t in v['status']['tasks']]
    print(f\"  {name}: connector={v['status']['connector']['state']} tasks={tasks}\")
"
    else
        echo "  Kafka Connect not reachable at ${CONNECT_URL} (profile cdc not running?)"
    fi

    section "Kafka consumer lag (events waiting for the sink; 0 = caught up)"
    kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "${GROUP}" \
        | awk 'NR>2 && $6 ~ /^[0-9]+$/ {lag+=$6; parts++} END {printf "  partitions=%d total_lag=%d\n", parts, lag}'

    section "Dead letters (events the sink could not apply; must stay 0)"
    kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic fineract.cdc.dlt \
        | awk -F: '{s+=$3} END {printf "  dead-lettered events=%d\n", s+0}'

    section "Sink counters (metrics endpoint inside the sink container)"
    docker compose --profile cdc exec -T sink bash -c \
        "exec 3<>/dev/tcp/127.0.0.1/9404; printf 'GET /metrics HTTP/1.0\r\n\r\n' >&3; cat <&3" 2>/dev/null \
        | grep -E '^fbi_sink_(batches_total|events_total|rows_written_total|dead_lettered_total|db_errors_total|fenced_retries_total|ownership_routes)' \
        | sed 's/^/  /' | head -24

    section "Who owns what: engine per source (the feature flag)"
    sql "SELECT region, engine, shadow_engine, cutover_state, sources, source_tables FROM meta.v_ownership_overview ORDER BY 1, 2;"

    section "Freshness: last write per source (engine, rows, seconds since last apply)"
    sql "SELECT source_id, engine, SUM(rows_applied) AS rows, MAX(seconds_since_last_apply) AS secs_since_last_apply FROM meta.v_table_freshness WHERE target = 'prod' GROUP BY 1, 2 ORDER BY 4 DESC NULLS FIRST LIMIT 10;"

    section "Completeness audit, last 5 minutes (consumed = applied + skipped + dead-lettered)"
    sql "SELECT table_name, SUM(events_consumed) AS consumed, SUM(rows_applied) AS applied, SUM(rows_skipped) AS skipped, SUM(events_dead_lettered) AS dead FROM meta.ingestion_audit WHERE minute_bucket > now() - interval '5 minutes' GROUP BY 1 ORDER BY 2 DESC;"

    section "Raw rows in the warehouse"
    sql "SELECT source_id, COUNT(*) FILTER (WHERE NOT _is_deleted) AS live_transactions, COUNT(*) FILTER (WHERE _is_deleted) AS tombstones FROM raw.raw_m_loan_transaction GROUP BY 1 ORDER BY 1 LIMIT 15;"
}

if [[ "${1:-}" == "--watch" ]]; then
    while true; do clear; show; sleep 5; done
else
    show
fi
