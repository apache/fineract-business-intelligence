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
set -a; source .env; set +a

CDC_PW="${FBI_SRC_DEMO_PASSWORD:-fbi_cdc_dev_only}"
SRC="${SOURCE_DB_CONTAINER:-fineract-bi-fineract-db}"
WH="${WH_CONTAINER:-fineract-bi-warehouse}"
SRC_ID="default"

step() { printf '\n\033[1m>>> %s\033[0m\n' "$*"; }
wh()   { docker exec -i "${WH}" psql -U "${WAREHOUSE_ADMIN_USER}" -d "${WAREHOUSE_DB_NAME}" -At -c "$1"; }
src()  { docker exec -i -e PGPASSWORD="${SOURCE_BOOTSTRAP_PASSWORD}" "${SRC}" \
             psql -v ON_ERROR_STOP=1 -q -U "${SOURCE_BOOTSTRAP_USER}" -d "${SOURCE_DB_NAME}" -At -c "$1"; }

ENVS=(-e SOURCE_DB_HOST="${SRC}" -e SOURCE_DB_PORT=5432 -e SOURCE_DB_NAME="${SOURCE_DB_NAME}"
      -e SOURCE_DB_USER=fbi_cdc -e FBI_SRC_DEFAULT_PASSWORD="${CDC_PW}" -e SOURCE_DB_SECRET_REF=env:FBI_SRC_DEMO_PASSWORD
      -e FBI_SRC_DEMO_PASSWORD="${CDC_PW}" -e SOURCE_DB_SCHEMA=bi_connector_source -e COB_LOOKBACK_HOURS=999999
      -e FBI_DEBEZIUM_PUBLICATION_MODE=disabled -e LOG_LEVEL=WARNING)
run() { docker compose --profile python run --rm -T --no-deps "${ENVS[@]}" extractor "$@"; }
cpc() { local actor="$1"; shift; run python -m controlplane.cli --actor "${actor}" "$@"; }

owners() { wh "SELECT engine || ' / shadow=' || coalesce(shadow_engine, '-') || ' / ' || cutover_state || ' (' || count(*) || ' tables)' FROM meta.ingestion_ownership WHERE source_id = '${SRC_ID}' GROUP BY engine, shadow_engine, cutover_state"; }

wait_for() {
    local waited=0
    until [[ "$(wh "$2")" == "t" ]]; do
        sleep 3; waited=$((waited + 3))
        [[ ${waited} -lt $3 ]] || { echo "TIMEOUT waiting for: $1"; exit 1; }
    done
    echo "ok: $1"
}

step "0. Prepare the demo source: replication role, read grants, publication"
src "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fbi_cdc')
     THEN CREATE ROLE fbi_cdc LOGIN REPLICATION PASSWORD '${CDC_PW}'; END IF; END \$\$;"
TABLES="$(python -c "import json; print(', '.join('public.' + t['table_name'] for t in json.load(open('contracts/fineract_tables.json'))['tables']))")"
src "GRANT CONNECT ON DATABASE ${SOURCE_DB_NAME} TO fbi_cdc;
     GRANT USAGE ON SCHEMA public, bi_connector_source TO fbi_cdc;
     GRANT SELECT ON ALL TABLES IN SCHEMA public, bi_connector_source TO fbi_cdc;
     DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'fbi_cdc')
     THEN CREATE PUBLICATION fbi_cdc FOR TABLE ${TABLES}; END IF; END \$\$;"

step "1. Register the source and give every table to the PYTHON engine, then ingest once"
cpc ops lifecycle --source-id "${SRC_ID}" --state ACTIVE >/dev/null 2>&1 || true
run python -m extractor.cli backfill --source-id "${SRC_ID}" >/dev/null
echo "owner now: $(owners)"

step "2. START SHADOW: Kafka CDC copies the source into raw_shadow while Python stays in charge"
cpc alice ownership-apply --source-id "${SRC_ID}" --action start_shadow --engine KAFKA \
    --reason "demo: migrate to CDC" >/dev/null
echo "owner now: $(owners)"
cpc alice reconcile-connectors | python -c "import json,sys; r=json.load(sys.stdin); print('connector actions:', [(a['kind'], a['name']) for a in r['actions']], 'errors:', r['errors'])"
wait_for "CDC snapshot reached the shadow (all loan transactions copied)" \
  "SELECT (SELECT count(*) FROM raw_shadow.raw_m_loan_transaction WHERE source_id='${SRC_ID}' AND NOT _is_deleted)
          = (SELECT count(*) FROM raw.raw_m_loan_transaction WHERE source_id='${SRC_ID}' AND NOT _is_deleted)
          AND (SELECT count(*) FROM raw_shadow.raw_m_loan_transaction WHERE source_id='${SRC_ID}') > 0" 180

step "3. CUTOVER GATE: compare shadow vs production for every table"
cpc alice cutover-gate --source-id "${SRC_ID}" --settle-seconds 0 \
  | python -c "import json,sys; r=json.load(sys.stdin); [print(f\"  {t:36s} passed={v['passed']} rows={v['compared_rows']} discrepancies={v['discrepancies']}\") for t,v in sorted(r.items())]"
echo "owner now: $(owners)"

step "4. Dual control: the requester cannot approve their own cutover (this must FAIL)"
if cpc alice ownership-apply --source-id "${SRC_ID}" --action cutover --approved-by alice --reason "self approval" >/dev/null 2>&1; then
    echo "UNEXPECTED: self-approval succeeded"; exit 1
else
    echo "rejected, as designed"
fi

step "5. CUTOVER approved by a second person: Kafka CDC now owns production"
cpc alice ownership-apply --source-id "${SRC_ID}" --action cutover --approved-by bob --reason "demo cutover" >/dev/null
echo "owner now: $(owners)"

step "6. A change in the source database flows through Debezium -> Kafka -> sink -> warehouse"
MARK="$(src "UPDATE m_loan SET last_modified_on_utc = clock_timestamp() WHERE id = (SELECT min(id) FROM m_loan) RETURNING last_modified_on_utc")"
echo "source row touched at ${MARK}"
wait_for "warehouse row carries that timestamp and was written by KAFKA" \
  "SELECT EXISTS (SELECT 1 FROM raw.raw_m_loan WHERE source_id='${SRC_ID}' AND _ingest_engine='KAFKA'
                  AND last_modified_on_utc >= '${MARK}'::timestamptz - interval '1 second')" 60

step "7. ROLLBACK to Python = the same workflow reversed (shadow with PYTHON, gate, cutover)"
cpc carol ownership-apply --source-id "${SRC_ID}" --action start_shadow --engine PYTHON --reason "demo rollback" >/dev/null
run python -m extractor.cli backfill --source-id "${SRC_ID}" >/dev/null
cpc carol cutover-gate --source-id "${SRC_ID}" --settle-seconds 0 \
  | python -c "import json,sys; r=json.load(sys.stdin); print('  tables passed:', sum(v['passed'] for v in r.values()), 'of', len(r))"
cpc carol ownership-apply --source-id "${SRC_ID}" --action cutover --approved-by dave --reason "demo rollback done" >/dev/null
cpc carol reconcile-connectors | python -c "import json,sys; r=json.load(sys.stdin); print('connector actions:', [(a['kind'], a['name']) for a in r['actions']])"
echo "owner now: $(owners)"

step "Audit trail of this demo (who did what, who approved)"
wh "SELECT to_char(created_at, 'HH24:MI:SS') || '  ' || action || '  actor=' || actor || coalesce('  approved_by=' || approved_by, '') || '  \"' || coalesce(reason, '') || '\"' FROM meta.ingestion_ownership_audit WHERE source_id='${SRC_ID}' AND table_name='m_loan' ORDER BY audit_id DESC LIMIT 8"
echo
echo "Done. Run scripts/pipeline_status.sh to see the final state."
