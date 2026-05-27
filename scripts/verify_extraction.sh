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

set -u

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

ensure_docker_prerequisites
load_environment

PASS=0
FAIL=0

check() {
  local label="$1"
  shift
  local out
  out=$("$@" 2>&1)
  local rc=$?
  if [[ $rc -eq 0 && -n "$out" ]]; then
    echo "  ✅ ${label}"
    PASS=$((PASS + 1))
  else
    echo "  ❌ ${label}"
    echo "     Output: ${out}" >&2
    FAIL=$((FAIL + 1))
  fi
}

echo "=== Extraction Verification ==="
echo ""

echo "--- Service Health ---"
check "Fineract DB is healthy" \
  docker compose -f "${COMPOSE_FILE}" exec -T fineract-db pg_isready -U "${SOURCE_BOOTSTRAP_USER}" -d postgres

check "Warehouse is healthy" \
  docker compose -f "${COMPOSE_FILE}" exec -T warehouse pg_isready -U "${WAREHOUSE_ADMIN_USER}" -d "${WAREHOUSE_DB_NAME}"

echo ""
echo "--- Warehouse Schemas ---"
for schema in raw staging intermediate analytics meta; do
  check "Schema '${schema}' exists" \
    docker compose -f "${COMPOSE_FILE}" exec -T warehouse psql -U "${WAREHOUSE_ADMIN_USER}" -d "${WAREHOUSE_DB_NAME}" -tAc \
      "SELECT 1 FROM information_schema.schemata WHERE schema_name = '${schema}'"
done

echo ""
echo "--- Raw Layer Data ---"
RAW_TABLES=(raw_m_loan raw_m_client raw_m_office raw_m_product_loan raw_m_loan_transaction raw_m_loan_delinquency_tag_history raw_m_currency raw_m_delinquency_range raw_m_delinquency_bucket raw_m_delinquency_bucket_mappings raw_batch_job_execution)
for table in "${RAW_TABLES[@]}"; do
  count=$(docker compose -f "${COMPOSE_FILE}" exec -T warehouse psql -U "${WAREHOUSE_LOADER_USER}" -d "${WAREHOUSE_DB_NAME}" -tAc \
    "SELECT COUNT(*) FROM raw.${table}" 2>/dev/null)
  echo "  📊 raw.${table}: ${count:-0} rows"
done

echo ""
echo "--- Pipeline State ---"
check "At least one successful pipeline run" \
  docker compose -f "${COMPOSE_FILE}" exec -T warehouse psql -U "${WAREHOUSE_LOADER_USER}" -d "${WAREHOUSE_DB_NAME}" -tAc \
    "SELECT 1 FROM meta.pipeline_state WHERE status = 'success' LIMIT 1"

check "Watermarks are populated" \
  docker compose -f "${COMPOSE_FILE}" exec -T warehouse psql -U "${WAREHOUSE_LOADER_USER}" -d "${WAREHOUSE_DB_NAME}" -tAc \
    "SELECT 1 FROM meta.watermarks LIMIT 1"

echo ""
echo "=== Results: ${PASS} passed, ${FAIL} failed ==="
if [[ "${FAIL}" -gt 0 ]]; then
  exit 1
fi
