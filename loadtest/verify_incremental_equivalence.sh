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

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"
# shellcheck disable=SC1091
set -a; source .env; set +a

SRC_CONTAINER="${SRC_CONTAINER:-fineract-bi-fineract-db}"
WH_CONTAINER="${WH_CONTAINER:-fineract-bi-warehouse}"

src_sql() { docker exec -i -e PGPASSWORD="${SOURCE_BOOTSTRAP_PASSWORD}" "${SRC_CONTAINER}" \
    psql -v ON_ERROR_STOP=1 -q -At -U "${SOURCE_BOOTSTRAP_USER}" -d "${SOURCE_DB_NAME}" -c "$1"; }
wh_sql() { docker exec -i "${WH_CONTAINER}" psql -v ON_ERROR_STOP=1 -q -At -U "${WAREHOUSE_ADMIN_USER}" -d "${WAREHOUSE_DB_NAME}" -c "$1"; }

fingerprint() {
    wh_sql "
      select string_agg(t || '=' || h, ' ' order by t) from (
        select 'fact' as t, md5(coalesce(string_agg(j, '|' order by j), '')) as h
          from (select (to_jsonb(f) - 'dbt_loaded_at')::text as j from analytics.fact_loan_snapshot f) x
        union all
        select 'par', md5(coalesce(string_agg(j, '|' order by j), ''))
          from (select (to_jsonb(m) - 'dbt_loaded_at')::text as j from analytics.mart_delinquency_par m) x
        union all
        select 'health', md5(coalesce(string_agg(j, '|' order by j), ''))
          from (select (to_jsonb(m) - 'dbt_loaded_at')::text as j from analytics.mart_portfolio_health m) x
        union all
        select 'repay', md5(coalesce(string_agg(j, '|' order by j), ''))
          from (select (to_jsonb(m) - 'dbt_loaded_at')::text as j from analytics.mart_repayment_behavior m) x
      ) s"
}

run_python_engine() {
    SOURCE_DB_HOST=localhost SOURCE_DB_PORT=5433 SOURCE_DB_USER="${SOURCE_REPLICA_USER}" \
    SOURCE_DB_PASSWORD="${SOURCE_REPLICA_PASSWORD}" SOURCE_DB_SCHEMA="${SOURCE_DB_SCHEMA:-bi_connector_source}" \
    WAREHOUSE_DB_HOST=localhost WAREHOUSE_DB_PORT=5434 WAREHOUSE_DB_USER="${WAREHOUSE_LOADER_USER}" \
    WAREHOUSE_DB_PASSWORD="${WAREHOUSE_LOADER_PASSWORD}" COB_LOOKBACK_HOURS=999999 LOG_LEVEL=WARNING \
    python -m extractor.cli "$1" >/dev/null
}

echo "[equivalence] applying source changes"
src_sql "
BEGIN;
-- Backdated repayment 200 days ago on the oldest active loan (beyond any fixed lookback).
INSERT INTO m_loan_transaction (id, loan_id, office_id, is_reversed, transaction_type_enum, transaction_date, amount,
    principal_portion_derived, interest_portion_derived, submitted_on_date, created_by, last_modified_by,
    created_on_utc, last_modified_on_utc, outstanding_loan_balance_derived)
SELECT (SELECT max(id) + 1 FROM m_loan_transaction), l.id, 1, false, 2, CURRENT_DATE - 200, 1234.56, 1000, 234.56,
       CURRENT_DATE - 200, 1, 1, NOW(), NOW(), 777.77
FROM m_loan l WHERE l.disbursedon_date < CURRENT_DATE - 250 ORDER BY l.id LIMIT 1;
-- Reverse one repayment.
UPDATE m_loan_transaction SET is_reversed = true, last_modified_on_utc = NOW()
WHERE id = (SELECT min(id) FROM m_loan_transaction WHERE transaction_type_enum = 2 AND NOT is_reversed);
-- Hard-delete one repayment (detected by the delete reconciliation).
DELETE FROM m_loan_transaction WHERE id = (SELECT max(id) FROM m_loan_transaction WHERE transaction_type_enum = 2 AND id < (SELECT max(id) FROM m_loan_transaction));
-- Move a client to another office (client-level change -> all its loans recomputed).
UPDATE m_client SET office_id = CASE WHEN office_id = 101 THEN 102 ELSE 101 END, last_modified_on_utc = NOW()
WHERE id = (SELECT min(id) FROM m_client WHERE id >= 100);
-- Change a schedule installment's obligations date in the past (affects DPD history).
UPDATE m_loan_repayment_schedule SET obligations_met_on_date = duedate + 45, last_modified_on_utc = NOW(),
       lastmodified_date = NOW()
WHERE id = (SELECT min(id) FROM m_loan_repayment_schedule WHERE obligations_met_on_date IS NOT NULL);
COMMIT;"

echo "[equivalence] ingesting changes (incremental + delete reconciliation)"
run_python_engine incremental
run_python_engine reconcile
echo "[equivalence] changes logged: $(wh_sql "select count(*) from meta.entity_change_log where change_id > (select last_change_id from meta.dbt_change_cursor)")"

echo "[equivalence] incremental dbt build"
docker compose exec -T dbt dbt build --exclude "test_type:singular" -q >/dev/null
A="$(fingerprint)"
echo "[equivalence] full-refresh dbt build"
docker compose exec -T dbt dbt build --full-refresh --exclude "test_type:singular" -q >/dev/null
B="$(fingerprint)"
echo "incremental : ${A}"
echo "full refresh: ${B}"
if [[ "${A}" == "${B}" ]]; then
    echo "[equivalence] PASSED - incremental build equals full recomputation"
else
    echo "[equivalence] FAILED - incremental build diverged from full recomputation" >&2
    exit 1
fi
