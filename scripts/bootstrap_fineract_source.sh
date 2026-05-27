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

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

FINERACT_PID_FILE="${RUNTIME_DIR}/fineract-devrun.pid"
FINERACT_LOG_FILE="${RUNTIME_DIR}/fineract-devrun.log"

start_fineract_backend() {
  if check_https_health "${FINERACT_HEALTH_URL}"; then
    echo "  ✅ Fineract backend is already running"
    return
  fi

  if [[ -f "${FINERACT_PID_FILE}" ]]; then
    local existing_pid
    existing_pid="$(cat "${FINERACT_PID_FILE}")"
    if kill -0 "${existing_pid}" >/dev/null 2>&1; then
      echo "  ⏳ Waiting for existing Fineract process (${existing_pid}) to be healthy..."
      wait_for_https_health "${FINERACT_HEALTH_URL}" 120
      echo "  ✅ Fineract backend is running"
      return
    fi
    rm -f "${FINERACT_PID_FILE}"
  fi

  echo "  🚀 Starting Fineract backend via PowerShell..."

  local repo_win log_win pid_win java_home_win
  repo_win="$(to_windows_path "${FINERACT_REPO_PATH}")"
  log_win="$(to_windows_path "${FINERACT_LOG_FILE}")"
  pid_win="$(to_windows_path "${FINERACT_PID_FILE}")"
  java_home_win="C:\\Program Files\\Java\\jdk-21"

  local ps1_file
  ps1_file="${RUNTIME_DIR}/start_fineract.ps1"

  cat > "${ps1_file}" <<POWERSHELL
Set-Location -LiteralPath '${repo_win}';
\$env:JAVA_HOME='${java_home_win}';
\$env:Path="\$env:JAVA_HOME\bin;\$env:Path";
\$env:JAVA_TOOL_OPTIONS='-Duser.timezone=UTC';
\$env:FINERACT_HIKARI_DRIVER_SOURCE_CLASS_NAME='org.postgresql.Driver';
\$env:FINERACT_HIKARI_JDBC_URL='jdbc:postgresql://localhost:${SOURCE_DB_HOST_PORT}/${SOURCE_TENANTS_DB_NAME}';
\$env:FINERACT_HIKARI_USERNAME='${SOURCE_APP_USER}';
\$env:FINERACT_HIKARI_PASSWORD='${SOURCE_APP_PASSWORD}';
\$env:FINERACT_DEFAULT_TENANTDB_HOSTNAME='localhost';
\$env:FINERACT_DEFAULT_TENANTDB_PORT='${SOURCE_DB_HOST_PORT}';
\$env:FINERACT_DEFAULT_TENANTDB_UID='${SOURCE_APP_USER}';
\$env:FINERACT_DEFAULT_TENANTDB_PWD='${SOURCE_APP_PASSWORD}';
\$env:FINERACT_DEFAULT_TENANTDB_NAME='${SOURCE_DB_NAME}';
\$env:FINERACT_DEFAULT_TENANTDB_IDENTIFIER='${FINERACT_TENANT_ID}';
\$env:FINERACT_DEFAULT_TENANTDB_TIMEZONE='Asia/Kolkata';
\$proc = Start-Process -FilePath 'cmd.exe' -WorkingDirectory '${repo_win}' -ArgumentList '/c gradlew.bat --no-daemon :fineract-provider:devRun > "${log_win}" 2>&1' -PassThru -WindowStyle Hidden;
Set-Content -LiteralPath '${pid_win}' -Value \$proc.Id;
Write-Host "Started Fineract with PID: \$proc.Id";
POWERSHELL

  powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$(to_windows_path "${ps1_file}")"

  echo "  ⏳ Waiting for Fineract backend to become healthy (up to 2 minutes)..."
  wait_for_https_health "${FINERACT_HEALTH_URL}" 120
  echo "  ✅ Fineract backend is running"
}

enable_business_date() {
  docker compose -f "${COMPOSE_FILE}" exec -T fineract-db \
    env PGPASSWORD="${SOURCE_BOOTSTRAP_PASSWORD}" \
    psql -h localhost -U "${SOURCE_BOOTSTRAP_USER}" -d "${SOURCE_DB_NAME}" \
    -c "UPDATE c_configuration SET enabled = TRUE WHERE name = 'enable-business-date';" >/dev/null
}

create_source_compatibility_views() {
  local sql_file="${RUNTIME_DIR}/create_views.sql"

  cat > "${sql_file}" <<EOF
CREATE SCHEMA IF NOT EXISTS ${SOURCE_DB_SCHEMA};

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_office AS
SELECT id, parent_id, hierarchy, external_id, name, opening_date,
    '1970-01-01 00:00:00+00'::timestamptz AS created_on_utc,
    '1970-01-01 00:00:00+00'::timestamptz AS last_modified_on_utc
FROM public.m_office;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_currency AS
SELECT id, code, decimal_places, currency_multiplesof, display_symbol, name, internationalized_name_code,
    '1970-01-01 00:00:00+00'::timestamptz AS created_on_utc,
    '1970-01-01 00:00:00+00'::timestamptz AS last_modified_on_utc
FROM public.m_currency;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_client AS
SELECT id, account_no, external_id, status_enum, activation_date, office_joining_date, office_id, staff_id, gender_cv_id, date_of_birth, legal_form_enum, submittedon_date, updated_on, created_on_utc, last_modified_on_utc
FROM public.m_client;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_product_loan AS
SELECT id, short_name, currency_code, currency_digits, currency_multiplesof, principal_amount, min_principal_amount, max_principal_amount, arrearstolerance_amount, name, description, nominal_interest_rate_per_period, annual_nominal_interest_rate, repay_every, repayment_period_frequency_enum, number_of_repayments, overdue_days_for_npa, start_date, close_date,
    '1970-01-01 00:00:00+00'::timestamptz AS created_on_utc,
    '1970-01-01 00:00:00+00'::timestamptz AS last_modified_on_utc
FROM public.m_product_loan;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_loan AS
SELECT id, account_no, external_id, client_id, product_id, loan_status_id, loan_type_enum, currency_code, currency_digits, currency_multiplesof, principal_amount_proposed, principal_amount, approved_principal, net_disbursal_amount, annual_nominal_interest_rate, nominal_interest_rate_per_period, interest_method_enum, interest_calculated_in_period_enum, term_frequency, term_period_frequency_enum, repay_every, repayment_period_frequency_enum, number_of_repayments, amortization_method_enum, submittedon_date, approvedon_date, expected_disbursedon_date, expected_firstrepaymenton_date, disbursedon_date, expected_maturedon_date, maturedon_date, principal_disbursed_derived, principal_repaid_derived, principal_writtenoff_derived, principal_outstanding_derived, interest_charged_derived, interest_repaid_derived, interest_writtenoff_derived, interest_outstanding_derived, fee_charges_outstanding_derived, penalty_charges_outstanding_derived, total_expected_repayment_derived, total_repayment_derived, total_writtenoff_derived, total_outstanding_derived, loan_counter, is_npa, created_on_utc, last_modified_on_utc
FROM public.m_loan;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_loan_transaction AS
SELECT id, loan_id, office_id, is_reversed, transaction_type_enum, transaction_date, amount, principal_portion_derived, interest_portion_derived, fee_charges_portion_derived, penalty_charges_portion_derived, outstanding_loan_balance_derived, submitted_on_date, created_on_utc, last_modified_on_utc
FROM public.m_loan_transaction;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_delinquency_range AS
SELECT id, classification, min_age_days, max_age_days, created_on_utc, last_modified_on_utc
FROM public.m_delinquency_range;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_delinquency_bucket AS
SELECT id, name, created_on_utc, last_modified_on_utc
FROM public.m_delinquency_bucket;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_delinquency_bucket_mappings AS
SELECT id, delinquency_range_id, delinquency_bucket_id, created_on_utc, last_modified_on_utc
FROM public.m_delinquency_bucket_mappings;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_loan_delinquency_tag_history AS
SELECT id, delinquency_range_id, loan_id, addedon_date, liftedon_date, created_on_utc, last_modified_on_utc
FROM public.m_loan_delinquency_tag_history;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.batch_job_execution AS
SELECT job_execution_id, status, start_time AT TIME ZONE 'UTC' AS start_time, end_time AT TIME ZONE 'UTC' AS end_time, exit_code, exit_message, create_time AT TIME ZONE 'UTC' AS created_on_utc, last_updated AT TIME ZONE 'UTC' AS last_modified_on_utc
FROM public.batch_job_execution;
EOF

  docker compose -f "${COMPOSE_FILE}" cp "${sql_file}" fineract-db:/tmp/create_views.sql
  docker compose -f "${COMPOSE_FILE}" exec -T fineract-db \
    env PGPASSWORD="${SOURCE_BOOTSTRAP_PASSWORD}" \
    psql -h localhost -U "${SOURCE_BOOTSTRAP_USER}" -d "${SOURCE_DB_NAME}" -f /tmp/create_views.sql >/dev/null
}

grant_reader_access() {
  local sql_file="${RUNTIME_DIR}/grant_reader.sql"

  cat > "${sql_file}" <<EOF
GRANT USAGE ON SCHEMA public TO ${SOURCE_REPLICA_USER};
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${SOURCE_REPLICA_USER};
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ${SOURCE_REPLICA_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO ${SOURCE_REPLICA_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO ${SOURCE_REPLICA_USER};
GRANT USAGE ON SCHEMA ${SOURCE_DB_SCHEMA} TO ${SOURCE_REPLICA_USER};
GRANT SELECT ON ALL TABLES IN SCHEMA ${SOURCE_DB_SCHEMA} TO ${SOURCE_REPLICA_USER};
EOF

  docker compose -f "${COMPOSE_FILE}" cp "${sql_file}" fineract-db:/tmp/grant_reader.sql
  docker compose -f "${COMPOSE_FILE}" exec -T fineract-db \
    env PGPASSWORD="${SOURCE_BOOTSTRAP_PASSWORD}" \
    psql -h localhost -U "${SOURCE_BOOTSTRAP_USER}" -d "${SOURCE_DB_NAME}" -f /tmp/grant_reader.sql >/dev/null
}

ensure_docker_prerequisites
load_environment
ensure_fineract_repo
require_command curl

echo "=== Bootstrapping Fineract Source ==="
echo ""

echo "Waiting for fineract-db to be healthy..."
wait_for_compose_service fineract-db 60 pg_isready -U "${SOURCE_BOOTSTRAP_USER}" -d postgres
echo "  ✅ fineract-db is healthy"

echo ""
echo "Starting Fineract backend..."
start_fineract_backend

echo ""
echo "Enabling business date..."
enable_business_date
echo "  ✅ Business date enabled"

echo ""
echo "Creating source compatibility views..."
create_source_compatibility_views
echo "  ✅ Compatibility views created"

echo ""
echo "Granting reader access..."
grant_reader_access
echo "  ✅ Reader access granted"

echo ""
echo "=== Fineract Source Bootstrap Complete ==="
