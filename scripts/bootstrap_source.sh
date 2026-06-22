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

SOURCE_DB_CONTAINER="${SOURCE_DB_CONTAINER:-fineract-db-1}"
SOURCE_DB_HOST="${SOURCE_DB_HOST:-localhost}"
SOURCE_DB_PORT="${SOURCE_DB_HOST_PORT:-5432}"

log()  { echo "[bootstrap-source] $*"; }
fail() { echo "[bootstrap-source] ERROR: $*" >&2; exit 1; }

run_sql() {
    local sql="$1"
    docker exec -i "${SOURCE_DB_CONTAINER}" \
        env PGPASSWORD="${SOURCE_BOOTSTRAP_PASSWORD}" \
        psql -v ON_ERROR_STOP=1 \
             -U "${SOURCE_BOOTSTRAP_USER}" \
             -d "${SOURCE_DB_NAME}" \
             -c "${sql}"
}

run_sql_stdin() {
    docker exec -i "${SOURCE_DB_CONTAINER}" \
        env PGPASSWORD="${SOURCE_BOOTSTRAP_PASSWORD}" \
        psql -v ON_ERROR_STOP=1 \
             -U "${SOURCE_BOOTSTRAP_USER}" \
             -d "${SOURCE_DB_NAME}"
}

log "Checking connection to '${SOURCE_DB_CONTAINER}'..."
docker exec "${SOURCE_DB_CONTAINER}" \
    env PGPASSWORD="${SOURCE_BOOTSTRAP_PASSWORD}" \
    pg_isready -U "${SOURCE_BOOTSTRAP_USER}" -d postgres \
    || fail "Cannot reach ${SOURCE_DB_CONTAINER}. Is it running?"
log "Connection OK"

log "Creating compatibility views in schema '${SOURCE_DB_SCHEMA}'..."

run_sql_stdin <<SQL
CREATE SCHEMA IF NOT EXISTS ${SOURCE_DB_SCHEMA};

-- m_office: no created_on_utc in real Fineract schema, use epoch fallback
CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_office AS
SELECT
    id, parent_id, hierarchy, external_id, name, opening_date,
    '1970-01-01 00:00:00+00'::timestamptz AS created_on_utc,
    '1970-01-01 00:00:00+00'::timestamptz AS last_modified_on_utc
FROM public.m_office;

-- m_currency: no audit columns in real Fineract schema
CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_currency AS
SELECT
    id, code, decimal_places, currency_multiplesof, display_symbol,
    name, internationalized_name_code,
    '1970-01-01 00:00:00+00'::timestamptz AS created_on_utc,
    '1970-01-01 00:00:00+00'::timestamptz AS last_modified_on_utc
FROM public.m_currency;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_client AS
SELECT
    id, account_no, external_id, status_enum, activation_date,
    office_joining_date, office_id, staff_id, gender_cv_id,
    date_of_birth, legal_form_enum, submittedon_date, updated_on,
    created_on_utc, last_modified_on_utc
FROM public.m_client;

-- m_product_loan: no created_on_utc in real Fineract schema
CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_product_loan AS
SELECT
    id, short_name, currency_code, currency_digits, currency_multiplesof,
    principal_amount, min_principal_amount, max_principal_amount,
    arrearstolerance_amount, name, description,
    nominal_interest_rate_per_period, annual_nominal_interest_rate,
    repay_every, repayment_period_frequency_enum, number_of_repayments,
    overdue_days_for_npa, start_date, close_date,
    '1970-01-01 00:00:00+00'::timestamptz AS created_on_utc,
    '1970-01-01 00:00:00+00'::timestamptz AS last_modified_on_utc
FROM public.m_product_loan;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_loan AS
SELECT
    id, account_no, external_id, client_id, product_id,
    loan_status_id, loan_type_enum, currency_code, currency_digits,
    currency_multiplesof, principal_amount_proposed, principal_amount,
    approved_principal, net_disbursal_amount,
    annual_nominal_interest_rate, nominal_interest_rate_per_period,
    interest_method_enum, interest_calculated_in_period_enum,
    term_frequency, term_period_frequency_enum, repay_every,
    repayment_period_frequency_enum, number_of_repayments,
    amortization_method_enum, submittedon_date, approvedon_date,
    expected_disbursedon_date, expected_firstrepaymenton_date,
    disbursedon_date, expected_maturedon_date, maturedon_date,
    principal_disbursed_derived, principal_repaid_derived,
    principal_writtenoff_derived, principal_outstanding_derived,
    interest_charged_derived, interest_repaid_derived,
    interest_writtenoff_derived, interest_outstanding_derived,
    fee_charges_outstanding_derived, penalty_charges_outstanding_derived,
    total_expected_repayment_derived, total_repayment_derived,
    total_writtenoff_derived, total_outstanding_derived,
    loan_counter, is_npa,
    created_on_utc, last_modified_on_utc
FROM public.m_loan;

-- m_loan_transaction: real schema has no submitted_on_date, use transaction_date
CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_loan_transaction AS
SELECT
    id, loan_id, office_id, is_reversed, transaction_type_enum,
    transaction_date, amount, principal_portion_derived,
    interest_portion_derived, fee_charges_portion_derived,
    penalty_charges_portion_derived, outstanding_loan_balance_derived,
    transaction_date AS submitted_on_date,
    created_on_utc, last_modified_on_utc
FROM public.m_loan_transaction;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_delinquency_range AS
SELECT id, classification, min_age_days, max_age_days,
       created_on_utc, last_modified_on_utc
FROM public.m_delinquency_range;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_delinquency_bucket AS
SELECT id, name, created_on_utc, last_modified_on_utc
FROM public.m_delinquency_bucket;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_delinquency_bucket_mappings AS
SELECT id, delinquency_range_id, delinquency_bucket_id,
       created_on_utc, last_modified_on_utc
FROM public.m_delinquency_bucket_mappings;

CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.m_loan_delinquency_tag_history AS
SELECT id, delinquency_range_id, loan_id, addedon_date, liftedon_date,
       created_on_utc, last_modified_on_utc
FROM public.m_loan_delinquency_tag_history;

-- batch_job_execution: real schema uses timestamp without time zone
CREATE OR REPLACE VIEW ${SOURCE_DB_SCHEMA}.batch_job_execution AS
SELECT
    job_execution_id,
    status,
    start_time   ::timestamptz AS start_time,
    end_time     ::timestamptz AS end_time,
    exit_code,
    exit_message,
    create_time  ::timestamptz AS created_on_utc,
    last_updated ::timestamptz AS last_modified_on_utc
FROM public.batch_job_execution;
SQL

log "Compatibility views created in schema '${SOURCE_DB_SCHEMA}'"

log "Creating replica user if not exists..."
run_sql "DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${SOURCE_REPLICA_USER}') THEN
        EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', '${SOURCE_REPLICA_USER}', '${SOURCE_REPLICA_PASSWORD}');
    END IF;
END
\$\$;"

log "Granting read access to '${SOURCE_REPLICA_USER}'..."
run_sql_stdin <<SQL
GRANT USAGE ON SCHEMA public TO ${SOURCE_REPLICA_USER};
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${SOURCE_REPLICA_USER};
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ${SOURCE_REPLICA_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO ${SOURCE_REPLICA_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO ${SOURCE_REPLICA_USER};
GRANT USAGE  ON SCHEMA ${SOURCE_DB_SCHEMA} TO ${SOURCE_REPLICA_USER};
GRANT SELECT ON ALL TABLES IN SCHEMA ${SOURCE_DB_SCHEMA} TO ${SOURCE_REPLICA_USER};
SQL

log "Read access granted to '${SOURCE_REPLICA_USER}'"
log "=== Source bootstrap complete. You can now run the pipeline. ==="
log "    bash scripts/run_pipeline.sh backfill"
