-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements. See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License. You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS raw_views;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS raw.raw_m_office (
    tenant_id TEXT NOT NULL,
    id BIGINT NOT NULL,
    parent_id BIGINT,
    hierarchy TEXT,
    external_id TEXT,
    name TEXT NOT NULL,
    opening_date DATE NOT NULL,
    created_on_utc TIMESTAMPTZ NOT NULL,
    last_modified_on_utc TIMESTAMPTZ NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS raw.raw_m_currency (
    tenant_id TEXT NOT NULL,
    id BIGINT NOT NULL,
    code TEXT NOT NULL,
    decimal_places SMALLINT NOT NULL,
    currency_multiplesof SMALLINT,
    display_symbol TEXT,
    name TEXT NOT NULL,
    internationalized_name_code TEXT NOT NULL,
    created_on_utc TIMESTAMPTZ NOT NULL,
    last_modified_on_utc TIMESTAMPTZ NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS raw.raw_m_client (
    tenant_id TEXT NOT NULL,
    id BIGINT NOT NULL,
    account_no TEXT NOT NULL,
    external_id TEXT,
    status_enum INTEGER NOT NULL,
    activation_date DATE,
    office_joining_date DATE,
    office_id BIGINT NOT NULL,
    staff_id BIGINT,
    gender_cv_id INTEGER,
    date_of_birth DATE,
    legal_form_enum INTEGER,
    submittedon_date DATE,
    updated_on DATE,
    created_on_utc TIMESTAMPTZ NOT NULL,
    last_modified_on_utc TIMESTAMPTZ NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS raw.raw_m_product_loan (
    tenant_id TEXT NOT NULL,
    id BIGINT NOT NULL,
    short_name TEXT NOT NULL,
    currency_code TEXT NOT NULL,
    currency_digits SMALLINT NOT NULL,
    currency_multiplesof SMALLINT,
    principal_amount NUMERIC(19, 6),
    min_principal_amount NUMERIC(19, 6),
    max_principal_amount NUMERIC(19, 6),
    arrearstolerance_amount NUMERIC(19, 6),
    name TEXT NOT NULL,
    description TEXT,
    nominal_interest_rate_per_period NUMERIC(19, 6),
    annual_nominal_interest_rate NUMERIC(19, 6),
    repay_every SMALLINT NOT NULL,
    repayment_period_frequency_enum SMALLINT NOT NULL,
    number_of_repayments SMALLINT NOT NULL,
    overdue_days_for_npa SMALLINT,
    start_date DATE,
    close_date DATE,
    created_on_utc TIMESTAMPTZ NOT NULL,
    last_modified_on_utc TIMESTAMPTZ NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS raw.raw_m_loan (
    tenant_id TEXT NOT NULL,
    id BIGINT NOT NULL,
    account_no TEXT NOT NULL,
    external_id TEXT,
    client_id BIGINT,
    product_id BIGINT,
    loan_status_id SMALLINT NOT NULL,
    loan_type_enum SMALLINT NOT NULL,
    currency_code TEXT NOT NULL,
    currency_digits SMALLINT NOT NULL,
    currency_multiplesof SMALLINT,
    principal_amount_proposed NUMERIC(19, 6) NOT NULL,
    principal_amount NUMERIC(19, 6) NOT NULL,
    approved_principal NUMERIC(19, 6) NOT NULL,
    net_disbursal_amount NUMERIC(19, 6) NOT NULL,
    annual_nominal_interest_rate NUMERIC(19, 6),
    nominal_interest_rate_per_period NUMERIC(19, 6),
    interest_method_enum SMALLINT NOT NULL,
    interest_calculated_in_period_enum SMALLINT NOT NULL,
    term_frequency SMALLINT NOT NULL,
    term_period_frequency_enum SMALLINT NOT NULL,
    repay_every SMALLINT NOT NULL,
    repayment_period_frequency_enum SMALLINT NOT NULL,
    number_of_repayments SMALLINT NOT NULL,
    amortization_method_enum SMALLINT NOT NULL,
    submittedon_date DATE,
    approvedon_date DATE,
    expected_disbursedon_date DATE,
    expected_firstrepaymenton_date DATE,
    disbursedon_date DATE,
    expected_maturedon_date DATE,
    maturedon_date DATE,
    principal_disbursed_derived NUMERIC(19, 6) NOT NULL,
    principal_repaid_derived NUMERIC(19, 6) NOT NULL,
    principal_writtenoff_derived NUMERIC(19, 6) NOT NULL,
    principal_outstanding_derived NUMERIC(19, 6) NOT NULL,
    interest_charged_derived NUMERIC(19, 6) NOT NULL,
    interest_repaid_derived NUMERIC(19, 6) NOT NULL,
    interest_writtenoff_derived NUMERIC(19, 6) NOT NULL,
    interest_outstanding_derived NUMERIC(19, 6) NOT NULL,
    fee_charges_outstanding_derived NUMERIC(19, 6) NOT NULL,
    penalty_charges_outstanding_derived NUMERIC(19, 6) NOT NULL,
    total_expected_repayment_derived NUMERIC(19, 6) NOT NULL,
    total_repayment_derived NUMERIC(19, 6) NOT NULL,
    total_writtenoff_derived NUMERIC(19, 6) NOT NULL,
    total_outstanding_derived NUMERIC(19, 6) NOT NULL,
    loan_counter SMALLINT,
    is_npa BOOLEAN NOT NULL,
    created_on_utc TIMESTAMPTZ NOT NULL,
    last_modified_on_utc TIMESTAMPTZ NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS raw.raw_m_loan_transaction (
    tenant_id TEXT NOT NULL,
    id BIGINT NOT NULL,
    loan_id BIGINT NOT NULL,
    office_id BIGINT NOT NULL,
    is_reversed BOOLEAN NOT NULL,
    transaction_type_enum SMALLINT NOT NULL,
    transaction_date DATE NOT NULL,
    amount NUMERIC(19, 6) NOT NULL,
    principal_portion_derived NUMERIC(19, 6),
    interest_portion_derived NUMERIC(19, 6),
    fee_charges_portion_derived NUMERIC(19, 6),
    penalty_charges_portion_derived NUMERIC(19, 6),
    outstanding_loan_balance_derived NUMERIC(19, 6),
    submitted_on_date DATE NOT NULL,
    created_on_utc TIMESTAMPTZ NOT NULL,
    last_modified_on_utc TIMESTAMPTZ NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS raw.raw_m_delinquency_range (
    tenant_id TEXT NOT NULL,
    id BIGINT NOT NULL,
    classification TEXT NOT NULL,
    min_age_days BIGINT NOT NULL,
    max_age_days BIGINT,
    created_on_utc TIMESTAMPTZ NOT NULL,
    last_modified_on_utc TIMESTAMPTZ NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS raw.raw_m_delinquency_bucket (
    tenant_id TEXT NOT NULL,
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    created_on_utc TIMESTAMPTZ NOT NULL,
    last_modified_on_utc TIMESTAMPTZ NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS raw.raw_m_delinquency_bucket_mappings (
    tenant_id TEXT NOT NULL,
    id BIGINT NOT NULL,
    delinquency_range_id BIGINT NOT NULL,
    delinquency_bucket_id BIGINT NOT NULL,
    created_on_utc TIMESTAMPTZ NOT NULL,
    last_modified_on_utc TIMESTAMPTZ NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS raw.raw_m_loan_delinquency_tag_history (
    tenant_id TEXT NOT NULL,
    id BIGINT NOT NULL,
    delinquency_range_id BIGINT NOT NULL,
    loan_id BIGINT NOT NULL,
    addedon_date DATE NOT NULL,
    liftedon_date DATE,
    created_on_utc TIMESTAMPTZ NOT NULL,
    last_modified_on_utc TIMESTAMPTZ NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS raw.raw_batch_job_execution (
    tenant_id TEXT NOT NULL,
    job_execution_id BIGINT NOT NULL,
    status TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    exit_code TEXT,
    exit_message TEXT,
    created_on_utc TIMESTAMPTZ NOT NULL,
    last_modified_on_utc TIMESTAMPTZ NOT NULL,
    source_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, job_execution_id)
);
