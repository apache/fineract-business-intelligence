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

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

CREATE TABLE public.batch_job_instance (
    job_instance_id bigint NOT NULL,
    version bigint,
    job_name character varying(100) NOT NULL,
    job_key character varying(32) NOT NULL
);

CREATE TABLE public.batch_job_execution (
    job_execution_id bigint NOT NULL,
    version bigint,
    job_instance_id bigint NOT NULL,
    create_time timestamp without time zone NOT NULL,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    status character varying(10),
    exit_code character varying(2500),
    exit_message character varying(2500),
    last_updated timestamp without time zone
);

CREATE TABLE public.m_office (
    id bigint NOT NULL,
    parent_id bigint,
    hierarchy character varying(100),
    external_id character varying(100),
    name character varying(50) NOT NULL,
    opening_date date NOT NULL
);

CREATE TABLE public.m_currency (
    id bigint NOT NULL,
    code character varying(3) NOT NULL,
    decimal_places smallint NOT NULL,
    currency_multiplesof smallint,
    display_symbol character varying(10),
    name character varying(50) NOT NULL,
    internationalized_name_code character varying(50) NOT NULL
);

CREATE TABLE public.m_client (
    id bigint NOT NULL,
    account_no character varying(20) NOT NULL,
    external_id character varying(100),
    status_enum integer DEFAULT 300 NOT NULL,
    sub_status integer,
    activation_date date,
    office_joining_date date,
    office_id bigint NOT NULL,
    transfer_to_office_id bigint,
    staff_id bigint,
    firstname character varying(50),
    middlename character varying(50),
    lastname character varying(50),
    fullname character varying(160),
    display_name character varying(160) NOT NULL,
    mobile_no character varying(50),
    is_staff boolean DEFAULT false NOT NULL,
    gender_cv_id integer,
    date_of_birth date,
    image_id bigint,
    closure_reason_cv_id integer,
    closedon_date date,
    updated_by bigint,
    updated_on date,
    submittedon_date date,
    activatedon_userid bigint,
    closedon_userid bigint,
    default_savings_product bigint,
    default_savings_account bigint,
    client_type_cv_id integer,
    client_classification_cv_id integer,
    reject_reason_cv_id integer,
    rejectedon_date date,
    rejectedon_userid bigint,
    withdraw_reason_cv_id integer,
    withdrawn_on_date date,
    withdraw_on_userid bigint,
    reactivated_on_date date,
    reactivated_on_userid bigint,
    legal_form_enum integer,
    reopened_on_date date,
    reopened_by_userid bigint,
    email_address character varying(150),
    proposed_transfer_date date,
    created_on_utc timestamp with time zone NOT NULL,
    created_by bigint NOT NULL,
    last_modified_by bigint NOT NULL,
    last_modified_on_utc timestamp with time zone NOT NULL
);

CREATE TABLE public.m_product_loan (
    id bigint NOT NULL,
    short_name character varying(4) NOT NULL,
    currency_code character varying(3) NOT NULL,
    currency_digits smallint NOT NULL,
    currency_multiplesof smallint,
    principal_amount numeric(19,6) DEFAULT NULL::numeric,
    min_principal_amount numeric(19,6) DEFAULT NULL::numeric,
    max_principal_amount numeric(19,6) DEFAULT NULL::numeric,
    arrearstolerance_amount numeric(19,6) DEFAULT NULL::numeric,
    name character varying(100) NOT NULL,
    description character varying(500),
    fund_id bigint,
    is_linked_to_floating_interest_rates boolean DEFAULT false NOT NULL,
    allow_variabe_installments boolean DEFAULT false NOT NULL,
    nominal_interest_rate_per_period numeric(19,6) DEFAULT NULL::numeric,
    min_nominal_interest_rate_per_period numeric(19,6) DEFAULT NULL::numeric,
    max_nominal_interest_rate_per_period numeric(19,6) DEFAULT NULL::numeric,
    interest_period_frequency_enum smallint,
    annual_nominal_interest_rate numeric(19,6) DEFAULT NULL::numeric,
    interest_method_enum smallint NOT NULL,
    interest_calculated_in_period_enum smallint DEFAULT 1 NOT NULL,
    allow_partial_period_interest_calcualtion boolean DEFAULT false NOT NULL,
    repay_every smallint NOT NULL,
    repayment_period_frequency_enum smallint NOT NULL,
    number_of_repayments smallint NOT NULL,
    min_number_of_repayments smallint,
    max_number_of_repayments smallint,
    grace_on_principal_periods smallint,
    recurring_moratorium_principal_periods smallint,
    grace_on_interest_periods smallint,
    grace_interest_free_periods smallint,
    amortization_method_enum smallint NOT NULL,
    accounting_type smallint NOT NULL,
    loan_transaction_strategy_id bigint,
    external_id character varying(100),
    include_in_borrower_cycle boolean DEFAULT false NOT NULL,
    use_borrower_cycle boolean DEFAULT false NOT NULL,
    start_date date,
    close_date date,
    allow_multiple_disbursals boolean DEFAULT false NOT NULL,
    max_disbursals integer,
    max_outstanding_loan_balance numeric(19,6) DEFAULT NULL::numeric,
    grace_on_arrears_ageing smallint,
    overdue_days_for_npa smallint,
    days_in_month_enum smallint DEFAULT 1 NOT NULL,
    days_in_year_enum smallint DEFAULT 1 NOT NULL,
    interest_recalculation_enabled boolean DEFAULT false NOT NULL,
    min_days_between_disbursal_and_first_repayment integer,
    hold_guarantee_funds boolean DEFAULT false NOT NULL,
    principal_threshold_for_last_installment numeric(5,2) DEFAULT 50 NOT NULL,
    account_moves_out_of_npa_only_on_arrears_completion boolean DEFAULT false NOT NULL,
    can_define_fixed_emi_amount boolean DEFAULT false NOT NULL,
    installment_amount_in_multiples_of numeric(19,6) DEFAULT NULL::numeric,
    can_use_for_topup boolean DEFAULT false NOT NULL,
    sync_expected_with_disbursement_date boolean DEFAULT false,
    is_equal_amortization boolean DEFAULT false NOT NULL,
    fixed_principal_percentage_per_installment numeric(5,2) DEFAULT NULL::numeric,
    disallow_expected_disbursements boolean DEFAULT false NOT NULL,
    allow_approved_disbursed_amounts_over_applied boolean DEFAULT false NOT NULL,
    over_applied_calculation_type character varying(10),
    over_applied_number integer,
    delinquency_bucket_id bigint,
    loan_transaction_strategy_code character varying(100) DEFAULT '-'::character varying NOT NULL,
    loan_transaction_strategy_name character varying(100) DEFAULT '-'::character varying NOT NULL,
    due_days_for_repayment_event integer,
    overdue_days_for_repayment_event integer,
    enable_down_payment boolean DEFAULT false NOT NULL,
    disbursed_amount_percentage_for_down_payment numeric(9,6) DEFAULT NULL::numeric,
    enable_installment_level_delinquency boolean DEFAULT false NOT NULL,
    enable_accrual_activity_posting boolean DEFAULT false NOT NULL,
    days_in_year_custom_strategy character varying(100),
    enable_income_capitalization boolean DEFAULT false NOT NULL,
    capitalized_income_calculation_type character varying(100),
    capitalized_income_strategy character varying(100),
    capitalized_income_type character varying(10),
    enable_buy_down_fee boolean DEFAULT false NOT NULL,
    buy_down_fee_calculation_type character varying(100),
    buy_down_fee_strategy character varying(100),
    buy_down_fee_income_type character varying(100),
    allow_full_term_for_tranche boolean DEFAULT false NOT NULL,
    enable_auto_repayment_for_down_payment boolean DEFAULT false NOT NULL,
    repayment_start_date_type_enum smallint DEFAULT 1 NOT NULL,
    loan_schedule_type character varying(20) DEFAULT 'CUMULATIVE'::character varying NOT NULL,
    loan_schedule_processing_type character varying(20) DEFAULT 'HORIZONTAL'::character varying NOT NULL,
    fixed_length smallint,
    supported_interest_refund_types text,
    charge_off_behaviour character varying(20) DEFAULT 'REGULAR'::character varying,
    interest_recognition_on_disbursement_date boolean DEFAULT false NOT NULL,
    is_merchant_buy_down_fee boolean DEFAULT true NOT NULL
);

CREATE TABLE public.m_loan (
    id bigint NOT NULL,
    account_no character varying(20) NOT NULL,
    external_id character varying(100),
    client_id bigint,
    group_id bigint,
    glim_id bigint,
    product_id bigint,
    fund_id bigint,
    loan_officer_id bigint,
    loanpurpose_cv_id integer,
    loan_status_id smallint NOT NULL,
    loan_type_enum smallint NOT NULL,
    currency_code character varying(3) NOT NULL,
    currency_digits smallint NOT NULL,
    currency_multiplesof smallint,
    principal_amount_proposed numeric(19,6) NOT NULL,
    principal_amount numeric(19,6) NOT NULL,
    approved_principal numeric(19,6) NOT NULL,
    net_disbursal_amount numeric(19,6) NOT NULL,
    arrearstolerance_amount numeric(19,6) DEFAULT NULL::numeric,
    is_floating_interest_rate boolean DEFAULT false,
    interest_rate_differential numeric(19,6) DEFAULT 0,
    nominal_interest_rate_per_period numeric(19,6) DEFAULT NULL::numeric,
    interest_period_frequency_enum smallint,
    annual_nominal_interest_rate numeric(19,6) DEFAULT NULL::numeric,
    interest_method_enum smallint NOT NULL,
    interest_calculated_in_period_enum smallint DEFAULT 1 NOT NULL,
    allow_partial_period_interest_calcualtion boolean DEFAULT false NOT NULL,
    term_frequency smallint DEFAULT 0 NOT NULL,
    term_period_frequency_enum smallint DEFAULT 2 NOT NULL,
    repay_every smallint NOT NULL,
    repayment_period_frequency_enum smallint NOT NULL,
    number_of_repayments smallint NOT NULL,
    grace_on_principal_periods smallint,
    recurring_moratorium_principal_periods smallint,
    grace_on_interest_periods smallint,
    grace_interest_free_periods smallint,
    amortization_method_enum smallint NOT NULL,
    submittedon_date date,
    approvedon_date date,
    approvedon_userid bigint,
    expected_disbursedon_date date,
    expected_firstrepaymenton_date date,
    interest_calculated_from_date date,
    disbursedon_date date,
    disbursedon_userid bigint,
    expected_maturedon_date date,
    maturedon_date date,
    closedon_date date,
    closedon_userid bigint,
    total_charges_due_at_disbursement_derived numeric(19,6) DEFAULT NULL::numeric,
    principal_disbursed_derived numeric(19,6) DEFAULT 0 NOT NULL,
    principal_repaid_derived numeric(19,6) DEFAULT 0 NOT NULL,
    principal_writtenoff_derived numeric(19,6) DEFAULT 0 NOT NULL,
    principal_outstanding_derived numeric(19,6) DEFAULT 0 NOT NULL,
    interest_charged_derived numeric(19,6) DEFAULT 0 NOT NULL,
    interest_repaid_derived numeric(19,6) DEFAULT 0 NOT NULL,
    interest_waived_derived numeric(19,6) DEFAULT 0 NOT NULL,
    interest_writtenoff_derived numeric(19,6) DEFAULT 0 NOT NULL,
    interest_outstanding_derived numeric(19,6) DEFAULT 0 NOT NULL,
    fee_charges_charged_derived numeric(19,6) DEFAULT 0 NOT NULL,
    fee_charges_repaid_derived numeric(19,6) DEFAULT 0 NOT NULL,
    fee_charges_waived_derived numeric(19,6) DEFAULT 0 NOT NULL,
    fee_charges_writtenoff_derived numeric(19,6) DEFAULT 0 NOT NULL,
    fee_charges_outstanding_derived numeric(19,6) DEFAULT 0 NOT NULL,
    penalty_charges_charged_derived numeric(19,6) DEFAULT 0 NOT NULL,
    penalty_charges_repaid_derived numeric(19,6) DEFAULT 0 NOT NULL,
    penalty_charges_waived_derived numeric(19,6) DEFAULT 0 NOT NULL,
    penalty_charges_writtenoff_derived numeric(19,6) DEFAULT 0 NOT NULL,
    penalty_charges_outstanding_derived numeric(19,6) DEFAULT 0 NOT NULL,
    total_expected_repayment_derived numeric(19,6) DEFAULT 0 NOT NULL,
    total_repayment_derived numeric(19,6) DEFAULT 0 NOT NULL,
    total_expected_costofloan_derived numeric(19,6) DEFAULT 0 NOT NULL,
    total_costofloan_derived numeric(19,6) DEFAULT 0 NOT NULL,
    total_waived_derived numeric(19,6) DEFAULT 0 NOT NULL,
    total_writtenoff_derived numeric(19,6) DEFAULT 0 NOT NULL,
    total_outstanding_derived numeric(19,6) DEFAULT 0 NOT NULL,
    total_overpaid_derived numeric(19,6) DEFAULT NULL::numeric,
    rejectedon_date date,
    rejectedon_userid bigint,
    rescheduledon_date date,
    rescheduledon_userid bigint,
    withdrawnon_date date,
    withdrawnon_userid bigint,
    writtenoffon_date date,
    loan_transaction_strategy_id bigint,
    sync_disbursement_with_meeting boolean,
    loan_counter smallint,
    loan_product_counter smallint,
    fixed_emi_amount numeric(19,6) DEFAULT NULL::numeric,
    max_outstanding_loan_balance numeric(19,6) DEFAULT NULL::numeric,
    grace_on_arrears_ageing smallint,
    is_npa boolean DEFAULT false NOT NULL,
    total_recovered_derived numeric(19,6) DEFAULT NULL::numeric,
    accrued_till date,
    interest_recalcualated_on date,
    days_in_month_enum smallint DEFAULT 1 NOT NULL,
    days_in_year_enum smallint DEFAULT 1 NOT NULL,
    interest_recalculation_enabled boolean DEFAULT false NOT NULL,
    guarantee_amount_derived numeric(19,6) DEFAULT NULL::numeric,
    create_standing_instruction_at_disbursement boolean,
    version integer DEFAULT 1 NOT NULL,
    writeoff_reason_cv_id integer,
    loan_sub_status_id smallint,
    is_topup boolean DEFAULT false NOT NULL,
    is_equal_amortization boolean DEFAULT false NOT NULL,
    fixed_principal_percentage_per_installment numeric(5,2) DEFAULT NULL::numeric,
    created_on_utc timestamp with time zone NOT NULL,
    created_by bigint NOT NULL,
    last_modified_by bigint NOT NULL,
    last_modified_on_utc timestamp with time zone NOT NULL,
    principal_adjustments_derived numeric(19,6) DEFAULT 0 NOT NULL,
    is_fraud boolean DEFAULT false NOT NULL,
    loan_transaction_strategy_code character varying(100) DEFAULT '-'::character varying NOT NULL,
    loan_transaction_strategy_name character varying(100) DEFAULT '-'::character varying NOT NULL,
    last_closed_business_date date,
    overpaidon_date date,
    is_charged_off boolean DEFAULT false NOT NULL,
    charged_off_on_date date,
    charge_off_reason_cv_id bigint,
    charged_off_by_userid bigint,
    enable_down_payment boolean DEFAULT false NOT NULL,
    disbursed_amount_percentage_for_down_payment numeric(9,6) DEFAULT NULL::numeric,
    enable_installment_level_delinquency boolean DEFAULT false NOT NULL,
    enable_accrual_activity_posting boolean DEFAULT false NOT NULL,
    days_in_year_custom_strategy character varying(100),
    enable_income_capitalization boolean DEFAULT false NOT NULL,
    capitalized_income_calculation_type character varying(100),
    capitalized_income_strategy character varying(100),
    capitalized_income_type character varying(10),
    capitalized_income_derived numeric(19,6) DEFAULT 0 NOT NULL,
    capitalized_income_adjustment_derived numeric(19,6) DEFAULT 0 NOT NULL,
    total_principal_derived numeric(19,6) DEFAULT 0 NOT NULL,
    enable_buy_down_fee boolean DEFAULT false NOT NULL,
    buy_down_fee_calculation_type character varying(100),
    buy_down_fee_strategy character varying(100),
    buy_down_fee_income_type character varying(100),
    allow_full_term_for_tranche boolean DEFAULT false NOT NULL,
    repayment_start_date_type_enum smallint,
    enable_auto_repayment_for_down_payment boolean DEFAULT false NOT NULL,
    loan_schedule_type character varying(20) DEFAULT 'CUMULATIVE'::character varying NOT NULL,
    loan_schedule_processing_type character varying(20) DEFAULT 'HORIZONTAL'::character varying NOT NULL,
    fee_adjustments_derived numeric(19,6) DEFAULT 0 NOT NULL,
    penalty_adjustments_derived numeric(19,6) DEFAULT 0 NOT NULL,
    fixed_length smallint,
    supported_interest_refund_types text,
    charge_off_behaviour character varying(20) DEFAULT 'REGULAR'::character varying,
    interest_recognition_on_disbursement_date boolean DEFAULT false NOT NULL,
    installment_amount_in_multiples_of numeric(19,6) DEFAULT NULL::numeric,
    is_merchant_buy_down_fee boolean DEFAULT true NOT NULL
);

CREATE TABLE public.m_loan_transaction (
    id bigint NOT NULL,
    loan_id bigint NOT NULL,
    office_id bigint NOT NULL,
    payment_detail_id bigint,
    is_reversed boolean NOT NULL,
    external_id character varying(100),
    transaction_type_enum smallint NOT NULL,
    transaction_date date NOT NULL,
    amount numeric(19,6) NOT NULL,
    principal_portion_derived numeric(19,6) DEFAULT NULL::numeric,
    interest_portion_derived numeric(19,6) DEFAULT NULL::numeric,
    fee_charges_portion_derived numeric(19,6) DEFAULT NULL::numeric,
    penalty_charges_portion_derived numeric(19,6) DEFAULT NULL::numeric,
    overpayment_portion_derived numeric(19,6) DEFAULT NULL::numeric,
    unrecognized_income_portion numeric(19,6) DEFAULT NULL::numeric,
    outstanding_loan_balance_derived numeric(19,6) DEFAULT NULL::numeric,
    submitted_on_date date NOT NULL,
    manually_adjusted_or_reversed boolean DEFAULT false,
    created_date timestamp without time zone,
    created_by bigint NOT NULL,
    last_modified_by bigint NOT NULL,
    created_on_utc timestamp with time zone NOT NULL,
    last_modified_on_utc timestamp with time zone NOT NULL,
    charge_refund_charge_type character varying(1),
    reversal_external_id character varying(100) DEFAULT NULL::character varying,
    reversed_on_date date,
    version bigint DEFAULT 1 NOT NULL,
    classification_cv_id bigint
);

CREATE TABLE public.m_delinquency_bucket (
    id bigint NOT NULL,
    name character varying(100) NOT NULL,
    created_by bigint NOT NULL,
    created_on_utc timestamp with time zone NOT NULL,
    version bigint NOT NULL,
    last_modified_by bigint NOT NULL,
    last_modified_on_utc timestamp with time zone NOT NULL,
    bucket_type character varying(50) DEFAULT 'REGULAR'::character varying NOT NULL
);

CREATE TABLE public.m_delinquency_range (
    id bigint NOT NULL,
    classification character varying(100) NOT NULL,
    min_age_days bigint NOT NULL,
    max_age_days bigint,
    created_by bigint NOT NULL,
    created_on_utc timestamp with time zone NOT NULL,
    version bigint NOT NULL,
    last_modified_by bigint NOT NULL,
    last_modified_on_utc timestamp with time zone NOT NULL
);

CREATE TABLE public.m_delinquency_bucket_mappings (
    id bigint NOT NULL,
    delinquency_range_id bigint NOT NULL,
    delinquency_bucket_id bigint NOT NULL,
    created_by bigint NOT NULL,
    created_on_utc timestamp with time zone NOT NULL,
    version bigint NOT NULL,
    last_modified_by bigint NOT NULL,
    last_modified_on_utc timestamp with time zone NOT NULL
);

CREATE TABLE public.m_loan_delinquency_tag_history (
    id bigint NOT NULL,
    delinquency_range_id bigint NOT NULL,
    loan_id bigint NOT NULL,
    addedon_date date NOT NULL,
    liftedon_date date,
    created_by bigint NOT NULL,
    created_on_utc timestamp with time zone NOT NULL,
    version bigint NOT NULL,
    last_modified_by bigint NOT NULL,
    last_modified_on_utc timestamp with time zone NOT NULL
);

ALTER TABLE ONLY public.batch_job_instance
    ADD CONSTRAINT batch_job_instance_pkey PRIMARY KEY (job_instance_id);
ALTER TABLE ONLY public.batch_job_instance
    ADD CONSTRAINT job_inst_un UNIQUE (job_name, job_key);
ALTER TABLE ONLY public.batch_job_execution
    ADD CONSTRAINT batch_job_execution_pkey PRIMARY KEY (job_execution_id);
ALTER TABLE ONLY public.batch_job_execution
    ADD CONSTRAINT job_inst_exec_fk FOREIGN KEY (job_instance_id) REFERENCES public.batch_job_instance(job_instance_id);
ALTER TABLE ONLY public.m_office
    ADD CONSTRAINT m_office_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.m_office
    ADD CONSTRAINT m_office_external_id_key UNIQUE (external_id);
ALTER TABLE ONLY public.m_office
    ADD CONSTRAINT m_office_name_key UNIQUE (name);
ALTER TABLE ONLY public.m_office
    ADD CONSTRAINT fk2291c477e2551dcc FOREIGN KEY (parent_id) REFERENCES public.m_office(id) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.m_currency
    ADD CONSTRAINT m_currency_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.m_currency
    ADD CONSTRAINT m_currency_code_key UNIQUE (code);
ALTER TABLE ONLY public.m_client
    ADD CONSTRAINT m_client_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.m_client
    ADD CONSTRAINT m_client_account_no_key UNIQUE (account_no);
ALTER TABLE ONLY public.m_product_loan
    ADD CONSTRAINT m_product_loan_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.m_product_loan
    ADD CONSTRAINT m_product_loan_name_key UNIQUE (name);
ALTER TABLE ONLY public.m_product_loan
    ADD CONSTRAINT m_product_loan_short_name_key UNIQUE (short_name);
ALTER TABLE ONLY public.m_loan
    ADD CONSTRAINT m_loan_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.m_loan
    ADD CONSTRAINT m_loan_account_no_key UNIQUE (account_no);
ALTER TABLE ONLY public.m_loan_transaction
    ADD CONSTRAINT m_loan_transaction_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.m_delinquency_bucket
    ADD CONSTRAINT m_delinquency_bucket_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.m_delinquency_bucket
    ADD CONSTRAINT m_delinquency_bucket_name_key UNIQUE (name);
ALTER TABLE ONLY public.m_delinquency_range
    ADD CONSTRAINT m_delinquency_range_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.m_delinquency_range
    ADD CONSTRAINT m_delinquency_range_classification_key UNIQUE (classification);
ALTER TABLE ONLY public.m_delinquency_bucket_mappings
    ADD CONSTRAINT m_delinquency_bucket_mappings_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.m_delinquency_bucket_mappings
    ADD CONSTRAINT "FK_m_delinquency_bucket_mapping" FOREIGN KEY (delinquency_bucket_id) REFERENCES public.m_delinquency_bucket(id) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.m_delinquency_bucket_mappings
    ADD CONSTRAINT "FK_m_delinquency_range_mapping" FOREIGN KEY (delinquency_range_id) REFERENCES public.m_delinquency_range(id) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.m_loan_delinquency_tag_history
    ADD CONSTRAINT m_loan_delinquency_tag_history_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.m_loan_delinquency_tag_history
    ADD CONSTRAINT "FK_m_delinquency_tags_loan" FOREIGN KEY (loan_id) REFERENCES public.m_loan(id) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.m_loan_delinquency_tag_history
    ADD CONSTRAINT "FK_m_delinquency_tags_range" FOREIGN KEY (delinquency_range_id) REFERENCES public.m_delinquency_range(id) ON UPDATE RESTRICT ON DELETE RESTRICT;

CREATE INDEX fk2291c477e2551dcc ON public.m_office USING btree (parent_id);
CREATE INDEX fkce00cab3e0dd567a ON public.m_client USING btree (office_id);
CREATE INDEX fkb6f935d87179a0cb ON public.m_loan USING btree (client_id);
CREATE INDEX fkb6f935d8c8d4b434 ON public.m_loan USING btree (product_id);
CREATE INDEX fkcfcea42640be0710 ON public.m_loan_transaction USING btree (loan_id);
CREATE INDEX "FK_m_loan_transaction_m_office" ON public.m_loan_transaction USING btree (office_id);
CREATE INDEX "FK_loan_status_id" ON public.m_loan USING btree (loan_status_id);
CREATE INDEX idx_batch_job_execution_job_instance_id ON public.batch_job_execution USING btree (job_instance_id);
CREATE INDEX ind_m_loan_delinquency_tag_history_loan_id ON public.m_loan_delinquency_tag_history USING btree (loan_id);
CREATE INDEX ind_m_loan_delinquency_tag_history_liftedon_date ON public.m_loan_delinquency_tag_history USING btree (liftedon_date);

ALTER TABLE public.m_client ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME public.m_client_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);
ALTER TABLE public.m_currency ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME public.m_currency_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);
ALTER TABLE public.m_delinquency_bucket ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME public.m_delinquency_bucket_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);
ALTER TABLE public.m_delinquency_bucket_mappings ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME public.m_delinquency_bucket_mappings_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);
ALTER TABLE public.m_delinquency_range ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME public.m_delinquency_range_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);
ALTER TABLE public.m_loan_delinquency_tag_history ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME public.m_loan_delinquency_tag_history_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);
ALTER TABLE public.m_loan ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME public.m_loan_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);
ALTER TABLE public.m_loan_transaction ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME public.m_loan_transaction_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);
ALTER TABLE public.m_office ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME public.m_office_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);
ALTER TABLE public.m_product_loan ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME public.m_product_loan_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);

INSERT INTO public.m_office (id, parent_id, hierarchy, external_id, name, opening_date)
VALUES (1, NULL, '.', '1', 'Head Office', '2009-01-01')
ON CONFLICT (id) DO NOTHING;
