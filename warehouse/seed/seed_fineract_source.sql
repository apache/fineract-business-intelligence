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

BEGIN;

DELETE FROM public.m_loan_delinquency_tag_history WHERE loan_id IN (SELECT id FROM public.m_loan WHERE client_id >= 100);
DELETE FROM public.m_loan_transaction               WHERE loan_id IN (SELECT id FROM public.m_loan WHERE client_id >= 100);
DELETE FROM public.m_loan                           WHERE client_id >= 100;
DELETE FROM public.m_client                         WHERE id >= 100;
DELETE FROM public.m_delinquency_bucket_mappings    WHERE id >= 100;
DELETE FROM public.m_delinquency_range              WHERE id >= 100;
DELETE FROM public.m_delinquency_bucket             WHERE id >= 100;
DELETE FROM public.m_product_loan                   WHERE id >= 100;
DELETE FROM public.m_office                         WHERE id >= 100;
DELETE FROM public.batch_job_execution              WHERE job_execution_id = 9001;
DELETE FROM public.batch_job_instance               WHERE job_instance_id  = 9001;

INSERT INTO public.m_office (id, parent_id, hierarchy, external_id, name, opening_date)
VALUES
    (101, 1, '.1.101.', 'OFF-NB', 'North Branch', '2019-04-01'),
    (102, 1, '.1.102.', 'OFF-SB', 'South Branch', '2019-07-01');

INSERT INTO public.m_delinquency_bucket (id, name, created_by, created_on_utc, version, last_modified_by, last_modified_on_utc)
VALUES (101, 'Standard Portfolio Delinquency Bucket', 1, NOW(), 1, 1, NOW());

INSERT INTO public.m_delinquency_range (id, classification, min_age_days, max_age_days, created_by, created_on_utc, version, last_modified_by, last_modified_on_utc)
VALUES
    (101, '1-30 DPD',  1,   30, 1, NOW(), 1, 1, NOW()),
    (102, '31-60 DPD', 31,  60, 1, NOW(), 1, 1, NOW()),
    (103, '61-90 DPD', 61,  90, 1, NOW(), 1, 1, NOW()),
    (104, '90+ DPD',   91, NULL, 1, NOW(), 1, 1, NOW());

INSERT INTO public.m_delinquency_bucket_mappings (id, delinquency_range_id, delinquency_bucket_id, created_by, created_on_utc, version, last_modified_by, last_modified_on_utc)
VALUES
    (101, 101, 101, 1, NOW(), 1, 1, NOW()),
    (102, 102, 101, 1, NOW(), 1, 1, NOW()),
    (103, 103, 101, 1, NOW(), 1, 1, NOW()),
    (104, 104, 101, 1, NOW(), 1, 1, NOW());

INSERT INTO public.m_product_loan (
    id, short_name, currency_code, currency_digits, currency_multiplesof,
    principal_amount, min_principal_amount, max_principal_amount,
    arrearstolerance_amount, name,
    nominal_interest_rate_per_period, annual_nominal_interest_rate,
    interest_method_enum, interest_calculated_in_period_enum,
    repay_every, repayment_period_frequency_enum, number_of_repayments,
    amortization_method_enum, accounting_type,
    overdue_days_for_npa, delinquency_bucket_id,
    loan_transaction_strategy_code, loan_transaction_strategy_name,
    loan_schedule_type, loan_schedule_processing_type,
    repayment_start_date_type_enum
) VALUES
    (101, 'MSME',  'USD', 2, 0,  5000,  500, 50000, 0, 'MSME Loan',
     1.5, 18, 0, 1, 1, 2, 24, 1, 1, 90, 101,
     'mifos-standard-strategy', 'Penalties, Fees, Interest, Principal order',
     'CUMULATIVE', 'HORIZONTAL', 1),
    (102, 'AGRI',  'USD', 2, 0,  3000,  500, 20000, 0, 'Agriculture Loan',
     1.2, 14, 0, 1, 1, 2, 12, 1, 1, 90, 101,
     'mifos-standard-strategy', 'Penalties, Fees, Interest, Principal order',
     'CUMULATIVE', 'HORIZONTAL', 1),
    (103, 'HOUS',  'USD', 2, 0, 10000, 5000, 75000, 0, 'Housing Loan',
     1.0, 12, 0, 1, 1, 2, 36, 1, 1, 90, 101,
     'mifos-standard-strategy', 'Penalties, Fees, Interest, Principal order',
     'CUMULATIVE', 'HORIZONTAL', 1),
    (104, 'EMRG',  'USD', 2, 0,  1500,  300,  5000, 0, 'Emergency Loan',
     2.0, 24, 0, 1, 1, 2,  6, 1, 1, 90, 101,
     'mifos-standard-strategy', 'Penalties, Fees, Interest, Principal order',
     'CUMULATIVE', 'HORIZONTAL', 1);

INSERT INTO public.m_client (
    id, account_no, status_enum, activation_date, office_joining_date,
    office_id, gender_cv_id, date_of_birth, legal_form_enum,
    display_name, submittedon_date,
    created_on_utc, created_by, last_modified_by, last_modified_on_utc
)
SELECT
    100 + id,
    'CL' || LPAD((100 + id)::text, 6, '0'),
    300,
    (current_date - (((id * 53) % 900) + 120)::int),
    (current_date - (((id * 53) % 900) + 120)::int),
    office_id,
    NULL,
    (current_date - (((25 + (id % 35)) * 365))::int),
    1,
    'Client ' || (100 + id),
    (current_date - (((id * 53) % 900) + 125)::int),
    NOW(), 1, 1, NOW()
FROM (VALUES
    ( 1,   1), ( 2,   1), ( 3,   1), ( 4,   1), ( 5,   1),
    ( 6,   1), ( 7,   1), ( 8,   1), ( 9,   1), (10,   1),
    (11, 101), (12, 101), (13, 101), (14, 101),
    (15, 101), (16, 101), (17, 101), (18, 101),
    (19, 102), (20, 102), (21, 102), (22, 102),
    (23, 102), (24, 102), (25, 102)
) AS t(id, office_id);

CREATE TEMP TABLE tmp_loan AS
SELECT
    (100 + loan_id)::bigint             AS loan_id,
    (100 + client_id)::bigint           AS client_id,
    office_id::bigint                   AS office_id,
    (100 + product_id)::bigint          AS product_id,
    principal::numeric(19,6)            AS principal_amount,
    vintage_months,
    overdue_days,
    CASE product_id WHEN 3 THEN 36 WHEN 4 THEN 6 ELSE 24 END AS term_months,
    CASE product_id
        WHEN 1 THEN 0.18 WHEN 2 THEN 0.14 WHEN 3 THEN 0.12 ELSE 0.24
    END                                 AS annual_rate
FROM (VALUES
    ( 1,  1,   1, 1, 10000, 36,   0),
    ( 2,  1,   1, 1,  8000, 24,   0),
    ( 3,  1,   1, 2,  5000, 18,  20),
    ( 4,  1,   1, 3, 20000, 30,   0),
    ( 5,  1,   1, 4,  2500,  5,   0),
    ( 6,  2,   1, 1,  7000, 30,   0),
    ( 7,  2,   1, 2,  4000, 22,   0),
    ( 8,  2,   1, 3, 15000, 28, 120),
    ( 9,  2,   1, 4,  2000,  4,   0),
    (10,  3,   1, 1,  6000, 20,   0),
    (11,  3,   1, 2,  3500, 12,  45),
    (12,  3,   1, 4,  1500,  3,   0),
    (13,  4,   1, 1, 12000, 32,   0),
    (14,  4,   1, 2,  5000, 18,   0),
    (15,  4,   1, 3, 18000, 24,  45),
    (16,  4,   1, 3, 22000, 36,   0),
    (17,  4,   1, 4,  3000,  6,   0),
    (18,  5,   1, 1,  5500, 14,   0),
    (19,  5,   1, 2,  3000,  8,  75),
    (20,  6,   1, 1,  8000, 26,   0),
    (21,  6,   1, 3, 12000, 20,   0),
    (22,  6,   1, 4,  1500,  2,  20),
    (23,  7,   1, 2,  4500, 16,   0),
    (24,  7,   1, 1,  7000, 10,   0),
    (25,  8,   1, 1,  9000, 33,   0),
    (26,  8,   1, 2,  6000, 25, 120),
    (27,  8,   1, 3, 14000, 18,   0),
    (28,  8,   1, 4,  2000,  4,   0),
    (29,  9,   1, 1,  5000, 12,   0),
    (30,  9,   1, 2,  3000,  7,   0),
    (31, 10,   1, 3, 16000, 22,   0),
    (32, 11, 101, 1,  7000, 28,   0),
    (33, 11, 101, 1,  5000, 16,   0),
    (34, 11, 101, 2,  3500, 10,  20),
    (35, 11, 101, 3, 11000, 24,   0),
    (36, 11, 101, 4,  2000,  5,   0),
    (37, 12, 101, 1,  6000, 20,   0),
    (38, 12, 101, 2,  4000, 14,  45),
    (39, 12, 101, 4,  1500,  3,   0),
    (40, 13, 101, 1,  4500, 10,   0),
    (41, 13, 101, 3,  9000, 18,   0),
    (42, 14, 101, 2,  3200,  8,   0),
    (43, 15, 101, 1,  8000, 30, 120),
    (44, 15, 101, 2,  5000, 22,   0),
    (45, 15, 101, 3, 13000, 26,   0),
    (46, 15, 101, 4,  2500,  6,   0),
    (47, 16, 101, 1,  5500, 18,   0),
    (48, 16, 101, 2,  3000, 12,  75),
    (49, 16, 101, 3, 10000, 24,   0),
    (50, 17, 101, 1,  4000, 14,   0),
    (51, 17, 101, 4,  1500,  4,   0),
    (52, 18, 101, 2,  3500, 10,   0),
    (53, 19, 102, 1,  9000, 32,   0),
    (54, 19, 102, 2,  6000, 20,   0),
    (55, 19, 102, 3, 17000, 28,  45),
    (56, 19, 102, 1,  7000, 12,   0),
    (57, 19, 102, 4,  2500,  4,   0),
    (58, 20, 102, 1,  5000, 16,   0),
    (59, 20, 102, 2,  4000, 10,  20),
    (60, 20, 102, 3, 11000, 22,   0),
    (61, 21, 102, 1,  6000, 18,   0),
    (62, 21, 102, 4,  2000,  5,   0),
    (63, 22, 102, 1,  8000, 24,   0),
    (64, 22, 102, 2,  5500, 18,  75),
    (65, 22, 102, 3, 14000, 30,   0),
    (66, 22, 102, 4,  3000,  7,   0),
    (67, 23, 102, 1,  4500, 12,   0),
    (68, 23, 102, 2,  3000,  8,   0),
    (69, 24, 102, 3, 12000, 20,   0),
    (70, 25, 102, 1,  6000, 26, 120),
    (71, 25, 102, 2,  4000, 14,   0)
) AS t(loan_id, client_id, office_id, product_id, principal, vintage_months, overdue_days);

ALTER TABLE tmp_loan ADD COLUMN repaid_frac          numeric(10,8);
ALTER TABLE tmp_loan ADD COLUMN principal_repaid      numeric(19,6);
ALTER TABLE tmp_loan ADD COLUMN principal_outstanding numeric(19,6);
ALTER TABLE tmp_loan ADD COLUMN interest_charged      numeric(19,6);
ALTER TABLE tmp_loan ADD COLUMN interest_repaid       numeric(19,6);
ALTER TABLE tmp_loan ADD COLUMN interest_outstanding  numeric(19,6);
ALTER TABLE tmp_loan ADD COLUMN total_outstanding     numeric(19,6);
ALTER TABLE tmp_loan ADD COLUMN disburse_date         date;
ALTER TABLE tmp_loan ADD COLUMN mature_date           date;

UPDATE tmp_loan SET
    repaid_frac   = LEAST(vintage_months::numeric / term_months, 0.90)
                    * CASE WHEN overdue_days > 0 THEN 0.55 ELSE 1.0 END,
    disburse_date = current_date - (vintage_months * 30)::int,
    mature_date   = current_date + ((term_months - vintage_months) * 30)::int;

UPDATE tmp_loan SET
    principal_repaid      = ROUND(principal_amount * repaid_frac, 6),
    principal_outstanding = ROUND(principal_amount * (1 - repaid_frac), 6),
    interest_charged      = ROUND(principal_amount * annual_rate, 6),
    interest_repaid       = ROUND(principal_amount * annual_rate * repaid_frac, 6),
    interest_outstanding  = ROUND(principal_amount * annual_rate * (1 - repaid_frac), 6);

UPDATE tmp_loan SET
    total_outstanding = principal_outstanding + interest_outstanding;

INSERT INTO public.m_loan (
    id, account_no, client_id, product_id,
    loan_status_id, loan_type_enum,
    currency_code, currency_digits, currency_multiplesof,
    principal_amount_proposed, principal_amount,
    approved_principal, net_disbursal_amount,
    annual_nominal_interest_rate, nominal_interest_rate_per_period,
    interest_method_enum, interest_calculated_in_period_enum,
    term_frequency, term_period_frequency_enum,
    repay_every, repayment_period_frequency_enum, number_of_repayments,
    amortization_method_enum,
    submittedon_date, approvedon_date,
    expected_disbursedon_date, disbursedon_date,
    expected_firstrepaymenton_date, expected_maturedon_date,
    principal_disbursed_derived, principal_repaid_derived,
    principal_writtenoff_derived, principal_outstanding_derived,
    interest_charged_derived, interest_repaid_derived,
    interest_waived_derived, interest_writtenoff_derived,
    interest_outstanding_derived,
    fee_charges_charged_derived, fee_charges_repaid_derived,
    fee_charges_waived_derived, fee_charges_writtenoff_derived,
    fee_charges_outstanding_derived,
    penalty_charges_charged_derived, penalty_charges_repaid_derived,
    penalty_charges_waived_derived, penalty_charges_writtenoff_derived,
    penalty_charges_outstanding_derived,
    total_expected_repayment_derived, total_repayment_derived,
    total_expected_costofloan_derived, total_costofloan_derived,
    total_waived_derived, total_writtenoff_derived, total_outstanding_derived,
    loan_counter, is_npa,
    loan_transaction_strategy_code, loan_transaction_strategy_name,
    loan_schedule_type, loan_schedule_processing_type,
    created_on_utc, created_by, last_modified_by, last_modified_on_utc
)
SELECT
    l.loan_id,
    'LN' || LPAD(l.loan_id::text, 7, '0'),
    l.client_id, l.product_id,
    300, 1,
    'USD', 2, 0,
    l.principal_amount, l.principal_amount,
    l.principal_amount, l.principal_amount,
    ROUND(l.annual_rate * 100, 4),
    ROUND(l.annual_rate * 100 / 12, 4),
    0, 1,
    l.term_months, 2,
    1, 2, l.term_months,
    1,
    l.disburse_date - 5, l.disburse_date - 3,
    l.disburse_date,     l.disburse_date,
    l.disburse_date + 30, l.mature_date,
    l.principal_amount,
    l.principal_repaid,
    0,
    l.principal_outstanding,
    l.interest_charged,
    l.interest_repaid,
    0, 0,
    l.interest_outstanding,
    0, 0, 0, 0, 0,
    0, 0, 0, 0, 0,
    ROUND(l.principal_amount * (1 + l.annual_rate), 6),
    ROUND(l.principal_repaid + l.interest_repaid, 6),
    ROUND(l.principal_amount * l.annual_rate, 6),
    ROUND(l.interest_repaid, 6),
    0, 0,
    l.total_outstanding,
    1,
    l.overdue_days >= 90,
    'mifos-standard-strategy',
    'Penalties, Fees, Interest, Principal order',
    'CUMULATIVE', 'HORIZONTAL',
    NOW(), 1, 1, NOW()
FROM tmp_loan l;

INSERT INTO public.m_loan_transaction (
    loan_id, office_id, is_reversed,
    transaction_type_enum, transaction_date, amount,
    principal_portion_derived, interest_portion_derived,
    fee_charges_portion_derived, penalty_charges_portion_derived,
    outstanding_loan_balance_derived,
    submitted_on_date, created_on_utc, created_by, last_modified_by, last_modified_on_utc
)
SELECT
    l.loan_id, l.office_id, FALSE,
    1, l.disburse_date, l.principal_amount,
    l.principal_amount, 0, 0, 0,
    l.principal_amount,
    l.disburse_date, NOW(), 1, 1, NOW()
FROM tmp_loan l;

INSERT INTO public.m_loan_transaction (
    loan_id, office_id, is_reversed,
    transaction_type_enum, transaction_date, amount,
    principal_portion_derived, interest_portion_derived,
    fee_charges_portion_derived, penalty_charges_portion_derived,
    outstanding_loan_balance_derived,
    submitted_on_date, created_on_utc, created_by, last_modified_by, last_modified_on_utc
)
SELECT
    l.loan_id, l.office_id, FALSE,
    2,
    l.disburse_date + (m.mn * 30),
    ROUND((l.principal_amount / l.term_months) + (l.principal_amount * l.annual_rate / 12), 6),
    ROUND(l.principal_amount / l.term_months, 6),
    ROUND(l.principal_amount * l.annual_rate / 12, 6),
    0, 0,
    GREATEST(l.principal_amount - ROUND(l.principal_amount / l.term_months, 6) * m.mn, 0),
    l.disburse_date + (m.mn * 30),
    NOW(), 1, 1, NOW()
FROM tmp_loan l
CROSS JOIN generate_series(1,
    CASE
        WHEN l.overdue_days = 0
            THEN LEAST(l.vintage_months, l.term_months)
        ELSE
            GREATEST(l.vintage_months - CEIL(l.overdue_days::numeric / 30)::int, 1)
    END
) AS m(mn);

INSERT INTO public.m_loan_delinquency_tag_history (
    delinquency_range_id, loan_id,
    addedon_date, liftedon_date,
    created_by, created_on_utc, version, last_modified_by, last_modified_on_utc
)
SELECT
    CASE
        WHEN l.overdue_days BETWEEN  1 AND  30 THEN 101
        WHEN l.overdue_days BETWEEN 31 AND  60 THEN 102
        WHEN l.overdue_days BETWEEN 61 AND  90 THEN 103
        ELSE 104
    END,
    l.loan_id,
    current_date - l.overdue_days,
    NULL,
    1, NOW(), 1, 1, NOW()
FROM tmp_loan l
WHERE l.overdue_days > 0;

INSERT INTO public.batch_job_instance (job_instance_id, version, job_name, job_key)
VALUES (9001, 1, 'LOAN_COB', md5('LOAN_COB_SEED'))
ON CONFLICT DO NOTHING;

INSERT INTO public.batch_job_execution
    (job_execution_id, version, job_instance_id, status,
     create_time, start_time, end_time,
     exit_code, exit_message, last_updated)
VALUES
    (9001, 1, 9001, 'COMPLETED',
     NOW() - INTERVAL '45 minutes',
     NOW() - INTERVAL '45 minutes',
     NOW() - INTERVAL '5 minutes',
     'COMPLETED', '',
     NOW() - INTERVAL '5 minutes');

DROP TABLE tmp_loan;

COMMIT;

SELECT entity, cnt FROM (
    SELECT 'offices'       AS entity, COUNT(*) AS cnt FROM public.m_office WHERE id >= 100  UNION ALL
    SELECT 'clients',                 COUNT(*)         FROM public.m_client WHERE id >= 100  UNION ALL
    SELECT 'loan_products',           COUNT(*)         FROM public.m_product_loan WHERE id >= 100 UNION ALL
    SELECT 'loans',                   COUNT(*)         FROM public.m_loan WHERE id >= 100    UNION ALL
    SELECT 'transactions',            COUNT(*)         FROM public.m_loan_transaction WHERE loan_id >= 100 UNION ALL
    SELECT 'delinq_tags',             COUNT(*)         FROM public.m_loan_delinquency_tag_history WHERE loan_id >= 100 UNION ALL
    SELECT 'delinq_ranges',           COUNT(*)         FROM public.m_delinquency_range WHERE id >= 100
) s ORDER BY entity;
