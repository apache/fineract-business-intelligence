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
DELETE FROM public.m_loan_repayment_schedule        WHERE loan_id IN (SELECT id FROM public.m_loan WHERE client_id >= 100);
DELETE FROM public.m_loan                           WHERE client_id >= 100;
DELETE FROM public.m_client                         WHERE id >= 100;
DELETE FROM public.m_product_loan                   WHERE id >= 100;
DELETE FROM public.m_delinquency_bucket_mappings    WHERE id >= 100;
DELETE FROM public.m_delinquency_range              WHERE id >= 100;
DELETE FROM public.m_delinquency_bucket             WHERE id >= 100;
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
    (101, 'MSME', 'USD', 2, 0,  8000, 500, 50000, 0, 'MSME Loan',
     1.5, 18, 0, 1, 1, 2, 36, 1, 1, 90, 101,
     'mifos-standard-strategy', 'Penalties, Fees, Interest, Principal order',
     'CUMULATIVE', 'HORIZONTAL', 1),
    (102, 'AGRI', 'USD', 2, 0,  5000, 500, 20000, 0, 'Agriculture Loan',
     1.2, 14, 0, 1, 1, 2, 36, 1, 1, 90, 101,
     'mifos-standard-strategy', 'Penalties, Fees, Interest, Principal order',
     'CUMULATIVE', 'HORIZONTAL', 1),
    (103, 'HOUS', 'USD', 2, 0, 15000, 5000, 75000, 0, 'Housing Loan',
     1.0, 12, 0, 1, 1, 2, 60, 1, 1, 90, 101,
     'mifos-standard-strategy', 'Penalties, Fees, Interest, Principal order',
     'CUMULATIVE', 'HORIZONTAL', 1),
    (104, 'EMRG', 'USD', 2, 0,  2000, 300,  5000, 0, 'Emergency Loan',
     2.0, 24, 0, 1, 1, 2, 24, 1, 1, 90, 101,
     'mifos-standard-strategy', 'Penalties, Fees, Interest, Principal order',
     'CUMULATIVE', 'HORIZONTAL', 1);

INSERT INTO public.m_client (
    id, account_no, status_enum, activation_date, office_joining_date,
    office_id, gender_cv_id, date_of_birth, legal_form_enum,
    display_name, submittedon_date,
    created_on_utc, created_by, last_modified_by, last_modified_on_utc
)
SELECT
    100 + s.id,
    'CL' || LPAD((100 + s.id)::text, 6, '0'),
    300,
    '2022-01-01'::date + (s.id * 5)::int,
    '2022-01-01'::date + (s.id * 5)::int,
    CASE WHEN s.id <= 30 THEN 1 WHEN s.id <= 55 THEN 101 ELSE 102 END,
    NULL,
    current_date - ((28 + (s.id % 20)) * 365)::int,
    1,
    'Client ' || (100 + s.id),
    '2022-01-01'::date + (s.id * 5)::int - 3,
    NOW(), 1, 1, NOW()
FROM generate_series(1, 80) AS s(id);

CREATE TEMP TABLE tmp_loan AS
SELECT
    (100 + loan_id)::bigint    AS loan_id,
    (100 + client_id)::bigint  AS client_id,
    office_id::bigint,
    (100 + product_id)::bigint AS product_id,
    principal::numeric(19,6)   AS principal_amount,
    disburse_date::date        AS disburse_date,
    overdue_days::int          AS overdue_days,
    term_months::int,
    annual_rate::numeric(10,6)
FROM (VALUES
( 1,  1,   1, 1, 12000, '2024-12-23',  0, 12, 0.18),
( 2,  1,   1, 2,  8000, '2024-12-23',  0, 12, 0.14),
( 3, 31, 101, 1, 10000, '2024-12-23',  0, 12, 0.18),
( 4, 32, 101, 3, 20000, '2024-12-23', 15, 12, 0.12),
( 5, 56, 102, 1, 11000, '2024-12-23',  0, 12, 0.18),
( 6, 32, 101, 2,  7000, '2024-12-23',  0, 18, 0.14),
( 7, 31, 101, 1,  9000, '2024-12-23',  0, 18, 0.18),
( 8, 56, 102, 3, 18000, '2024-12-23', 45, 18, 0.12),
( 9,  2,   1, 1, 13000, '2025-01-10',  0, 12, 0.18),
(10,  2,   1, 2,  8500, '2025-01-10',  0, 12, 0.14),
(11, 33, 101, 3, 22000, '2025-01-10', 20, 12, 0.12),
(12, 57, 102, 1, 10500, '2025-01-10',  0, 12, 0.18),
(13, 58, 102, 2,  7500, '2025-01-10',  0, 18, 0.14),
(14, 57, 102, 1,  9500, '2025-01-10',  0, 18, 0.18),
(15, 33, 101, 2,  6500, '2025-01-10',  0, 12, 0.14),
(16, 58, 102, 3, 17000, '2025-01-10', 35, 18, 0.12),
(17,  4,   1, 1, 12500, '2025-02-10',  0, 12, 0.18),
(18,  4,   1, 2,  8000, '2025-02-10',  0, 12, 0.14),
(19, 34, 101, 3, 21000, '2025-02-10', 25, 18, 0.12),
(20, 59, 102, 1, 10000, '2025-02-10',  0, 12, 0.18),
(21,  3,   1, 1,  9000, '2025-02-10',  0, 18, 0.18),
(22, 34, 101, 2,  7000, '2025-02-10',  0, 12, 0.14),
(23, 59, 102, 3, 19000, '2025-02-10', 50, 18, 0.12),
(24,  3,   1, 1, 11000, '2025-02-10',  0, 12, 0.18),
(25,  5,   1, 1, 13000, '2025-03-10',  0, 12, 0.18),
(26, 35, 101, 2,  8500, '2025-03-10',  0, 12, 0.14),
(27, 35, 101, 3, 23000, '2025-03-10', 30, 18, 0.12),
(28, 60, 102, 1, 10500, '2025-03-10',  0, 12, 0.18),
(29, 60, 102, 1,  9500, '2025-03-10',  0, 18, 0.18),
(30,  5,   1, 2,  7500, '2025-03-10',  0, 12, 0.14),
(31, 61, 102, 3, 20000, '2025-03-10', 40, 18, 0.12),
(32, 61, 102, 1, 12000, '2025-03-10',  0, 12, 0.18),
(33,  7,   1, 1, 12000, '2025-04-10',  0, 12, 0.18),
(34,  7,   1, 2,  8000, '2025-04-10',  0, 12, 0.14),
(35, 36, 101, 3, 22000, '2025-04-10', 10, 18, 0.12),
(36, 62, 102, 1,  9500, '2025-04-10',  0, 18, 0.18),
(37,  6,   1, 1,  8500, '2025-04-10',  0, 12, 0.18),
(38, 36, 101, 2,  7000, '2025-04-10',  0, 12, 0.14),
(39, 62, 102, 3, 19000, '2025-04-10', 55, 18, 0.12),
(40,  6,   1, 1, 11000, '2025-04-10',  0, 12, 0.18),
(41,  8,   1, 1, 13000, '2025-05-10',  0, 12, 0.18),
(42, 38, 101, 2,  8500, '2025-05-10',  0, 18, 0.14),
(43, 63, 102, 3, 24000, '2025-05-10',  5, 18, 0.12),
(44,  8,   1, 1,  9500, '2025-05-10',  0, 12, 0.18),
(45, 37, 101, 1,  8000, '2025-05-10',  0, 12, 0.18),
(46, 63, 102, 2,  7500, '2025-05-10',  0, 18, 0.14),
(47, 38, 101, 3, 21000, '2025-05-10', 25, 18, 0.12),
(48, 37, 101, 1, 12000, '2025-05-10',  0, 12, 0.18),
(49,  9,   1, 1, 12500, '2025-06-10',  0, 12, 0.18),
(50, 40, 101, 2,  8000, '2025-06-10',  0, 12, 0.14),
(51, 64, 102, 3, 23000, '2025-06-10', 35, 18, 0.12),
(52,  9,   1, 1,  9000, '2025-06-10',  0, 12, 0.18),
(53, 39, 101, 1,  7500, '2025-06-10',  0, 18, 0.18),
(54, 64, 102, 2,  7000, '2025-06-10',  0, 12, 0.14),
(55, 40, 101, 3, 20000, '2025-06-10', 45, 18, 0.12),
(56, 39, 101, 1, 11000, '2025-06-10',  0, 12, 0.18),
(57, 10,   1, 1, 11000, '2025-07-10',  0, 12, 0.18),
(58, 42, 101, 2,  7500, '2025-07-10',  0, 12, 0.14),
(59, 65, 102, 3, 22000, '2025-07-10', 15, 18, 0.12),
(60, 10,   1, 1,  9000, '2025-07-10',  0, 12, 0.18),
(61, 41, 101, 1,  7000, '2025-07-10',  0, 18, 0.18),
(62, 65, 102, 2,  6500, '2025-07-10',  0, 12, 0.14),
(63, 42, 101, 3, 19000, '2025-07-10', 60, 18, 0.12),
(64, 41, 101, 1, 10000, '2025-07-10',  0, 12, 0.18),
(65, 11,   1, 1, 12000, '2025-08-10',  0, 12, 0.18),
(66, 44, 101, 2,  8000, '2025-08-10',  0, 12, 0.14),
(67, 66, 102, 3, 21000, '2025-08-10', 20, 18, 0.12),
(68, 11,   1, 1,  8500, '2025-08-10',  0, 12, 0.18),
(69, 43, 101, 1,  7500, '2025-08-10',  0, 18, 0.18),
(70, 66, 102, 2,  7000, '2025-08-10',  0, 12, 0.14),
(71, 44, 101, 3, 20000, '2025-08-10', 30, 18, 0.12),
(72, 43, 101, 1, 11000, '2025-08-10',  0, 12, 0.18),
(73, 12,   1, 1, 12000, '2025-09-10',  0, 12, 0.18),
(74, 46, 101, 2,  8000, '2025-09-10',  0, 12, 0.14),
(75, 67, 102, 3, 22000, '2025-09-10', 10, 18, 0.12),
(76, 12,   1, 1,  9000, '2025-09-10',  0, 12, 0.18),
(77, 45, 101, 1,  8000, '2025-09-10',  0, 18, 0.18),
(78, 67, 102, 2,  7000, '2025-09-10',  0, 12, 0.14),
(79, 46, 101, 3, 20000, '2025-09-10', 50, 18, 0.12),
(80, 45, 101, 1, 11000, '2025-09-10',  0, 12, 0.18),
(81, 13,   1, 1, 13000, '2025-10-10',  0, 12, 0.18),
(82, 48, 101, 3, 23000, '2025-10-10', 25, 18, 0.12),
(83, 68, 102, 2,  8500, '2025-10-10',  0, 12, 0.14),
(84, 13,   1, 1,  9000, '2025-10-10',  0, 12, 0.18),
(85, 47, 101, 1,  8000, '2025-10-10',  0, 18, 0.18),
(86, 68, 102, 2,  7000, '2025-10-10',  0, 12, 0.14),
(87, 48, 101, 3, 21000, '2025-10-10', 40, 18, 0.12),
(88, 47, 101, 1, 12000, '2025-10-10',  0, 12, 0.18),
(89, 14,   1, 1, 12000, '2025-11-10',  0, 12, 0.18),
(90, 50, 101, 3, 22000, '2025-11-10',  5, 18, 0.12),
(91, 69, 102, 2,  8000, '2025-11-10',  0, 12, 0.14),
(92, 14,   1, 1,  9500, '2025-11-10',  0, 12, 0.18),
(93, 49, 101, 1,  8500, '2025-11-10',  0, 18, 0.18),
(94, 69, 102, 2,  7000, '2025-11-10',  0, 12, 0.14),
(95, 50, 101, 3, 20000, '2025-11-10', 35, 18, 0.12),
(96, 49, 101, 1, 11000, '2025-11-10',  0, 12, 0.18),
(97, 15,   1, 1, 12500, '2025-12-10',  0, 12, 0.18),
(98, 52, 101, 3, 21000, '2025-12-10', 15, 18, 0.12),
(99, 70, 102, 1,  9000, '2025-12-10',  0, 12, 0.18),
(100, 15,   1, 2,  8000, '2025-12-10',  0, 12, 0.14),
(101, 51, 101, 1,  7500, '2025-12-10',  0, 18, 0.18),
(102, 70, 102, 2,  7000, '2025-12-10',  0, 12, 0.14),
(103, 52, 101, 3, 19000, '2025-12-10', 55, 18, 0.12),
(104, 51, 101, 1, 10000, '2025-12-10',  0, 12, 0.18),
(105, 16,   1, 3, 22000, '2026-01-10',  0, 18, 0.12),
(106, 54, 101, 2,  8500, '2026-01-10',  0, 12, 0.14),
(107, 71, 102, 1,  9000, '2026-01-10',  0, 12, 0.18),
(108, 16,   1, 1,  8000, '2026-01-10',  0, 12, 0.18),
(109, 54, 101, 2,  7500, '2026-01-10',  0, 18, 0.14),
(110, 71, 102, 3, 20000, '2026-01-10', 20, 18, 0.12),
(111, 53, 101, 1, 11000, '2026-01-10',  0, 12, 0.18),
(112, 53, 101, 1, 10000, '2026-01-10',  0, 12, 0.18),
(113, 55, 101, 3, 21000, '2026-02-10',  0, 18, 0.12),
(114, 72, 102, 1,  9000, '2026-02-10',  0, 12, 0.18),
(115, 17,   1, 2,  8000, '2026-02-10',  0, 12, 0.14),
(116, 55, 101, 1,  7500, '2026-02-10',  0, 18, 0.18),
(117, 73, 102, 2,  7000, '2026-02-10',  0, 12, 0.14),
(118, 17,   1, 3, 20000, '2026-02-10', 30, 18, 0.12),
(119, 73, 102, 1, 11000, '2026-02-10',  0, 12, 0.18),
(120, 72, 102, 1, 10000, '2026-02-10',  0, 12, 0.18),
(121, 18,   1, 1,  9500, '2026-03-10',  0, 12, 0.18),
(122, 32, 101, 2,  8000, '2026-03-10',  0, 18, 0.14),
(123, 74, 102, 1,  7500, '2026-03-10',  0, 12, 0.18),
(124, 18,   1, 3, 22000, '2026-03-10', 10, 18, 0.12),
(125, 31, 101, 1, 12000, '2026-03-10',  0, 12, 0.18),
(126, 74, 102, 2,  8500, '2026-03-10',  0, 12, 0.14),
(127, 31, 101, 1, 10000, '2026-03-10',  0, 18, 0.18),
(128, 32, 101, 3, 20000, '2026-03-10', 25, 18, 0.12),
(129, 19,   1, 1,  9000, '2026-04-10',  0, 12, 0.18),
(130, 34, 101, 2,  8000, '2026-04-10',  0, 12, 0.14),
(131, 75, 102, 3, 21000, '2026-04-10',  0, 18, 0.12),
(132, 19,   1, 1, 12000, '2026-04-10',  0, 12, 0.18),
(133, 33, 101, 1, 10000, '2026-04-10',  0, 18, 0.18),
(134, 75, 102, 2,  7500, '2026-04-10',  0, 12, 0.14),
(135, 34, 101, 3, 19000, '2026-04-10', 40, 18, 0.12),
(136, 33, 101, 1, 11000, '2026-04-10',  0, 12, 0.18),
(137, 20,   1, 1,  9500, '2026-05-10',  0, 12, 0.18),
(138, 35, 101, 2,  8500, '2026-05-10',  0, 18, 0.14),
(139, 76, 102, 3, 22000, '2026-05-10',  0, 18, 0.12),
(140, 20,   1, 1, 13000, '2026-05-10',  0, 12, 0.18),
(141, 35, 101, 1, 11000, '2026-05-10',  0, 12, 0.18),
(142, 76, 102, 2,  8000, '2026-05-10',  0, 18, 0.14),
(143, 36, 101, 3, 20000, '2026-06-10',  0, 18, 0.12),
(144, 36, 101, 1, 10000, '2026-06-10',  0, 12, 0.18)
) AS t(loan_id, client_id, office_id, product_id, principal, disburse_date, overdue_days, term_months, annual_rate);

ALTER TABLE tmp_loan ADD COLUMN vintage_months       int;
ALTER TABLE tmp_loan ADD COLUMN mature_date           date;
ALTER TABLE tmp_loan ADD COLUMN principal_repaid      numeric(19,6);
ALTER TABLE tmp_loan ADD COLUMN principal_outstanding numeric(19,6);
ALTER TABLE tmp_loan ADD COLUMN interest_charged      numeric(19,6);
ALTER TABLE tmp_loan ADD COLUMN interest_repaid       numeric(19,6);
ALTER TABLE tmp_loan ADD COLUMN interest_outstanding  numeric(19,6);
ALTER TABLE tmp_loan ADD COLUMN total_outstanding     numeric(19,6);

UPDATE tmp_loan SET
    vintage_months = GREATEST(
        EXTRACT(YEAR FROM AGE(current_date, disburse_date))::int * 12
        + EXTRACT(MONTH FROM AGE(current_date, disburse_date))::int,
        1
    ),
    mature_date = disburse_date + (term_months * 30)::int;

UPDATE tmp_loan SET
    interest_charged = ROUND(principal_amount * annual_rate, 6);

UPDATE tmp_loan SET
    principal_repaid = CASE
        WHEN overdue_days = 0
            THEN ROUND(principal_amount * LEAST(vintage_months::numeric / term_months, 1.0) * 0.90, 6)
        ELSE
            ROUND(principal_amount * LEAST(vintage_months::numeric / term_months, 1.0) * 0.35, 6)
    END,
    interest_repaid = CASE
        WHEN overdue_days = 0
            THEN ROUND(interest_charged * LEAST(vintage_months::numeric / term_months, 1.0) * 0.90, 6)
        ELSE
            ROUND(interest_charged * LEAST(vintage_months::numeric / term_months, 1.0) * 0.35, 6)
    END;

UPDATE tmp_loan SET
    principal_outstanding = ROUND(principal_amount - principal_repaid, 6),
    interest_outstanding = ROUND(interest_charged - interest_repaid, 6);

UPDATE tmp_loan SET total_outstanding = principal_outstanding + interest_outstanding;

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
    ROUND(l.principal_amount + l.interest_charged, 6),
    ROUND(l.principal_repaid + l.interest_repaid, 6),
    ROUND(l.interest_charged, 6),
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
    loan_id, office_id, is_reversed, transaction_type_enum, transaction_date, amount,
    principal_portion_derived, interest_portion_derived,
    fee_charges_portion_derived, penalty_charges_portion_derived,
    overpayment_portion_derived,
    outstanding_loan_balance_derived,
    submitted_on_date, created_on_utc, created_by, last_modified_by, last_modified_on_utc
)
SELECT
    l.loan_id, l.office_id, FALSE,
    1, l.disburse_date, l.principal_amount,
    l.principal_amount, 0, 0, 0, 0,
    l.principal_amount,
    l.disburse_date, NOW(), 1, 1, NOW()
FROM tmp_loan l;

INSERT INTO public.m_loan_transaction (
    loan_id, office_id, is_reversed, transaction_type_enum, transaction_date, amount,
    principal_portion_derived, interest_portion_derived,
    fee_charges_portion_derived, penalty_charges_portion_derived,
    overpayment_portion_derived,
    outstanding_loan_balance_derived,
    submitted_on_date, created_on_utc, created_by, last_modified_by, last_modified_on_utc
)
SELECT
    l.loan_id, l.office_id, FALSE,
    2,
    l.disburse_date + (m.mn * 30),
    (
        CASE
            WHEN l.loan_id % 5 = 0 AND split_part = 1
                THEN ROUND(l.principal_amount / l.term_months * 0.60, 6)
            WHEN l.loan_id % 5 = 0 AND split_part = 2
                THEN ROUND(l.principal_amount / l.term_months * 0.40, 6)
            ELSE
                ROUND(l.principal_amount / l.term_months, 6)
        END
        +
        CASE
            WHEN l.loan_id % 5 = 0 AND split_part = 1
                THEN ROUND(l.interest_charged / 12 * 0.60, 6)
            WHEN l.loan_id % 5 = 0 AND split_part = 2
                THEN ROUND(l.interest_charged / 12 * 0.40, 6)
            ELSE
                ROUND(l.interest_charged / 12, 6)
        END
        +
        CASE
            WHEN l.loan_id % 3 = 0
                THEN
                    CASE
                        WHEN l.loan_id % 5 = 0 AND split_part = 1 THEN 6.00
                        WHEN l.loan_id % 5 = 0 AND split_part = 2 THEN 4.00
                        ELSE 10.00
                    END
            ELSE 0
        END
    ),
    CASE
        WHEN l.loan_id % 5 = 0 AND split_part = 1
            THEN ROUND(l.principal_amount / l.term_months * 0.60, 6)
        WHEN l.loan_id % 5 = 0 AND split_part = 2
            THEN ROUND(l.principal_amount / l.term_months * 0.40, 6)
        ELSE
            ROUND(l.principal_amount / l.term_months, 6)
    END,
    CASE
        WHEN l.loan_id % 5 = 0 AND split_part = 1
            THEN ROUND(l.interest_charged / 12 * 0.60, 6)
        WHEN l.loan_id % 5 = 0 AND split_part = 2
            THEN ROUND(l.interest_charged / 12 * 0.40, 6)
        ELSE
            ROUND(l.interest_charged / 12, 6)
    END,
    CASE
        WHEN l.loan_id % 3 = 0
            THEN
                CASE
                    WHEN l.loan_id % 5 = 0 AND split_part = 1 THEN 6.00
                    WHEN l.loan_id % 5 = 0 AND split_part = 2 THEN 4.00
                    ELSE 10.00
                END
        ELSE 0
    END,
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
) AS m(mn)
CROSS JOIN (SELECT 1 AS split_part UNION SELECT 2) AS sp
WHERE (l.disburse_date + (m.mn * 30) <= current_date)
  AND (l.loan_id % 5 = 0 OR sp.split_part = 1);

INSERT INTO public.m_loan_repayment_schedule (
    loan_id, fromdate, duedate, installment,
    principal_amount, principal_completed_derived,
    interest_amount, interest_completed_derived, interest_waived_derived,
    fee_charges_amount, fee_charges_completed_derived, fee_charges_waived_derived,
    penalty_charges_amount, penalty_charges_completed_derived, penalty_charges_waived_derived,
    total_paid_in_advance_derived, total_paid_late_derived,
    completed_derived, obligations_met_on_date, is_re_aged,
    created_by, last_modified_by, created_on_utc, last_modified_on_utc, lastmodified_date
)
SELECT
    l.loan_id,
    l.disburse_date + ((m.mn - 1) * 30),
    l.disburse_date + (m.mn * 30),
    m.mn,
    ROUND(l.principal_amount / l.term_months, 6),
    CASE
        WHEN (l.disburse_date + (m.mn * 30)) < (current_date - l.overdue_days)
            THEN ROUND(l.principal_amount / l.term_months, 6)
        ELSE 0
    END,
    ROUND(l.interest_charged / 12, 6),
    CASE
        WHEN (l.disburse_date + (m.mn * 30)) < (current_date - l.overdue_days)
            THEN ROUND(l.interest_charged / 12, 6)
        ELSE 0
    END,
    0,
    CASE WHEN l.loan_id % 3 = 0 THEN 10.00 ELSE 0 END,
    CASE WHEN l.loan_id % 3 = 0 AND (l.disburse_date + (m.mn * 30)) < (current_date - l.overdue_days)
        THEN 10.00 ELSE 0 END,
    0,
    CASE
        WHEN l.overdue_days > 0
         AND m.mn = GREATEST(l.vintage_months - CEIL(l.overdue_days::numeric / 30)::int, 1)
            THEN ROUND(l.overdue_days::numeric * l.annual_rate * (l.principal_amount / l.term_months) / 365, 6)
        WHEN l.overdue_days > 0
         AND m.mn > GREATEST(l.vintage_months - CEIL(l.overdue_days::numeric / 30)::int, 1)
         AND (l.disburse_date + (m.mn * 30)) <= current_date
            THEN ROUND((current_date - (l.disburse_date + (m.mn * 30)))::numeric * l.annual_rate * (l.principal_amount / l.term_months) / 365, 6)
        ELSE 0
    END,
    CASE
        WHEN (l.disburse_date + (m.mn * 30)) < (current_date - l.overdue_days)
            THEN
                CASE
                    WHEN l.overdue_days > 0
                     AND m.mn = GREATEST(l.vintage_months - CEIL(l.overdue_days::numeric / 30)::int, 1)
                        THEN
                            CASE
                                WHEN l.loan_id % 4 = 0
                                    THEN ROUND(0.50 * l.overdue_days::numeric * l.annual_rate * (l.principal_amount / l.term_months) / 365, 6)
                                ELSE ROUND(l.overdue_days::numeric * l.annual_rate * (l.principal_amount / l.term_months) / 365, 6)
                            END
                    ELSE 0
                END
        ELSE 0
    END,
    CASE
        WHEN (l.disburse_date + (m.mn * 30)) < (current_date - l.overdue_days)
            THEN
                CASE
                    WHEN l.overdue_days > 0
                     AND m.mn = GREATEST(l.vintage_months - CEIL(l.overdue_days::numeric / 30)::int, 1)
                     AND l.loan_id % 4 = 0
                        THEN ROUND(0.50 * l.overdue_days::numeric * l.annual_rate * (l.principal_amount / l.term_months) / 365, 6)
                    ELSE 0
                END
        WHEN (l.disburse_date + (m.mn * 30)) >= (current_date - l.overdue_days)
         AND (l.disburse_date + (m.mn * 30)) <= current_date
         AND l.loan_id % 4 = 0
            THEN ROUND(0.50 * (current_date - (l.disburse_date + (m.mn * 30)))::numeric * l.annual_rate * (l.principal_amount / l.term_months) / 365, 6)
        ELSE 0
    END,
    CASE
        WHEN (l.disburse_date + (m.mn * 30)) < (current_date - l.overdue_days)
             AND l.overdue_days = 0
             AND m.mn % 5 = 0
            THEN ROUND((l.principal_amount / l.term_months) + (l.interest_charged / 12), 6)
        ELSE 0
    END,
    CASE
        WHEN l.overdue_days > 0
         AND m.mn = GREATEST(l.vintage_months - CEIL(l.overdue_days::numeric / 30)::int, 1)
            THEN ROUND(l.overdue_days::numeric * l.annual_rate * (l.principal_amount / l.term_months) / 365, 6)
        WHEN l.overdue_days > 0
         AND m.mn > GREATEST(l.vintage_months - CEIL(l.overdue_days::numeric / 30)::int, 1)
         AND (l.disburse_date + (m.mn * 30)) <= current_date
            THEN ROUND((current_date - (l.disburse_date + (m.mn * 30)))::numeric * l.annual_rate * (l.principal_amount / l.term_months) / 365, 6)
        ELSE 0
    END,
    (l.disburse_date + (m.mn * 30)) < (current_date - l.overdue_days),
    CASE
        WHEN (l.disburse_date + (m.mn * 30)) < (current_date - l.overdue_days)
             AND l.overdue_days > 0
             AND m.mn = GREATEST(l.vintage_months - CEIL(l.overdue_days::numeric / 30)::int, 1)
            THEN l.disburse_date + (m.mn * 30) + l.overdue_days
        WHEN (l.disburse_date + (m.mn * 30)) < (current_date - l.overdue_days)
             AND l.overdue_days = 0
             AND m.mn % 5 = 0
            THEN l.disburse_date + (m.mn * 30) - 5
        WHEN (l.disburse_date + (m.mn * 30)) < (current_date - l.overdue_days)
            THEN l.disburse_date + (m.mn * 30)
        ELSE NULL
    END,
    (l.loan_id % 20 = 0 AND m.mn = 3),
    1, 1,
    NOW(), NOW(),
    NOW()
FROM tmp_loan l
CROSS JOIN generate_series(1, l.term_months) AS m(mn);

INSERT INTO public.m_loan_transaction (
    loan_id, office_id, is_reversed, transaction_type_enum, transaction_date, amount,
    principal_portion_derived, interest_portion_derived,
    fee_charges_portion_derived, penalty_charges_portion_derived,
    overpayment_portion_derived,
    outstanding_loan_balance_derived,
    submitted_on_date, created_on_utc, created_by, last_modified_by, last_modified_on_utc
)
SELECT
    l.loan_id, l.office_id, FALSE,
    8,
    current_date - l.overdue_days + 30,
    ROUND(l.principal_amount * 0.20, 6),
    ROUND(l.principal_amount * 0.20, 6),
    0, 0, 0,
    0,
    GREATEST(l.principal_amount - ROUND(l.principal_amount * 0.20, 6), 0),
    current_date - l.overdue_days + 30,
    NOW(), 1, 1, NOW()
FROM tmp_loan l
WHERE l.overdue_days >= 45;

INSERT INTO public.m_loan_delinquency_tag_history (
    delinquency_range_id, loan_id, addedon_date, liftedon_date,
    created_by, created_on_utc, version, last_modified_by, last_modified_on_utc
)
SELECT
    101,
    l.loan_id,
    l.disburse_date + 90,
    CASE
        WHEN l.overdue_days >= 30  THEN l.disburse_date + 120
        ELSE NULL
    END,
    1, NOW(), 1, 1, NOW()
FROM tmp_loan l
WHERE l.overdue_days > 0;

INSERT INTO public.m_loan_delinquency_tag_history (
    delinquency_range_id, loan_id, addedon_date, liftedon_date,
    created_by, created_on_utc, version, last_modified_by, last_modified_on_utc
)
SELECT
    102,
    l.loan_id,
    l.disburse_date + 120,
    CASE
        WHEN l.overdue_days >= 60  THEN l.disburse_date + 150
        ELSE NULL
    END,
    1, NOW(), 1, 1, NOW()
FROM tmp_loan l
WHERE l.overdue_days >= 30;

INSERT INTO public.m_loan_delinquency_tag_history (
    delinquency_range_id, loan_id, addedon_date, liftedon_date,
    created_by, created_on_utc, version, last_modified_by, last_modified_on_utc
)
SELECT
    103,
    l.loan_id,
    l.disburse_date + 150,
    CASE
        WHEN l.overdue_days >= 90  THEN l.disburse_date + 180
        ELSE NULL
    END,
    1, NOW(), 1, 1, NOW()
FROM tmp_loan l
WHERE l.overdue_days >= 60;

INSERT INTO public.m_loan_delinquency_tag_history (
    delinquency_range_id, loan_id, addedon_date, liftedon_date,
    created_by, created_on_utc, version, last_modified_by, last_modified_on_utc
)
SELECT
    104,
    l.loan_id,
    l.disburse_date + 180,
    NULL,
    1, NOW(), 1, 1, NOW()
FROM tmp_loan l
WHERE l.overdue_days >= 90;

INSERT INTO public.batch_job_instance (job_instance_id, version, job_name, job_key)
VALUES (9001, 1, 'LOAN_COB', md5('LOAN_COB_SEED'))
ON CONFLICT DO NOTHING;

INSERT INTO public.batch_job_execution
    (job_execution_id, version, job_instance_id, status,
     create_time, start_time, end_time, exit_code, exit_message, last_updated)
VALUES
    (9001, 1, 9001, 'COMPLETED',
     NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes',
     NOW() - INTERVAL '5 minutes', 'COMPLETED', '', NOW() - INTERVAL '5 minutes');

DROP TABLE tmp_loan;

COMMIT;

SELECT entity, cnt FROM (
    SELECT 'offices'      AS entity, COUNT(*) AS cnt FROM public.m_office      WHERE id >= 100 UNION ALL
    SELECT 'clients',                COUNT(*)         FROM public.m_client      WHERE id >= 100 UNION ALL
    SELECT 'loan_products',          COUNT(*)         FROM public.m_product_loan WHERE id >= 100 UNION ALL
    SELECT 'loans',                  COUNT(*)         FROM public.m_loan        WHERE id >= 100 UNION ALL
    SELECT 'transactions',           COUNT(*)         FROM public.m_loan_transaction WHERE loan_id >= 100 UNION ALL
    SELECT 'delinq_tags',            COUNT(*)         FROM public.m_loan_delinquency_tag_history WHERE loan_id >= 100 UNION ALL
    SELECT 'delinq_ranges',          COUNT(*)         FROM public.m_delinquency_range WHERE id >= 100
) s ORDER BY entity;
