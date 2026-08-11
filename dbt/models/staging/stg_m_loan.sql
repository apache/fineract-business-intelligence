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

with source as (
    select * from {{ source('raw', 'raw_m_loan') }}
)

select
    tenant_id,
    id as loan_id,
    account_no as loan_account_no,
    external_id as loan_external_id,
    client_id,
    product_id,
    loan_status_id,
    loan_type_enum,
    currency_code,
    currency_digits,
    currency_multiplesof,
    principal_amount_proposed,
    principal_amount,
    approved_principal,
    net_disbursal_amount,
    annual_nominal_interest_rate,
    nominal_interest_rate_per_period,
    interest_method_enum,
    interest_calculated_in_period_enum,
    term_frequency,
    term_period_frequency_enum,
    repay_every,
    repayment_period_frequency_enum,
    number_of_repayments,
    amortization_method_enum,
    submittedon_date,
    approvedon_date,
    expected_disbursedon_date,
    expected_firstrepaymenton_date,
    disbursedon_date,
    expected_maturedon_date,
    maturedon_date,
    principal_disbursed_derived,
    principal_repaid_derived,
    principal_writtenoff_derived,
    principal_outstanding_derived,
    interest_charged_derived,
    interest_repaid_derived,
    interest_writtenoff_derived,
    interest_outstanding_derived,
    fee_charges_outstanding_derived,
    penalty_charges_outstanding_derived,
    total_expected_repayment_derived,
    total_repayment_derived,
    total_writtenoff_derived,
    total_outstanding_derived,
    loan_counter,
    is_npa,
    created_on_utc,
    last_modified_on_utc
from source
