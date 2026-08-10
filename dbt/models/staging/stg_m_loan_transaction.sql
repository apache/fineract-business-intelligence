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
    select * from {{ source('raw', 'raw_m_loan_transaction') }}
)

select
    tenant_id,
    id as loan_transaction_id,
    loan_id,
    office_id,
    transaction_type_enum,
    transaction_date,
    amount,
    coalesce(principal_portion_derived, 0) as principal_portion_derived,
    coalesce(interest_portion_derived, 0) as interest_portion_derived,
    coalesce(fee_charges_portion_derived, 0) as fee_charges_portion_derived,
    coalesce(penalty_charges_portion_derived, 0) as penalty_charges_portion_derived,
    coalesce(overpayment_portion_derived, 0) as overpayment_portion_derived,
    outstanding_loan_balance_derived,
    is_reversed,
    submitted_on_date,
    created_on_utc,
    last_modified_on_utc
from source
