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
    select * from {{ source('raw', 'raw_m_loan_repayment_schedule') }}
)

select
    tenant_id,
    id as loan_repayment_schedule_id,
    loan_id,
    fromdate,
    duedate,
    installment,
    coalesce(principal_amount, 0) as principal_amount,
    coalesce(principal_completed_derived, 0) as principal_completed_derived,
    coalesce(principal_writtenoff_derived, 0) as principal_writtenoff_derived,
    coalesce(interest_amount, 0) as interest_amount,
    coalesce(interest_completed_derived, 0) as interest_completed_derived,
    coalesce(interest_writtenoff_derived, 0) as interest_writtenoff_derived,
    coalesce(interest_waived_derived, 0) as interest_waived_derived,
    coalesce(fee_charges_amount, 0) as fee_charges_amount,
    coalesce(fee_charges_completed_derived, 0) as fee_charges_completed_derived,
    coalesce(fee_charges_writtenoff_derived, 0) as fee_charges_writtenoff_derived,
    coalesce(fee_charges_waived_derived, 0) as fee_charges_waived_derived,
    coalesce(penalty_charges_amount, 0) as penalty_charges_amount,
    coalesce(penalty_charges_completed_derived, 0) as penalty_charges_completed_derived,
    coalesce(penalty_charges_writtenoff_derived, 0) as penalty_charges_writtenoff_derived,
    coalesce(penalty_charges_waived_derived, 0) as penalty_charges_waived_derived,
    coalesce(total_paid_in_advance_derived, 0) as total_paid_in_advance_derived,
    coalesce(total_paid_late_derived, 0) as total_paid_late_derived,
    completed_derived,
    obligations_met_on_date,
    is_re_aged,
    lastmodified_date
from source
