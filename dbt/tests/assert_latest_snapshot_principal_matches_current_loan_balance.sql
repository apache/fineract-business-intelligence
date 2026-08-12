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

with latest_snapshot as (
    select max(snapshot_date) as snapshot_date
    from analytics.fact_loan_snapshot
),

latest_transaction_balance as (
    select distinct on (tenant_id, loan_id)
        tenant_id,
        loan_id,
        outstanding_loan_balance_derived
    from raw.raw_m_loan_transaction
    where
        is_reversed = false
        and outstanding_loan_balance_derived is not null
    order by tenant_id asc, loan_id asc, transaction_date desc, id desc
)

select
    f.tenant_id,
    f.loan_id,
    f.principal_outstanding as reconstructed_principal_outstanding,
    t.outstanding_loan_balance_derived as latest_ledger_balance
from analytics.fact_loan_snapshot f
inner join latest_snapshot ls
    on f.snapshot_date = ls.snapshot_date
inner join latest_transaction_balance t
    on
        f.tenant_id = t.tenant_id
        and f.loan_id = t.loan_id
where abs(f.principal_outstanding - t.outstanding_loan_balance_derived) > 0.01
