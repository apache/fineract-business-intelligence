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

with expected as (
    select
        s.source_id,
        sum(
            coalesce(s.principal_amount, 0) + coalesce(s.interest_amount, 0)
            + coalesce(s.fee_charges_amount, 0) + coalesce(s.penalty_charges_amount, 0)
        ) as due_amount,
        count(*) as installments
    from {{ ref('stg_m_loan_repayment_schedule') }} s
    inner join {{ ref('stg_m_loan') }} l
        on
            s.source_id = l.source_id
            and s.tenant_id = l.tenant_id
            and s.loan_id = l.loan_id
    inner join {{ ref('stg_m_client') }} c
        on
            l.source_id = c.source_id
            and l.tenant_id = c.tenant_id
            and l.client_id = c.client_id
    inner join {{ ref('dim_office') }} o
        on
            c.source_id = o.source_id
            and c.tenant_id = o.tenant_id
            and c.office_id = o.office_id
    inner join {{ ref('dim_product') }} p
        on
            l.source_id = p.source_id
            and l.tenant_id = p.tenant_id
            and l.product_id = p.product_id
    group by s.source_id
),

actual as (
    select
        source_id,
        sum(contractually_due_amount) as due_amount,
        sum(total_installments_due) as installments
    from {{ ref('mart_repayment_behavior') }}
    group by source_id
)

select
    coalesce(e.source_id, a.source_id) as source_id,
    e.due_amount as schedule_due_amount,
    a.due_amount as mart_due_amount,
    e.installments as schedule_installments,
    a.installments as mart_installments
from expected e
full outer join actual a on e.source_id = a.source_id
where
    abs(coalesce(e.due_amount, 0) - coalesce(a.due_amount, 0)) > 0.01
    or coalesce(e.installments, 0) <> coalesce(a.installments, 0)
