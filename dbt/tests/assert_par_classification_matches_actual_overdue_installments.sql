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
    select
        source_id,
        max(snapshot_date) as snapshot_date
    from {{ ref('fact_loan_snapshot') }}
    group by source_id
),

flagged_par_30_loans as (
    select
        f.source_id,
        f.tenant_id,
        f.loan_id,
        f.snapshot_date
    from {{ ref('fact_loan_snapshot') }} f
    inner join latest_snapshot ls
        on
            f.source_id = ls.source_id
            and f.snapshot_date = ls.snapshot_date
    where f.is_par_30 = true
),

genuinely_overdue_30_plus as (
    select distinct
        r.source_id,
        r.tenant_id,
        r.loan_id
    from {{ source('raw', 'raw_m_loan_repayment_schedule') }} r
    inner join latest_snapshot ls
        on r.source_id = ls.source_id
    where
        not r._is_deleted
        and r.duedate <= ls.snapshot_date - 30
        and (r.obligations_met_on_date is null or r.obligations_met_on_date > ls.snapshot_date)
)

select
    p.source_id,
    p.tenant_id,
    p.loan_id,
    p.snapshot_date
from flagged_par_30_loans p
left join genuinely_overdue_30_plus o
    on
        p.source_id = o.source_id
        and p.tenant_id = o.tenant_id
        and p.loan_id = o.loan_id
where o.loan_id is null
