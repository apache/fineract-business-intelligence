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

{{
    config(
        materialized='ephemeral'
    )
}}

with loan_base as (
    select
        l.tenant_id,
        l.loan_id,
        l.client_id,
        c.client_hash,
        c.office_id,
        l.product_id,
        l.currency_code,
        l.disbursedon_date,
        coalesce(l.maturedon_date, current_date) as effective_maturity_date,
        l.principal_disbursed_derived,
        l.principal_outstanding_derived,
        l.total_outstanding_derived,
        l.is_npa
    from {{ ref('stg_m_loan') }} l
    inner join {{ ref('stg_m_client') }} c
        on
            l.tenant_id = c.tenant_id
            and l.client_id = c.client_id
    where
        l.loan_status_id = 300
        and l.disbursedon_date is not null
),

date_spine as (
    select generate_series(
        '{{ var("historical_start_date", "2010-01-01") }}'::date,
        current_date,
        make_interval(days => 1)
    )::date as snapshot_date
),

loan_snapshots as (
    select
        lb.tenant_id,
        ds.snapshot_date,
        lb.loan_id,
        lb.client_id,
        lb.client_hash,
        lb.office_id,
        lb.product_id,
        lb.currency_code,
        lb.principal_disbursed_derived,
        lb.principal_outstanding_derived,
        lb.total_outstanding_derived,
        lb.is_npa
    from loan_base lb
    inner join date_spine ds
        on
            ds.snapshot_date >= lb.disbursedon_date
            and ds.snapshot_date <= lb.effective_maturity_date
),

delinquency_matches as (
    select
        ls.*,
        d.delinquency_event_id,
        d.bucket_id,
        d.bucket_name,
        d.delinquency_range_classification,
        d.min_age_days,
        d.max_age_days,
        row_number() over (
            partition by ls.tenant_id, ls.loan_id, ls.snapshot_date
            order by d.addedon_date desc, d.delinquency_event_id desc
        ) as rn
    from loan_snapshots ls
    left join {{ ref('stg_m_delinquency') }} d
        on
            ls.tenant_id = d.tenant_id
            and ls.loan_id = d.loan_id
            and d.addedon_date <= ls.snapshot_date
            and (d.liftedon_date is null or d.liftedon_date > ls.snapshot_date)
)

select
    tenant_id,
    snapshot_date,
    loan_id,
    client_id,
    client_hash,
    office_id,
    product_id,
    currency_code,
    coalesce(bucket_id, 0)::bigint as bucket_key,
    coalesce(bucket_name, 'Current') as bucket_name,
    coalesce(delinquency_range_classification, 'Current') as delinquency_range_classification,
    coalesce(min_age_days, 0) as days_past_due_lower_bound,
    coalesce(max_age_days, 0) as days_past_due_upper_bound,
    principal_disbursed_derived,
    principal_outstanding_derived as principal_outstanding,
    total_outstanding_derived as total_outstanding,
    is_npa,
    case
        when coalesce(min_age_days, 0) = 0 then 'Performing'
        when coalesce(min_age_days, 0) < 30 then 'Watch-list'
        when coalesce(min_age_days, 0) < 60 then 'PAR 30-59'
        when coalesce(min_age_days, 0) < 90 then 'PAR 60-89'
        else 'PAR 90+'
    end as standard_par_band,
    coalesce(min_age_days, 0) >= 30 as is_par_30,
    coalesce(min_age_days, 0) >= 60 as is_par_60,
    coalesce(min_age_days, 0) >= 90 as is_par_90,
    coalesce(min_age_days, 0) > 0
    and coalesce(min_age_days, 0) < 30 as is_watch_list
from delinquency_matches
where rn = 1
