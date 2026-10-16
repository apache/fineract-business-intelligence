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

with scope as (
    select * from {{ ref('int_snapshot_scope') }}
),

loan_base as (
    select
        l.source_id,
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
        l.total_expected_repayment_derived,
        l.is_npa,
        s.from_date
    from {{ ref('stg_m_loan') }} l
    inner join scope s
        on
            l.source_id = s.source_id
            and l.tenant_id = s.tenant_id
            and l.loan_id = s.loan_id
    inner join {{ ref('stg_m_client') }} c
        on
            l.source_id = c.source_id
            and l.tenant_id = c.tenant_id
            and l.client_id = c.client_id
    where l.disbursedon_date is not null
),

date_spine as (
    select generate_series(
        coalesce((select min(from_date) from scope), current_date),
        current_date,
        make_interval(days => 1)
    )::date as snapshot_date
),

loan_snapshots as materialized (
    select
        row_number() over () as snap_id,
        lb.source_id,
        lb.tenant_id,
        ds.snapshot_date,
        lb.loan_id,
        lb.client_id,
        lb.client_hash,
        lb.office_id,
        lb.product_id,
        lb.currency_code,
        lb.principal_disbursed_derived,
        lb.total_expected_repayment_derived,
        lb.is_npa
    from loan_base lb
    inner join date_spine ds
        on
            ds.snapshot_date >= greatest(lb.disbursedon_date, lb.from_date)
            and ds.snapshot_date <= lb.effective_maturity_date
),

scoped_transactions as (
    select
        t.source_id,
        t.tenant_id,
        t.loan_id,
        t.loan_transaction_id,
        t.transaction_date,
        t.outstanding_loan_balance_derived,
        sum(
            t.principal_portion_derived + t.interest_portion_derived
            + t.fee_charges_portion_derived + t.penalty_charges_portion_derived
        ) over (
            partition by t.source_id, t.tenant_id, t.loan_id
            order by t.transaction_date, t.loan_transaction_id
            rows between unbounded preceding and current row
        ) as cumulative_obligation_satisfied
    from {{ ref('stg_m_loan_transaction') }} t
    inner join scope s
        on
            t.source_id = s.source_id
            and t.tenant_id = s.tenant_id
            and t.loan_id = s.loan_id
    where t.is_reversed = false
),

installment_status as (
    select
        r.source_id,
        r.tenant_id,
        r.loan_id,
        r.duedate,
        coalesce(r.obligations_met_on_date, date '9999-12-31') as resolved_date
    from {{ ref('stg_m_loan_repayment_schedule') }} r
    inner join scope s
        on
            r.source_id = s.source_id
            and r.tenant_id = s.tenant_id
            and r.loan_id = s.loan_id
),

installment_events as (
    select
        source_id,
        tenant_id,
        loan_id,
        duedate as event_date
    from installment_status
    union
    select
        source_id,
        tenant_id,
        loan_id,
        resolved_date as event_date
    from installment_status
    where resolved_date < date '9999-12-31'
),

oldest_unpaid_as_of_event as (
    select
        e.source_id,
        e.tenant_id,
        e.loan_id,
        e.event_date,
        coalesce(
            min(i.duedate) filter (where i.duedate < e.event_date and i.resolved_date > e.event_date),
            date '9999-12-31'
        ) as oldest_unpaid_duedate
    from installment_events e
    inner join installment_status i
        on
            e.source_id = i.source_id
            and e.tenant_id = i.tenant_id
            and e.loan_id = i.loan_id
    group by e.source_id, e.tenant_id, e.loan_id, e.event_date
),

timeline as (
    select
        snap_id,
        source_id,
        tenant_id,
        loan_id,
        snapshot_date as point_date,
        1 as kind,
        null::bigint as txn_id,
        null::numeric as balance,
        null::numeric as cumulative_obligation,
        null::date as oldest_unpaid
    from loan_snapshots
    union all
    select
        null::bigint as snap_id,
        source_id,
        tenant_id,
        loan_id,
        transaction_date as point_date,
        0 as kind,
        loan_transaction_id as txn_id,
        outstanding_loan_balance_derived as balance,
        cumulative_obligation_satisfied as cumulative_obligation,
        null::date as oldest_unpaid
    from scoped_transactions
    union all
    select
        null::bigint as snap_id,
        source_id,
        tenant_id,
        loan_id,
        event_date as point_date,
        0 as kind,
        null::bigint as txn_id,
        null::numeric as balance,
        null::numeric as cumulative_obligation,
        oldest_unpaid_duedate as oldest_unpaid
    from oldest_unpaid_as_of_event
),

grouped as (
    select
        *,
        count(balance) over w as balance_group,
        count(cumulative_obligation) over w as obligation_group,
        count(oldest_unpaid) over w as dpd_group
    from timeline
    window w as (
        partition by source_id, tenant_id, loan_id
        order by point_date, kind, txn_id nulls first
        rows between unbounded preceding and current row
    )
),

carried as (
    select
        snap_id,
        source_id,
        tenant_id,
        loan_id,
        point_date as snapshot_date,
        kind,
        max(balance) over (partition by source_id, tenant_id, loan_id, balance_group) as balance_as_of,
        max(cumulative_obligation) over (
            partition by source_id, tenant_id, loan_id, obligation_group
        ) as obligation_as_of,
        max(oldest_unpaid) over (partition by source_id, tenant_id, loan_id, dpd_group) as oldest_unpaid_as_of
    from grouped
),

as_of as (
    select
        snap_id,
        source_id,
        tenant_id,
        loan_id,
        snapshot_date,
        balance_as_of,
        coalesce(obligation_as_of, 0) as obligation_satisfied_to_date,
        case
            when oldest_unpaid_as_of is null or oldest_unpaid_as_of >= date '9999-12-31' then 0
            else snapshot_date - oldest_unpaid_as_of
        end as days_past_due
    from carried
    where kind = 1
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
            partition by ls.snap_id
            order by d.addedon_date desc, d.delinquency_event_id desc
        ) as rn
    from loan_snapshots ls
    left join {{ ref('stg_m_delinquency') }} d
        on
            ls.source_id = d.source_id
            and ls.tenant_id = d.tenant_id
            and ls.loan_id = d.loan_id
            and d.addedon_date <= ls.snapshot_date
            and (d.liftedon_date is null or d.liftedon_date > ls.snapshot_date)
)

select
    dm.source_id,
    dm.tenant_id,
    dm.snapshot_date,
    dm.loan_id,
    dm.client_id,
    dm.client_hash,
    dm.office_id,
    dm.product_id,
    dm.currency_code,
    coalesce(dm.bucket_id, 0)::bigint as bucket_key,
    coalesce(dm.bucket_name, 'Current') as bucket_name,
    coalesce(dm.delinquency_range_classification, 'Current') as delinquency_range_classification,
    coalesce(dm.min_age_days, 0) as days_past_due_lower_bound,
    coalesce(dm.max_age_days, 0) as days_past_due_upper_bound,
    dm.principal_disbursed_derived,
    coalesce(a.balance_as_of, dm.principal_disbursed_derived) as principal_outstanding,
    greatest(dm.total_expected_repayment_derived - a.obligation_satisfied_to_date, 0) as total_outstanding,
    dm.is_npa,
    case
        when a.days_past_due = 0 then 'Performing'
        when a.days_past_due < 30 then 'Watch-list'
        when a.days_past_due < 60 then 'PAR 30-59'
        when a.days_past_due < 90 then 'PAR 60-89'
        else 'PAR 90+'
    end as standard_par_band,
    a.days_past_due >= 30 as is_par_30,
    a.days_past_due >= 60 as is_par_60,
    a.days_past_due >= 90 as is_par_90,
    a.days_past_due > 0 and a.days_past_due < 30 as is_watch_list
from delinquency_matches dm
inner join as_of a
    on dm.snap_id = a.snap_id
where dm.rn = 1
