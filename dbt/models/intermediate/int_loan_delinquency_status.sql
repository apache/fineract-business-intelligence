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
        l.total_expected_repayment_derived,
        l.is_npa
    from {{ ref('stg_m_loan') }} l
    inner join {{ ref('stg_m_client') }} c
        on l.tenant_id = c.tenant_id
       and l.client_id = c.client_id
    where l.disbursedon_date is not null
),

date_spine as (
    select generate_series(
        '{{ var("historical_start_date", "2010-01-01") }}'::date,
        current_date,
        make_interval(days => 1)
    )::date as snapshot_date
),
loan_snapshots as materialized (
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
        lb.total_expected_repayment_derived,
        lb.is_npa
    from loan_base lb
    inner join date_spine ds
        on
            ds.snapshot_date >= lb.disbursedon_date
            and ds.snapshot_date <= lb.effective_maturity_date
),

principal_outstanding_as_of as (
    select
        ls.tenant_id,
        ls.loan_id,
        ls.snapshot_date,
        coalesce(tb.outstanding_loan_balance_derived, ls.principal_disbursed_derived) as principal_outstanding
    from loan_snapshots ls
    left join lateral (
        select outstanding_loan_balance_derived
        from {{ ref('stg_m_loan_transaction') }} tb
        where tb.tenant_id = ls.tenant_id
          and tb.loan_id = ls.loan_id
          and tb.transaction_date <= ls.snapshot_date
          and tb.is_reversed = false
          and tb.outstanding_loan_balance_derived is not null
        order by tb.transaction_date desc, tb.loan_transaction_id desc
        limit 1
    ) tb on true
),

transaction_obligations as (
    select
        tenant_id,
        loan_id,
        transaction_date,
        loan_transaction_id,
        principal_portion_derived + interest_portion_derived
            + fee_charges_portion_derived + penalty_charges_portion_derived
                as obligation_satisfied_amount
    from {{ ref('stg_m_loan_transaction') }}
    where is_reversed = false
),
cumulative_obligations as materialized (
    select
        tenant_id,
        loan_id,
        transaction_date,
        loan_transaction_id,
        sum(obligation_satisfied_amount) over (
            partition by tenant_id, loan_id
            order by transaction_date, loan_transaction_id
            rows between unbounded preceding and current row
        ) as cumulative_obligation_satisfied
    from transaction_obligations
),
latest_obligation_transaction as (
    select
        ls.tenant_id,
        ls.loan_id,
        ls.snapshot_date,
        t.loan_transaction_id
    from loan_snapshots ls
    left join lateral (
        select loan_transaction_id
        from {{ ref('stg_m_loan_transaction') }} t
        where t.tenant_id = ls.tenant_id
          and t.loan_id = ls.loan_id
          and t.transaction_date <= ls.snapshot_date
          and t.is_reversed = false
        order by t.transaction_date desc, t.loan_transaction_id desc
        limit 1
    ) t on true
),
obligation_satisfied_as_of as (
    select
        lot.tenant_id,
        lot.loan_id,
        lot.snapshot_date,
        coalesce(co.cumulative_obligation_satisfied, 0) as obligation_satisfied_to_date
    from latest_obligation_transaction lot
    left join cumulative_obligations co
        on  co.tenant_id          = lot.tenant_id
        and co.loan_id             = lot.loan_id
        and co.loan_transaction_id = lot.loan_transaction_id
),

installment_status as (
    select
        tenant_id,
        loan_id,
        duedate,
        coalesce(obligations_met_on_date, date '9999-12-31') as resolved_date
    from {{ ref('stg_m_loan_repayment_schedule') }}
),
installment_events as (
    select tenant_id, loan_id, duedate as event_date
    from installment_status
    union
    select tenant_id, loan_id, resolved_date as event_date
    from installment_status
    where resolved_date < date '9999-12-31'
),
oldest_unpaid_as_of_event as (
    select
        e.tenant_id,
        e.loan_id,
        e.event_date,
        coalesce(
            (
                select min(i.duedate)
                from installment_status i
                where i.tenant_id = e.tenant_id
                  and i.loan_id = e.loan_id
                  and i.duedate < e.event_date
                  and i.resolved_date > e.event_date
            ),
            date '9999-12-31'
        ) as oldest_unpaid_duedate
    from installment_events e
),
dpd_timeline as (
    select
        ls.tenant_id,
        ls.loan_id,
        ls.snapshot_date,
        oe.oldest_unpaid_duedate,
        count(oe.oldest_unpaid_duedate) over (
            partition by ls.tenant_id, ls.loan_id
            order by ls.snapshot_date
        ) as fill_group
    from loan_snapshots ls
    left join oldest_unpaid_as_of_event oe
        on  oe.tenant_id   = ls.tenant_id
        and oe.loan_id      = ls.loan_id
        and oe.event_date   = ls.snapshot_date
),
loan_dpd_as_of as (
    select
        tenant_id,
        loan_id,
        snapshot_date,
        case
            when max(oldest_unpaid_duedate) over (partition by tenant_id, loan_id, fill_group) >= date '9999-12-31'
                then 0
            else snapshot_date - max(oldest_unpaid_duedate) over (partition by tenant_id, loan_id, fill_group)
        end as days_past_due
    from dpd_timeline
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
    dm.tenant_id,
    dm.snapshot_date,
    dm.loan_id,
    dm.client_id,
    dm.client_hash,
    dm.office_id,
    dm.product_id,
    dm.currency_code,
    coalesce(dm.bucket_id, 0)::bigint                              as bucket_key,
    coalesce(dm.bucket_name, 'Current')                            as bucket_name,
    coalesce(dm.delinquency_range_classification, 'Current')       as delinquency_range_classification,
    coalesce(dm.min_age_days, 0)                                   as days_past_due_lower_bound,
    coalesce(dm.max_age_days, 0)                                   as days_past_due_upper_bound,
    dm.principal_disbursed_derived,
    poa.principal_outstanding,
    greatest(dm.total_expected_repayment_derived - osa.obligation_satisfied_to_date, 0)
                                                                    as total_outstanding,
    dm.is_npa,
    case
        when coalesce(dpd.days_past_due, 0) = 0      then 'Performing'
        when coalesce(dpd.days_past_due, 0) < 30     then 'Watch-list'
        when coalesce(dpd.days_past_due, 0) < 60     then 'PAR 30-59'
        when coalesce(dpd.days_past_due, 0) < 90     then 'PAR 60-89'
        else                                               'PAR 90+'
    end                                                             as standard_par_band,
    coalesce(dpd.days_past_due, 0) >= 30                           as is_par_30,
    coalesce(dpd.days_past_due, 0) >= 60                           as is_par_60,
    coalesce(dpd.days_past_due, 0) >= 90                           as is_par_90,
    coalesce(dpd.days_past_due, 0) > 0
        and coalesce(dpd.days_past_due, 0) < 30                    as is_watch_list
from delinquency_matches dm
inner join principal_outstanding_as_of poa
    on  dm.tenant_id     = poa.tenant_id
    and dm.loan_id        = poa.loan_id
    and dm.snapshot_date  = poa.snapshot_date
inner join obligation_satisfied_as_of osa
    on  dm.tenant_id     = osa.tenant_id
    and dm.loan_id        = osa.loan_id
    and dm.snapshot_date  = osa.snapshot_date
left join loan_dpd_as_of dpd
    on  dm.tenant_id     = dpd.tenant_id
    and dm.loan_id        = dpd.loan_id
    and dm.snapshot_date  = dpd.snapshot_date
where dm.rn = 1
