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

with snapshot_base as (
    select
        snapshot_date,
        tenant_id,
        office_id,
        product_id,
        currency_code,
        loan_id,
        client_hash,
        principal_outstanding,
        total_outstanding,
        is_npa,
        is_par_30,
        is_par_60,
        is_par_90,
        is_watch_list
    from {{ ref('fact_loan_snapshot') }}
),

stock_metrics as (
    select
        snapshot_date,
        tenant_id,
        office_id,
        product_id,
        currency_code,

        count(distinct loan_id)                                                 as active_loan_count,
        count(distinct client_hash)                                             as active_borrower_count,

        sum(principal_outstanding)                                              as gross_loan_portfolio,
        sum(total_outstanding)                                                  as total_outstanding_amount,
        sum(greatest(total_outstanding - principal_outstanding, 0))             as interest_outstanding_amount,

        sum(case when is_npa
                then principal_outstanding else 0 end)                          as npa_outstanding_amount,
        count(distinct case when is_npa then loan_id end)                       as npa_loan_count,

        sum(case when is_par_30
                then principal_outstanding else 0 end)                          as par_outstanding_amount,
        count(distinct case when is_par_30 then loan_id end)                    as par_loan_count,

        sum(case when not is_par_30 and not is_watch_list
                then principal_outstanding else 0 end)                          as performing_outstanding_amount,
        count(distinct case when not is_par_30 and not is_watch_list
                            then loan_id end)                                    as performing_loan_count
    from snapshot_base
    group by 1, 2, 3, 4, 5
),

loan_dimensions as (
    select
        l.tenant_id,
        l.loan_id,
        c.office_id,
        l.product_id,
        l.currency_code
    from {{ ref('stg_m_loan') }} l
    inner join {{ ref('stg_m_client') }} c
        on l.tenant_id = c.tenant_id
       and l.client_id = c.client_id
),

flow_metrics as (
    select
        t.transaction_date                                                      as snapshot_date,
        t.tenant_id,
        ld.office_id,
        ld.product_id,
        ld.currency_code,

        sum(case when t.transaction_type_enum = 1
                then t.amount else 0 end)                                       as disbursed_amount_on_date,
        count(distinct case when t.transaction_type_enum = 1
                            then t.loan_id end)                                  as disbursed_loan_count_on_date,

        sum(case when t.transaction_type_enum = 2
                then t.principal_portion_derived else 0 end)                    as principal_collected_on_date,
        sum(case when t.transaction_type_enum = 2
                then t.amount else 0 end)                                       as collected_amount_on_date,

        sum(case when t.transaction_type_enum = 6
                then t.amount else 0 end)                                       as writeoff_amount_on_date,
        count(distinct case when t.transaction_type_enum = 6
                            then t.loan_id end)                                  as writeoff_count_on_date
    from {{ ref('stg_m_loan_transaction') }} t
    inner join loan_dimensions ld
        on t.tenant_id = ld.tenant_id
       and t.loan_id = ld.loan_id
    where t.is_reversed = false
    group by 1, 2, 3, 4, 5
),

combined as (
    select
        sm.snapshot_date,
        sm.tenant_id,
        sm.office_id,
        sm.product_id,
        sm.currency_code,

        sm.active_loan_count,
        sm.active_borrower_count,
        sm.gross_loan_portfolio,
        sm.total_outstanding_amount,
        sm.interest_outstanding_amount,
        sm.npa_outstanding_amount,
        sm.npa_loan_count,
        sm.par_outstanding_amount,
        sm.par_loan_count,
        sm.performing_outstanding_amount,
        sm.performing_loan_count,

        coalesce(fm.disbursed_amount_on_date, 0)                                as disbursed_amount_on_date,
        coalesce(fm.disbursed_loan_count_on_date, 0)                           as disbursed_loan_count_on_date,
        coalesce(fm.principal_collected_on_date, 0)                            as principal_collected_on_date,
        coalesce(fm.collected_amount_on_date, 0)                               as collected_amount_on_date,
        coalesce(fm.writeoff_amount_on_date, 0)                                as writeoff_amount_on_date,
        coalesce(fm.writeoff_count_on_date, 0)                                 as writeoff_count_on_date
    from stock_metrics sm
    left join flow_metrics fm
        on  sm.snapshot_date  = fm.snapshot_date
        and sm.tenant_id      = fm.tenant_id
        and sm.office_id      = fm.office_id
        and sm.product_id     = fm.product_id
        and sm.currency_code  = fm.currency_code
),

detail as (
    select
        c.snapshot_date,
        c.tenant_id,
        c.office_id,
        o.office_name,
        c.product_id,
        p.product_name,
        c.currency_code,

        c.active_loan_count,
        c.active_borrower_count,
        c.gross_loan_portfolio,
        c.total_outstanding_amount,
        c.interest_outstanding_amount,

        {{ safe_divide('c.gross_loan_portfolio', 'c.active_loan_count::numeric') }}      as average_loan_size,
        {{ safe_divide('c.gross_loan_portfolio', 'c.active_borrower_count::numeric') }}  as average_exposure_per_borrower,

        c.npa_outstanding_amount,
        c.npa_loan_count,
        {{ safe_divide('c.npa_outstanding_amount', 'c.gross_loan_portfolio') }}          as npa_ratio,

        c.par_outstanding_amount,
        c.par_loan_count,
        {{ safe_divide('c.par_outstanding_amount', 'c.gross_loan_portfolio') }}          as par_ratio,

        c.performing_outstanding_amount,
        c.performing_loan_count,
        {{ safe_divide('c.performing_outstanding_amount', 'c.gross_loan_portfolio') }}   as performing_ratio,

        c.disbursed_amount_on_date,
        c.disbursed_loan_count_on_date,
        c.principal_collected_on_date,
        c.collected_amount_on_date,
        c.writeoff_amount_on_date,
        c.writeoff_count_on_date
    from combined c
    inner join {{ ref('dim_office') }} o
        on c.office_id = o.office_id
    inner join {{ ref('dim_product') }} p
        on c.product_id = p.product_id
)

select * from detail
