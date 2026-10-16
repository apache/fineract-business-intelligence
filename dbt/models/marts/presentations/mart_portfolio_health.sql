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

-- depends_on: {{ ref('int_snapshot_scope') }}
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='append_new_columns',
        indexes=[{'columns': ['source_id', 'snapshot_date']}, {'columns': ['snapshot_date']}],
        pre_hook=["{{ heavy_model_settings() }}", "{{ mart_delete_recompute_window('snapshot_date') }}"]
    )
}}

with snapshot_base as (
    select
        f.snapshot_date,
        f.source_id,
        f.tenant_id,
        f.office_id,
        f.product_id,
        f.currency_code,
        f.loan_id,
        f.client_hash,
        f.principal_outstanding,
        f.total_outstanding,
        f.is_npa,
        f.is_par_30,
        f.is_par_60,
        f.is_par_90,
        f.is_watch_list
    from {{ ref('fact_loan_snapshot') }} as f
    {% if is_incremental() %}
        inner join ({{ mart_recompute_window('snapshot_date') }}) as w
            on
                f.source_id = w.source_id
                and f.snapshot_date >= w.recompute_from
    {% endif %}
),

stock_metrics as (
    select
        snapshot_date,
        source_id,
        tenant_id,
        office_id,
        product_id,
        currency_code,
        {{ metric_active_loan_count() }} as active_loan_count,
        {{ metric_active_borrower_count() }} as active_borrower_count,
        {{ metric_gross_loan_portfolio() }} as gross_loan_portfolio,
        sum(total_outstanding) as total_outstanding_amount,
        sum(greatest(total_outstanding - principal_outstanding, 0)) as interest_outstanding_amount,
        {{ metric_npa_amount() }} as npa_outstanding_amount,
        {{ metric_npa_loan_count() }} as npa_loan_count,
        {{ metric_par_amount(30) }} as par_outstanding_amount,
        {{ metric_par_loan_count(30) }} as par_loan_count,
        {{ metric_performing_amount() }} as performing_outstanding_amount,
        {{ metric_performing_loan_count() }} as performing_loan_count
    from snapshot_base
    group by 1, 2, 3, 4, 5, 6
),

loan_dimensions as (
    select
        l.source_id,
        l.tenant_id,
        l.loan_id,
        c.office_id,
        l.product_id,
        l.currency_code
    from {{ ref('stg_m_loan') }} as l
    inner join {{ ref('stg_m_client') }} as c
        on
            l.source_id = c.source_id
            and l.tenant_id = c.tenant_id
            and l.client_id = c.client_id
),

flow_metrics as (
    select
        t.transaction_date as snapshot_date,
        t.source_id,
        t.tenant_id,
        ld.office_id,
        ld.product_id,
        ld.currency_code,
        sum(case when t.transaction_type_enum = 1 then t.amount else 0 end) as disbursed_amount_on_date,
        count(distinct case when t.transaction_type_enum = 1 then t.loan_id end) as disbursed_loan_count_on_date,
        sum(case when t.transaction_type_enum = 2 then t.principal_portion_derived else 0 end)
            as principal_collected_on_date,
        sum(case when t.transaction_type_enum = 2 then t.amount else 0 end) as collected_amount_on_date,
        sum(case when t.transaction_type_enum = 6 then t.amount else 0 end) as writeoff_amount_on_date,
        count(distinct case when t.transaction_type_enum = 6 then t.loan_id end) as writeoff_count_on_date
    from {{ ref('stg_m_loan_transaction') }} as t
    inner join loan_dimensions as ld
        on
            t.source_id = ld.source_id
            and t.tenant_id = ld.tenant_id
            and t.loan_id = ld.loan_id
    {% if is_incremental() %}
        inner join ({{ mart_recompute_window('snapshot_date') }}) as w
            on
                t.source_id = w.source_id
                and t.transaction_date >= w.recompute_from
    {% endif %}
    where t.is_reversed = false
    group by 1, 2, 3, 4, 5, 6
),

combined as (
    select
        sm.*,
        coalesce(fm.disbursed_amount_on_date, 0) as disbursed_amount_on_date,
        coalesce(fm.disbursed_loan_count_on_date, 0) as disbursed_loan_count_on_date,
        coalesce(fm.principal_collected_on_date, 0) as principal_collected_on_date,
        coalesce(fm.collected_amount_on_date, 0) as collected_amount_on_date,
        coalesce(fm.writeoff_amount_on_date, 0) as writeoff_amount_on_date,
        coalesce(fm.writeoff_count_on_date, 0) as writeoff_count_on_date
    from stock_metrics as sm
    left join flow_metrics as fm
        on
            sm.snapshot_date = fm.snapshot_date
            and sm.source_id = fm.source_id
            and sm.tenant_id = fm.tenant_id
            and sm.office_id = fm.office_id
            and sm.product_id = fm.product_id
            and sm.currency_code = fm.currency_code
)

select
    c.snapshot_date,
    c.source_id,
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
    {{ safe_divide('c.gross_loan_portfolio', 'c.active_loan_count::numeric') }} as average_loan_size,
    {{ safe_divide('c.gross_loan_portfolio', 'c.active_borrower_count::numeric') }} as average_exposure_per_borrower,
    c.npa_outstanding_amount,
    c.npa_loan_count,
    {{ metric_ratio('c.npa_outstanding_amount', 'c.gross_loan_portfolio') }} as npa_ratio,
    c.par_outstanding_amount,
    c.par_loan_count,
    {{ metric_ratio('c.par_outstanding_amount', 'c.gross_loan_portfolio') }} as par_ratio,
    c.performing_outstanding_amount,
    c.performing_loan_count,
    {{ metric_ratio('c.performing_outstanding_amount', 'c.gross_loan_portfolio') }} as performing_ratio,
    c.disbursed_amount_on_date,
    c.disbursed_loan_count_on_date,
    c.principal_collected_on_date,
    c.collected_amount_on_date,
    c.writeoff_amount_on_date,
    c.writeoff_count_on_date,
    now() as dbt_loaded_at
from combined as c
inner join {{ ref('dim_office') }} as o
    on
        c.source_id = o.source_id
        and c.tenant_id = o.tenant_id
        and c.office_id = o.office_id
inner join {{ ref('dim_product') }} as p
    on
        c.source_id = p.source_id
        and c.tenant_id = p.tenant_id
        and c.product_id = p.product_id
