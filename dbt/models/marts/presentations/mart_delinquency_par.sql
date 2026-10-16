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
        indexes=[{'columns': ['source_id', 'snapshot_date']}, {'columns': ['snapshot_date', 'bucket_key']}],
        pre_hook=["{{ heavy_model_settings() }}", "{{ mart_delete_recompute_window('snapshot_date') }}"]
    )
}}

with portfolio_base as (
    select
        f.snapshot_date,
        f.source_id,
        f.tenant_id,
        f.office_id,
        f.product_id,
        f.currency_code,
        f.loan_id,
        f.principal_outstanding,
        f.total_outstanding,
        f.is_npa,
        f.is_watch_list,
        f.is_par_30,
        f.is_par_60,
        f.is_par_90,
        f.standard_par_band,
        f.bucket_key,
        f.bucket_name
    from {{ ref('fact_loan_snapshot') }} as f
    {% if is_incremental() %}
        inner join ({{ mart_recompute_window('snapshot_date') }}) as w
            on
                f.source_id = w.source_id
                and f.snapshot_date >= w.recompute_from
    {% endif %}
),

portfolio_totals as (
    select
        snapshot_date,
        source_id,
        tenant_id,
        office_id,
        product_id,
        currency_code,
        {{ metric_active_loan_count() }} as total_loan_count,
        {{ metric_gross_loan_portfolio() }} as total_portfolio_amount,
        {{ metric_watch_list_amount() }} as watch_list_amount,
        {{ metric_par_band_amount(30, 60) }} as par_30_59_amount,
        {{ metric_par_band_amount(60, 90) }} as par_60_89_amount,
        {{ metric_par_amount(90) }} as par_90_plus_amount,
        {{ metric_par_amount(30) }} as par_30_amount,
        {{ metric_par_amount(60) }} as par_60_amount,
        {{ metric_par_amount(90) }} as par_90_amount,
        {{ metric_npa_amount() }} as npa_amount,
        {{ metric_performing_amount() }} as current_amount,
        {{ metric_watch_list_loan_count() }} as watch_list_loan_count,
        {{ metric_par_loan_count(30) }} as par_30_loan_count,
        {{ metric_par_loan_count(60) }} as par_60_loan_count,
        {{ metric_par_loan_count(90) }} as par_90_loan_count,
        {{ metric_npa_loan_count() }} as npa_loan_count,
        {{ metric_performing_loan_count() }} as current_loan_count
    from portfolio_base
    group by 1, 2, 3, 4, 5, 6
),

bucket_distribution as (
    select
        snapshot_date,
        source_id,
        tenant_id,
        office_id,
        product_id,
        currency_code,
        bucket_key,
        bucket_name,
        standard_par_band,
        count(distinct loan_id) as bucket_loan_count,
        sum(principal_outstanding) as bucket_outstanding_amount
    from portfolio_base
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
),

grouped as (
    select
        bd.snapshot_date,
        bd.source_id,
        bd.tenant_id,
        bd.office_id,
        bd.product_id,
        bd.currency_code,
        bd.bucket_key,
        bd.bucket_name,
        bd.standard_par_band,
        bd.bucket_loan_count,
        bd.bucket_outstanding_amount
    from bucket_distribution as bd
    union all
    select
        pt.snapshot_date,
        pt.source_id,
        pt.tenant_id,
        pt.office_id,
        pt.product_id,
        pt.currency_code,
        -1::bigint as bucket_key,
        'All Portfolio' as bucket_name,
        'All Portfolio' as standard_par_band,
        pt.total_loan_count as bucket_loan_count,
        pt.total_portfolio_amount as bucket_outstanding_amount
    from portfolio_totals as pt
)

select
    g.snapshot_date,
    g.source_id,
    g.tenant_id,
    g.office_id,
    o.office_name,
    g.product_id,
    p.product_name,
    g.currency_code,
    g.bucket_key,
    g.bucket_name,
    g.standard_par_band,
    g.bucket_loan_count,
    g.bucket_outstanding_amount,
    pt.total_loan_count,
    pt.total_portfolio_amount,
    pt.watch_list_amount,
    pt.par_30_59_amount,
    pt.par_60_89_amount,
    pt.par_90_plus_amount,
    pt.par_30_amount,
    pt.par_60_amount,
    pt.par_90_amount,
    pt.npa_amount,
    pt.current_amount,
    pt.watch_list_loan_count,
    pt.par_30_loan_count,
    pt.par_60_loan_count,
    pt.par_90_loan_count,
    pt.npa_loan_count,
    pt.current_loan_count,
    {{ metric_ratio('pt.par_30_amount', 'pt.total_portfolio_amount') }} as par_30_ratio,
    {{ metric_ratio('pt.par_60_amount', 'pt.total_portfolio_amount') }} as par_60_ratio,
    {{ metric_ratio('pt.par_90_amount', 'pt.total_portfolio_amount') }} as par_90_ratio,
    {{ metric_ratio('pt.npa_amount', 'pt.total_portfolio_amount') }} as npa_ratio,
    {{ safe_divide('pt.par_30_loan_count::numeric', 'pt.total_loan_count::numeric') }} as par_30_rate,
    {{ safe_divide('pt.par_60_loan_count::numeric', 'pt.total_loan_count::numeric') }} as par_60_rate,
    {{ safe_divide('pt.par_90_loan_count::numeric', 'pt.total_loan_count::numeric') }} as par_90_rate,
    {{ safe_divide('pt.total_portfolio_amount', 'pt.total_loan_count::numeric') }} as average_loan_outstanding,
    now() as dbt_loaded_at
from grouped as g
inner join portfolio_totals as pt
    on
        g.snapshot_date = pt.snapshot_date
        and g.source_id = pt.source_id
        and g.tenant_id = pt.tenant_id
        and g.office_id = pt.office_id
        and g.product_id = pt.product_id
        and g.currency_code = pt.currency_code
inner join {{ ref('dim_office') }} as o
    on
        g.source_id = o.source_id
        and g.tenant_id = o.tenant_id
        and g.office_id = o.office_id
inner join {{ ref('dim_product') }} as p
    on
        g.source_id = p.source_id
        and g.tenant_id = p.tenant_id
        and g.product_id = p.product_id
