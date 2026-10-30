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

{% macro mart_upper_run_id() -%}
    (
        select coalesce(max(c.last_run_id), 0)
        from {{ source('meta', 'dbt_change_cursor') }} as c
        where c.model_name = 'fact_loan_snapshot'
    )
{%- endmacro %}

{% macro mart_recompute_window(consumer, from_column='from_date') -%}
    select
        d.source_id,
        d.office_id,
        d.product_id,
        min(d.{{ from_column }}) as recompute_from
    from {{ source('meta', 'dbt_mart_dirty') }} as d
    where
        d.run_id > coalesce(
            (
                select c.last_run_id
                from {{ source('meta', 'dbt_mart_cursor') }} as c
                where c.model_name = '{{ consumer }}'
            ),
            0
        )
        and d.run_id <= {{ mart_upper_run_id() }}
        and d.{{ from_column }} is not null
    group by d.source_id, d.office_id, d.product_id
{%- endmacro %}

{% macro mart_delete_recompute_window(consumer, date_column, from_column='from_date') -%}
    {%- if is_incremental() -%}
        delete from {{ this }} as m
        using ({{ mart_recompute_window(consumer, from_column) }}) as w
        where
            m.source_id = w.source_id
            and m.office_id is not distinct from w.office_id
            and m.product_id is not distinct from w.product_id
            and m.{{ date_column }} >= w.recompute_from
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}

{% macro record_dirty_partitions(phase) -%}
    insert into {{ source('meta', 'dbt_mart_dirty') }} (run_id, source_id, office_id, product_id, from_date, changed_from)
    {%- if phase == 'before' %}
    select
        w.run_id,
        f.source_id,
        f.office_id,
        f.product_id,
        min(s.from_date),
        min(s.changed_from)
    from {{ this }} as f
    inner join {{ ref('int_snapshot_scope') }} as s
        on
            f.source_id = s.source_id
            and f.tenant_id = s.tenant_id
            and f.loan_id = s.loan_id
    cross join {{ ref('int_change_window') }} as w
    group by w.run_id, f.source_id, f.office_id, f.product_id
    {%- else %}
    select
        w.run_id,
        l.source_id,
        c.office_id,
        l.product_id,
        min(s.from_date),
        min(s.changed_from)
    from {{ ref('int_snapshot_scope') }} as s
    inner join {{ ref('stg_m_loan') }} as l
        on
            s.source_id = l.source_id
            and s.tenant_id = l.tenant_id
            and s.loan_id = l.loan_id
    inner join {{ ref('stg_m_client') }} as c
        on
            l.source_id = c.source_id
            and l.tenant_id = c.tenant_id
            and l.client_id = c.client_id
    cross join {{ ref('int_change_window') }} as w
    group by w.run_id, l.source_id, c.office_id, l.product_id
    {%- endif %}
{%- endmacro %}

{% macro advance_fact_cursor() -%}
    insert into {{ source('meta', 'dbt_change_cursor') }} (model_name, last_change_id, last_txid, last_run_id, updated_at)
    select 'fact_loan_snapshot', w.max_change_id, w.horizon_txid, w.run_id, now()
    from {{ ref('int_change_window') }} as w
    where w.invocation_id = '{{ invocation_id }}'
    on conflict (model_name) do update
    set last_change_id = excluded.last_change_id,
        last_txid = excluded.last_txid,
        last_run_id = excluded.last_run_id,
        updated_at = excluded.updated_at
{%- endmacro %}

{% macro advance_mart_cursor() -%}
    insert into {{ source('meta', 'dbt_mart_cursor') }} (model_name, last_run_id, updated_at)
    select '{{ this.identifier }}', {{ mart_upper_run_id() }}, now()
    on conflict (model_name) do update
    set last_run_id = excluded.last_run_id,
        updated_at = excluded.updated_at
{%- endmacro %}

{% macro builds_models() -%}
    {{- return(flags.WHICH in ('run', 'build')) -}}
{%- endmacro %}

{% macro acquire_run_lock() -%}
    {%- if execute and builds_models() -%}
        select meta.acquire_dbt_lock('{{ invocation_id }}')
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}

{% macro finish_run() -%}
    {%- if execute and builds_models() -%}
        delete from meta.dbt_mart_dirty
        where run_id <= (
            select case when count(*) < 3 then 0 else min(last_run_id) end
            from meta.dbt_mart_cursor
            where model_name in ('mart_delinquency_par', 'mart_portfolio_health', 'mart_repayment_behavior')
        );
        select meta.release_dbt_lock('{{ invocation_id }}')
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}

{% macro analyze_raw_sources() -%}
    {%- if execute and var('analyze_raw_on_run_start', true) -%}
        {%- for node in graph.sources.values() if node.source_name == 'raw' -%}
            analyze {{ node.schema }}.{{ node.identifier }};
        {%- endfor -%}
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}

{% macro heavy_model_settings() -%}
    {%- if var('heavy_model_disable_nestloop', true) -%}set local enable_nestloop = off;{%- endif %}
    set local work_mem = '{{ var("heavy_model_work_mem", "128MB") }}'
{%- endmacro %}
