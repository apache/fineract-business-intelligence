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

{% macro fact_in_run() -%}
    {%- set ns = namespace(found=false) -%}
    {%- if execute -%}
        {%- for resource in selected_resources if resource.endswith('.fact_loan_snapshot') -%}
            {%- set ns.found = true -%}
        {%- endfor -%}
    {%- endif -%}
    {{- return(ns.found) -}}
{%- endmacro %}

{% macro mart_upper_run_id() -%}
    {%- if fact_in_run() -%}
        (select w.run_id from intermediate.int_change_window as w)
    {%- else -%}
        (
            select coalesce(max(c.last_run_id), 0)
            from {{ source('meta', 'dbt_change_cursor') }} as c
            where c.model_name = 'fact_loan_snapshot'
        )
    {%- endif -%}
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

{% macro advance_change_cursor(results) -%}
    {%- if execute -%}
        {%- set marts = ['mart_delinquency_par', 'mart_portfolio_health', 'mart_repayment_behavior'] -%}
        {%- set fact_chain = ['int_change_window', 'int_snapshot_scope', 'fact_loan_snapshot'] -%}
        {%- set model_results = results | selectattr('node.resource_type', 'equalto', 'model') | list -%}
        {%- set built = model_results | selectattr('status', 'equalto', 'success') | map(attribute='node.name') | list -%}
        {%- set statements = [] -%}
        {%- if fact_chain | reject('in', built) | list | length == 0 -%}
            {%- do statements.append(
                "insert into meta.dbt_change_cursor (model_name, last_change_id, last_txid, last_run_id, updated_at) "
                ~ "select 'fact_loan_snapshot', max_change_id, horizon_txid, run_id, now() "
                ~ "from intermediate.int_change_window "
                ~ "on conflict (model_name) do update set last_change_id = excluded.last_change_id, "
                ~ "last_txid = excluded.last_txid, last_run_id = excluded.last_run_id, updated_at = excluded.updated_at"
            ) -%}
        {%- endif -%}
        {%- for mart in marts if mart in built -%}
            {%- do statements.append(
                "insert into meta.dbt_mart_cursor (model_name, last_run_id, updated_at) select '" ~ mart ~ "', "
                ~ mart_upper_run_id() ~ ", now() "
                ~ "on conflict (model_name) do update set last_run_id = excluded.last_run_id, updated_at = excluded.updated_at"
            ) -%}
        {%- endfor -%}
        {%- if statements | length > 0 -%}
            {%- do statements.append(
                "delete from meta.dbt_mart_dirty where run_id <= ("
                ~ "select case when count(*) < " ~ (marts | length) ~ " then 0 else min(last_run_id) end "
                ~ "from meta.dbt_mart_cursor where model_name in ('" ~ (marts | join("', '")) ~ "'))"
            ) -%}
            {{ statements | join(';\n') }}
        {%- else -%}
            select 1
        {%- endif -%}
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
