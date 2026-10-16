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

{% macro mart_recompute_window(date_column, scope_column='from_date') -%}
    select
        coalesce(sf.source_id, mm.source_id) as source_id,
        least(
            coalesce(sf.scope_from, mm.max_date + 1),
            coalesce(mm.max_date + 1, sf.scope_from)
        ) as recompute_from
    from (
        select
            source_id,
            min({{ scope_column }}) as scope_from
        from {{ ref('int_snapshot_scope') }}
        group by source_id
    ) as sf
    full outer join (
        select
            source_id,
            max({{ date_column }}) as max_date
        from {{ this }}
        group by source_id
    ) as mm
        on sf.source_id = mm.source_id
{%- endmacro %}

{% macro mart_delete_recompute_window(date_column, scope_column='from_date') -%}
    {%- if is_incremental() -%}
        delete from {{ this }} as m
        using ({{ mart_recompute_window(date_column, scope_column) }}) as w
        where m.source_id = w.source_id and m.{{ date_column }} >= w.recompute_from
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}

{% macro advance_change_cursor(results) -%}
    {%- if execute -%}
        {%- set failed = results | selectattr('status', 'in', ['error', 'fail', 'runtime error']) | list -%}
        {%- set built = results | selectattr('node.name', 'equalto', 'int_change_window')
                                | selectattr('status', 'equalto', 'success') | list -%}
        {%- if failed | length == 0 and built | length > 0 -%}
            insert into meta.dbt_change_cursor (model_name, last_change_id, updated_at)
            select 'fact_loan_snapshot', max_change_id, now()
            from intermediate.int_change_window
            on conflict (model_name) do update
            set last_change_id = excluded.last_change_id, updated_at = excluded.updated_at
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
