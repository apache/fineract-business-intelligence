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
        indexes=[
            {'columns': ['source_id', 'tenant_id', 'loan_id', 'snapshot_date'], 'unique': True},
            {'columns': ['snapshot_date']},
        ],
        pre_hook=[
            "{{ heavy_model_settings() }}",
            "{% if is_incremental() %}
            delete from {{ this }} f
            using {{ ref('int_snapshot_scope') }} s
            where f.source_id = s.source_id
              and f.tenant_id = s.tenant_id
              and f.loan_id = s.loan_id
              and f.snapshot_date >= s.from_date
            {% else %}select 1{% endif %}"
        ],
        post_hook=[
            "{% if var('snapshot_daily_retention_days', none) is not none %}
            delete from {{ this }}
            where snapshot_date < current_date - {{ var('snapshot_daily_retention_days') | int }}
              and snapshot_date <> (date_trunc('month', snapshot_date) + interval '1 month - 1 day')::date
            {% else %}select 1{% endif %}"
        ]
    )
}}

select
    md5(source_id || '::' || tenant_id || '::' || loan_id::text || '::' || snapshot_date::text)
        as snapshot_key,
    source_id,
    tenant_id,
    to_char(snapshot_date, 'YYYYMMDD')::bigint as date_key,
    snapshot_date,
    loan_id,
    client_id,
    client_hash,
    office_id,
    product_id,
    currency_code,
    bucket_key,
    bucket_name,
    delinquency_range_classification,
    standard_par_band,
    days_past_due_lower_bound,
    days_past_due_upper_bound,
    principal_disbursed_derived,
    principal_outstanding,
    total_outstanding,
    is_npa,
    is_watch_list,
    is_par_30,
    is_par_60,
    is_par_90,
    now() as dbt_loaded_at
from {{ ref('int_loan_delinquency_status') }}
