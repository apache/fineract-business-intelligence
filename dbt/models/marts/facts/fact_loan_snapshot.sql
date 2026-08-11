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
        materialized='incremental',
        unique_key='snapshot_key',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns'
    )
}}

{% set lookback_days = var('snapshot_incremental_lookback_days', 120) %}

select
    md5(tenant_id || '::' || loan_id::text || '::' || snapshot_date::text)
        as snapshot_key,
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
    is_par_90
from {{ ref('int_loan_delinquency_status') }}
{% if is_incremental() %}
    where snapshot_date >= (
        (
            select
                coalesce(
                    max(snapshot_date),
                    '{{ var("historical_start_date", "2010-01-01") }}'::date
                )
            from {{ this }}
        ) - interval '{{ lookback_days }} days'
    )::date
{% endif %}
