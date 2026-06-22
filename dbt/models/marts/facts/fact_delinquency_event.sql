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
        unique_key='delinquency_event_key'
    )
}}

with ordered_events as (
    select
        tenant_id,
        delinquency_event_id,
        loan_id,
        delinquency_range_id,
        bucket_id                               as bucket_key,
        bucket_name,
        delinquency_range_classification,
        min_age_days,
        max_age_days,
        addedon_date                            as event_start_date,
        liftedon_date                           as event_end_date,
        lag(bucket_id) over (
            partition by tenant_id, loan_id
            order by addedon_date, delinquency_event_id
        )                                       as previous_bucket_key,
        lag(min_age_days) over (
            partition by tenant_id, loan_id
            order by addedon_date, delinquency_event_id
        )                                       as previous_min_age_days
    from {{ ref('stg_m_delinquency') }}
)

select
    md5(tenant_id || '::' || delinquency_event_id::text)
                                                as delinquency_event_key,
    tenant_id,
    delinquency_event_id,
    loan_id,
    delinquency_range_id,
    coalesce(bucket_key, 0)::bigint             as bucket_key,
    coalesce(bucket_name, 'Current')            as bucket_name,
    delinquency_range_classification,
    coalesce(previous_bucket_key, 0)::bigint    as previous_bucket_key,
    coalesce(min_age_days, 0)                   as min_age_days,
    coalesce(max_age_days, 0)                   as max_age_days,
    case
        when coalesce(min_age_days, 0) = 0      then 'Current'
        when coalesce(min_age_days, 0) < 30     then 'Watch-list'
        when coalesce(min_age_days, 0) < 60     then 'PAR 30-59'
        when coalesce(min_age_days, 0) < 90     then 'PAR 60-89'
        else                                         'PAR 90+'
    end                                         as standard_par_band,
    case
        when coalesce(previous_min_age_days, 0) = 0     then 'Current'
        when coalesce(previous_min_age_days, 0) < 30    then 'Watch-list'
        when coalesce(previous_min_age_days, 0) < 60    then 'PAR 30-59'
        when coalesce(previous_min_age_days, 0) < 90    then 'PAR 60-89'
        else                                                  'PAR 90+'
    end                                         as previous_standard_par_band,
    event_start_date,
    event_end_date,
    coalesce(
        event_end_date - event_start_date,
        current_date - event_start_date
    )                                           as event_duration_days,
    event_end_date is null                      as is_active_event
from ordered_events
