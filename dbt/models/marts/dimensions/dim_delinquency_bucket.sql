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

with source_buckets as (
    select
        bucket_id as bucket_key,
        max(bucket_name) as bucket_name,
        min(min_age_days) as min_age_days,
        max(coalesce(max_age_days, 99999)) as max_age_days
    from {{ ref('stg_m_delinquency') }}
    where bucket_id is not null
    group by bucket_id
)

select
    0::bigint as bucket_key,
    'Performing' as bucket_name,
    0 as min_age_days,
    0 as max_age_days,
    'Performing' as standard_par_band,
    0 as sort_order
union all
select
    bucket_key,
    bucket_name,
    min_age_days,
    max_age_days,
    case
        when min_age_days < 30 then 'Watch-list'
        when min_age_days < 60 then 'PAR 30-59'
        when min_age_days < 90 then 'PAR 60-89'
        else 'PAR 90+'
    end as standard_par_band,
    case
        when min_age_days between 1 and 29 then 1
        when min_age_days between 30 and 59 then 2
        when min_age_days between 60 and 89 then 3
        when min_age_days >= 90 then 4
        else 99
    end as sort_order
from source_buckets
