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

with delinquency_events as (
    select
        d.tenant_id,
        d.id as delinquency_event_id,
        d.loan_id,
        d.delinquency_range_id,
        r.classification as delinquency_range_classification,
        r.min_age_days,
        r.max_age_days,
        m.delinquency_bucket_id as bucket_id,
        b.name as bucket_name,
        d.addedon_date,
        d.liftedon_date,
        d.created_on_utc,
        d.last_modified_on_utc
    from {{ source('raw', 'raw_m_loan_delinquency_tag_history') }} d
    left join {{ source('raw', 'raw_m_delinquency_range') }} r
        on
            d.tenant_id = r.tenant_id
            and d.delinquency_range_id = r.id
    left join {{ source('raw', 'raw_m_delinquency_bucket_mappings') }} m
        on
            r.tenant_id = m.tenant_id
            and r.id = m.delinquency_range_id
    left join {{ source('raw', 'raw_m_delinquency_bucket') }} b
        on
            m.tenant_id = b.tenant_id
            and m.delinquency_bucket_id = b.id
)

select * from delinquency_events
