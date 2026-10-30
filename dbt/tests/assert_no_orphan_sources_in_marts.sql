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

with live_sources as (
    select source_id
    from {{ source('meta', 'source_registry') }}
    where lifecycle_state <> 'RETIRED'
),

derived_sources as (
    select distinct source_id
    from {{ ref('fact_loan_snapshot') }}
    union
    select distinct source_id
    from {{ ref('fact_delinquency_event') }}
    union
    select distinct source_id
    from {{ ref('mart_delinquency_par') }}
    union
    select distinct source_id
    from {{ ref('mart_portfolio_health') }}
    union
    select distinct source_id
    from {{ ref('mart_repayment_behavior') }}
)

select d.source_id
from derived_sources d
left join live_sources l
    on d.source_id = l.source_id
where l.source_id is null
