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

{{ config(
    materialized='table',
    indexes=[{'columns': ['source_id', 'tenant_id', 'loan_id'], 'unique': True}],
    post_hook=['analyze {{ this }}']
) }}

{% set fact = adapter.get_relation(database=target.database, schema='analytics', identifier='fact_loan_snapshot') %}

with change_window as (
    select * from {{ ref('int_change_window') }}
),

loans as (
    select
        source_id,
        tenant_id,
        loan_id,
        client_id,
        disbursedon_date,
        coalesce(maturedon_date, current_date) as effective_maturity_date
    from {{ ref('stg_m_loan') }}
    where disbursedon_date is not null
),

changes as (
    select c.*
    from {{ source('meta', 'entity_change_log') }} c
    cross join change_window w
    where
        not w.full_rebuild
        and c.txid >= w.last_txid
        and c.txid < w.horizon_txid
),

fact_max as (
    {% if fact is not none %}
        select
            source_id,
            max(snapshot_date) as max_date
        from {{ fact }}
        group by source_id
    {% else %}
        select
            null::text as source_id,
            null::date as max_date
        where false
    {% endif %}
),

scope_rows as (
    select
        l.source_id,
        l.tenant_id,
        l.loan_id,
        date '1900-01-01' as from_date,
        false as is_roll_forward
    from loans l
    cross join change_window w
    where w.full_rebuild

    union all

    select
        c.source_id,
        c.tenant_id,
        c.entity_id as loan_id,
        c.min_affected_date as from_date,
        false as is_roll_forward
    from changes c
    where c.entity = 'loan'

    union all

    select
        l.source_id,
        l.tenant_id,
        l.loan_id,
        date '1900-01-01' as from_date,
        false as is_roll_forward
    from changes c
    inner join loans l
        on
            c.source_id = l.source_id
            and c.tenant_id = l.tenant_id
            and c.entity_id = l.client_id
    where c.entity = 'client'

    union all

    select
        l.source_id,
        l.tenant_id,
        l.loan_id,
        date '1900-01-01' as from_date,
        false as is_roll_forward
    from changes c
    inner join loans l
        on c.source_id = l.source_id
    where c.entity in ('global', 'source')

    union all

    select
        l.source_id,
        l.tenant_id,
        l.loan_id,
        coalesce(fm.max_date + 1, date '1900-01-01') as from_date,
        true as is_roll_forward
    from loans l
    cross join change_window w
    left join fact_max fm
        on l.source_id = fm.source_id
    where
        not w.full_rebuild
        and (
            fm.max_date is null
            or (l.effective_maturity_date > fm.max_date and fm.max_date < current_date)
        )
)

select
    source_id,
    tenant_id,
    loan_id,
    greatest(min(from_date), '{{ var("historical_start_date") }}'::date) as from_date,
    min(from_date) filter (where not is_roll_forward) as changed_from
from scope_rows
group by source_id, tenant_id, loan_id
