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

{{ config(materialized='table', post_hook=['analyze {{ this }}']) }}

{% set fact = adapter.get_relation(database=target.database, schema='analytics', identifier='fact_loan_snapshot') %}

select
    coalesce(
        (
            select c.last_txid
            from {{ source('meta', 'dbt_change_cursor') }} c
            where c.model_name = 'fact_loan_snapshot'
        ),
        '0'::xid8
    ) as last_txid,
    pg_snapshot_xmin(pg_current_snapshot()) as horizon_txid,
    nextval('meta.dbt_run_seq')::bigint as run_id,
    coalesce(
        (select max(l.change_id) from {{ source('meta', 'entity_change_log') }} l),
        0
    )::bigint as max_change_id,
    {{ 'true' if (flags.FULL_REFRESH or fact is none) else 'false' }} as full_rebuild,
    now() as window_built_at
