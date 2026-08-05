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

with forbidden as (
    select unnest(array[
        'date_of_birth',
        'display_name',
        'firstname',
        'lastname',
        'mobile_no',
        'email_address',
        'account_no',
        'external_id'
    ]) as column_name
),

analytics_columns as (
    select
        c.table_name,
        c.column_name
    from information_schema.columns c
    where c.table_schema = '{{ target.schema }}'
      and c.table_name in ('dim_client', 'mart_repayment_behavior', 'mart_delinquency_par')
)

select
    ac.table_name,
    ac.column_name,
    'direct identifier exposed in the analytics layer' as violation
from analytics_columns ac
inner join forbidden f
    on ac.column_name = f.column_name

union all

select
    ac.table_name,
    ac.column_name,
    'raw client_id must be replaced by client_hash' as violation
from analytics_columns ac
where ac.table_name = 'dim_client'
  and ac.column_name = 'client_id'
