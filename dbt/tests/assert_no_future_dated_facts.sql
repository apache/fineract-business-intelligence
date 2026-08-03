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

select
    'mart_delinquency_par'  as model_name,
    snapshot_date           as offending_date
from {{ ref('mart_delinquency_par') }}
where snapshot_date > current_date

union all

select
    'mart_portfolio_health' as model_name,
    snapshot_date           as offending_date
from {{ ref('mart_portfolio_health') }}
where snapshot_date > current_date

union all

select
    'fact_loan_snapshot'    as model_name,
    snapshot_date           as offending_date
from {{ ref('fact_loan_snapshot') }}
where snapshot_date > current_date
