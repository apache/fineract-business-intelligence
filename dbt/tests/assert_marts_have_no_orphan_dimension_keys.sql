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
    'mart_delinquency_par' as model_name,
    p.office_id,
    p.product_id
from {{ ref('mart_delinquency_par') }} p
left join {{ ref('dim_office') }} o
    on p.office_id = o.office_id
left join {{ ref('dim_product') }} pr
    on p.product_id = pr.product_id
where o.office_id is null
   or pr.product_id is null

union all

select
    'mart_repayment_behavior' as model_name,
    r.office_id,
    r.product_id
from {{ ref('mart_repayment_behavior') }} r
left join {{ ref('dim_office') }} o
    on r.office_id = o.office_id
left join {{ ref('dim_product') }} pr
    on r.product_id = pr.product_id
where o.office_id is null
   or pr.product_id is null
