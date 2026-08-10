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
    tenant_id,
    office_id,
    parent_office_id,
    office_name,
    hierarchy,
    office_external_id,
    opening_date,
    greatest(
        1,
        array_length(
            regexp_split_to_array(
                trim(both '.' from coalesce(hierarchy, '.')),
                '\.'
            ),
            1
        )
    ) as office_level
from {{ ref('stg_m_office') }}
