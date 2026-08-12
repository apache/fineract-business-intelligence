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

{% set username = (current_username() or '') | replace("'", "''") %}
select *
from analytics.mart_portfolio_health
where '{{ username }}' != ''
and (
    exists (
        select 1
        from meta.user_office_mapping uom
        where uom.username = '{{ username }}'
          and uom.role_name = 'ADMIN'
    )
    or office_id in (
        select office_id
        from meta.user_office_mapping
        where username = '{{ username }}'
          and office_id is not null
    )
)
