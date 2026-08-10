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

with source as (
    select * from {{ source('raw', 'raw_m_client') }}
)

select
    tenant_id,
    id as client_id,
    account_no as client_account_no,
    external_id as client_external_id,
    md5(tenant_id || '::' || id::text) as client_hash,
    status_enum,
    activation_date,
    office_joining_date,
    office_id,
    staff_id,
    gender_cv_id,
    legal_form_enum,
    submittedon_date,
    updated_on,
    created_on_utc,
    last_modified_on_utc,
    case
        when date_of_birth is null
            then 'Unknown'
        when extract(year from age(current_date, date_of_birth)) < 25
            then '18-24'
        when extract(year from age(current_date, date_of_birth)) < 35
            then '25-34'
        when extract(year from age(current_date, date_of_birth)) < 45
            then '35-44'
        when extract(year from age(current_date, date_of_birth)) < 55
            then '45-54'
        else '55+'
    end as age_band
from source
