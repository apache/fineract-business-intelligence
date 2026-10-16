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

with executions as (
    select * from {{ source('raw', 'raw_batch_job_execution') }}
    where not _is_deleted
),

instances as (
    select * from {{ source('raw', 'raw_batch_job_instance') }}
    where not _is_deleted
)

select
    e.source_id,
    e.tenant_id,
    e.job_execution_id,
    e.job_instance_id,
    i.job_name,
    e.status,
    e.start_time,
    e.end_time,
    e.exit_code,
    e.created_on_utc,
    e.last_modified_on_utc
from executions e
left join instances i
    on
        e.source_id = i.source_id
        and e.tenant_id = i.tenant_id
        and e.job_instance_id = i.job_instance_id
