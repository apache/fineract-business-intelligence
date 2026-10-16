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

with known_macros as (
    select unnest(array[
        'metric_gross_loan_portfolio',
        'metric_par_amount',
        'metric_npa_amount',
        'metric_ratio',
        'metric_watch_list_amount',
        'metric_performing_amount',
        'metric_active_loan_count',
        'metric_active_borrower_count',
        'metric_collection_efficiency'
    ]) as macro_name
)

select
    d.metric_name,
    d.dbt_macro
from {{ ref('metric_definitions') }} d
left join known_macros k
    on split_part(d.dbt_macro, '(', 1) = k.macro_name
where k.macro_name is null
