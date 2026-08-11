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

with date_spine as (
    select generate_series(
        '{{ var("historical_start_date", "2010-01-01") }}'::date,
        '{{ var("future_end_date", "2035-12-31") }}'::date,
        make_interval(days => 1)
    )::date as date_day
)

select
    to_char(date_day, 'YYYYMMDD')::bigint as date_key,
    date_day,
    extract(year from date_day)::int as year_number,
    extract(quarter from date_day)::int as quarter_number,
    extract(month from date_day)::int as month_number,
    to_char(date_day, 'Month') as month_name,
    to_char(date_day, 'Mon') as month_short_name,
    extract(week from date_day)::int as week_number,
    extract(isodow from date_day)::int as day_of_week_number,
    to_char(date_day, 'Day') as day_name,
    (extract(isodow from date_day) in (6, 7)) as is_weekend,
    date_trunc('month', date_day)::date as first_day_of_month,
    (date_trunc('month', date_day) + interval '1 month - 1 day')::date as last_day_of_month,
    date_trunc('quarter', date_day)::date as first_day_of_quarter
from date_spine
