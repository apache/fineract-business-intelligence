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

{% macro metric_gross_loan_portfolio() -%}
    sum(principal_outstanding)
{%- endmacro %}

{% macro metric_par_amount(days) -%}
    sum(case when is_par_{{ days }} then principal_outstanding else 0 end)
{%- endmacro %}

{% macro metric_par_band_amount(from_days, to_days) -%}
    sum(case when is_par_{{ from_days }} and not is_par_{{ to_days }} then principal_outstanding else 0 end)
{%- endmacro %}

{% macro metric_npa_amount() -%}
    sum(case when is_npa then principal_outstanding else 0 end)
{%- endmacro %}

{% macro metric_watch_list_amount() -%}
    sum(case when is_watch_list then principal_outstanding else 0 end)
{%- endmacro %}

{% macro metric_performing_amount() -%}
    sum(case when not is_par_30 and not is_watch_list then principal_outstanding else 0 end)
{%- endmacro %}

{% macro metric_active_loan_count() -%}
    count(distinct loan_id)
{%- endmacro %}

{% macro metric_active_borrower_count() -%}
    count(distinct client_hash)
{%- endmacro %}

{% macro metric_par_loan_count(days) -%}
    count(distinct case when is_par_{{ days }} then loan_id end)
{%- endmacro %}

{% macro metric_npa_loan_count() -%}
    count(distinct case when is_npa then loan_id end)
{%- endmacro %}

{% macro metric_watch_list_loan_count() -%}
    count(distinct case when is_watch_list then loan_id end)
{%- endmacro %}

{% macro metric_performing_loan_count() -%}
    count(distinct case when not is_par_30 and not is_watch_list then loan_id end)
{%- endmacro %}

{% macro metric_ratio(numerator, denominator) -%}
    {{ safe_divide(numerator, denominator) }}
{%- endmacro %}

{% macro metric_collection_efficiency(collected, due) -%}
    {{ safe_divide(collected, due) }}
{%- endmacro %}
