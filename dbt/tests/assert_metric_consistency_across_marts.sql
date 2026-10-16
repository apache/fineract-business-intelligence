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

with par_rollup as (
    select *
    from {{ ref('mart_delinquency_par') }}
    where bucket_key = -1
)

select
    h.source_id,
    h.snapshot_date,
    h.office_id,
    h.product_id,
    h.currency_code,
    h.gross_loan_portfolio,
    d.total_portfolio_amount,
    h.par_outstanding_amount,
    d.par_30_amount,
    h.npa_outstanding_amount,
    d.npa_amount
from {{ ref('mart_portfolio_health') }} h
full outer join par_rollup d
    on
        h.source_id = d.source_id
        and h.tenant_id = d.tenant_id
        and h.snapshot_date = d.snapshot_date
        and h.office_id = d.office_id
        and h.product_id = d.product_id
        and h.currency_code = d.currency_code
where
    h.source_id is null
    or d.source_id is null
    or abs(h.gross_loan_portfolio - d.total_portfolio_amount) > 0.005
    or abs(h.par_outstanding_amount - d.par_30_amount) > 0.005
    or abs(h.npa_outstanding_amount - d.npa_amount) > 0.005
    or h.active_loan_count <> d.total_loan_count
