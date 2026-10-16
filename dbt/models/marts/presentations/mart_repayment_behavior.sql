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

-- depends_on: {{ ref('int_snapshot_scope') }}
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='append_new_columns',
        indexes=[{'columns': ['source_id', 'reporting_date']}, {'columns': ['reporting_date']}],
        pre_hook=["{{ heavy_model_settings() }}", "{{ mart_delete_recompute_window('reporting_date', 'changed_from') }}"]
    )
}}

with recompute as (
    {% if is_incremental() %}
        {{ mart_recompute_window('reporting_date', 'changed_from') }}
    {% else %}
        select distinct
            source_id,
            date '1900-01-01' as recompute_from
        from {{ ref('stg_m_loan') }}
    {% endif %}
),

loan_dimensions as (
    select
        l.source_id,
        l.tenant_id,
        l.loan_id,
        l.product_id,
        l.currency_code,
        l.client_id,
        c.client_hash,
        c.office_id
    from {{ ref('stg_m_loan') }} as l
    inner join {{ ref('stg_m_client') }} as c
        on
            l.source_id = c.source_id
            and l.tenant_id = c.tenant_id
            and l.client_id = c.client_id
),

transactions as (
    select
        t.source_id,
        t.tenant_id,
        t.loan_id,
        t.transaction_type_enum,
        t.transaction_date,
        t.amount,
        t.principal_portion_derived,
        t.interest_portion_derived,
        t.fee_charges_portion_derived,
        t.penalty_charges_portion_derived,
        t.overpayment_portion_derived,
        t.outstanding_loan_balance_derived,
        ld.office_id,
        ld.product_id,
        ld.currency_code,
        ld.client_id,
        ld.client_hash
    from {{ ref('stg_m_loan_transaction') }} as t
    inner join loan_dimensions as ld
        on
            t.source_id = ld.source_id
            and t.tenant_id = ld.tenant_id
            and t.loan_id = ld.loan_id
    inner join recompute as w
        on
            t.source_id = w.source_id
            and t.transaction_date >= w.recompute_from
    where t.is_reversed = false
),

repayment_aggregated as (
    select
        source_id,
        tenant_id,
        transaction_date as reporting_date,
        office_id,
        product_id,
        currency_code,
        client_hash,
        count(*) as repayment_transaction_count,
        count(distinct loan_id) as repaid_loan_count,
        count(distinct client_id) as repaying_borrower_count,
        sum(amount) as repayment_amount,
        sum(principal_portion_derived) as principal_collected,
        sum(interest_portion_derived) as interest_collected,
        sum(fee_charges_portion_derived) as fee_collected,
        sum(penalty_charges_portion_derived) as penalty_collected,
        sum(coalesce(overpayment_portion_derived, 0)) as overpayment_collected,
        sum(coalesce(outstanding_loan_balance_derived, 0)) as post_transaction_outstanding_balance
    from transactions
    where transaction_type_enum = 2
    group by 1, 2, 3, 4, 5, 6, 7
),

recovery_aggregated as (
    select
        source_id,
        tenant_id,
        transaction_date as reporting_date,
        office_id,
        product_id,
        currency_code,
        client_hash,
        sum(amount) as recovery_repayment_amount,
        count(distinct loan_id) as recovery_loan_count
    from transactions
    where transaction_type_enum = 8
    group by 1, 2, 3, 4, 5, 6, 7
),

due_amounts as (
    select
        s.source_id,
        s.tenant_id,
        s.duedate as reporting_date,
        ld.office_id,
        ld.product_id,
        ld.currency_code,
        ld.client_hash,
        sum(
            coalesce(s.principal_amount, 0) + coalesce(s.interest_amount, 0)
            + coalesce(s.fee_charges_amount, 0) + coalesce(s.penalty_charges_amount, 0)
        ) as contractually_due_amount,
        sum(coalesce(s.penalty_charges_amount, 0)) as overdue_penalty_charged,
        sum(coalesce(s.penalty_charges_waived_derived, 0)) as overdue_penalty_waived,
        sum(
            coalesce(s.interest_waived_derived, 0) + coalesce(s.fee_charges_waived_derived, 0)
            + coalesce(s.penalty_charges_waived_derived, 0)
        ) as waived_amount,
        sum(coalesce(s.total_paid_in_advance_derived, 0)) as paid_in_advance_amount,
        sum(coalesce(s.total_paid_late_derived, 0)) as paid_late_amount,
        count(
            case
                when s.completed_derived and s.obligations_met_on_date < s.duedate then 1
            end
        ) as early_payment_count,
        count(
            case
                when s.completed_derived and s.obligations_met_on_date = s.duedate then 1
            end
        ) as on_time_payment_count,
        count(
            case
                when s.completed_derived and s.obligations_met_on_date > s.duedate then 1
            end
        ) as late_payment_count,
        count(case when s.is_re_aged then 1 end) as restructured_installment_count,
        count(*) as total_installments_due
    from {{ ref('stg_m_loan_repayment_schedule') }} as s
    inner join loan_dimensions as ld
        on
            s.source_id = ld.source_id
            and s.tenant_id = ld.tenant_id
            and s.loan_id = ld.loan_id
    inner join recompute as w
        on
            s.source_id = w.source_id
            and s.duedate >= w.recompute_from
    group by 1, 2, 3, 4, 5, 6, 7
),

combined as (
    select
        coalesce(ra.source_id, da.source_id) as source_id,
        coalesce(ra.tenant_id, da.tenant_id) as tenant_id,
        coalesce(ra.reporting_date, da.reporting_date) as reporting_date,
        coalesce(ra.office_id, da.office_id) as office_id,
        coalesce(ra.product_id, da.product_id) as product_id,
        coalesce(ra.currency_code, da.currency_code) as currency_code,
        coalesce(ra.client_hash, da.client_hash) as client_hash,
        coalesce(ra.repayment_transaction_count, 0) as repayment_transaction_count,
        coalesce(ra.repaid_loan_count, 0) as repaid_loan_count,
        coalesce(ra.repaying_borrower_count, 0) as repaying_borrower_count,
        coalesce(ra.repayment_amount, 0) as repayment_amount,
        coalesce(ra.repayment_amount, 0) as actual_collected_amount,
        coalesce(da.contractually_due_amount, 0) as contractually_due_amount,
        coalesce(ra.principal_collected, 0) as principal_collected,
        coalesce(ra.interest_collected, 0) as interest_collected,
        coalesce(ra.fee_collected, 0) as fee_collected,
        coalesce(ra.penalty_collected, 0) as penalty_collected,
        coalesce(ra.overpayment_collected, 0) as overpayment_collected,
        coalesce(ra.post_transaction_outstanding_balance, 0) as post_transaction_outstanding_balance,
        coalesce(da.waived_amount, 0) as waived_amount,
        coalesce(da.paid_in_advance_amount, 0) as paid_in_advance_amount,
        coalesce(da.paid_late_amount, 0) as paid_late_amount,
        coalesce(da.early_payment_count, 0) as early_payment_count,
        coalesce(da.on_time_payment_count, 0) as on_time_payment_count,
        coalesce(da.late_payment_count, 0) as late_payment_count,
        coalesce(da.restructured_installment_count, 0) as restructured_installment_count,
        coalesce(da.overdue_penalty_charged, 0) as overdue_penalty_charged,
        coalesce(da.overdue_penalty_waived, 0) as overdue_penalty_waived,
        coalesce(da.total_installments_due, 0) as total_installments_due
    from repayment_aggregated as ra
    full outer join due_amounts as da
        on
            ra.source_id = da.source_id
            and ra.tenant_id = da.tenant_id
            and ra.reporting_date = da.reporting_date
            and ra.office_id = da.office_id
            and ra.product_id = da.product_id
            and ra.currency_code = da.currency_code
            and ra.client_hash = da.client_hash
)

select
    c.reporting_date,
    c.source_id,
    c.tenant_id,
    c.office_id,
    o.office_name,
    c.product_id,
    p.product_name,
    c.currency_code,
    c.client_hash,
    c.repayment_transaction_count,
    c.repaid_loan_count,
    c.repaying_borrower_count,
    c.repayment_amount,
    c.actual_collected_amount,
    c.contractually_due_amount,
    {{ metric_collection_efficiency('c.actual_collected_amount', 'c.contractually_due_amount') }}
        as collection_efficiency_ratio,
    c.post_transaction_outstanding_balance,
    c.principal_collected,
    c.interest_collected,
    c.fee_collected,
    c.penalty_collected,
    c.overpayment_collected,
    c.waived_amount,
    c.paid_in_advance_amount,
    c.paid_late_amount,
    c.early_payment_count,
    c.on_time_payment_count,
    c.late_payment_count,
    c.restructured_installment_count,
    c.overdue_penalty_charged,
    c.overdue_penalty_waived,
    c.total_installments_due,
    coalesce(rec.recovery_repayment_amount, 0) as recovery_repayment_amount,
    coalesce(rec.recovery_loan_count, 0) as recovery_loan_count,
    now() as dbt_loaded_at
from combined as c
inner join {{ ref('dim_office') }} as o
    on
        c.source_id = o.source_id
        and c.tenant_id = o.tenant_id
        and c.office_id = o.office_id
inner join {{ ref('dim_product') }} as p
    on
        c.source_id = p.source_id
        and c.tenant_id = p.tenant_id
        and c.product_id = p.product_id
left join recovery_aggregated as rec
    on
        c.source_id = rec.source_id
        and c.tenant_id = rec.tenant_id
        and c.reporting_date = rec.reporting_date
        and c.office_id = rec.office_id
        and c.product_id = rec.product_id
        and c.currency_code = rec.currency_code
        and c.client_hash = rec.client_hash
