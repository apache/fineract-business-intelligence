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


{{
    config(
        materialized   = 'incremental',
        unique_key     = ['tenant_id', 'reporting_date', 'office_id', 'product_id', 'currency_code', 'client_hash'],
        incremental_strategy = 'delete+insert',
        on_schema_change = 'sync_all_columns',
    )
}}

with loan_dimensions as (
    select
        l.tenant_id,
        l.loan_id,
        l.product_id,
        l.currency_code,
        l.client_id,
        c.client_hash,
        c.office_id
    from {{ ref('stg_m_loan') }} l
    inner join {{ ref('stg_m_client') }} c
        on l.tenant_id = c.tenant_id
       and l.client_id = c.client_id
),

repayment_transactions as (
    select
        t.tenant_id,
        t.transaction_date                                  as reporting_date,
        ld.office_id,
        ld.product_id,
        ld.currency_code,
        t.loan_id,
        ld.client_id,
        ld.client_hash,

        t.amount                                            as repayment_amount,
        t.principal_portion_derived                         as principal_collected,
        t.interest_portion_derived                          as interest_collected,
        t.fee_charges_portion_derived                       as fee_collected,
        t.penalty_charges_portion_derived                   as penalty_collected,
        coalesce(t.overpayment_portion_derived, 0)          as overpayment_collected,

        coalesce(t.outstanding_loan_balance_derived, 0)     as post_transaction_outstanding_balance
    from {{ ref('stg_m_loan_transaction') }} t
    inner join loan_dimensions ld
        on t.tenant_id = ld.tenant_id
       and t.loan_id   = ld.loan_id
    where t.transaction_type_enum = 2
      and t.is_reversed = false
),

recovery_transactions as (
    select
        t.tenant_id,
        t.transaction_date                                  as reporting_date,
        ld.office_id,
        ld.product_id,
        ld.currency_code,
        ld.client_hash,
        t.loan_id,
        t.amount                                            as recovery_amount
    from {{ ref('stg_m_loan_transaction') }} t
    inner join loan_dimensions ld
        on t.tenant_id = ld.tenant_id
       and t.loan_id   = ld.loan_id
    where t.transaction_type_enum = 8
      and t.is_reversed = false
),

due_amounts as (
    select
        s.tenant_id,
        s.duedate                                           as reporting_date,
        ld.office_id,
        ld.product_id,
        ld.currency_code,
        ld.client_hash,

        sum(
            coalesce(s.principal_amount, 0)
            + coalesce(s.interest_amount, 0)
            + coalesce(s.fee_charges_amount, 0)
            + coalesce(s.penalty_charges_amount, 0)
        )                                                   as contractually_due_amount,

        sum(coalesce(s.penalty_charges_amount, 0))          as overdue_penalty_charged,
        sum(coalesce(s.penalty_charges_waived_derived, 0))  as overdue_penalty_waived,

        sum(coalesce(s.principal_completed_derived, 0))     as schedule_principal_received,
        sum(coalesce(s.interest_completed_derived, 0))      as schedule_interest_received,
        sum(coalesce(s.fee_charges_completed_derived, 0))   as schedule_fee_received,

        sum(
            coalesce(s.interest_waived_derived, 0)
            + coalesce(s.fee_charges_waived_derived, 0)
            + coalesce(s.penalty_charges_waived_derived, 0)
        )                                                   as waived_amount,

        sum(coalesce(s.total_paid_in_advance_derived, 0))   as paid_in_advance_amount,
        sum(coalesce(s.total_paid_late_derived, 0))         as paid_late_amount,

        count(case
            when s.completed_derived
             and s.obligations_met_on_date is not null
             and s.obligations_met_on_date < s.duedate
            then 1
        end)                                                as early_payment_count,

        count(case
            when s.completed_derived
             and s.obligations_met_on_date is not null
             and s.obligations_met_on_date = s.duedate
            then 1
        end)                                                as on_time_payment_count,

        count(case
            when s.completed_derived
             and s.obligations_met_on_date is not null
             and s.obligations_met_on_date > s.duedate
            then 1
        end)                                                as late_payment_count,

        count(case when s.is_re_aged = true then 1 end)    as restructured_installment_count,

        count(*)                                            as total_installments_due
    from {{ source('raw', 'raw_m_loan_repayment_schedule') }} s
    inner join loan_dimensions ld
        on s.tenant_id = ld.tenant_id
       and s.loan_id   = ld.loan_id
    group by 1, 2, 3, 4, 5, 6
),

repayment_aggregated as (
    select
        tenant_id,
        reporting_date,
        office_id,
        product_id,
        currency_code,
        client_hash,

        count(*)                                            as repayment_transaction_count,
        count(distinct loan_id)                             as repaid_loan_count,
        count(distinct client_id)                           as repaying_borrower_count,

        sum(repayment_amount)                               as repayment_amount,
        sum(principal_collected)                            as principal_collected,
        sum(interest_collected)                             as interest_collected,
        sum(fee_collected)                                  as fee_collected,
        sum(penalty_collected)                              as penalty_collected,
        sum(overpayment_collected)                          as overpayment_collected,
        sum(post_transaction_outstanding_balance)           as post_transaction_outstanding_balance
    from repayment_transactions
    group by 1, 2, 3, 4, 5, 6
),

recovery_aggregated as (
    select
        tenant_id,
        reporting_date,
        office_id,
        product_id,
        currency_code,
        client_hash,
        sum(recovery_amount)                                as recovery_repayment_amount,
        count(distinct loan_id)                             as recovery_loan_count
    from recovery_transactions
    group by 1, 2, 3, 4, 5, 6
),

combined as (
    select
        coalesce(ra.tenant_id, da.tenant_id)                as tenant_id,
        coalesce(ra.reporting_date, da.reporting_date)      as reporting_date,
        coalesce(ra.office_id, da.office_id)                as office_id,
        coalesce(ra.product_id, da.product_id)              as product_id,
        coalesce(ra.currency_code, da.currency_code)        as currency_code,
        coalesce(ra.client_hash, da.client_hash)            as client_hash,

        coalesce(ra.repayment_transaction_count, 0)         as repayment_transaction_count,
        coalesce(ra.repaid_loan_count, 0)                   as repaid_loan_count,
        coalesce(ra.repaying_borrower_count, 0)             as repaying_borrower_count,
        coalesce(ra.repayment_amount, 0)                    as repayment_amount,

        coalesce(ra.repayment_amount, 0)                    as actual_collected_amount,
        coalesce(da.contractually_due_amount, 0)            as contractually_due_amount,

        coalesce(ra.principal_collected, 0)                 as principal_collected,
        coalesce(ra.interest_collected, 0)                  as interest_collected,
        coalesce(ra.fee_collected, 0)                       as fee_collected,
        coalesce(ra.penalty_collected, 0)                   as penalty_collected,
        coalesce(ra.overpayment_collected, 0)               as overpayment_collected,
        coalesce(ra.post_transaction_outstanding_balance, 0) as post_transaction_outstanding_balance,

        coalesce(da.waived_amount, 0)                       as waived_amount,
        coalesce(da.paid_in_advance_amount, 0)              as paid_in_advance_amount,
        coalesce(da.paid_late_amount, 0)                    as paid_late_amount,
        coalesce(da.early_payment_count, 0)                 as early_payment_count,
        coalesce(da.on_time_payment_count, 0)               as on_time_payment_count,
        coalesce(da.late_payment_count, 0)                  as late_payment_count,
        coalesce(da.restructured_installment_count, 0)      as restructured_installment_count,
        coalesce(da.overdue_penalty_charged, 0)             as overdue_penalty_charged,
        coalesce(da.overdue_penalty_waived, 0)              as overdue_penalty_waived,
        coalesce(da.total_installments_due, 0)              as total_installments_due
    from repayment_aggregated ra
    full outer join due_amounts da
        on  ra.tenant_id      = da.tenant_id
        and ra.reporting_date = da.reporting_date
        and ra.office_id      = da.office_id
        and ra.product_id     = da.product_id
        and ra.currency_code  = da.currency_code
        and ra.client_hash    = da.client_hash
),

detail as (
    select
        c.reporting_date,
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

        {{ safe_divide('c.actual_collected_amount', 'c.contractually_due_amount') }}
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

        coalesce(rec.recovery_repayment_amount, 0)          as recovery_repayment_amount,
        coalesce(rec.recovery_loan_count, 0)                as recovery_loan_count
    from combined c
    inner join {{ ref('dim_office') }} o
        on c.office_id = o.office_id
    inner join {{ ref('dim_product') }} p
        on c.product_id = p.product_id
    left join recovery_aggregated rec
        on  c.tenant_id      = rec.tenant_id
        and c.reporting_date = rec.reporting_date
        and c.office_id      = rec.office_id
        and c.product_id     = rec.product_id
        and c.currency_code  = rec.currency_code
        and c.client_hash    = rec.client_hash
)

select * from detail
