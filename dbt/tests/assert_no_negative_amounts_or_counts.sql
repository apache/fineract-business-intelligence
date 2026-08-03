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
    reporting_date,
    office_id,
    product_id,
    currency_code,
    actual_collected_amount,
    contractually_due_amount,
    waived_amount,
    recovery_repayment_amount,
    early_payment_count,
    on_time_payment_count,
    late_payment_count,
    restructured_installment_count
from {{ ref('mart_repayment_behavior') }}
where actual_collected_amount        < 0
   or contractually_due_amount       < 0
   or principal_collected            < 0
   or interest_collected             < 0
   or fee_collected                  < 0
   or penalty_collected              < 0
   or overpayment_collected          < 0
   or waived_amount                  < 0
   or recovery_repayment_amount      < 0
   or repayment_transaction_count    < 0
   or repaying_borrower_count        < 0
   or repaid_loan_count              < 0
   or early_payment_count            < 0
   or on_time_payment_count          < 0
   or late_payment_count             < 0
   or restructured_installment_count < 0
   or total_installments_due         < 0
