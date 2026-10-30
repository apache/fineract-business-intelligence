/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.bi.ingestion.loadgen;

import java.time.Instant;
import java.time.LocalDate;

public final class SyntheticPayloads {

    static final long BASE_EPOCH_DAY = LocalDate.of(2025, 1, 1).toEpochDay();

    private SyntheticPayloads() {
    }

    public static long amountCents(long lsn) {
        return Math.floorMod(lsn * 2_654_435_761L, 10_000_000L) + 100;
    }

    static String money(long cents) {
        long abs = Math.abs(cents);
        String frac = Long.toString(abs % 100);
        return (cents < 0 ? "-" : "") + (abs / 100) + "." + (frac.length() == 1 ? "0" + frac : frac) + "0000";
    }

    public static void row(StringBuilder sb, String table, long key, long lsn, long tsMicros, int padding) {
        String iso = Instant.ofEpochSecond(Math.floorDiv(tsMicros, 1_000_000L), Math.floorMod(tsMicros, 1_000_000L) * 1_000L)
                .toString();
        long day = BASE_EPOCH_DAY + Math.floorMod(lsn, 600);
        long cents = amountCents(lsn);
        sb.append('{');
        switch (table) {
            case "m_loan_transaction" -> sb.append("\"id\":").append(key)
                    .append(",\"loan_id\":").append(key / 8 + 1)
                    .append(",\"office_id\":").append(key % 50 + 1)
                    .append(",\"is_reversed\":false")
                    .append(",\"transaction_type_enum\":").append(key % 5 == 0 ? 1 : 2)
                    .append(",\"transaction_date\":").append(day)
                    .append(",\"amount\":\"").append(money(cents)).append('"')
                    .append(",\"principal_portion_derived\":\"").append(money(cents * 8 / 10)).append('"')
                    .append(",\"interest_portion_derived\":\"").append(money(cents - cents * 8 / 10)).append('"')
                    .append(",\"fee_charges_portion_derived\":\"0.000000\"")
                    .append(",\"penalty_charges_portion_derived\":\"0.000000\"")
                    .append(",\"outstanding_loan_balance_derived\":\"").append(money(cents * 3)).append('"')
                    .append(",\"submitted_on_date\":").append(day)
                    .append(",\"created_on_utc\":\"").append(iso).append('"')
                    .append(",\"last_modified_on_utc\":\"").append(iso).append('"');
            case "m_loan" -> sb.append("\"id\":").append(key)
                    .append(",\"account_no\":\"LN").append(key).append('"')
                    .append(",\"client_id\":").append(key)
                    .append(",\"product_id\":1")
                    .append(",\"loan_status_id\":300")
                    .append(",\"loan_type_enum\":1")
                    .append(",\"currency_code\":\"INR\"")
                    .append(",\"currency_digits\":2")
                    .append(",\"principal_amount\":\"").append(money(cents * 10)).append('"')
                    .append(",\"approved_principal\":\"").append(money(cents * 10)).append('"')
                    .append(",\"disbursedon_date\":").append(day)
                    .append(",\"principal_disbursed_derived\":\"").append(money(cents * 10)).append('"')
                    .append(",\"principal_outstanding_derived\":\"").append(money(cents * 6)).append('"')
                    .append(",\"total_expected_repayment_derived\":\"").append(money(cents * 12)).append('"')
                    .append(",\"total_outstanding_derived\":\"").append(money(cents * 7)).append('"')
                    .append(",\"is_npa\":").append(lsn % 97 == 0)
                    .append(",\"created_on_utc\":\"").append(iso).append('"')
                    .append(",\"last_modified_on_utc\":\"").append(iso).append('"');
            case "m_loan_repayment_schedule" -> sb.append("\"id\":").append(key)
                    .append(",\"loan_id\":").append(key / 12 + 1)
                    .append(",\"fromdate\":").append(day - 30)
                    .append(",\"duedate\":").append(day)
                    .append(",\"installment\":").append(key % 12 + 1)
                    .append(",\"principal_amount\":\"").append(money(cents)).append('"')
                    .append(",\"interest_amount\":\"").append(money(cents / 5)).append('"')
                    .append(",\"completed_derived\":").append(lsn % 3 == 0)
                    .append(",\"is_re_aged\":false")
                    .append(",\"lastmodified_date\":").append(tsMicros);
            case "m_client" -> sb.append("\"id\":").append(key)
                    .append(",\"account_no\":\"CL").append(key).append('"')
                    .append(",\"status_enum\":300")
                    .append(",\"office_id\":").append(key % 50 + 1)
                    .append(",\"date_of_birth\":").append(BASE_EPOCH_DAY - 7_000 - key % 15_000)
                    .append(",\"activation_date\":").append(day)
                    .append(",\"created_on_utc\":\"").append(iso).append('"')
                    .append(",\"last_modified_on_utc\":\"").append(iso).append('"');
            default -> sb.append("\"id\":").append(key);
        }
        if (padding > 0) {
            sb.append(",\"_padding\":\"");
            for (int i = 0; i < padding; i++) {
                sb.append((char) ('a' + (i % 26)));
            }
            sb.append('"');
        }
        sb.append('}');
    }

    public static String envelope(String sourceId, String table, String pk, long key, long lsn, char op,
            long tsMicros, int padding) {
        StringBuilder sb = new StringBuilder(512 + padding);
        sb.append("{\"before\":");
        if (op == 'd') {
            sb.append("{\"").append(pk).append("\":").append(key).append('}');
        } else {
            sb.append("null");
        }
        sb.append(",\"after\":");
        if (op == 'd') {
            sb.append("null");
        } else {
            row(sb, table, key, lsn, tsMicros, padding);
        }
        long tsMs = tsMicros / 1_000L;
        sb.append(",\"source\":{\"version\":\"loadgen\",\"connector\":\"postgresql\",\"name\":\"").append(sourceId)
                .append("\",\"ts_ms\":").append(tsMs).append(",\"ts_us\":").append(tsMicros)
                .append(",\"snapshot\":\"false\",\"db\":\"fineract\",\"schema\":\"public\",\"table\":\"").append(table)
                .append("\",\"lsn\":").append(lsn).append("},\"op\":\"").append(op).append("\",\"ts_ms\":").append(tsMs)
                .append('}');
        return sb.toString();
    }
}
