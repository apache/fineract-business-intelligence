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
package org.apache.fineract.bi.ingestion.cdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.fineract.bi.ingestion.contract.TableContract;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DebeziumEnvelopeParserTest {

    private Contract contract;
    private DebeziumEnvelopeParser parser;

    @BeforeClass
    public void load() {
        contract = ContractLoader.load(Path.of("../../contracts/fineract_tables.json"));
        parser = new DebeziumEnvelopeParser(contract);
    }

    private static byte[] json(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private String value(ChangeEvent e, String table, String column) {
        TableContract t = contract.requireTable(table);
        return e.values()[t.columnIndex(column)];
    }

    @Test
    public void parsesUpsertWithTypedValues() throws Exception {
        String env = "{\"before\":null,\"after\":{\"id\":7,\"loan_id\":3,\"office_id\":1,\"is_reversed\":false,"
                + "\"transaction_type_enum\":2,\"transaction_date\":20089,\"amount\":\"1250.500000\","
                + "\"created_on_utc\":\"2026-01-01T10:00:00.123456Z\"},"
                + "\"source\":{\"name\":\"branch-001\",\"table\":\"m_loan_transaction\",\"lsn\":987654,\"ts_ms\":1767261600000},"
                + "\"op\":\"u\"}";
        ChangeEvent e = parser.parse("fineract.cdc.m_loan_transaction", json(env)).orElseThrow();
        assertThat(e.sourceId()).isEqualTo("branch-001");
        assertThat(e.op()).isEqualTo(ChangeEvent.Op.UPSERT);
        assertThat(e.lsn()).isEqualTo(987654L);
        assertThat(e.sourceTsMicros()).isEqualTo(1767261600000_000L);
        assertThat(value(e, "m_loan_transaction", "id")).isEqualTo("7");
        assertThat(value(e, "m_loan_transaction", "is_reversed")).isEqualTo("f");
        assertThat(value(e, "m_loan_transaction", "transaction_date")).isEqualTo("2025-01-01");
        assertThat(value(e, "m_loan_transaction", "amount")).isEqualTo("1250.500000");
        assertThat(value(e, "m_loan_transaction", "created_on_utc")).isEqualTo("2026-01-01T10:00:00.123456Z");
        assertThat(value(e, "m_loan_transaction", "penalty_charges_portion_derived")).isNull();
    }

    @Test
    public void deleteUsesBeforeImage() throws Exception {
        String env = "{\"before\":{\"id\":42},\"after\":null,\"source\":{\"name\":\"s1\",\"table\":\"m_loan\",\"lsn\":5},"
                + "\"op\":\"d\"}";
        ChangeEvent e = parser.parse("fineract.cdc.m_loan", json(env)).orElseThrow();
        assertThat(e.op()).isEqualTo(ChangeEvent.Op.DELETE);
        assertThat(value(e, "m_loan", "id")).isEqualTo("42");
    }

    @Test
    public void microTimestampAndRenamedColumnsAreMapped() throws Exception {
        String sched = "{\"after\":{\"id\":1,\"loan_id\":2,\"duedate\":0,\"lastmodified_date\":1767261600123456},"
                + "\"source\":{\"name\":\"s1\",\"table\":\"m_loan_repayment_schedule\",\"lsn\":1},\"op\":\"c\"}";
        ChangeEvent e = parser.parse("fineract.cdc.m_loan_repayment_schedule", json(sched)).orElseThrow();
        assertThat(value(e, "m_loan_repayment_schedule", "lastmodified_date")).isEqualTo("2026-01-01T10:00:00.123456Z");
        assertThat(value(e, "m_loan_repayment_schedule", "duedate")).isEqualTo("1970-01-01");

        String job = "{\"after\":{\"job_execution_id\":9,\"create_time\":1767261600000000,\"last_updated\":1767261600000000},"
                + "\"source\":{\"name\":\"s1\",\"table\":\"batch_job_execution\",\"lsn\":1},\"op\":\"r\"}";
        ChangeEvent j = parser.parse("fineract.cdc.batch_job_execution", json(job)).orElseThrow();
        assertThat(value(j, "batch_job_execution", "created_on_utc")).isEqualTo("2026-01-01T10:00:00Z");
    }

    @Test
    public void syntheticColumnsAreAlwaysNull() throws Exception {
        String env = "{\"after\":{\"id\":1,\"name\":\"HO\",\"created_on_utc\":\"2020-01-01T00:00:00Z\"},"
                + "\"source\":{\"name\":\"s1\",\"table\":\"m_office\",\"lsn\":1},\"op\":\"c\"}";
        ChangeEvent e = parser.parse("fineract.cdc.m_office", json(env)).orElseThrow();
        assertThat(value(e, "m_office", "created_on_utc")).isNull();
        assertThat(value(e, "m_office", "name")).isEqualTo("HO");
    }

    @Test
    public void toastPlaceholderIsReportedNotWritten() throws Exception {
        String env = "{\"after\":{\"id\":1,\"description\":\"__debezium_unavailable_value\",\"name\":\"P\"},"
                + "\"source\":{\"name\":\"s1\",\"table\":\"m_product_loan\",\"lsn\":1},\"op\":\"u\"}";
        ChangeEvent e = parser.parse("fineract.cdc.m_product_loan", json(env)).orElseThrow();
        assertThat(e.unavailable()).containsExactly("description");
        assertThat(value(e, "m_product_loan", "description")).isNull();
    }

    @Test
    public void schemaWrappedEnvelopeIsUnwrapped() throws Exception {
        String env = "{\"schema\":{},\"payload\":{\"after\":{\"id\":3},\"source\":{\"name\":\"s1\",\"table\":\"m_client\","
                + "\"lsn\":1},\"op\":\"c\"}}";
        assertThat(parser.parse("fineract.cdc.m_client", json(env))).isPresent();
    }

    @Test
    public void tombstonesAndMessagesCarryNoChange() throws Exception {
        assertThat(parser.parse("fineract.cdc.m_client", null)).isEqualTo(Optional.empty());
        assertThat(parser.parse("fineract.cdc.m_client", new byte[0])).isEmpty();
        assertThat(parser.parse("fineract.cdc.m_client",
                json("{\"op\":\"m\",\"source\":{\"name\":\"s1\"}}"))).isEmpty();
    }

    @Test
    public void poisonEventsAreClassified() {
        assertReason("fineract.cdc.m_client", "not json", "MALFORMED_JSON");
        assertReason("fineract.cdc.m_client", "{\"source\":{\"name\":\"s\"}}", "MISSING_OP");
        assertReason("fineract.cdc.m_client", "{\"op\":\"c\",\"after\":{\"id\":1},\"source\":{}}", "MISSING_SOURCE");
        assertReason("fineract.cdc.m_client",
                "{\"op\":\"t\",\"source\":{\"name\":\"s\",\"table\":\"m_client\"}}", "TRUNCATE_UNSUPPORTED");
        assertReason("fineract.cdc.m_client",
                "{\"op\":\"c\",\"after\":{\"id\":1},\"source\":{\"name\":\"s\",\"table\":\"m_loan\"}}", "TABLE_MISMATCH");
        assertReason("fineract.cdc.m_client",
                "{\"op\":\"c\",\"after\":{\"account_no\":\"x\"},\"source\":{\"name\":\"s\",\"table\":\"m_client\"}}",
                "MISSING_PRIMARY_KEY");
        assertReason("fineract.cdc.m_client",
                "{\"op\":\"c\",\"after\":{\"id\":\"abc\"},\"source\":{\"name\":\"s\",\"table\":\"m_client\"}}",
                "TYPE_MISMATCH");
        assertReason("fineract.cdc.unknown_table", "{}", "UNKNOWN_TOPIC");
    }

    private void assertReason(String topic, String body, String reason) {
        assertThatThrownBy(() -> parser.parse(topic, json(body)))
                .isInstanceOf(PoisonEventException.class)
                .satisfies(e -> assertThat(((PoisonEventException) e).reason()).isEqualTo(reason));
    }
}
