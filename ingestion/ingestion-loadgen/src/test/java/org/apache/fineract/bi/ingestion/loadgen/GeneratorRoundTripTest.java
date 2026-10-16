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

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.apache.fineract.bi.ingestion.cdc.ChangeEvent;
import org.apache.fineract.bi.ingestion.cdc.DebeziumEnvelopeParser;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.testng.annotations.Test;

public class GeneratorRoundTripTest {

    private final Contract contract = ContractLoader.load(Path.of("../../contracts/fineract_tables.json"));
    private final DebeziumEnvelopeParser parser = new DebeziumEnvelopeParser(contract);

    @Test
    public void everyGeneratedTableParses() throws Exception {
        for (String table : List.of("m_loan_transaction", "m_loan", "m_loan_repayment_schedule", "m_client")) {
            for (char op : new char[] {'c', 'u', 'd'}) {
                String env = SyntheticPayloads.envelope("loadsrc-00001", table, "id", 17, 1_000_123L, op,
                        1_767_261_600_000_000L, 64);
                ChangeEvent e = parser.parse("fineract.cdc." + table, env.getBytes(StandardCharsets.UTF_8)).orElseThrow();
                assertThat(e.sourceId()).isEqualTo("loadsrc-00001");
                assertThat(e.lsn()).isEqualTo(1_000_123L);
                assertThat(e.op()).isEqualTo(op == 'd' ? ChangeEvent.Op.DELETE : ChangeEvent.Op.UPSERT);
                assertThat(e.values()[contract.requireTable(table).primaryKeyIndex()]).isEqualTo("17");
            }
        }
    }

    @Test
    public void amountIsAPureFunctionOfTheVersion() throws Exception {
        long lsn = 987_654_321L;
        String env = SyntheticPayloads.envelope("s", "m_loan_transaction", "id", 1, lsn, 'u', 0L, 0);
        ChangeEvent e = parser.parse("fineract.cdc.m_loan_transaction", env.getBytes(StandardCharsets.UTF_8)).orElseThrow();
        String amount = e.values()[contract.requireTable("m_loan_transaction").columnIndex("amount")];
        assertThat(new BigDecimal(amount).movePointRight(2).longValueExact()).isEqualTo(SyntheticPayloads.amountCents(lsn));
    }

    @Test
    public void groundTruthKeepsTheLatestVersionPerKey() {
        long base = 1_000L;
        GroundTruth gt = new GroundTruth(2, List.of("m_loan_transaction"), 10, base);
        gt.record(0, 0, 3, base + 5, false);
        gt.record(0, 0, 3, base + 9, false);
        gt.record(0, 0, 3, base + 7, true);
        gt.record(0, 0, 4, base + 2, false);
        gt.record(0, 0, 4, base + 8, true);
        gt.record(1, 0, 3, base + 1, false);
        GroundTruth.Expected s0 = gt.expected(0, 0);
        assertThat(s0.live()).isEqualTo(1);
        assertThat(s0.deleted()).isEqualTo(1);
        assertThat(s0.lsnSum()).isEqualTo(9);
        assertThat(s0.amountCentsSum()).isEqualTo(SyntheticPayloads.amountCents(base + 9));
        assertThat(gt.expected(1, 0).live()).isEqualTo(1);
        assertThat(gt.isLive(0, 0, 4)).isFalse();
    }

    @Test
    public void weightedTablePickAndMoneyFormatting() {
        double[] cumulative = {0.5, 0.8, 1.0};
        assertThat(EventProducer.pick(cumulative, 0.1)).isZero();
        assertThat(EventProducer.pick(cumulative, 0.79)).isEqualTo(1);
        assertThat(EventProducer.pick(cumulative, 0.99)).isEqualTo(2);
        assertThat(SyntheticPayloads.money(12_305)).isEqualTo("123.050000");
        assertThat(SyntheticPayloads.money(7)).isEqualTo("0.070000");
    }
}
