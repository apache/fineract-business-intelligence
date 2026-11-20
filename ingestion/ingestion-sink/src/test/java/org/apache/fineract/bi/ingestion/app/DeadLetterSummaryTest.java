
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
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.bi.ingestion.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.testng.annotations.Test;

public class DeadLetterSummaryTest {

    private static final Contract CONTRACT =
            ContractLoader.load(Path.of("../../contracts/fineract_tables.json"));

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static DeadLetterRecord record(
            int dltPartition,
            long dltOffset,
            int sourcePartition,
            long sourceOffset,
            String reason,
            String source,
            boolean omitted) {

        RecordHeaders headers = new RecordHeaders();
        headers.add("fbi.dlt.reason", bytes(reason));
        headers.add("fbi.dlt.message", bytes("test rejection"));
        headers.add("fbi.source.topic", bytes("fineract.cdc.batch_job_instance"));
        headers.add("fbi.source.partition", bytes(Integer.toString(sourcePartition)));
        headers.add("fbi.source.offset", bytes(Long.toString(sourceOffset)));

        byte[] value = omitted
                ? null
                : bytes("{\"source\":{\"name\":\"" + source + "\"},\"op\":\"c\"}");

        if (omitted) {
            headers.add("fbi.dlt.value_omitted", bytes("true"));
            headers.add("fbi.dlt.value_bytes", bytes("500"));
        }

        return DeadLetterRecord.parse(
                "fineract.cdc.dlt", dltPartition, dltOffset, headers, value, CONTRACT);
    }

    @Test
    public void groupsRecordsByReasonTableAndSource() {
        DeadLetterRecord first =
                record(0, 20, 0, 100, "WAREHOUSE_REJECTED", "bank-a", false);

        DeadLetterRecord second =
                record(0, 22, 0, 105, "WAREHOUSE_REJECTED", "bank-a", false);

        DeadLetterRecord third =
                record(1, 30, 1, 200, "WAREHOUSE_REJECTED", "bank-b", false);

        List<DeadLetterSummary.Group> groups =
                DeadLetterSummary.summarize(List.of(first, second, third));

        assertThat(groups).hasSize(2);

        DeadLetterSummary.Group bankA = groups.stream()
                .filter(g -> g.key().sourceId().equals("bank-a"))
                .findFirst()
                .orElseThrow();

        assertThat(bankA.key().reason()).isEqualTo("WAREHOUSE_REJECTED");
        assertThat(bankA.key().table()).isEqualTo("batch_job_instance");
        assertThat(bankA.count()).isEqualTo(2);
        assertThat(bankA.payloadAvailable()).isEqualTo(2);
        assertThat(bankA.omittedPayloads()).isZero();

        assertThat(bankA.originalOffsets()).containsExactly(
                new DeadLetterSummary.OffsetRange(
                        "fineract.cdc.batch_job_instance", 0, 100, 105));

        assertThat(bankA.dltOffsets()).containsExactly(
                new DeadLetterSummary.OffsetRange(
                        "fineract.cdc.dlt", 0, 20, 22));
    }

    @Test
    public void keepsOriginalOffsetRangesSeparateByPartition() {
        DeadLetterRecord first =
                record(0, 10, 0, 100, "TYPE_MISMATCH", "bank-a", false);

        DeadLetterRecord second =
                record(0, 11, 1, 200, "TYPE_MISMATCH", "bank-a", false);

        List<DeadLetterSummary.Group> groups =
                DeadLetterSummary.summarize(List.of(first, second));

        assertThat(groups).hasSize(1);

        assertThat(groups.get(0).originalOffsets()).containsExactly(
                new DeadLetterSummary.OffsetRange(
                        "fineract.cdc.batch_job_instance", 0, 100, 100),
                new DeadLetterSummary.OffsetRange(
                        "fineract.cdc.batch_job_instance", 1, 200, 200));
    }

    @Test
public void countsOmittedPayloadsWithoutTreatingThemAsAvailable() {
    DeadLetterRecord available =
            record(0, 10, 0, 100, "WAREHOUSE_REJECTED", "bank-a", false);

    DeadLetterRecord omitted =
            record(0, 11, 0, 101, "WAREHOUSE_REJECTED", "bank-a", true);

    List<DeadLetterSummary.Group> groups =
            DeadLetterSummary.summarize(List.of(available, omitted));

    assertThat(groups).hasSize(2);

    DeadLetterSummary.Group bankA = groups.stream()
            .filter(g -> g.key().sourceId().equals("bank-a"))
            .findFirst()
            .orElseThrow();

    assertThat(bankA.count()).isEqualTo(1);
    assertThat(bankA.payloadAvailable()).isEqualTo(1);
    assertThat(bankA.omittedPayloads()).isZero();

    DeadLetterSummary.Group unknown = groups.stream()
            .filter(g -> g.key().sourceId().equals(DeadLetterSummary.UNKNOWN_SOURCE))
            .findFirst()
            .orElseThrow();

    assertThat(unknown.count()).isEqualTo(1);
    assertThat(unknown.payloadAvailable()).isZero();
    assertThat(unknown.omittedPayloads()).isEqualTo(1);
}

    @Test
    public void groupsMalformedPayloadsUnderUnknownSource() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("fbi.dlt.reason", bytes("MALFORMED_JSON"));
        headers.add("fbi.source.topic", bytes("fineract.cdc.batch_job_instance"));
        headers.add("fbi.source.partition", bytes("0"));
        headers.add("fbi.source.offset", bytes("50"));

        DeadLetterRecord malformed = DeadLetterRecord.parse(
                "fineract.cdc.dlt", 0, 10, headers, bytes("{broken"), CONTRACT);

        DeadLetterSummary.Group group =
                DeadLetterSummary.summarize(List.of(malformed)).get(0);

        assertThat(group.key().sourceId())
                .isEqualTo(DeadLetterSummary.UNKNOWN_SOURCE);
        assertThat(group.count()).isEqualTo(1);
        assertThat(group.payloadAvailable()).isEqualTo(1);
    }

    @Test
    public void sortsGroupsDeterministically() {
        DeadLetterRecord bankB =
                record(0, 10, 0, 100, "TYPE_MISMATCH", "bank-b", false);

        DeadLetterRecord bankA =
                record(0, 11, 0, 101, "TYPE_MISMATCH", "bank-a", false);

        DeadLetterRecord poison =
                record(0, 12, 0, 102, "MALFORMED_JSON", "bank-a", false);

        List<DeadLetterSummary.Group> groups =
                DeadLetterSummary.summarize(List.of(bankB, bankA, poison));

        assertThat(groups).extracting(g -> g.key().reason())
                .containsExactly("MALFORMED_JSON", "TYPE_MISMATCH", "TYPE_MISMATCH");

        assertThat(groups.get(1).key().sourceId()).isEqualTo("bank-a");
        assertThat(groups.get(2).key().sourceId()).isEqualTo("bank-b");
    }

    @Test
    public void returnsNoGroupsForAnEmptyDlt() {
        assertThat(DeadLetterSummary.summarize(List.of())).isEmpty();
    }
}
