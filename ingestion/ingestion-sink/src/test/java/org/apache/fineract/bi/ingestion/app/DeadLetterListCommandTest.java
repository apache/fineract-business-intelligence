
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
package org.apache.fineract.bi.ingestion.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

public class DeadLetterListCommandTest {

    private static final String DLT = "fineract.cdc.dlt";

    private static String render(
            DeadLetterReader.ScanResult scan,
            List<DeadLetterSummary.Group> groups) {

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            DeadLetterListCommand.render(DLT, scan, groups, out);
        }

        return bytes.toString(StandardCharsets.UTF_8);
    }

    @Test
    public void displaysGroupedCountsAndPartitionSpecificOffsetRanges() {
        TopicPartition dlt0 = new TopicPartition(DLT, 0);

        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(
                2,
                Map.of(dlt0, new DeadLetterReader.PartitionBounds(10, 12)));

        DeadLetterSummary.Group group = new DeadLetterSummary.Group(
                new DeadLetterSummary.Key("WAREHOUSE_REJECTED", "batch_job_instance", "bank-a"),
                2,
                2,
                0,
                List.of(
                        new DeadLetterSummary.OffsetRange(
                                "fineract.cdc.batch_job_instance", 0, 100, 105),
                        new DeadLetterSummary.OffsetRange(
                                "fineract.cdc.batch_job_instance", 1, 200, 200)),
                List.of(new DeadLetterSummary.OffsetRange(DLT, 0, 10, 11)));

        String output = render(scan, List.of(group));

        assertThat(output)
                .contains("Dead-letter inspection")
                .contains("Records scanned: 2")
                .contains("Partitions scanned: 1")
                .contains("WAREHOUSE_REJECTED")
                .contains("batch_job_instance")
                .contains("bank-a")
                .contains("Original offsets")
                .contains("fineract.cdc.batch_job_instance[0]:100..105")
                .contains("fineract.cdc.batch_job_instance[1]:200..200")
                .contains("fineract.cdc.dlt[0]:10..11");
    }

    @Test
    public void displaysEmptyDltMessage() {
        DeadLetterReader.ScanResult scan =
                new DeadLetterReader.ScanResult(0, Map.of());

        String output = render(scan, List.of());

        assertThat(output)
                .contains("Records scanned: 0")
                .contains("No dead-lettered records found.");
    }

    @Test
    public void escapesControlCharactersInDisplayedMetadata() {
        DeadLetterReader.ScanResult scan =
                new DeadLetterReader.ScanResult(1, Map.of());

        DeadLetterSummary.Group group = new DeadLetterSummary.Group(
                new DeadLetterSummary.Key("BAD\nREASON", "batch_job_instance", "bank-a"),
                1,
                1,
                0,
                List.of(),
                List.of());

        String output = render(scan, List.of(group));

        assertThat(output)
                .contains("BAD\\nREASON")
                .doesNotContain("BAD\nREASON");
    }
}
