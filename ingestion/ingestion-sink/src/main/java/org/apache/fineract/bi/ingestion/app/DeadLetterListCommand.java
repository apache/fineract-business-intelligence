
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

import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.kafka.common.TopicPartition;

public final class DeadLetterListCommand {

    private DeadLetterListCommand() {
    }

    public static void run(SinkConfig config, Contract contract) throws Exception {
        DeadLetterSummary.Accumulator accumulator = new DeadLetterSummary.Accumulator();
        DeadLetterReader.ScanResult scan;

        try (DeadLetterReader reader = new DeadLetterReader(config, contract)) {
            scan = reader.scan(accumulator::add);
        }

        render(config.dltTopic(), scan, accumulator.snapshot(), accumulator.invalidReplayRecords(), System.out);
    }

    static void render(
            String topic,
            DeadLetterReader.ScanResult scan,
            List<DeadLetterSummary.Group> groups,
            PrintStream out) {
        render(topic, scan, groups, List.of(), out);
    }

    static void render(
            String topic,
            DeadLetterReader.ScanResult scan,
            List<DeadLetterSummary.Group> groups,
            List<DeadLetterRecord> invalidReplayRecords,
            PrintStream out) {

        out.println("Dead-letter inspection");
        out.println("======================");
        out.println("Topic: " + escape(topic));
        out.println("Records scanned: " + scan.recordsRead());
        out.println("Partitions scanned: " + scan.partitions().size());

        scan.partitions().entrySet().stream()
                .sorted(Map.Entry.comparingByKey(
                        Comparator.comparing(TopicPartition::topic)
                                .thenComparingInt(TopicPartition::partition)))
                .forEach(entry -> {
                    TopicPartition partition = entry.getKey();
                    DeadLetterReader.PartitionBounds bounds = entry.getValue();

                    out.printf("  %s[%d]: offsets [%d, %d)%n",
                            escape(partition.topic()),
                            partition.partition(),
                            bounds.beginningOffset(),
                            bounds.endOffset());
                });

        out.println();

        if (groups.isEmpty()) {
            out.println("No dead-lettered records found.");
            return;
        }

        out.println("Reason\tTable\tSource\tCount\tPayload available\tPayload omitted"
                + "\tOriginal offsets\tDLT offsets");

        for (DeadLetterSummary.Group group : groups) {
            DeadLetterSummary.Key key = group.key();

            out.println(String.join("\t",
                    escape(key.reason()),
                    escape(key.table()),
                    escape(key.sourceId()),
                    Long.toString(group.count()),
                    Long.toString(group.payloadAvailable()),
                    Long.toString(group.omittedPayloads()),
                    formatRanges(group.originalOffsets()),
                    formatRanges(group.dltOffsets())));
        }

        if (invalidReplayRecords != null && !invalidReplayRecords.isEmpty()) {
            out.println();
            out.println("Warning: Found " + invalidReplayRecords.size()
                    + " record(s) with invalid replay identity metadata (refused by dlt-replay):");
            for (DeadLetterRecord r : invalidReplayRecords) {
                out.printf("  %s[%d]@%d: %s%n",
                        escape(r.dltTopic()), r.dltPartition(), r.dltOffset(),
                        escape(r.replayIdentityError()));
            }
        }
    }

    private static String formatRanges(List<DeadLetterSummary.OffsetRange> ranges) {
        if (ranges.isEmpty()) {
            return "-";
        }

        return ranges.stream()
                .map(range -> escape(range.topic()) + "[" + range.partition() + "]:"
                        + range.oldestOffset() + ".." + range.newestOffset())
                .collect(Collectors.joining(", "));
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }

        return value.replace("\\", "\\\\")
                .replace("\t", "\\t")
                .replace("\r", "\\r")
                .replace("\n", "\\n");
    }
}
