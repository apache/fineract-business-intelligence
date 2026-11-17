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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

public class DeadLetterReaderTest {

    private static final String DLT = "fineract.cdc.dlt";

    private static final Contract CONTRACT =
            ContractLoader.load(Path.of("../../contracts/fineract_tables.json"));

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static ConsumerRecord<byte[], byte[]> record(
            int partition, long offset, String source, long sourceOffset) {

        byte[] payload = bytes(
                "{\"source\":{\"name\":\"" + source + "\"},\"op\":\"c\"}");

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(DLT, partition, offset, null, payload);

        record.headers().add("fbi.dlt.reason", bytes("WAREHOUSE_REJECTED"));
        record.headers().add("fbi.dlt.message", bytes("test rejection"));
        record.headers().add("fbi.source.topic", bytes("fineract.cdc.batch_job_instance"));
        record.headers().add("fbi.source.partition", bytes("0"));
        record.headers().add("fbi.source.offset", bytes(Long.toString(sourceOffset)));

        return record;
    }

    @Test
    public void emptyDltReturnsEmptyCompletedResult() {
        MockConsumer<byte[], byte[]> consumer =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        consumer.updatePartitions(DLT, List.of());

        try (DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT)) {
            DeadLetterReader.ScanResult result = reader.scan(record -> {
                throw new AssertionError("No records should be visited");
            });

            assertThat(result.recordsScanned()).isZero();
            assertThat(result.recordsMatched()).isZero();
            assertThat(result.bytesScanned()).isZero();
            assertThat(result.recordsRead()).isZero();
            assertThat(result.partitions()).isEmpty();
            assertThat(result.stopReason()).isEqualTo(DeadLetterReader.StopReason.COMPLETED);
        }
    }

    @Test
    public void scansEveryPartitionWithinTheCapturedOffsetBounds() {
        MockConsumer<byte[], byte[]> consumer =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        TopicPartition p0 = new TopicPartition(DLT, 0);
        TopicPartition p1 = new TopicPartition(DLT, 1);

        consumer.updatePartitions(DLT, List.of(
                new PartitionInfo(DLT, 0, null, null, null),
                new PartitionInfo(DLT, 1, null, null, null)));

        consumer.updateBeginningOffsets(Map.of(p0, 10L, p1, 20L));
        consumer.updateEndOffsets(Map.of(p0, 11L, p1, 22L));

        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(0, 10L, "bank-a", 100L));
        });
        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(1, 20L, "bank-b", 200L));
            consumer.addRecord(record(1, 21L, "bank-c", 201L));

            consumer.addRecord(record(1, 22L, "bank-d", 202L));
        });

        List<DeadLetterRecord> records = new ArrayList<>();

        try (DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT)) {
            DeadLetterReader.ScanResult result = reader.scan(record -> {
                records.add(record);
                return new DeadLetterReader.VisitResult(true, false);
            });

            assertThat(result.recordsRead()).isEqualTo(3);
            assertThat(result.recordsScanned()).isEqualTo(3);
            assertThat(result.recordsMatched()).isEqualTo(3);
            assertThat(result.bytesScanned()).isPositive();
            assertThat(result.stopReason()).isEqualTo(DeadLetterReader.StopReason.COMPLETED);
            assertThat(records).hasSize(3);
            assertThat(records).extracting(DeadLetterRecord::sourceId)
                    .containsExactly("bank-a", "bank-b", "bank-c");

            assertThat(result.partitions())
                    .containsEntry(p0, new DeadLetterReader.PartitionBounds(10L, 11L))
                    .containsEntry(p1, new DeadLetterReader.PartitionBounds(20L, 22L));
        }
    }

    @Test
    public void partitionTraversalIsAscendingRegardlessOfOrderReturnedByPartitionMetadata() {
        MockConsumer<byte[], byte[]> consumer =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        TopicPartition p0 = new TopicPartition(DLT, 0);
        TopicPartition p1 = new TopicPartition(DLT, 1);
        TopicPartition p2 = new TopicPartition(DLT, 2);

        consumer.updatePartitions(DLT, List.of(
                new PartitionInfo(DLT, 2, null, null, null),
                new PartitionInfo(DLT, 0, null, null, null),
                new PartitionInfo(DLT, 1, null, null, null)));

        consumer.updateBeginningOffsets(Map.of(p0, 0L, p1, 0L, p2, 0L));
        consumer.updateEndOffsets(Map.of(p0, 1L, p1, 1L, p2, 1L));

        consumer.schedulePollTask(() -> consumer.addRecord(record(0, 0L, "bank-0", 100L)));
        consumer.schedulePollTask(() -> consumer.addRecord(record(1, 0L, "bank-1", 150L)));
        consumer.schedulePollTask(() -> consumer.addRecord(record(2, 0L, "bank-2", 200L)));

        List<DeadLetterRecord> visited = new ArrayList<>();

        try (DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT)) {
            DeadLetterReader.ScanResult result = reader.scan(record -> {
                visited.add(record);
                return new DeadLetterReader.VisitResult(true, false);
            });

            assertThat(result.stopReason()).isEqualTo(DeadLetterReader.StopReason.COMPLETED);
            assertThat(visited).extracting(DeadLetterRecord::dltPartition)
                    .containsExactly(0, 1, 2);
            assertThat(visited).extracting(DeadLetterRecord::sourceId)
                    .containsExactly("bank-0", "bank-1", "bank-2");
        }
    }

    @Test
    public void offsetsWithinEachPartitionAreAscending() {
        MockConsumer<byte[], byte[]> consumer =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        TopicPartition p0 = new TopicPartition(DLT, 0);
        TopicPartition p1 = new TopicPartition(DLT, 1);

        consumer.updatePartitions(DLT, List.of(
                new PartitionInfo(DLT, 0, null, null, null),
                new PartitionInfo(DLT, 1, null, null, null)));

        consumer.updateBeginningOffsets(Map.of(p0, 10L, p1, 20L));
        consumer.updateEndOffsets(Map.of(p0, 13L, p1, 23L));

        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(0, 10L, "bank-0-10", 100L));
            consumer.addRecord(record(0, 11L, "bank-0-11", 101L));
            consumer.addRecord(record(0, 12L, "bank-0-12", 102L));
        });
        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(1, 20L, "bank-1-20", 200L));
            consumer.addRecord(record(1, 21L, "bank-1-21", 201L));
            consumer.addRecord(record(1, 22L, "bank-1-22", 202L));
        });

        List<Long> p0Offsets = new ArrayList<>();
        List<Long> p1Offsets = new ArrayList<>();

        try (DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT)) {
            DeadLetterReader.ScanResult result = reader.scan(record -> {
                if (record.dltPartition() == 0) {
                    p0Offsets.add(record.dltOffset());
                } else if (record.dltPartition() == 1) {
                    p1Offsets.add(record.dltOffset());
                }
                return new DeadLetterReader.VisitResult(true, false);
            });

            assertThat(result.stopReason()).isEqualTo(DeadLetterReader.StopReason.COMPLETED);
            assertThat(p0Offsets).containsExactly(10L, 11L, 12L);
            assertThat(p1Offsets).containsExactly(20L, 21L, 22L);
        }
    }

    @Test
    public void matchingRecordLimitStopsScanImmediatelyAndDoesNotVisitLaterPartitions() {
        MockConsumer<byte[], byte[]> consumer =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        TopicPartition p0 = new TopicPartition(DLT, 0);
        TopicPartition p1 = new TopicPartition(DLT, 1);

        consumer.updatePartitions(DLT, List.of(
                new PartitionInfo(DLT, 0, null, null, null),
                new PartitionInfo(DLT, 1, null, null, null)));

        consumer.updateBeginningOffsets(Map.of(p0, 0L, p1, 0L));
        consumer.updateEndOffsets(Map.of(p0, 2L, p1, 2L));

        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(0, 0L, "bank-0-0", 100L));
            consumer.addRecord(record(0, 1L, "bank-0-1", 101L));
        });

        List<DeadLetterRecord> visited = new ArrayList<>();
        DeadLetterReader.ScanOptions options = new DeadLetterReader.ScanOptions(1, DeadLetterReader.DEFAULT_MAX_BYTES);

        try (DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT)) {
            DeadLetterReader.ScanResult result = reader.scan(options, record -> {
                visited.add(record);
                return new DeadLetterReader.VisitResult(true, false);
            });

            assertThat(result.stopReason()).isEqualTo(DeadLetterReader.StopReason.RECORD_LIMIT_REACHED);
            assertThat(result.recordsScanned()).isEqualTo(1);
            assertThat(result.recordsMatched()).isEqualTo(1);
            assertThat(visited).hasSize(1);
            assertThat(visited.get(0).dltPartition()).isEqualTo(0);
            assertThat(visited.get(0).dltOffset()).isEqualTo(0L);

            assertThat(consumer.assignment()).doesNotContain(p1);
        }
    }

    @Test
    public void nonmatchingRecordsDoNotConsumeMatchLimit() {
        MockConsumer<byte[], byte[]> consumer =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        TopicPartition p0 = new TopicPartition(DLT, 0);
        TopicPartition p1 = new TopicPartition(DLT, 1);

        consumer.updatePartitions(DLT, List.of(
                new PartitionInfo(DLT, 0, null, null, null),
                new PartitionInfo(DLT, 1, null, null, null)));

        consumer.updateBeginningOffsets(Map.of(p0, 0L, p1, 0L));
        consumer.updateEndOffsets(Map.of(p0, 3L, p1, 2L));

        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(0, 0L, "skip-1", 100L));
            consumer.addRecord(record(0, 1L, "skip-2", 101L));
            consumer.addRecord(record(0, 2L, "match-1", 102L));
        });
        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(1, 0L, "match-2", 200L));
            consumer.addRecord(record(1, 1L, "match-3", 201L));
        });

        List<DeadLetterRecord> matched = new ArrayList<>();
        DeadLetterReader.ScanOptions options = new DeadLetterReader.ScanOptions(2, DeadLetterReader.DEFAULT_MAX_BYTES);

        try (DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT)) {
            DeadLetterReader.ScanResult result = reader.scan(options, record -> {
                boolean matches = record.sourceId().startsWith("match");
                if (matches) {
                    matched.add(record);
                    return new DeadLetterReader.VisitResult(true, false);
                }
                return new DeadLetterReader.VisitResult(false, false);
            });

            assertThat(result.stopReason()).isEqualTo(DeadLetterReader.StopReason.RECORD_LIMIT_REACHED);
            assertThat(result.recordsScanned()).isEqualTo(4);
            assertThat(result.recordsMatched()).isEqualTo(2);
            assertThat(matched).hasSize(2);
            assertThat(matched).extracting(DeadLetterRecord::sourceId).containsExactly("match-1", "match-2");
        }
    }

    @Test
    public void byteBudgetStopsScanningBeforeRecordThatWouldExceedRemainingBudget() {
        MockConsumer<byte[], byte[]> consumer =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        TopicPartition p0 = new TopicPartition(DLT, 0);
        TopicPartition p1 = new TopicPartition(DLT, 1);

        consumer.updatePartitions(DLT, List.of(
                new PartitionInfo(DLT, 0, null, null, null),
                new PartitionInfo(DLT, 1, null, null, null)));

        consumer.updateBeginningOffsets(Map.of(p0, 0L, p1, 0L));
        consumer.updateEndOffsets(Map.of(p0, 2L, p1, 1L));

        ConsumerRecord<byte[], byte[]> r1 = record(0, 0L, "bank-0-0", 100L);
        ConsumerRecord<byte[], byte[]> r2 = record(0, 1L, "bank-0-1", 101L);

        long r1Bytes = DeadLetterReader.recordByteSize(r1);

        consumer.schedulePollTask(() -> {
            consumer.addRecord(r1);
            consumer.addRecord(r2);
        });

        List<DeadLetterRecord> visited = new ArrayList<>();
        DeadLetterReader.ScanOptions options = new DeadLetterReader.ScanOptions(Long.MAX_VALUE, r1Bytes);

        try (DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT)) {
            DeadLetterReader.ScanResult result = reader.scan(options, record -> {
                visited.add(record);
                return new DeadLetterReader.VisitResult(true, false);
            });

            assertThat(result.stopReason()).isEqualTo(DeadLetterReader.StopReason.BYTE_BUDGET_REACHED);
            assertThat(result.recordsScanned()).isEqualTo(1);
            assertThat(result.bytesScanned()).isEqualTo(r1Bytes);
            assertThat(visited).hasSize(1);
            assertThat(visited.get(0).dltOffset()).isEqualTo(0L);

            assertThat(consumer.assignment()).doesNotContain(p1);
        }
    }

    @Test
    public void recordThatDoesNotFitBudgetIsNotParsedOrVisited() {
        MockConsumer<byte[], byte[]> consumer =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        TopicPartition p0 = new TopicPartition(DLT, 0);

        consumer.updatePartitions(DLT, List.of(
                new PartitionInfo(DLT, 0, null, null, null)));

        consumer.updateBeginningOffsets(Map.of(p0, 0L));
        consumer.updateEndOffsets(Map.of(p0, 2L));

        ConsumerRecord<byte[], byte[]> r1 = record(0, 0L, "bank-valid", 100L);
        ConsumerRecord<byte[], byte[]> r2 = new ConsumerRecord<>(DLT, 0, 1L, null, bytes("{invalid_json_payload"));
        r2.headers().add("fbi.dlt.reason", bytes("UNKNOWN"));

        long r1Bytes = DeadLetterReader.recordByteSize(r1);

        consumer.schedulePollTask(() -> {
            consumer.addRecord(r1);
            consumer.addRecord(r2);
        });

        List<DeadLetterRecord> visited = new ArrayList<>();
        DeadLetterReader.ScanOptions options = new DeadLetterReader.ScanOptions(Long.MAX_VALUE, r1Bytes);

        try (DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT)) {
            DeadLetterReader.ScanResult result = reader.scan(options, record -> {
                visited.add(record);
                return new DeadLetterReader.VisitResult(true, false);
            });

            assertThat(result.stopReason()).isEqualTo(DeadLetterReader.StopReason.BYTE_BUDGET_REACHED);
            assertThat(result.recordsScanned()).isEqualTo(1);
            assertThat(visited).hasSize(1);
            assertThat(visited.get(0).sourceId()).isEqualTo("bank-valid");
        }
    }

    @Test
    public void exactScanStatisticsAreReturned() {
        MockConsumer<byte[], byte[]> consumer =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        TopicPartition p0 = new TopicPartition(DLT, 0);

        consumer.updatePartitions(DLT, List.of(
                new PartitionInfo(DLT, 0, null, null, null)));

        consumer.updateBeginningOffsets(Map.of(p0, 10L));
        consumer.updateEndOffsets(Map.of(p0, 13L));

        ConsumerRecord<byte[], byte[]> r1 = record(0, 10L, "bank-1", 100L);
        ConsumerRecord<byte[], byte[]> r2 = record(0, 11L, "bank-2", 101L);
        ConsumerRecord<byte[], byte[]> r3 = record(0, 12L, "bank-3", 102L);

        long expectedBytes = DeadLetterReader.recordByteSize(r1)
                + DeadLetterReader.recordByteSize(r2)
                + DeadLetterReader.recordByteSize(r3);

        consumer.schedulePollTask(() -> {
            consumer.addRecord(r1);
            consumer.addRecord(r2);
            consumer.addRecord(r3);
        });

        try (DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT)) {
            DeadLetterReader.ScanResult result = reader.scan(record -> {
                boolean matches = !"bank-2".equals(record.sourceId());
                return new DeadLetterReader.VisitResult(matches, false);
            });

            assertThat(result.recordsScanned()).isEqualTo(3);
            assertThat(result.recordsMatched()).isEqualTo(2);
            assertThat(result.bytesScanned()).isEqualTo(expectedBytes);
            assertThat(result.stopReason()).isEqualTo(DeadLetterReader.StopReason.COMPLETED);
            assertThat(result.partitions()).containsEntry(p0, new DeadLetterReader.PartitionBounds(10L, 13L));
        }
    }

    @Test
    public void visitorRequestedTerminationWorks() {
        MockConsumer<byte[], byte[]> consumer =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        TopicPartition p0 = new TopicPartition(DLT, 0);
        TopicPartition p1 = new TopicPartition(DLT, 1);

        consumer.updatePartitions(DLT, List.of(
                new PartitionInfo(DLT, 0, null, null, null),
                new PartitionInfo(DLT, 1, null, null, null)));

        consumer.updateBeginningOffsets(Map.of(p0, 0L, p1, 0L));
        consumer.updateEndOffsets(Map.of(p0, 2L, p1, 2L));

        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(0, 0L, "bank-0", 100L));
            consumer.addRecord(record(0, 1L, "bank-1", 101L));
        });

        List<DeadLetterRecord> visited = new ArrayList<>();

        try (DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT)) {
            DeadLetterReader.ScanResult result = reader.scan(record -> {
                visited.add(record);
                return new DeadLetterReader.VisitResult(true, true);
            });

            assertThat(result.stopReason()).isEqualTo(DeadLetterReader.StopReason.VISITOR_REQUESTED);
            assertThat(result.recordsScanned()).isEqualTo(1);
            assertThat(result.recordsMatched()).isEqualTo(1);
            assertThat(visited).hasSize(1);
            assertThat(consumer.assignment()).doesNotContain(p1);
        }
    }

    @Test
    public void recordsAppendedAfterCapturedEndOffsetRemainIgnored() {
        MockConsumer<byte[], byte[]> consumer =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        TopicPartition p0 = new TopicPartition(DLT, 0);

        consumer.updatePartitions(DLT, List.of(
                new PartitionInfo(DLT, 0, null, null, null)));

        consumer.updateBeginningOffsets(Map.of(p0, 10L));
        consumer.updateEndOffsets(Map.of(p0, 11L));

        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(0, 10L, "bank-within", 100L));
            consumer.addRecord(record(0, 11L, "bank-after-1", 101L));
            consumer.addRecord(record(0, 12L, "bank-after-2", 102L));
        });

        List<DeadLetterRecord> visited = new ArrayList<>();

        try (DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT)) {
            DeadLetterReader.ScanResult result = reader.scan(record -> {
                visited.add(record);
                return new DeadLetterReader.VisitResult(true, false);
            });

            assertThat(result.stopReason()).isEqualTo(DeadLetterReader.StopReason.COMPLETED);
            assertThat(result.recordsScanned()).isEqualTo(1);
            assertThat(visited).hasSize(1);
            assertThat(visited.get(0).sourceId()).isEqualTo("bank-within");
        }
    }

    @Test
    public void scanOptionsValidatesPositiveArguments() {
        assertThatThrownBy(() -> new DeadLetterReader.ScanOptions(0, 1000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxMatches must be positive");

        assertThatThrownBy(() -> new DeadLetterReader.ScanOptions(-1, 1000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxMatches must be positive");

        assertThatThrownBy(() -> new DeadLetterReader.ScanOptions(10, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBytes must be positive");

        assertThatThrownBy(() -> new DeadLetterReader.ScanOptions(10, -500L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBytes must be positive");
    }
}
