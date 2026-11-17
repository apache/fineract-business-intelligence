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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Reads a bounded snapshot of the Dead Letter Topic (DLT) sequentially partition-by-partition.
 *
 * <p>Scanning semantics:
 * <ul>
 *   <li><b>Sequential partition traversal:</b> Partitions are discovered and sorted in ascending order by
 *       partition number. Partitions are assigned and scanned one at a time. This guarantees deterministic
 *       traversal within each partition, but does not provide globally oldest-first ordering across partitions.</li>
 *   <li><b>Snapshot offset bounds:</b> Beginning and end offsets for all partitions are captured before scanning
 *       begins. Records appended after this snapshot are ignored.</li>
 *   <li><b>Match-limit semantics:</b> Scanning terminates when the number of records reported as matched by the
 *       visitor reaches {@link ScanOptions#maxMatches()}. Non-matching records do not consume this limit.</li>
 *   <li><b>Logical byte-budget semantics:</b> Enforced against the cumulative logical record content
 *       (key bytes, value bytes, and UTF-8 header key and value bytes) before parsing or visiting a record.
 *       If a record would exceed the remaining budget, scanning stops immediately with
 *       {@link StopReason#BYTE_BUDGET_REACHED} without parsing or visiting that record.
 *       This is a logical record-content budget, not an exact wire-level network byte counter, nor a strict
 *       bound on Kafka's internal fetch buffers.</li>
 * </ul>
 *
 * <p>The reader never commits consumer offsets or joins the sink consumer group.
 */
public final class DeadLetterReader implements AutoCloseable {

    public static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024;

    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(250);

    public record PartitionBounds(long beginningOffset, long endOffset) {
    }

    public enum StopReason {
        COMPLETED,
        RECORD_LIMIT_REACHED,
        BYTE_BUDGET_REACHED,
        VISITOR_REQUESTED
    }

    public record VisitResult(boolean matched, boolean stop) {
    }

    @FunctionalInterface
    public interface Visitor {
        VisitResult visit(DeadLetterRecord record);
    }

    public record ScanOptions(long maxMatches, long maxBytes) {
        public ScanOptions {
            if (maxMatches <= 0) {
                throw new IllegalArgumentException("maxMatches must be positive: " + maxMatches);
            }
            if (maxBytes <= 0) {
                throw new IllegalArgumentException("maxBytes must be positive: " + maxBytes);
            }
        }

        public static ScanOptions defaults() {
            return new ScanOptions(Long.MAX_VALUE, DEFAULT_MAX_BYTES);
        }
    }

    public static final ScanOptions DEFAULT_OPTIONS = ScanOptions.defaults();

    public record ScanResult(
            long recordsScanned,
            long recordsMatched,
            long bytesScanned,
            Map<TopicPartition, PartitionBounds> partitions,
            StopReason stopReason) {

        public ScanResult {
            partitions = partitions != null
                    ? Collections.unmodifiableMap(new LinkedHashMap<>(partitions))
                    : Map.of();
            Objects.requireNonNull(stopReason, "stopReason");
        }

        public ScanResult(long recordsScanned, Map<TopicPartition, PartitionBounds> partitions) {
            this(recordsScanned, recordsScanned, 0L, partitions, StopReason.COMPLETED);
        }

        public long recordsRead() {
            return recordsScanned;
        }
    }

    private final org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> consumer;
    private final String topic;
    private final Contract contract;

    public DeadLetterReader(SinkConfig config, Contract contract) {
        this(createConsumer(config), config.dltTopic(), contract);
    }

    DeadLetterReader(
            org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> consumer,
            String topic,
            Contract contract) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.contract = Objects.requireNonNull(contract, "contract");
    }

    private static org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> createConsumer(SinkConfig config) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, config.writerId() + "-dlt-reader");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 50 * 1024 * 1024);
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 5 * 1024 * 1024);
        properties.putAll(config.consumerOverrides());

        // A DLT inspection consumer must never commit offsets or join the sink group.
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    /**
     * Scans using default options and a stoppable Visitor.
     */
    public ScanResult scan(Visitor visitor) {
        return scan(DEFAULT_OPTIONS, visitor);
    }

    /**
     * Reads a bounded snapshot of the DLT sequentially partition-by-partition.
     * Enforces the record match limit and record-content byte budget from ScanOptions.
     */
    public ScanResult scan(ScanOptions options, Visitor visitor) {
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(visitor, "visitor");

        List<PartitionInfo> information = consumer.partitionsFor(topic, REQUEST_TIMEOUT);
        if (information == null || information.isEmpty()) {
            return new ScanResult(0, 0, 0, Map.of(), StopReason.COMPLETED);
        }

        List<TopicPartition> sortedPartitions = information.stream()
                .map(p -> new TopicPartition(p.topic(), p.partition()))
                .sorted(Comparator.comparingInt(TopicPartition::partition))
                .toList();

        Map<TopicPartition, Long> beginnings = consumer.beginningOffsets(sortedPartitions, REQUEST_TIMEOUT);
        Map<TopicPartition, Long> ends = consumer.endOffsets(sortedPartitions, REQUEST_TIMEOUT);
        Map<TopicPartition, PartitionBounds> bounds = new LinkedHashMap<>();

        for (TopicPartition partition : sortedPartitions) {
            Long beginning = beginnings.get(partition);
            Long end = ends.get(partition);
            if (beginning == null || end == null || beginning > end) {
                throw new IllegalStateException("Invalid DLT offset range for " + partition);
            }
            bounds.put(partition, new PartitionBounds(beginning, end));
        }

        long recordsScanned = 0;
        long recordsMatched = 0;
        long bytesScanned = 0;
        StopReason stopReason = StopReason.COMPLETED;
        boolean stopScan = false;

        for (TopicPartition partition : sortedPartitions) {
            if (stopScan) {
                break;
            }

            PartitionBounds range = bounds.get(partition);
            long beginning = range.beginningOffset();
            long end = range.endOffset();

            if (beginning >= end) {
                continue;
            }

            consumer.assign(List.of(partition));
            consumer.seek(partition, beginning);

            while (consumer.position(partition, REQUEST_TIMEOUT) < end) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT);

                boolean reachedEnd = false;
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (record.offset() < beginning) {
                        continue;
                    }
                    if (record.offset() >= end) {
                        reachedEnd = true;
                        break;
                    }

                    long recordBytes = recordByteSize(record);
                    if (recordBytes > options.maxBytes() - bytesScanned) {
                        stopReason = StopReason.BYTE_BUDGET_REACHED;
                        stopScan = true;
                        break;
                    }

                    bytesScanned += recordBytes;
                    recordsScanned++;

                    DeadLetterRecord parsed = DeadLetterRecord.parse(record, contract);
                    VisitResult visitResult = visitor.visit(parsed);

                    if (visitResult.matched()) {
                        recordsMatched++;
                    }

                    if (visitResult.stop()) {
                        stopReason = StopReason.VISITOR_REQUESTED;
                        stopScan = true;
                        break;
                    }

                    if (recordsMatched >= options.maxMatches()) {
                        stopReason = StopReason.RECORD_LIMIT_REACHED;
                        stopScan = true;
                        break;
                    }
                }

                if (stopScan || reachedEnd) {
                    break;
                }
            }
        }

        return new ScanResult(recordsScanned, recordsMatched, bytesScanned, bounds, stopReason);
    }

    static long recordByteSize(ConsumerRecord<byte[], byte[]> record) {
        long keyBytes = record.key() != null ? record.key().length : 0L;
        long valueBytes = record.value() != null ? record.value().length : 0L;
        long headerBytes = 0L;
        if (record.headers() != null) {
            for (Header header : record.headers()) {
                if (header != null) {
                    if (header.key() != null) {
                        headerBytes = Math.addExact(headerBytes, (long) header.key().getBytes(StandardCharsets.UTF_8).length);
                    }
                    if (header.value() != null) {
                        headerBytes = Math.addExact(headerBytes, (long) header.value().length);
                    }
                }
            }
        }
        return Math.addExact(Math.addExact(keyBytes, valueBytes), headerBytes);
    }

    @Override
    public void close() {
        consumer.close();
    }
}
