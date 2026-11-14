
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

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.clients.consumer.Consumer;

public final class DeadLetterReader implements AutoCloseable {

    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(250);

    public record PartitionBounds(long beginningOffset, long endOffset) {
    }

    public record ScanResult(long recordsRead, Map<TopicPartition, PartitionBounds> partitions) {
        public ScanResult {
            partitions = Map.copyOf(partitions);
        }
    }

    private final Consumer<byte[], byte[]> consumer;
    private final String topic;
    private final Contract contract;

    public DeadLetterReader(SinkConfig config, Contract contract) {
        this(createConsumer(config), config.dltTopic(), contract);
    }

    DeadLetterReader(Consumer<byte[], byte[]> consumer, String topic, Contract contract) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.contract = Objects.requireNonNull(contract, "contract");
    }

    private static Consumer<byte[], byte[]> createConsumer(SinkConfig config) {
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
     * Reads a bounded snapshot of the DLT and streams each record to the supplied visitor.
     * No consumer-group offsets are committed.
     */
    public ScanResult scan(java.util.function.Consumer<DeadLetterRecord> visitor) {
        Objects.requireNonNull(visitor, "visitor");

        List<PartitionInfo> information = consumer.partitionsFor(topic, REQUEST_TIMEOUT);
        if (information == null || information.isEmpty()) {
            return new ScanResult(0, Map.of());
        }

        Set<TopicPartition> assigned = new LinkedHashSet<>();
        for (PartitionInfo partition : information) {
            assigned.add(new TopicPartition(partition.topic(), partition.partition()));
        }

        consumer.assign(assigned);

        Map<TopicPartition, Long> beginnings = consumer.beginningOffsets(assigned, REQUEST_TIMEOUT);
        Map<TopicPartition, Long> ends = consumer.endOffsets(assigned, REQUEST_TIMEOUT);
        Map<TopicPartition, PartitionBounds> bounds = new LinkedHashMap<>();

        for (TopicPartition partition : assigned) {
            Long beginning = beginnings.get(partition);
            Long end = ends.get(partition);
            if (beginning == null || end == null || beginning > end) {
                throw new IllegalStateException("Invalid DLT offset range for " + partition);
            }
            bounds.put(partition, new PartitionBounds(beginning, end));
            consumer.seek(partition, beginning);
        }

        long recordsRead = 0;

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT);

            for (ConsumerRecord<byte[], byte[]> record : records) {
                TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                PartitionBounds range = bounds.get(partition);

                // Ignore records appended after this scan's end-offset snapshot.
                if (range == null || record.offset() < range.beginningOffset()
                        || record.offset() >= range.endOffset()) {
                    continue;
                }

                visitor.accept(DeadLetterRecord.parse(record, contract));
                recordsRead++;
            }

            boolean complete = true;
            for (TopicPartition partition : assigned) {
                if (consumer.position(partition, REQUEST_TIMEOUT) < ends.get(partition)) {
                    complete = false;
                    break;
                }
            }

            if (complete) {
                break;
            }
        }

        return new ScanResult(recordsRead, bounds);
    }

    @Override
    public void close() {
        consumer.close();
    }
}
