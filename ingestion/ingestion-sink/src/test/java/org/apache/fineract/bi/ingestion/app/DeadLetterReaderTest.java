
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
            consumer.addRecord(record(1, 20L, "bank-b", 200L));
            consumer.addRecord(record(1, 21L, "bank-c", 201L));

            // This record is beyond the captured end offset and must be ignored.
            consumer.addRecord(record(1, 22L, "bank-d", 202L));
        });

        List<DeadLetterRecord> records = new ArrayList<>();

        try (DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT)) {
            DeadLetterReader.ScanResult result = reader.scan(records::add);

            assertThat(result.recordsRead()).isEqualTo(3);
            assertThat(records).hasSize(3);
            assertThat(records).extracting(DeadLetterRecord::sourceId)
                    .containsExactlyInAnyOrder("bank-a", "bank-b", "bank-c");

            assertThat(result.partitions())
                    .containsEntry(p0, new DeadLetterReader.PartitionBounds(10L, 11L))
                    .containsEntry(p1, new DeadLetterReader.PartitionBounds(20L, 22L));
        }
    }

    @Test
    public void returnsAnEmptyResultWhenTheDltHasNoPartitions() {
        MockConsumer<byte[], byte[]> consumer =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        consumer.updatePartitions(DLT, List.of());

        try (DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT)) {
            DeadLetterReader.ScanResult result = reader.scan(record -> {
                throw new AssertionError("No records should be visited");
            });

            assertThat(result.recordsRead()).isZero();
            assertThat(result.partitions()).isEmpty();
        }
    }
}
