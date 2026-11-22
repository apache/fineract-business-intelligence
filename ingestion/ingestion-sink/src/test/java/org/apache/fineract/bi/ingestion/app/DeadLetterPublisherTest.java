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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.fineract.bi.ingestion.sink.ReplayIdentity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testng.annotations.Test;

public class DeadLetterPublisherTest {

    private static String header(ProducerRecord<byte[], byte[]> r, String name) {
        Header h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static DeadLetterPublisher.DeadLetter letter(byte[] value) {
        return new DeadLetterPublisher.DeadLetter(
                new ConsumerRecord<>("fineract.cdc.m_loan", 2, 9L, null, value), "TYPE_MISMATCH", "bad");
    }

    @Test
    public void generatesFreshDltIdForEveryPhysicalRecord() throws Exception {
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        DeadLetterPublisher publisher = new DeadLetterPublisher(producer, "dlt", 100);

        publisher.publish(List.of(letter("{}".getBytes(StandardCharsets.UTF_8))));
        publisher.publish(List.of(letter("{}".getBytes(StandardCharsets.UTF_8))));

        assertThat(producer.history()).hasSize(2);
        String id1 = header(producer.history().get(0), ReplayIdentity.DLT_ID_HEADER);
        String id2 = header(producer.history().get(1), ReplayIdentity.DLT_ID_HEADER);

        assertThat(id1).isNotNull();
        assertThat(id2).isNotNull();
        assertThat(UUID.fromString(id1)).isNotNull();
        assertThat(UUID.fromString(id2)).isNotNull();
        assertThat(id1).isNotEqualTo(id2);
    }

    @Test
    public void aValueLargerThanTheLimitIsReplacedByAMetadataOnlyRecord() throws Exception {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        new DeadLetterPublisher(producer, "dlt", 100).publish(List.of(letter(new byte[500])));
        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(sent.value()).isNull();
        assertThat(header(sent, "fbi.dlt.value_omitted")).isEqualTo("true");
        assertThat(header(sent, "fbi.dlt.value_bytes")).isEqualTo("500");
        assertThat(header(sent, "fbi.source.offset")).isEqualTo("9");
        assertThat(header(sent, ReplayIdentity.DLT_ID_HEADER)).isNotNull();
    }

    @Test
    public void aSmallValueIsKeptWithItsReason() throws Exception {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        new DeadLetterPublisher(producer, "dlt", 100).publish(List.of(letter("{}".getBytes(StandardCharsets.UTF_8))));
        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(new String(sent.value(), StandardCharsets.UTF_8)).isEqualTo("{}");
        assertThat(header(sent, "fbi.dlt.reason")).isEqualTo("TYPE_MISMATCH");
        assertThat(header(sent, "fbi.dlt.value_omitted")).isNull();
        assertThat(header(sent, ReplayIdentity.DLT_ID_HEADER)).isNotNull();
    }

    @Test
    public void aBrokerSideSizeRejectionFallsBackToTheMetadataOnlyRecordInsteadOfRetryingForever() throws Exception {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(
                true, null, new ByteArraySerializer(), new ByteArraySerializer()) {
            private int sends;

            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
                if (sends++ == 0) {
                    CompletableFuture<RecordMetadata> failed = new CompletableFuture<>();
                    failed.completeExceptionally(new RecordTooLargeException("too large"));
                    return failed;
                }
                return super.send(record);
            }
        };
        new DeadLetterPublisher(producer, "dlt", 10_000).publish(List.of(letter(new byte[50])));
        assertThat(producer.history()).hasSize(1);
        assertThat(producer.history().get(0).value()).isNull();
        assertThat(header(producer.history().get(0), "fbi.dlt.value_omitted")).isEqualTo("true");
    }

    @Test
    public void preservesReplayIdentityHeadersWhenReDeadLettering() throws Exception {
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        ConsumerRecord<byte[], byte[]> r = new ConsumerRecord<>("fineract.cdc.m_loan", 2, 9L, null, "{}".getBytes(StandardCharsets.UTF_8));
        UUID replayId = UUID.randomUUID();
        r.headers().add("fbi.replay.id", replayId.toString().getBytes(StandardCharsets.UTF_8));
        r.headers().add("fbi.replay.dlt.topic", "orig.dlt".getBytes(StandardCharsets.UTF_8));
        r.headers().add("fbi.replay.dlt.partition", "1".getBytes(StandardCharsets.UTF_8));
        r.headers().add("fbi.replay.dlt.offset", "42".getBytes(StandardCharsets.UTF_8));

        new DeadLetterPublisher(producer, "dlt", 100).publish(List.of(new DeadLetterPublisher.DeadLetter(r, "WAREHOUSE_REJECTED", "err")));

        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(header(sent, "fbi.replay.id")).isEqualTo(replayId.toString());
        assertThat(header(sent, "fbi.replay.dlt.topic")).isEqualTo("orig.dlt");
        assertThat(header(sent, "fbi.replay.dlt.partition")).isEqualTo("1");
        assertThat(header(sent, "fbi.replay.dlt.offset")).isEqualTo("42");
        assertThat(header(sent, ReplayIdentity.DLT_ID_HEADER)).isNotNull();
        assertThat(header(sent, ReplayIdentity.DLT_ID_HEADER)).isNotEqualTo(replayId.toString());
    }

    @Test
    public void doesNotSynthesizeCoordinateOnlyReplayIdForLegacyThreeHeaderRecord() throws Exception {
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        ConsumerRecord<byte[], byte[]> r = new ConsumerRecord<>("fineract.cdc.m_loan", 2, 9L, null, "{}".getBytes(StandardCharsets.UTF_8));
        r.headers().add("fbi.replay.dlt.topic", "orig.dlt".getBytes(StandardCharsets.UTF_8));
        r.headers().add("fbi.replay.dlt.partition", "1".getBytes(StandardCharsets.UTF_8));
        r.headers().add("fbi.replay.dlt.offset", "42".getBytes(StandardCharsets.UTF_8));

        new DeadLetterPublisher(producer, "dlt", 100).publish(List.of(new DeadLetterPublisher.DeadLetter(r, "WAREHOUSE_REJECTED", "err")));

        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(header(sent, "fbi.replay.id")).isNull();
        assertThat(header(sent, "fbi.replay.dlt.topic")).isEqualTo("orig.dlt");
        assertThat(header(sent, "fbi.replay.dlt.partition")).isEqualTo("1");
        assertThat(header(sent, "fbi.replay.dlt.offset")).isEqualTo("42");
        assertThat(header(sent, ReplayIdentity.DLT_ID_HEADER)).isNotNull();
    }

    @Test
    public void strengthenedIdentitySurvivesReplayAndRedeadLetterCycle() throws Exception {
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        DeadLetterPublisher publisher = new DeadLetterPublisher(producer, "fineract.cdc.dlt", 100);

        ConsumerRecord<byte[], byte[]> original = new ConsumerRecord<>("fineract.cdc.m_loan", 2, 9L, null, "{}".getBytes(StandardCharsets.UTF_8));
        publisher.publish(List.of(new DeadLetterPublisher.DeadLetter(original, "WAREHOUSE_REJECTED", "initial failure")));

        ProducerRecord<byte[], byte[]> firstDlt = producer.history().get(0);
        UUID dltId = UUID.fromString(header(firstDlt, ReplayIdentity.DLT_ID_HEADER));

        ConsumerRecord<byte[], byte[]> dltConsumerRecord = new ConsumerRecord<>(
                "fineract.cdc.dlt", 0, 10L, 1000L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                0, 0, null, "{}".getBytes(StandardCharsets.UTF_8),
                firstDlt.headers(), java.util.Optional.empty());

        org.apache.fineract.bi.ingestion.contract.Contract contract =
                org.apache.fineract.bi.ingestion.contract.ContractLoader.load(
                        java.nio.file.Path.of("../../contracts/fineract_tables.json"));
        DeadLetterRecord parsedDlt = DeadLetterRecord.parse(dltConsumerRecord, contract);
        ReplayIdentity replayIdentity = parsedDlt.replayIdentity();
        assertThat(replayIdentity.replayId()).isEqualTo(dltId);

        ProducerRecord<byte[], byte[]> replayedRecord = new ProducerRecord<>(
                parsedDlt.originalTopic(), parsedDlt.originalPartition(), parsedDlt.key(), parsedDlt.value());
        ReplayIdentityHeaders.add(replayedRecord.headers(), replayIdentity);

        ConsumerRecord<byte[], byte[]> secondCdcFailure = new ConsumerRecord<>(
                replayedRecord.topic(), replayedRecord.partition(), 100L,
                replayedRecord.key(), replayedRecord.value());
        for (Header h : replayedRecord.headers()) {
            secondCdcFailure.headers().add(h.key(), h.value());
        }

        publisher.publish(List.of(new DeadLetterPublisher.DeadLetter(secondCdcFailure, "WAREHOUSE_REJECTED", "second failure")));

        ProducerRecord<byte[], byte[]> secondDlt = producer.history().get(1);
        assertThat(header(secondDlt, "fbi.replay.id")).isEqualTo(dltId.toString());
        assertThat(header(secondDlt, "fbi.replay.dlt.topic")).isEqualTo("fineract.cdc.dlt");
        assertThat(header(secondDlt, "fbi.replay.dlt.partition")).isEqualTo("0");
        assertThat(header(secondDlt, "fbi.replay.dlt.offset")).isEqualTo("10");
        assertThat(header(secondDlt, ReplayIdentity.DLT_ID_HEADER)).isNotNull();
        assertThat(header(secondDlt, ReplayIdentity.DLT_ID_HEADER)).isNotEqualTo(dltId.toString());
    }

    @Test
    public void fallbackReplayCyclePreservesStrengthenedIdentityAcrossRedeadLettering() throws Exception {
        org.apache.kafka.common.header.internals.RecordHeaders headers =
                new org.apache.kafka.common.header.internals.RecordHeaders();
        headers.add("fbi.dlt.reason", "WAREHOUSE_REJECTED".getBytes(StandardCharsets.UTF_8));
        headers.add("fbi.dlt.message", "test error".getBytes(StandardCharsets.UTF_8));
        headers.add("fbi.source.topic", "fineract.cdc.m_loan".getBytes(StandardCharsets.UTF_8));
        headers.add("fbi.source.partition", "1".getBytes(StandardCharsets.UTF_8));
        headers.add("fbi.source.offset", "50".getBytes(StandardCharsets.UTF_8));
        headers.add(ReplayIdentity.TOPIC_HEADER, "orig.dlt".getBytes(StandardCharsets.UTF_8));
        headers.add(ReplayIdentity.PARTITION_HEADER, "2".getBytes(StandardCharsets.UTF_8));
        headers.add(ReplayIdentity.OFFSET_HEADER, "88".getBytes(StandardCharsets.UTF_8));

        byte[] payload = "{\"source\":{\"name\":\"bank-a\",\"table\":\"m_loan\"},\"op\":\"c\"}".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> legacyDltRecord = new ConsumerRecord<>(
                "fineract.cdc.dlt", 0, 10L, 1000L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                0, 0, "key1".getBytes(StandardCharsets.UTF_8), payload,
                headers, java.util.Optional.empty());

        org.apache.fineract.bi.ingestion.contract.Contract contract =
                org.apache.fineract.bi.ingestion.contract.ContractLoader.load(
                        java.nio.file.Path.of("../../contracts/fineract_tables.json"));
        DeadLetterRecord parsedRecord = DeadLetterRecord.parse(legacyDltRecord, contract);
        ReplayIdentity fallbackIdentity = parsedRecord.replayIdentity();

        assertThat(parsedRecord.hasExplicitReplayId()).isFalse();
        assertThat(parsedRecord.dltId()).isNull();
        assertThat(fallbackIdentity.replayId()).isNotNull();
        assertThat(fallbackIdentity.replayId())
                .isNotEqualTo(ReplayIdentity.legacyLineageId("orig.dlt", 2, 88L));

        ProducerRecord<byte[], byte[]> replayedRecord = new ProducerRecord<>(
                parsedRecord.originalTopic(), parsedRecord.originalPartition(), parsedRecord.key(), parsedRecord.value());
        ReplayIdentityHeaders.add(replayedRecord.headers(), fallbackIdentity);

        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        DeadLetterPublisher publisher = new DeadLetterPublisher(producer, "fineract.cdc.dlt", 100);

        ConsumerRecord<byte[], byte[]> secondCdcFailure = new ConsumerRecord<>(
                replayedRecord.topic(), replayedRecord.partition(), 101L,
                replayedRecord.key(), replayedRecord.value());
        for (Header h : replayedRecord.headers()) {
            secondCdcFailure.headers().add(h.key(), h.value());
        }

        publisher.publish(List.of(new DeadLetterPublisher.DeadLetter(secondCdcFailure, "WAREHOUSE_REJECTED", "second error")));

        ProducerRecord<byte[], byte[]> secondDlt = producer.history().get(0);
        assertThat(header(secondDlt, ReplayIdentity.ID_HEADER)).isEqualTo(fallbackIdentity.replayId().toString());
        assertThat(header(secondDlt, ReplayIdentity.TOPIC_HEADER)).isEqualTo("orig.dlt");
        assertThat(header(secondDlt, ReplayIdentity.PARTITION_HEADER)).isEqualTo("2");
        assertThat(header(secondDlt, ReplayIdentity.OFFSET_HEADER)).isEqualTo("88");

        String newDltId = header(secondDlt, ReplayIdentity.DLT_ID_HEADER);
        assertThat(newDltId).isNotNull();
        assertThat(newDltId).isNotEqualTo(fallbackIdentity.replayId().toString());

        ConsumerRecord<byte[], byte[]> secondDltConsumerRecord = new ConsumerRecord<>(
                "fineract.cdc.dlt", 0, 15L, 2000L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                0, 0, secondDlt.key(), secondDlt.value(),
                secondDlt.headers(), java.util.Optional.empty());
        DeadLetterRecord secondParsed = DeadLetterRecord.parse(secondDltConsumerRecord, contract);
        assertThat(secondParsed.hasExplicitReplayId()).isTrue();
        assertThat(secondParsed.replayIdentity().replayId()).isEqualTo(fallbackIdentity.replayId());
        assertThat(secondParsed.replayIdentity().replayId()).isNotEqualTo(UUID.fromString(newDltId));
    }

    @Test
    public void preservesRawMalformedReplayHeadersWithoutFailingPublishing() throws Exception {
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        ConsumerRecord<byte[], byte[]> r = new ConsumerRecord<>("fineract.cdc.m_loan", 2, 9L, null, "{}".getBytes(StandardCharsets.UTF_8));
        byte[] malformedBytes = new byte[] {(byte) 0xC3, (byte) 0x28};
        r.headers().add("fbi.replay.dlt.topic", malformedBytes);
        r.headers().add("fbi.replay.dlt.partition", "invalid".getBytes(StandardCharsets.UTF_8));

        new DeadLetterPublisher(producer, "dlt", 100).publish(List.of(new DeadLetterPublisher.DeadLetter(r, "MALFORMED_REPLAY_IDENTITY", "err")));

        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(sent.headers().lastHeader("fbi.replay.dlt.topic").value()).isEqualTo(malformedBytes);
        assertThat(header(sent, "fbi.replay.dlt.partition")).isEqualTo("invalid");
    }
}
