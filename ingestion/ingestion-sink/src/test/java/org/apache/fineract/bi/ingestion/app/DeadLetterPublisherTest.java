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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
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
    public void aValueLargerThanTheLimitIsReplacedByAMetadataOnlyRecord() throws Exception {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        new DeadLetterPublisher(producer, "dlt", 100).publish(List.of(letter(new byte[500])));
        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(sent.value()).isNull();
        assertThat(header(sent, "fbi.dlt.value_omitted")).isEqualTo("true");
        assertThat(header(sent, "fbi.dlt.value_bytes")).isEqualTo("500");
        assertThat(header(sent, "fbi.source.offset")).isEqualTo("9");
    }

    @Test
    public void aSmallValueIsKeptWithItsReason() throws Exception {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        new DeadLetterPublisher(producer, "dlt", 100).publish(List.of(letter("{}".getBytes(StandardCharsets.UTF_8))));
        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(new String(sent.value(), StandardCharsets.UTF_8)).isEqualTo("{}");
        assertThat(header(sent, "fbi.dlt.reason")).isEqualTo("TYPE_MISMATCH");
        assertThat(header(sent, "fbi.dlt.value_omitted")).isNull();
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
        r.headers().add("fbi.replay.dlt.topic", "orig.dlt".getBytes(StandardCharsets.UTF_8));
        r.headers().add("fbi.replay.dlt.partition", "1".getBytes(StandardCharsets.UTF_8));
        r.headers().add("fbi.replay.dlt.offset", "42".getBytes(StandardCharsets.UTF_8));

        new DeadLetterPublisher(producer, "dlt", 100).publish(List.of(new DeadLetterPublisher.DeadLetter(r, "WAREHOUSE_REJECTED", "err")));

        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(header(sent, "fbi.replay.dlt.topic")).isEqualTo("orig.dlt");
        assertThat(header(sent, "fbi.replay.dlt.partition")).isEqualTo("1");
        assertThat(header(sent, "fbi.replay.dlt.offset")).isEqualTo("42");
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
