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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class DeadLetterPublisher implements AutoCloseable {

    public record DeadLetter(ConsumerRecord<byte[], byte[]> record, String reason, String message) {
    }

    private final KafkaProducer<byte[], byte[]> producer;
    private final String topic;

    public DeadLetterPublisher(SinkConfig config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.writerId() + "-dlt");
        props.putAll(config.producerOverrides());
        this.producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
        this.topic = config.dltTopic();
    }

    public void publish(List<DeadLetter> letters) throws ExecutionException, InterruptedException {
        List<Future<RecordMetadata>> futures = new ArrayList<>(letters.size());
        for (DeadLetter d : letters) {
            ConsumerRecord<byte[], byte[]> r = d.record();
            ProducerRecord<byte[], byte[]> out = new ProducerRecord<>(topic, null, r.key(), r.value());
            out.headers().add("fbi.dlt.reason", bytes(d.reason()));
            out.headers().add("fbi.dlt.message", bytes(d.message() == null ? "" : d.message()));
            out.headers().add("fbi.source.topic", bytes(r.topic()));
            out.headers().add("fbi.source.partition", bytes(Integer.toString(r.partition())));
            out.headers().add("fbi.source.offset", bytes(Long.toString(r.offset())));
            futures.add(producer.send(out));
        }
        producer.flush();
        for (Future<RecordMetadata> f : futures) {
            f.get();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        producer.close();
    }
}
