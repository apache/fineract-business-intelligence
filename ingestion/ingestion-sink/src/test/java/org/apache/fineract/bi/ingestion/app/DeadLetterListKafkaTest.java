
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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.fineract.bi.ingestion.config.Env;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DeadLetterListKafkaTest {

    private static final String DLT = "fineract.cdc.dlt-list-it";
    private static final String SOURCE_TOPIC = "fineract.cdc.batch_job_instance";

    private KafkaContainer kafka;
    private KafkaProducer<byte[], byte[]> producer;
    private Contract contract;

    @BeforeClass
    public void startKafka() throws Exception {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new SkipException("Docker is not available");
        }

        contract = ContractLoader.load(Path.of("../../contracts/fineract_tables.json"));

        kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.1"));
        kafka.start();

        try (AdminClient admin = AdminClient.create(
                Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(DLT, 2, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }

        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafka.getBootstrapServers());
        properties.put("key.serializer", ByteArraySerializer.class.getName());
        properties.put("value.serializer", ByteArraySerializer.class.getName());
        properties.put("acks", "all");

        producer = new KafkaProducer<>(properties);
    }

    @AfterClass(alwaysRun = true)
    public void stopKafka() {
        if (producer != null) {
            producer.close();
        }
        if (kafka != null) {
            kafka.stop();
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private void publish(
            int dltPartition,
            String sourceId,
            int sourcePartition,
            long sourceOffset) throws Exception {

        String payload = "{\"source\":{\"name\":\"" + sourceId
                + "\",\"table\":\"batch_job_instance\"},\"op\":\"c\"}";

        ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(DLT, dltPartition, null, bytes(payload));

        record.headers().add("fbi.dlt.reason", bytes("WAREHOUSE_REJECTED"));
        record.headers().add("fbi.dlt.message", bytes("integration test rejection"));
        record.headers().add("fbi.source.topic", bytes(SOURCE_TOPIC));
        record.headers().add("fbi.source.partition", bytes(Integer.toString(sourcePartition)));
        record.headers().add("fbi.source.offset", bytes(Long.toString(sourceOffset)));

        producer.send(record).get(10, TimeUnit.SECONDS);
    }

    @Test
    public void readsAndGroupsRecordsAcrossRealKafkaDltPartitions() throws Exception {
        publish(0, "bank-a", 0, 100);
        publish(0, "bank-a", 1, 200);
        publish(1, "bank-b", 0, 300);
        producer.flush();

        Map<String, String> environment = new HashMap<>();
        environment.put("SINK_BOOTSTRAP_SERVERS", kafka.getBootstrapServers());
        environment.put("SINK_DLT_TOPIC", DLT);
        environment.put("SINK_WRITER_ID", "dlt-list-integration-test");

        SinkConfig config = SinkConfig.from(Env.of(environment), environment);

        DeadLetterSummary.Accumulator accumulator =
                new DeadLetterSummary.Accumulator();

        DeadLetterReader.ScanResult scan;

        try (DeadLetterReader reader = new DeadLetterReader(config, contract)) {
            scan = reader.scan(accumulator::add);
        }

        List<DeadLetterSummary.Group> groups = accumulator.snapshot();

        assertThat(scan.recordsRead()).isEqualTo(3);
        assertThat(scan.partitions()).hasSize(2);
        assertThat(groups).hasSize(2);

        DeadLetterSummary.Group bankA = groups.stream()
                .filter(group -> group.key().sourceId().equals("bank-a"))
                .findFirst()
                .orElseThrow();

        assertThat(bankA.count()).isEqualTo(2);
        assertThat(bankA.payloadAvailable()).isEqualTo(2);
        assertThat(bankA.originalOffsets()).hasSize(2);

        DeadLetterSummary.Group bankB = groups.stream()
                .filter(group -> group.key().sourceId().equals("bank-b"))
                .findFirst()
                .orElseThrow();

        assertThat(bankB.count()).isEqualTo(1);
        assertThat(bankB.payloadAvailable()).isEqualTo(1);
        assertThat(bankB.originalOffsets()).hasSize(1);
    }
}
