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

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.fineract.bi.ingestion.config.Env;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.testng.annotations.Test;

public class SinkConfigAndTopicPlanTest {

    private final Contract contract = ContractLoader.load(Path.of("../../contracts/fineract_tables.json"));

    @Test
    public void defaultsAndKafkaPassThrough() {
        Map<String, String> env = Map.of(
                "SINK_THREADS", "6",
                "SINK_KAFKA_SECURITY_PROTOCOL", "SASL_SSL",
                "SINK_KAFKA_SASL_MECHANISM", "SCRAM-SHA-512",
                "SINK_PRODUCER_LINGER_MS", "5");
        SinkConfig c = SinkConfig.from(Env.of(env), env);
        assertThat(c.threads()).isEqualTo(6);
        assertThat(c.dbPoolSize()).isEqualTo(8);
        assertThat(c.consumerOverrides()).containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("sasl.mechanism", "SCRAM-SHA-512");
        assertThat(c.producerOverrides()).containsEntry("linger.ms", "5");
        assertThat(c.unknownSourcePolicy()).isEqualTo("dlt");
    }

    @Test
    public void defaultTopicPatternExcludesControlTopics() {
        Pattern p = Pattern.compile(SinkConfig.from(Env.of(Map.of()), Map.of()).topicPattern());
        assertThat(p.matcher("fineract.cdc.m_loan_transaction").matches()).isTrue();
        assertThat(p.matcher("fineract.cdc.dlt").matches()).isFalse();
        assertThat(p.matcher("fineract.cdc.signals").matches()).isFalse();
        assertThat(p.matcher("fineract.cdc.heartbeats").matches()).isFalse();
        assertThat(p.matcher("__debezium-heartbeat.s1").matches()).isFalse();
    }

    @Test
    public void topicPlanScalesAndCapsPartitions() {
        List<TopicProvisioner.Plan> full = TopicProvisioner.plan(contract, Env.of(Map.of()));
        TopicProvisioner.Plan txn = full.stream()
                .filter(p -> p.topic().equals("fineract.cdc.m_loan_transaction")).findFirst().orElseThrow();
        assertThat(txn.partitions()).isEqualTo(48);
        assertThat(full).extracting(TopicProvisioner.Plan::topic)
                .contains("fineract.cdc.dlt", "fineract.cdc.signals", "fineract.cdc.heartbeats");

        List<TopicProvisioner.Plan> laptop = TopicProvisioner.plan(contract,
                Env.of(Map.of("TOPIC_PARTITION_SCALE", "0.25", "TOPIC_MAX_PARTITIONS", "6")));
        assertThat(laptop).allSatisfy(p -> assertThat(p.partitions()).isBetween(1, 6));
        assertThat(laptop.stream().filter(p -> p.topic().endsWith("m_loan_transaction")).findFirst().orElseThrow()
                .partitions()).isEqualTo(6);
    }
}
