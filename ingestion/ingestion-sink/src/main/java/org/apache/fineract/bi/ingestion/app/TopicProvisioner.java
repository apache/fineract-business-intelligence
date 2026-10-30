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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.fineract.bi.ingestion.config.Env;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.TableContract;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TopicProvisioner {

    private static final Logger LOG = LoggerFactory.getLogger(TopicProvisioner.class);

    public record Plan(String topic, int partitions, Map<String, String> configs) {
    }

    private TopicProvisioner() {
    }

    public static List<Plan> plan(Contract contract, Env env) {
        double scale = env.decimal("TOPIC_PARTITION_SCALE", 1.0);
        int max = env.integer("TOPIC_MAX_PARTITIONS", 0);
        int minIsr = env.integer("TOPIC_MIN_INSYNC_REPLICAS", 2);
        String retention = env.string("TOPIC_RETENTION_MS", Long.toString(72L * 3600 * 1000));
        List<Plan> plans = new ArrayList<>();
        for (TableContract t : contract.tables()) {
            int partitions = Math.max(1, (int) Math.round(t.kafkaPartitions() * scale));
            if (max > 0) {
                partitions = Math.min(partitions, max);
            }
            Map<String, String> cfg = new LinkedHashMap<>();
            cfg.put("retention.ms", retention);
            cfg.put("min.insync.replicas", Integer.toString(minIsr));
            cfg.put("cleanup.policy", "delete");
            cfg.put("max.message.bytes", env.string("TOPIC_MAX_MESSAGE_BYTES", "4194304"));
            plans.add(new Plan(contract.topicFor(t.tableName()), partitions, cfg));
        }
        plans.add(new Plan(contract.topicPrefix() + ".dlt", Math.max(1, Math.min(max > 0 ? max : 6, 6)), Map.of(
                "retention.ms", env.string("TOPIC_DLT_RETENTION_MS", Long.toString(14L * 24 * 3600 * 1000)),
                "max.message.bytes", env.string("TOPIC_MAX_MESSAGE_BYTES", "4194304"),
                "min.insync.replicas", Integer.toString(minIsr))));
        plans.add(new Plan(contract.topicPrefix() + ".signals", 1, Map.of(
                "min.insync.replicas", Integer.toString(minIsr))));
        plans.add(new Plan(contract.topicPrefix() + ".heartbeats", 1, Map.of(
                "retention.ms", env.string("TOPIC_HEARTBEAT_RETENTION_MS", Long.toString(3600L * 1000)),
                "min.insync.replicas", Integer.toString(minIsr))));
        return plans;
    }

    public static void provision(Contract contract, Env env) throws Exception {
        short rf = (short) env.integer("TOPIC_REPLICATION_FACTOR", 3);
        boolean allowIncrease = env.bool("TOPIC_ALLOW_PARTITION_INCREASE", false);
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, env.string("SINK_BOOTSTRAP_SERVERS", "kafka:9092"));
        props.putAll(Env.prefixed(System.getenv(), "SINK_KAFKA_"));
        try (Admin admin = Admin.create(props)) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> create = new ArrayList<>();
            Map<String, NewPartitions> grow = new LinkedHashMap<>();
            List<Plan> plans = plan(contract, env);
            Map<String, TopicDescription> described = admin.describeTopics(
                    plans.stream().map(Plan::topic).filter(existing::contains).toList()).allTopicNames().get();
            for (Plan p : plans) {
                if (!existing.contains(p.topic())) {
                    create.add(new NewTopic(p.topic(), p.partitions(), rf).configs(p.configs()));
                    continue;
                }
                int current = described.get(p.topic()).partitions().size();
                if (current < p.partitions()) {
                    if (allowIncrease) {
                        grow.put(p.topic(), NewPartitions.increaseTo(p.partitions()));
                    } else {
                        LOG.warn("Topic {} has {} partitions, plan wants {}; set TOPIC_ALLOW_PARTITION_INCREASE=true "
                                + "to grow it (remaps keys - run as a controlled migration)", p.topic(), current, p.partitions());
                    }
                }
            }
            if (!create.isEmpty()) {
                admin.createTopics(create).all().get();
            }
            if (!grow.isEmpty()) {
                admin.createPartitions(grow).all().get();
            }
            for (Plan p : plans) {
                LOG.info("topic {} partitions={} {}", p.topic(), p.partitions(),
                        existing.contains(p.topic()) ? "(exists)" : "(created)");
            }
        }
    }
}
