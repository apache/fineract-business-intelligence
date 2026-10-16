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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import org.apache.fineract.bi.ingestion.config.Env;

public record SinkConfig(
        String bootstrapServers,
        String groupId,
        String topicPattern,
        int threads,
        int maxBatchEvents,
        long maxBatchWaitMs,
        long pollTimeoutMs,
        int maxPollRecords,
        int fetchMinBytes,
        int fetchMaxWaitMs,
        int maxPollIntervalMs,
        String dbUrl,
        String dbUser,
        String dbPassword,
        int dbPoolSize,
        long ownershipRefreshMs,
        String dltTopic,
        String unknownSourcePolicy,
        long progressFlushMs,
        String writerId,
        int metricsPort,
        long maxBackoffMs,
        boolean startFromEarliestWithoutDbOffset,
        Properties consumerOverrides,
        Properties producerOverrides) {

    public static SinkConfig from(Env env, Map<String, String> environment) {
        int threads = env.integer("SINK_THREADS", 2);
        return new SinkConfig(
                env.string("SINK_BOOTSTRAP_SERVERS", "kafka:9092"),
                env.string("SINK_GROUP_ID", "fbi-warehouse-sink"),
                env.string("SINK_TOPIC_PATTERN", "fineract\\.cdc\\.(?!signals$|dlt$|heartbeats$)[a-z0-9_]+"),
                threads,
                env.integer("SINK_MAX_BATCH_EVENTS", 5000),
                env.longValue("SINK_MAX_BATCH_WAIT_MS", 500),
                env.longValue("SINK_POLL_TIMEOUT_MS", 200),
                env.integer("SINK_MAX_POLL_RECORDS", 2000),
                env.integer("SINK_FETCH_MIN_BYTES", 65_536),
                env.integer("SINK_FETCH_MAX_WAIT_MS", 200),
                env.integer("SINK_MAX_POLL_INTERVAL_MS", 300_000),
                env.string("SINK_DB_URL", "jdbc:postgresql://warehouse:5432/analytics"),
                env.string("SINK_DB_USER", "analytics_loader"),
                env.string("SINK_DB_PASSWORD", ""),
                env.integer("SINK_DB_POOL_SIZE", threads + 2),
                env.longValue("SINK_OWNERSHIP_REFRESH_MS", 5_000),
                env.string("SINK_DLT_TOPIC", "fineract.cdc.dlt"),
                env.string("SINK_UNKNOWN_SOURCE_POLICY", "dlt"),
                env.longValue("SINK_PROGRESS_FLUSH_MS", 5_000),
                env.string("SINK_WRITER_ID", "sink-" + hostname()),
                env.integer("SINK_METRICS_PORT", 9404),
                env.longValue("SINK_MAX_BACKOFF_MS", 30_000),
                env.bool("SINK_START_FROM_EARLIEST_WITHOUT_DB_OFFSET", true),
                Env.prefixed(environment, "SINK_KAFKA_"),
                Env.prefixed(environment, "SINK_PRODUCER_"));
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }
}
