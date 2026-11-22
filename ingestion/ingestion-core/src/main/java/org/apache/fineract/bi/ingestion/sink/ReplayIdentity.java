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
package org.apache.fineract.bi.ingestion.sink;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

public record ReplayIdentity(UUID replayId, String dltTopic, int dltPartition, long dltOffset) {

    public static final String ID_HEADER = "fbi.replay.id";
    public static final String DLT_ID_HEADER = "fbi.dlt.id";
    public static final String TOPIC_HEADER = "fbi.replay.dlt.topic";
    public static final String PARTITION_HEADER = "fbi.replay.dlt.partition";
    public static final String OFFSET_HEADER = "fbi.replay.dlt.offset";

    public ReplayIdentity {
        Objects.requireNonNull(replayId, "replayId");
        Objects.requireNonNull(dltTopic, "dltTopic");
        if (dltTopic.isBlank()) {
            throw new IllegalArgumentException("DLT topic must not be blank");
        }
        if (dltPartition < 0) {
            throw new IllegalArgumentException("DLT partition must not be negative");
        }
        if (dltOffset < 0) {
            throw new IllegalArgumentException("DLT offset must not be negative");
        }
    }

    public ReplayIdentity(String dltTopic, int dltPartition, long dltOffset) {
        this(legacyLineageId(dltTopic, dltPartition, dltOffset), dltTopic, dltPartition, dltOffset);
    }

    public ReplayIdentity(
            String dltTopic,
            int dltPartition,
            long dltOffset,
            long dltTimestamp,
            String originalTopic,
            int originalPartition,
            long originalOffset) {
        this(
                legacyFallbackId(
                        dltTopic,
                        dltPartition,
                        dltOffset,
                        dltTimestamp,
                        originalTopic,
                        originalPartition,
                        originalOffset),
                dltTopic,
                dltPartition,
                dltOffset);
    }

    public ReplayIdentity(
            String dltTopic,
            int dltPartition,
            long dltOffset,
            long dltTimestamp,
            String sourceId,
            String table,
            String originalTopic,
            int originalPartition,
            long originalOffset) {
        this(
                legacyFallbackId(
                        dltTopic,
                        dltPartition,
                        dltOffset,
                        dltTimestamp,
                        sourceId,
                        table,
                        originalTopic,
                        originalPartition,
                        originalOffset),
                dltTopic,
                dltPartition,
                dltOffset);
    }

    public static UUID legacyLineageId(String topic, int partition, long offset) {
        Objects.requireNonNull(topic, "topic");
        String key = "fbi:replay:" + topic + ":" + partition + ":" + offset;
        return UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8));
    }

    public static UUID legacyFallbackId(
            String dltTopic,
            int dltPartition,
            long dltOffset,
            long dltTimestamp,
            String originalTopic,
            int originalPartition,
            long originalOffset) {
        Objects.requireNonNull(dltTopic, "dltTopic");
        String key = "fbi:dlt:" + dltTopic + ":" + dltPartition + ":" + dltOffset
                + ":" + dltTimestamp
                + ":" + (originalTopic == null ? "" : originalTopic)
                + ":" + originalPartition
                + ":" + originalOffset;
        return UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8));
    }

    public static UUID legacyFallbackId(
            String dltTopic,
            int dltPartition,
            long dltOffset,
            long dltTimestamp,
            String sourceId,
            String table,
            String originalTopic,
            int originalPartition,
            long originalOffset) {
        Objects.requireNonNull(dltTopic, "dltTopic");
        if (sourceId == null && table == null) {
            return legacyFallbackId(
                    dltTopic,
                    dltPartition,
                    dltOffset,
                    dltTimestamp,
                    originalTopic,
                    originalPartition,
                    originalOffset);
        }
        StringBuilder sb = new StringBuilder("fbi:dlt:v2:");
        encodeField(sb, dltTopic);
        sb.append(dltPartition).append(';');
        sb.append(dltOffset).append(';');
        sb.append(dltTimestamp).append(';');
        encodeField(sb, sourceId);
        encodeField(sb, table);
        encodeField(sb, originalTopic);
        sb.append(originalPartition).append(';');
        sb.append(originalOffset);
        return UUID.nameUUIDFromBytes(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void encodeField(StringBuilder sb, String val) {
        if (val == null) {
            sb.append("-1;");
        } else {
            sb.append(val.length()).append(':').append(val).append(';');
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReplayIdentity that)) {
            return false;
        }
        return Objects.equals(replayId, that.replayId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(replayId);
    }
}
