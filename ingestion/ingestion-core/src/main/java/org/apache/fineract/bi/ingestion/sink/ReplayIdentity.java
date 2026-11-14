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

import java.util.Objects;

/**
 * Identifies the original dead-letter record that initiated a replay.
 *
 * The identity remains stable when a replayed event is published again
 * or dead-lettered again at a different Kafka offset.
 */
public record ReplayIdentity(String dltTopic, int dltPartition, long dltOffset) {

    public static final String TOPIC_HEADER = "fbi.replay.dlt.topic";
    public static final String PARTITION_HEADER = "fbi.replay.dlt.partition";
    public static final String OFFSET_HEADER = "fbi.replay.dlt.offset";

    public ReplayIdentity {
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
}
