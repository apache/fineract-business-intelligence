
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class DeadLetterSummary {

    public static final String UNKNOWN_SOURCE = "<unknown>";

    private DeadLetterSummary() {
    }

    public record Key(String reason, String table, String sourceId) {
    }

    public record OffsetRange(
            String topic,
            int partition,
            long oldestOffset,
            long newestOffset) {
    }

    public record Group(
            Key key,
            long count,
            long payloadAvailable,
            long omittedPayloads,
            List<OffsetRange> originalOffsets,
            List<OffsetRange> dltOffsets) {

        public Group {
            originalOffsets = List.copyOf(originalOffsets);
            dltOffsets = List.copyOf(dltOffsets);
        }
    }

    private record PartitionKey(String topic, int partition) {
    }

    private static final class MutableRange {
        private final String topic;
        private final int partition;
        private long oldest;
        private long newest;

        MutableRange(String topic, int partition, long offset) {
            this.topic = topic;
            this.partition = partition;
            this.oldest = offset;
            this.newest = offset;
        }

        void add(long offset) {
            oldest = Math.min(oldest, offset);
            newest = Math.max(newest, offset);
        }

        OffsetRange freeze() {
            return new OffsetRange(topic, partition, oldest, newest);
        }
    }

    private static final class MutableGroup {
        private long count;
        private long payloadAvailable;
        private long omittedPayloads;

        private final Map<PartitionKey, MutableRange> originalOffsets = new HashMap<>();
        private final Map<PartitionKey, MutableRange> dltOffsets = new HashMap<>();

        void add(DeadLetterRecord record) {
            count++;

            if (record.payloadAvailable()) {
                payloadAvailable++;
            } else {
                omittedPayloads++;
            }

            addOffset(originalOffsets, record.originalTopic(),
                    record.originalPartition(), record.originalOffset());

            addOffset(dltOffsets, record.dltTopic(),
                    record.dltPartition(), record.dltOffset());
        }

        private static void addOffset(
                Map<PartitionKey, MutableRange> ranges,
                String topic,
                int partition,
                long offset) {

            PartitionKey key = new PartitionKey(topic, partition);
            ranges.compute(key, (ignored, current) -> {
                if (current == null) {
                    return new MutableRange(topic, partition, offset);
                }
                current.add(offset);
                return current;
            });
        }

        List<OffsetRange> freezeRanges(Map<PartitionKey, MutableRange> ranges) {
            return ranges.values().stream()
                    .map(MutableRange::freeze)
                    .sorted(Comparator.comparing(OffsetRange::topic)
                            .thenComparingInt(OffsetRange::partition))
                    .toList();
        }

        Group freeze(Key key) {
            return new Group(key, count, payloadAvailable, omittedPayloads,
                    freezeRanges(originalOffsets), freezeRanges(dltOffsets));
        }
    }

    public static final class Accumulator {

        private final Map<Key, MutableGroup> groups = new HashMap<>();
        private final List<DeadLetterRecord> invalidReplayRecords = new ArrayList<>();

        public void add(DeadLetterRecord record) {
            Objects.requireNonNull(record, "record");

            if (record.hasInvalidReplayIdentity()) {
                invalidReplayRecords.add(record);
            }

            Key key = new Key(
                    record.reason(),
                    record.table(),
                    record.sourceId() == null ? UNKNOWN_SOURCE : record.sourceId());

            groups.computeIfAbsent(key, ignored -> new MutableGroup()).add(record);
        }

        public List<DeadLetterRecord> invalidReplayRecords() {
            return List.copyOf(invalidReplayRecords);
        }

        public List<Group> snapshot() {
            List<Group> result = new ArrayList<>(groups.size());

            groups.forEach((key, group) -> result.add(group.freeze(key)));

            result.sort(Comparator.comparing((Group g) -> g.key().reason())
                    .thenComparing(g -> g.key().table())
                    .thenComparing(g -> g.key().sourceId()));

            return List.copyOf(result);
        }
    }


    public static List<Group> summarize(Iterable<DeadLetterRecord> records) {
    Objects.requireNonNull(records, "records");

    Accumulator accumulator = new Accumulator();

    for (DeadLetterRecord record : records) {
        accumulator.add(record);
    }

    return accumulator.snapshot();
}


}
