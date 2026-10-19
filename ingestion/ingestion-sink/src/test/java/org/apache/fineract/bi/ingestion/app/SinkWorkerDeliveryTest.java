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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.fineract.bi.ingestion.cdc.DebeziumEnvelopeParser;
import org.apache.fineract.bi.ingestion.config.Env;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.fineract.bi.ingestion.routing.OwnershipSnapshot;
import org.apache.fineract.bi.ingestion.routing.OwnershipView;
import org.apache.fineract.bi.ingestion.sink.BatchWriter;
import org.apache.fineract.bi.ingestion.sink.PartitionOffset;
import org.apache.fineract.bi.ingestion.sink.RoutedEvent;
import org.apache.fineract.bi.ingestion.sink.SkipCheck;
import org.apache.fineract.bi.ingestion.sink.WarehouseWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SinkWorkerDeliveryTest {

    private static final String TOPIC = "fineract.cdc.batch_job_instance";
    private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);

    static class FakeWriter implements BatchWriter {
        final List<List<Long>> batches = new ArrayList<>();
        final List<Collection<PartitionOffset>> offsets = new ArrayList<>();
        final List<Collection<SkipCheck>> skips = new ArrayList<>();
        java.util.function.Consumer<List<Long>> before = ids -> { };

        @Override
        public List<WarehouseWriter.Applied> write(Connection c, Collection<RoutedEvent> events,
                Collection<PartitionOffset> offs, String group, Collection<SkipCheck> skip) throws SQLException {
            List<Long> ids = new ArrayList<>();
            for (RoutedEvent e : events) {
                ids.add(Long.parseLong(e.event().values()[0]));
            }
            before.accept(ids);
            batches.add(ids);
            offsets.add(new ArrayList<>(offs));
            skips.add(new ArrayList<>(skip));
            return List.of();
        }

        List<Long> allIds() {
            List<Long> all = new ArrayList<>();
            batches.forEach(all::addAll);
            return all;
        }
    }

    static final class FakeLetters implements DeadLetterSink {
        final List<DeadLetterPublisher.DeadLetter> letters = new ArrayList<>();

        @Override
        public void publish(List<DeadLetterPublisher.DeadLetter> l) {
            letters.addAll(l);
        }
    }

    static final class View implements OwnershipView {
        final AtomicReference<OwnershipSnapshot> current = new AtomicReference<>();

        View(OwnershipSnapshot snapshot) {
            current.set(snapshot);
        }

        @Override
        public OwnershipSnapshot snapshot() {
            return current.get();
        }

        @Override
        public OwnershipSnapshot refresh(boolean force) {
            return current.get();
        }
    }

    private Contract contract;
    private FakeWriter writer;
    private FakeLetters letters;
    private MockConsumer<byte[], byte[]> consumer;
    private SinkMetrics metrics;

    @BeforeMethod
    public void setUp() {
        contract = ContractLoader.load(Path.of("../../contracts/fineract_tables.json"));
        writer = new FakeWriter();
        letters = new FakeLetters();
        consumer = new MockConsumer<>("earliest");
        metrics = new SinkMetrics();
    }

    private static OwnershipSnapshot snapshot(String state, String engine) {
        return new OwnershipSnapshot(
                Map.of(OwnershipSnapshot.key("bank-a", "batch_job_instance"),
                        new OwnershipSnapshot.Entry(engine, null, state, 7),
                        OwnershipSnapshot.key("bank-b", "batch_job_instance"),
                        new OwnershipSnapshot.Entry(engine, null, state, 7)),
                Map.of("bank-a", new OwnershipSnapshot.SourceMeta("t-a", 0),
                        "bank-b", new OwnershipSnapshot.SourceMeta("t-b", 0)),
                System.currentTimeMillis());
    }

    private SinkWorker worker(View view, Map<String, String> extra) {
        Map<String, String> env = new HashMap<>(Map.of(
                "SINK_MAX_BACKOFF_MS", "20", "SINK_MAX_BATCH_WAIT_MS", "1", "SINK_HEALTH_STUCK_MS", "50"));
        env.putAll(extra);
        SinkConfig config = SinkConfig.from(Env.of(env), env);
        SinkWorker w = new SinkWorker(0, config, () -> null, view, new DebeziumEnvelopeParser(contract), writer, letters,
                metrics, new ProgressReporter(null, "test"), p -> consumer);
        w.attach(consumer);
        consumer.assign(List.of(P0, P1));
        consumer.updateBeginningOffsets(Map.of(P0, 0L, P1, 0L));
        consumer.updateEndOffsets(Map.of(P0, 1_000L, P1, 1_000L));
        return w;
    }

    private static ConsumerRecord<byte[], byte[]> record(TopicPartition tp, long offset, String source, long id) {
        String json = "{\"after\":{\"job_instance_id\":" + id + ",\"job_name\":\"j" + id
                + "\",\"job_key\":\"k\"},\"source\":{\"name\":\"" + source
                + "\",\"table\":\"batch_job_instance\",\"lsn\":" + (100 + id) + ",\"ts_ms\":1767261600000},\"op\":\"c\"}";
        return new ConsumerRecord<>(tp.topic(), tp.partition(), offset, null, json.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void revokingPartitionsDropsOnlyTheirRecordsAndNeverFlushesFromTheCallback() {
        SinkWorker w = worker(new View(snapshot("STEADY", "KAFKA")), Map.of());
        w.enqueue(record(P0, 0, "bank-a", 1));
        w.enqueue(record(P0, 1, "bank-a", 2));
        w.enqueue(record(P1, 0, "bank-a", 3));
        w.enqueue(record(P1, 1, "bank-a", 4));

        w.rebalanceListener().onPartitionsRevoked(Set.of(P0));

        assertThat(writer.batches).as("a rebalance callback must never write to the warehouse").isEmpty();
        assertThat(w.buffered()).isEqualTo(2);
        Map<TopicPartition, OffsetAndMetadata> commit = w.processWithRetry(consumer);
        assertThat(writer.allIds()).containsExactly(3L, 4L);
        assertThat(commit).containsOnlyKeys(P1);
        assertThat(commit.get(P1).offset()).isEqualTo(2L);
    }

    @Test
    public void aFailedAttemptNeverLosesTheRecordsOfPartitionsThatAreKept() {
        int[] attempts = {0};
        SinkWorker[] holder = new SinkWorker[1];
        FakeWriter flaky = new FakeWriter() {
            @Override
            public List<WarehouseWriter.Applied> write(Connection c, Collection<RoutedEvent> events,
                    Collection<PartitionOffset> offs, String group, Collection<SkipCheck> skip) throws SQLException {
                if (attempts[0]++ == 0) {
                    holder[0].rebalanceListener().onPartitionsRevoked(Set.of(P0));
                    throw new SQLException("warehouse down", "08006");
                }
                return super.write(c, events, offs, group, skip);
            }
        };
        writer = flaky;
        SinkWorker w = worker(new View(snapshot("STEADY", "KAFKA")), Map.of());
        holder[0] = w;
        w.enqueue(record(P0, 0, "bank-a", 1));
        w.enqueue(record(P1, 0, "bank-a", 2));
        w.enqueue(record(P1, 1, "bank-a", 3));

        Map<TopicPartition, OffsetAndMetadata> commit = w.processWithRetry(consumer);

        assertThat(attempts[0]).isEqualTo(2);
        assertThat(flaky.allIds()).as("the kept partition's records survive the failed attempt").containsExactly(2L, 3L);
        assertThat(commit).containsOnlyKeys(P1);
        assertThat(commit.get(P1).offset()).isEqualTo(2L);
        assertThat(w.buffered()).isZero();
    }

    @Test
    public void aTransientWarehouseOutageIsRetriedUntilTheBatchIsApplied() {
        int[] attempts = {0};
        FakeWriter flaky = new FakeWriter() {
            @Override
            public List<WarehouseWriter.Applied> write(Connection c, Collection<RoutedEvent> events,
                    Collection<PartitionOffset> offs, String group, Collection<SkipCheck> skip) throws SQLException {
                if (attempts[0]++ < 3) {
                    throw new SQLException("connection reset", "08006");
                }
                return super.write(c, events, offs, group, skip);
            }
        };
        writer = flaky;
        SinkWorker w = worker(new View(snapshot("STEADY", "KAFKA")), Map.of());
        w.enqueue(record(P0, 0, "bank-a", 1));
        Map<TopicPartition, OffsetAndMetadata> commit = w.processWithRetry(consumer);
        assertThat(attempts[0]).isEqualTo(4);
        assertThat(flaky.allIds()).containsExactly(1L);
        assertThat(commit.get(P0).offset()).isEqualTo(1L);
        assertThat(w.isHealthy(System.currentTimeMillis())).isTrue();
    }

    @Test
    public void onePoisonRowIsIsolatedAndDeadLetteredWhileTheRestOfTheBatchIsApplied() {
        FakeWriter strict = new FakeWriter() {
            @Override
            public List<WarehouseWriter.Applied> write(Connection c, Collection<RoutedEvent> events,
                    Collection<PartitionOffset> offs, String group, Collection<SkipCheck> skip) throws SQLException {
                for (RoutedEvent e : events) {
                    if ("3".equals(e.event().values()[0])) {
                        throw new SQLException("numeric field overflow", "22003");
                    }
                }
                return super.write(c, events, offs, group, skip);
            }
        };
        writer = strict;
        SinkWorker w = worker(new View(snapshot("STEADY", "KAFKA")), Map.of());
        for (int i = 1; i <= 5; i++) {
            w.enqueue(record(P0, i - 1, i <= 2 ? "bank-a" : "bank-b", i));
        }

        Map<TopicPartition, OffsetAndMetadata> commit = w.processWithRetry(consumer);

        assertThat(strict.allIds()).containsExactlyInAnyOrder(1L, 2L, 4L, 5L);
        assertThat(letters.letters).hasSize(1);
        assertThat(letters.letters.get(0).reason()).isEqualTo("WAREHOUSE_REJECTED");
        assertThat(letters.letters.get(0).record().offset()).isEqualTo(2L);
        assertThat(commit.get(P0).offset()).isEqualTo(5L);
        assertThat(w.buffered()).isZero();
        assertThat(w.isAlive()).isTrue();
    }

    @Test
    public void aPermanentSchemaErrorStopsTheWorkerInsteadOfRetryingForever() {
        FakeWriter broken = new FakeWriter() {
            @Override
            public List<WarehouseWriter.Applied> write(Connection c, Collection<RoutedEvent> events,
                    Collection<PartitionOffset> offs, String group, Collection<SkipCheck> skip) throws SQLException {
                throw new SQLException("Unknown contract table", "FBI02");
            }
        };
        writer = broken;
        SinkWorker w = worker(new View(snapshot("STEADY", "KAFKA")), Map.of());
        w.enqueue(record(P0, 0, "bank-a", 1));
        w.processWithRetry(consumer);
        assertThat(w.isAlive()).isFalse();
    }

    @Test
    public void healthTurnsRedWhenBatchesKeepFailingLongerThanTheThreshold() throws Exception {
        FakeWriter down = new FakeWriter() {
            @Override
            public List<WarehouseWriter.Applied> write(Connection c, Collection<RoutedEvent> events,
                    Collection<PartitionOffset> offs, String group, Collection<SkipCheck> skip) throws SQLException {
                throw new SQLException("connection refused", "08001");
            }
        };
        writer = down;
        SinkWorker w = worker(new View(snapshot("STEADY", "KAFKA")), Map.of());
        w.enqueue(record(P0, 0, "bank-a", 1));
        Thread t = new Thread(() -> w.processWithRetry(consumer));
        t.start();
        long deadline = System.currentTimeMillis() + 5_000;
        while (w.isHealthy(System.currentTimeMillis()) && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(w.isHealthy(System.currentTimeMillis())).as("stuck for longer than SINK_HEALTH_STUCK_MS").isFalse();
        w.shutdown();
        t.join(5_000);
    }

    @Test
    public void aPausedSourceOnlyHoldsItsOwnPartitionWhileOtherTenantsKeepFlowing() {
        OwnershipSnapshot mixed = new OwnershipSnapshot(
                Map.of(OwnershipSnapshot.key("bank-a", "batch_job_instance"),
                        new OwnershipSnapshot.Entry("KAFKA", null, "STEADY", 7),
                        OwnershipSnapshot.key("bank-b", "batch_job_instance"),
                        new OwnershipSnapshot.Entry("KAFKA", null, "PAUSED", 7)),
                Map.of("bank-a", new OwnershipSnapshot.SourceMeta("t-a", 0),
                        "bank-b", new OwnershipSnapshot.SourceMeta("t-b", 0)),
                System.currentTimeMillis());
        View view = new View(mixed);
        SinkWorker w = worker(view, Map.of());
        w.enqueue(record(P0, 0, "bank-a", 1));
        w.enqueue(record(P1, 0, "bank-a", 2));
        w.enqueue(record(P1, 1, "bank-b", 3));
        w.enqueue(record(P1, 2, "bank-a", 4));

        Map<TopicPartition, OffsetAndMetadata> commit = w.processWithRetry(consumer);
        assertThat(writer.allIds()).containsExactly(1L, 2L);
        assertThat(commit.get(P0).offset()).isEqualTo(1L);
        assertThat(commit.get(P1).offset()).isEqualTo(1L);
        assertThat(w.buffered()).isEqualTo(2);
        assertThat(w.heldPartitions()).containsExactly(P1);
        assertThat(consumer.paused()).containsExactly(P1);

        view.current.set(snapshot("STEADY", "KAFKA"));
        Map<TopicPartition, OffsetAndMetadata> later = w.processWithRetry(consumer);
        assertThat(writer.allIds()).containsExactly(1L, 2L, 3L, 4L);
        assertThat(later.get(P1).offset()).isEqualTo(3L);
        assertThat(w.heldPartitions()).isEmpty();
        assertThat(consumer.paused()).isEmpty();
    }

    @Test
    public void skippedEventsAreVerifiedAgainstTheirSnapshotEpochInTheSameTransaction() {
        OwnershipSnapshot pythonOwned = new OwnershipSnapshot(
                Map.of(OwnershipSnapshot.key("bank-a", "batch_job_instance"),
                        new OwnershipSnapshot.Entry("PYTHON", null, "STEADY", 9)),
                Map.of("bank-a", new OwnershipSnapshot.SourceMeta("t-a", 0)),
                System.currentTimeMillis());
        SinkWorker w = worker(new View(pythonOwned), Map.of());
        w.enqueue(record(P0, 0, "bank-a", 1));
        w.processWithRetry(consumer);
        assertThat(writer.allIds()).isEmpty();
        assertThat(writer.skips).hasSize(1);
        assertThat(writer.skips.get(0)).containsExactly(new SkipCheck("bank-a", "batch_job_instance", 9L));
        assertThat(writer.offsets.get(0)).extracting(PartitionOffset::nextOffset).containsExactly(1L);
    }

    @Test
    public void aStoredOffsetOlderThanTheLogStartIsAFatalGapUnlessResetIsAllowed() {
        SinkWorker w = worker(new View(snapshot("STEADY", "KAFKA")), Map.of());
        consumer.updateBeginningOffsets(Map.of(P0, 500L, P1, 0L));
        w.useOffsetLoader((c, group, topics) -> Map.of(TOPIC + "#0", 10L, TOPIC + "#1", 5L));
        assertThatThrownBy(() -> w.rebalanceListener().onPartitionsAssigned(Set.of(P0, P1)))
                .isInstanceOf(SinkWorker.SinkFatalException.class)
                .hasMessageContaining("older than the log start");

        SinkWorker lenient = worker(new View(snapshot("STEADY", "KAFKA")), Map.of("SINK_ALLOW_OFFSET_RESET", "true"));
        consumer.updateBeginningOffsets(Map.of(P0, 500L, P1, 0L));
        lenient.useOffsetLoader((c, group, topics) -> Map.of(TOPIC + "#0", 10L, TOPIC + "#1", 5L));
        lenient.rebalanceListener().onPartitionsAssigned(Set.of(P0, P1));
        assertThat(consumer.position(P1)).isEqualTo(5L);
    }

    @Test
    public void storedOffsetsInsideTheLogAreResumedExactly() {
        SinkWorker w = worker(new View(snapshot("STEADY", "KAFKA")), Map.of());
        w.useOffsetLoader((c, group, topics) -> Map.of(TOPIC + "#0", 40L, TOPIC + "#1", 7L));
        w.rebalanceListener().onPartitionsAssigned(Set.of(P0, P1));
        assertThat(consumer.position(P0)).isEqualTo(40L);
        assertThat(consumer.position(P1)).isEqualTo(7L);
    }
}
