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

import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.fineract.bi.ingestion.cdc.ChangeEvent;
import org.apache.fineract.bi.ingestion.cdc.DebeziumEnvelopeParser;
import org.apache.fineract.bi.ingestion.cdc.PoisonEventException;
import org.apache.fineract.bi.ingestion.routing.OwnershipSnapshot;
import org.apache.fineract.bi.ingestion.routing.OwnershipView;
import org.apache.fineract.bi.ingestion.routing.Route;
import org.apache.fineract.bi.ingestion.sink.BatchWriter;
import org.apache.fineract.bi.ingestion.sink.FencedWriteException;
import org.apache.fineract.bi.ingestion.sink.PartitionOffset;
import org.apache.fineract.bi.ingestion.sink.RoutedEvent;
import org.apache.fineract.bi.ingestion.sink.SkipCheck;
import org.apache.fineract.bi.ingestion.sink.SqlFailures;
import org.apache.fineract.bi.ingestion.sink.WarehouseWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SinkWorker implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SinkWorker.class);

    static final class SinkFatalException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        SinkFatalException(String message) {
            super(message);
        }
    }

    record Outcome(Map<TopicPartition, OffsetAndMetadata> commit, Map<TopicPartition, Long> heldFrom) {
    }

    @FunctionalInterface
    interface OffsetLoader {
        Map<String, Long> load(Connection c, String consumerGroup, Collection<String> topics) throws SQLException;
    }

    private final int index;
    private final SinkConfig config;
    private final ConnectionSource connections;
    private final OwnershipView ownership;
    private final DebeziumEnvelopeParser parser;
    private final BatchWriter writer;
    private final DeadLetterSink deadLetters;
    private final SinkMetrics metrics;
    private final ProgressReporter progress;
    private final Function<Properties, Consumer<byte[], byte[]>> consumerFactory;
    private OffsetLoader offsetLoader = WarehouseWriter::loadOffsets;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Random random = new Random();
    private final List<ConsumerRecord<byte[], byte[]>> buffer = new ArrayList<>();
    private final Set<TopicPartition> held = new HashSet<>();
    private volatile boolean alive = true;
    private volatile boolean fatal;
    private volatile long failingSince;
    private Consumer<byte[], byte[]> consumer;

    public SinkWorker(int index, SinkConfig config, ConnectionSource connections, OwnershipView ownership,
            DebeziumEnvelopeParser parser, WarehouseWriter writer, DeadLetterSink deadLetters,
            SinkMetrics metrics, ProgressReporter progress) {
        this(index, config, connections, ownership, parser, writer, deadLetters, metrics, progress,
                p -> new KafkaConsumer<>(p, new ByteArrayDeserializer(), new ByteArrayDeserializer()));
    }

    SinkWorker(int index, SinkConfig config, ConnectionSource connections, OwnershipView ownership,
            DebeziumEnvelopeParser parser, BatchWriter writer, DeadLetterSink deadLetters,
            SinkMetrics metrics, ProgressReporter progress,
            Function<Properties, Consumer<byte[], byte[]>> consumerFactory) {
        this.index = index;
        this.config = config;
        this.connections = connections;
        this.ownership = ownership;
        this.parser = parser;
        this.writer = writer;
        this.deadLetters = deadLetters;
        this.metrics = metrics;
        this.progress = progress;
        this.consumerFactory = consumerFactory;
    }

    public boolean isAlive() {
        return alive && !fatal;
    }

    void enqueue(ConsumerRecord<byte[], byte[]> record) {
        buffer.add(record);
    }

    int buffered() {
        return buffer.size();
    }

    Set<TopicPartition> heldPartitions() {
        return held;
    }

    void attach(Consumer<byte[], byte[]> c) {
        this.consumer = c;
    }

    void useOffsetLoader(OffsetLoader loader) {
        this.offsetLoader = loader;
    }

    public boolean isHealthy(long nowMillis) {
        long since = failingSince;
        return isAlive() && (since == 0 || nowMillis - since < config.healthStuckMs());
    }

    public void shutdown() {
        running.set(false);
        Consumer<byte[], byte[]> c = consumer;
        if (c != null) {
            c.wakeup();
        }
    }

    Properties consumerProperties() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, config.writerId() + "-" + index);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(config.maxPollRecords()));
        p.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, Integer.toString(config.fetchMinBytes()));
        p.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Integer.toString(config.fetchMaxWaitMs()));
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(config.maxPollIntervalMs()));
        p.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.putAll(config.consumerOverrides());
        return p;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("sink-worker-" + index);
        try (Consumer<byte[], byte[]> c = consumerFactory.apply(consumerProperties())) {
            consumer = c;
            new KafkaClientMetrics(c).bindTo(metrics.registry());
            c.subscribe(Pattern.compile(config.topicPattern()), new Rebalance());
            loop(c);
        } catch (WakeupException e) {
            if (running.get()) {
                LOG.error("Unexpected wakeup", e);
            }
        } catch (RuntimeException e) {
            if (isFatalCause(e)) {
                fatal = true;
            }
            LOG.error("Sink worker {} crashed", index, e);
        } finally {
            alive = false;
            consumer = null;
            LOG.info("Sink worker {} stopped", index);
        }
    }

    void loop(Consumer<byte[], byte[]> c) {
        long batchStartedAt = 0;
        while (running.get()) {
            ConsumerRecords<byte[], byte[]> records = c.poll(Duration.ofMillis(config.pollTimeoutMs()));
            for (ConsumerRecord<byte[], byte[]> r : records) {
                buffer.add(r);
            }
            long now = System.currentTimeMillis();
            if (!buffer.isEmpty() && batchStartedAt == 0) {
                batchStartedAt = now;
            }
            boolean full = buffer.size() >= config.maxBatchEvents();
            boolean lingered = !buffer.isEmpty() && now - batchStartedAt >= config.maxBatchWaitMs();
            if (full || lingered) {
                Map<TopicPartition, OffsetAndMetadata> committed = processWithRetry(c);
                if (!committed.isEmpty()) {
                    c.commitAsync(committed, (offsets, e) -> {
                        if (e != null) {
                            LOG.debug("Informational Kafka commit failed (warehouse offsets are authoritative)", e);
                        }
                    });
                }
                batchStartedAt = buffer.isEmpty() ? 0 : System.currentTimeMillis();
            }
        }
    }

    Map<TopicPartition, OffsetAndMetadata> processWithRetry(Consumer<byte[], byte[]> c) {
        long backoff = 200;
        int fencedInARow = 0;
        boolean paused = false;
        Map<TopicPartition, OffsetAndMetadata> commit = new HashMap<>();
        try {
            while (running.get() && !buffer.isEmpty()) {
                try {
                    List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>(buffer);
                    Outcome outcome = process(batch);
                    settle(c, batch, outcome);
                    mergeCommit(commit, outcome.commit());
                    failingSince = 0;
                    return commit;
                } catch (FencedWriteException e) {
                    metrics.fenced();
                    fencedInARow++;
                    LOG.info("Batch fenced ({}), refreshing ownership and re-routing", e.getMessage());
                    refreshOwnership(true);
                    if (fencedInARow < 5) {
                        continue;
                    }
                    markFailing();
                } catch (SQLException e) {
                    SqlFailures.Kind kind = SqlFailures.classify(e);
                    if (kind == SqlFailures.Kind.FATAL) {
                        fail("Permanent warehouse error, stopping this worker: " + e.getMessage(), e);
                        return commit;
                    }
                    if (kind == SqlFailures.Kind.DATA) {
                        LOG.warn("Batch rejected by the warehouse ({}), isolating the offending record", e.getMessage());
                        try {
                            mergeCommit(commit, isolate(c, new ArrayList<>(buffer)));
                            failingSince = 0;
                            continue;
                        } catch (SQLException inner) {
                            if (SqlFailures.classify(inner) == SqlFailures.Kind.FATAL) {
                                fail("Permanent warehouse error, stopping this worker: " + inner.getMessage(), inner);
                                return commit;
                            }
                            metrics.dbError();
                            markFailing();
                            LOG.warn("Isolation interrupted, retrying in {} ms: {}", backoff, inner.getMessage());
                        } catch (KafkaException | ExecutionException inner) {
                            metrics.dbError();
                            markFailing();
                            LOG.warn("Dead-lettering failed, retrying in {} ms: {}", backoff, inner.getMessage());
                        } catch (InterruptedException inner) {
                            Thread.currentThread().interrupt();
                            return commit;
                        }
                    } else {
                        metrics.dbError();
                        markFailing();
                        LOG.warn("Batch failed, retrying in {} ms: {}", backoff, e.getMessage());
                    }
                } catch (KafkaException | ExecutionException e) {
                    metrics.dbError();
                    markFailing();
                    LOG.warn("Batch failed, retrying in {} ms: {}", backoff, e.getMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return commit;
                }
                if (!paused) {
                    c.pause(c.assignment());
                    paused = true;
                }
                sleepPolling(c, jitter(backoff));
                backoff = Math.min(backoff * 2, config.maxBackoffMs());
            }
            return commit;
        } finally {
            if (paused && running.get()) {
                Set<TopicPartition> resume = new HashSet<>(c.paused());
                resume.removeAll(held);
                c.resume(resume);
            }
        }
    }

    private long jitter(long backoff) {
        return backoff / 2 + (long) (random.nextDouble() * (backoff / 2.0));
    }

    private void markFailing() {
        if (failingSince == 0) {
            failingSince = System.currentTimeMillis();
        }
    }

    private void fail(String message, Exception cause) {
        LOG.error(message, cause);
        fatal = true;
        running.set(false);
    }

    private static void mergeCommit(Map<TopicPartition, OffsetAndMetadata> into,
            Map<TopicPartition, OffsetAndMetadata> from) {
        from.forEach((tp, o) -> into.merge(tp, o, (a, b) -> a.offset() >= b.offset() ? a : b));
    }

    private void sleepPolling(Consumer<byte[], byte[]> c, long millis) {
        long until = System.currentTimeMillis() + millis;
        while (running.get() && System.currentTimeMillis() < until && !buffer.isEmpty()) {
            ConsumerRecords<byte[], byte[]> stray =
                    c.poll(Duration.ofMillis(Math.min(200, Math.max(1, until - System.currentTimeMillis()))));
            for (TopicPartition tp : stray.partitions()) {
                c.seek(tp, stray.records(tp).get(0).offset());
            }
            c.pause(c.assignment());
        }
    }

    private void refreshOwnership(boolean force) {
        try {
            ownership.refresh(force);
        } catch (SQLException e) {
            LOG.warn("Ownership refresh failed", e);
        }
    }

    private static TopicPartition partitionOf(ConsumerRecord<byte[], byte[]> r) {
        return new TopicPartition(r.topic(), r.partition());
    }

    private void settle(Consumer<byte[], byte[]> c, List<ConsumerRecord<byte[], byte[]>> batch, Outcome outcome) {
        Set<ConsumerRecord<byte[], byte[]>> processed = Collections.newSetFromMap(new IdentityHashMap<>());
        for (ConsumerRecord<byte[], byte[]> r : batch) {
            Long heldFrom = outcome.heldFrom().get(partitionOf(r));
            if (heldFrom == null || r.offset() < heldFrom) {
                processed.add(r);
            }
        }
        buffer.removeIf(processed::contains);
        Set<TopicPartition> assigned = c.assignment();
        Set<TopicPartition> release = new HashSet<>(held);
        release.removeAll(outcome.heldFrom().keySet());
        release.retainAll(assigned);
        if (!release.isEmpty()) {
            c.resume(release);
        }
        Set<TopicPartition> hold = new HashSet<>(outcome.heldFrom().keySet());
        hold.retainAll(assigned);
        if (!hold.isEmpty()) {
            c.pause(hold);
            metrics.hold();
            if (!hold.equals(held)) {
                LOG.warn("Holding partitions {} while their ownership is paused", hold);
            }
        }
        held.clear();
        held.addAll(hold);
    }

    Map<TopicPartition, OffsetAndMetadata> isolate(Consumer<byte[], byte[]> c,
            List<ConsumerRecord<byte[], byte[]>> batch)
            throws SQLException, ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> commit = new HashMap<>();
        Deque<List<ConsumerRecord<byte[], byte[]>>> work = new ArrayDeque<>();
        work.add(batch);
        while (!work.isEmpty()) {
            List<ConsumerRecord<byte[], byte[]>> part = work.pollFirst();
            try {
                Outcome outcome = process(part);
                settle(c, part, outcome);
                mergeCommit(commit, outcome.commit());
            } catch (SQLException e) {
                if (SqlFailures.classify(e) != SqlFailures.Kind.DATA) {
                    throw e;
                }
                if (part.size() == 1) {
                    mergeCommit(commit, quarantine(part.get(0), e));
                } else {
                    int mid = part.size() / 2;
                    work.addFirst(new ArrayList<>(part.subList(mid, part.size())));
                    work.addFirst(new ArrayList<>(part.subList(0, mid)));
                }
            }
        }
        return commit;
    }

    private Map<TopicPartition, OffsetAndMetadata> quarantine(ConsumerRecord<byte[], byte[]> record, SQLException e)
            throws SQLException, ExecutionException, InterruptedException {
        String reason = "WAREHOUSE_REJECTED";
        String message = e.getSQLState() + ": " + e.getMessage();
        deadLetters.publish(List.of(new DeadLetterPublisher.DeadLetter(record, reason, message)));
        TopicPartition tp = partitionOf(record);
        try (Connection conn = connections.get()) {
            writer.write(conn, List.of(),
                    List.of(new PartitionOffset(tp.topic(), tp.partition(), record.offset() + 1)),
                    config.groupId(), List.of());
        }
        buffer.removeIf(r -> r == record);
        metrics.deadLettered(reason, 1);
        String table = record.topic().substring(record.topic().lastIndexOf('.') + 1);
        progress.audit(table, 1, 0, 0, 1);
        LOG.error("Record {}-{}@{} rejected by the warehouse and dead-lettered: {}",
                record.topic(), record.partition(), record.offset(), message);
        return Map.of(tp, new OffsetAndMetadata(record.offset() + 1));
    }

    Outcome process(List<ConsumerRecord<byte[], byte[]>> batch)
            throws SQLException, ExecutionException, InterruptedException {
        long started = System.nanoTime();
        OwnershipSnapshot snap = ownership.snapshot();
        List<RoutedEvent> writes = new ArrayList<>(batch.size());
        List<DeadLetterPublisher.DeadLetter> letters = new ArrayList<>();
        List<SkipCheck> skips = new ArrayList<>();
        Map<TopicPartition, Long> next = new HashMap<>();
        Map<TopicPartition, Long> heldFrom = new HashMap<>();
        Map<String, long[]> perTable = new HashMap<>();

        for (ConsumerRecord<byte[], byte[]> r : batch) {
            TopicPartition tp = partitionOf(r);
            if (heldFrom.containsKey(tp)) {
                continue;
            }
            String table = r.topic().substring(r.topic().lastIndexOf('.') + 1);
            long[] counts = perTable.computeIfAbsent(table, k -> new long[4]);
            Optional<ChangeEvent> parsed;
            try {
                parsed = parser.parse(r.topic(), r.value());
            } catch (PoisonEventException e) {
                counts[0]++;
                counts[3]++;
                next.merge(tp, r.offset() + 1, Math::max);
                letters.add(new DeadLetterPublisher.DeadLetter(r, e.reason(), e.getMessage()));
                continue;
            }
            if (parsed.isEmpty()) {
                counts[0]++;
                counts[2]++;
                next.merge(tp, r.offset() + 1, Math::max);
                continue;
            }
            ChangeEvent event = parsed.get();
            Route route = snap.route(event.sourceId(), event.table());
            if (route.target() == Route.Target.HOLD) {
                heldFrom.put(tp, r.offset());
                continue;
            }
            counts[0]++;
            next.merge(tp, r.offset() + 1, Math::max);
            for (String col : event.absent()) {
                metrics.absentColumn(event.table(), col);
            }
            switch (route.target()) {
                case PROD, SHADOW -> {
                    writes.add(new RoutedEvent(event, route));
                    counts[1]++;
                }
                case SKIP -> {
                    counts[2]++;
                    skips.add(new SkipCheck(event.sourceId(), event.table(), route.epoch()));
                }
                case UNKNOWN_SOURCE -> {
                    if ("skip".equalsIgnoreCase(config.unknownSourcePolicy())) {
                        counts[2]++;
                        skips.add(new SkipCheck(event.sourceId(), event.table(), SkipCheck.MUST_BE_UNREGISTERED));
                    } else {
                        letters.add(new DeadLetterPublisher.DeadLetter(
                                r, "UNKNOWN_SOURCE", "Source " + event.sourceId() + " is not registered"));
                        counts[3]++;
                    }
                }
                default -> throw new IllegalStateException("Unhandled route " + route.target());
            }
        }
        if (!letters.isEmpty()) {
            deadLetters.publish(letters);
        }
        List<PartitionOffset> offsets = new ArrayList<>(next.size());
        next.forEach((tp, off) -> offsets.add(new PartitionOffset(tp.topic(), tp.partition(), off)));
        List<WarehouseWriter.Applied> applied = List.of();
        if (!offsets.isEmpty()) {
            try (Connection conn = connections.get()) {
                applied = writer.write(conn, writes, offsets, config.groupId(), skips);
            }
        }
        long nowMicros = System.currentTimeMillis() * 1_000L;
        Map<String, long[]> perRoute = new HashMap<>();
        for (RoutedEvent w : writes) {
            ChangeEvent e = w.event();
            metrics.latencyMicros(nowMicros - e.sourceTsMicros());
            long[] agg = perRoute.computeIfAbsent(
                    e.sourceId() + '\u0000' + e.table() + '\u0000' + w.route().stageTarget(), k -> new long[] {0, 0, -1});
            agg[0]++;
            agg[1] = Math.max(agg[1], e.sourceTsMicros());
            if (e.lsn() != null) {
                agg[2] = Math.max(agg[2], e.lsn());
            }
        }
        perRoute.forEach((k, v) -> {
            String[] parts = k.split("\u0000", 3);
            progress.applied(parts[0], parts[1], parts[2], v[0], v[1], v[2] >= 0 ? v[2] : null);
        });
        perTable.forEach((table, v) -> {
            metrics.events(table, "applied", v[1]);
            metrics.events(table, "skipped", v[2]);
            metrics.events(table, "dead_lettered", v[3]);
            progress.audit(table, v[0], v[1], v[2], v[3]);
        });
        for (DeadLetterPublisher.DeadLetter d : letters) {
            metrics.deadLettered(d.reason(), 1);
        }
        for (WarehouseWriter.Applied a : applied) {
            metrics.rowsWritten(a.table(), a.target(), a.rows());
        }
        metrics.batch(System.nanoTime() - started);
        Map<TopicPartition, OffsetAndMetadata> commit = new HashMap<>();
        next.forEach((tp, off) -> commit.put(tp, new OffsetAndMetadata(off)));
        return new Outcome(commit, heldFrom);
    }

    private static boolean isFatalCause(Throwable t) {
        for (Throwable x = t; x != null; x = x.getCause()) {
            if (x instanceof SinkFatalException) {
                return true;
            }
        }
        return false;
    }

    void dropBuffered(Collection<TopicPartition> partitions) {
        buffer.removeIf(r -> partitions.contains(partitionOf(r)));
        held.removeAll(partitions);
    }

    Rebalance rebalanceListener() {
        return new Rebalance();
    }

    final class Rebalance implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            dropBuffered(partitions);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            dropBuffered(partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            if (partitions.isEmpty()) {
                return;
            }
            Set<String> topics = new HashSet<>();
            partitions.forEach(tp -> topics.add(tp.topic()));
            Map<String, Long> stored = loadOffsetsWithRetry(topics);
            Map<TopicPartition, Long> beginning = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            List<TopicPartition> fromBeginning = new ArrayList<>();
            int fromWarehouse = 0;
            for (TopicPartition tp : partitions) {
                Long offset = stored.get(tp.topic() + "#" + tp.partition());
                if (offset == null) {
                    if (config.startFromEarliestWithoutDbOffset()) {
                        fromBeginning.add(tp);
                    }
                    continue;
                }
                Long low = beginning.get(tp);
                Long high = end.get(tp);
                boolean behind = low != null && offset < low;
                boolean ahead = high != null && offset > high;
                if (behind || ahead) {
                    String problem = behind
                            ? "its stored offset " + offset + " is older than the log start " + low
                                    + " (the topic's retention expired events that were never applied)"
                            : "its stored offset " + offset + " is beyond the log end " + high
                                    + " (the topic was recreated or truncated)";
                    metrics.offsetGap();
                    if (!config.allowOffsetReset()) {
                        throw new SinkFatalException("Partition " + tp + ": " + problem
                                + ". Refusing to continue silently; reconcile the source, then set"
                                + " SINK_ALLOW_OFFSET_RESET=true or fix the stored offset.");
                    }
                    LOG.error("Partition {}: {}; restarting from the beginning (SINK_ALLOW_OFFSET_RESET)", tp, problem);
                    fromBeginning.add(tp);
                    continue;
                }
                consumer.seek(tp, offset);
                fromWarehouse++;
            }
            if (!fromBeginning.isEmpty()) {
                consumer.seekToBeginning(fromBeginning);
            }
            LOG.info("Assigned {} partitions ({} from warehouse offsets, {} from beginning)",
                    partitions.size(), fromWarehouse, fromBeginning.size());
        }

        private Map<String, Long> loadOffsetsWithRetry(Set<String> topics) {
            long backoff = 500;
            long deadline = System.currentTimeMillis() + Math.min(config.maxPollIntervalMs() / 2, 60_000);
            while (true) {
                try (Connection c = connections.get()) {
                    return offsetLoader.load(c, config.groupId(), topics);
                } catch (SQLException e) {
                    if (System.currentTimeMillis() + backoff > deadline || !running.get()) {
                        throw new KafkaException("Cannot load warehouse offsets", e);
                    }
                    LOG.warn("Warehouse unavailable while loading offsets, retrying in {} ms: {}", backoff, e.getMessage());
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new KafkaException("Interrupted while loading offsets", ie);
                    }
                    backoff = Math.min(backoff * 2, 5_000);
                }
            }
        }
    }
}
