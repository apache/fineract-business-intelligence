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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.apache.fineract.bi.ingestion.cdc.ChangeEvent;
import org.apache.fineract.bi.ingestion.cdc.DebeziumEnvelopeParser;
import org.apache.fineract.bi.ingestion.cdc.PoisonEventException;
import org.apache.fineract.bi.ingestion.routing.OwnershipCache;
import org.apache.fineract.bi.ingestion.routing.OwnershipSnapshot;
import org.apache.fineract.bi.ingestion.routing.Route;
import org.apache.fineract.bi.ingestion.sink.FencedWriteException;
import org.apache.fineract.bi.ingestion.sink.PartitionOffset;
import org.apache.fineract.bi.ingestion.sink.RoutedEvent;
import org.apache.fineract.bi.ingestion.sink.WarehouseWriter;
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

    static final class HoldException extends Exception {
        private static final long serialVersionUID = 1L;

        HoldException(String message) {
            super(message);
        }
    }

    private final int index;
    private final SinkConfig config;
    private final DataSource dataSource;
    private final OwnershipCache ownership;
    private final DebeziumEnvelopeParser parser;
    private final WarehouseWriter writer;
    private final DeadLetterPublisher deadLetters;
    private final SinkMetrics metrics;
    private final ProgressReporter progress;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final List<ConsumerRecord<byte[], byte[]>> buffer = new ArrayList<>();
    private volatile boolean alive = true;
    private KafkaConsumer<byte[], byte[]> consumer;

    public SinkWorker(int index, SinkConfig config, DataSource dataSource, OwnershipCache ownership,
            DebeziumEnvelopeParser parser, WarehouseWriter writer, DeadLetterPublisher deadLetters,
            SinkMetrics metrics, ProgressReporter progress) {
        this.index = index;
        this.config = config;
        this.dataSource = dataSource;
        this.ownership = ownership;
        this.parser = parser;
        this.writer = writer;
        this.deadLetters = deadLetters;
        this.metrics = metrics;
        this.progress = progress;
    }

    public boolean isAlive() {
        return alive;
    }

    public void shutdown() {
        running.set(false);
        KafkaConsumer<byte[], byte[]> c = consumer;
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
        try (KafkaConsumer<byte[], byte[]> c =
                new KafkaConsumer<>(consumerProperties(), new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            consumer = c;
            KafkaClientMetrics clientMetrics = new KafkaClientMetrics(c);
            clientMetrics.bindTo(metrics.registry());
            c.subscribe(Pattern.compile(config.topicPattern()), new Rebalance());
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
                    if (committed != null && !committed.isEmpty()) {
                        c.commitAsync(committed, (offsets, e) -> {
                            if (e != null) {
                                LOG.debug("Informational Kafka commit failed (warehouse offsets are authoritative)", e);
                            }
                        });
                    }
                    batchStartedAt = 0;
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                LOG.error("Unexpected wakeup", e);
            }
        } catch (RuntimeException e) {
            LOG.error("Sink worker {} crashed", index, e);
        } finally {
            alive = false;
            consumer = null;
            LOG.info("Sink worker {} stopped", index);
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> processWithRetry(KafkaConsumer<byte[], byte[]> c) {
        long backoff = 200;
        int fencedInARow = 0;
        boolean paused = false;
        try {
            while (running.get() && !buffer.isEmpty()) {
                try {
                    Map<TopicPartition, OffsetAndMetadata> result = process(new ArrayList<>(buffer));
                    buffer.clear();
                    return result;
                } catch (FencedWriteException e) {
                    metrics.fenced();
                    fencedInARow++;
                    LOG.info("Batch fenced ({}), refreshing ownership and re-routing", e.getMessage());
                    refreshOwnership(true);
                    if (fencedInARow < 5) {
                        continue;
                    }
                } catch (HoldException e) {
                    metrics.hold();
                    LOG.warn("Holding batch: {}", e.getMessage());
                    refreshOwnership(false);
                } catch (SQLException | KafkaException | java.util.concurrent.ExecutionException e) {
                    metrics.dbError();
                    LOG.warn("Batch failed, retrying in {} ms: {}", backoff, e.getMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
                if (!paused) {
                    c.pause(c.assignment());
                    paused = true;
                }
                sleepPolling(c, backoff);
                backoff = Math.min(backoff * 2, config.maxBackoffMs());
            }
            return null;
        } finally {
            if (paused && running.get()) {
                c.resume(c.paused());
            }
        }
    }

    private void sleepPolling(KafkaConsumer<byte[], byte[]> c, long millis) {
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

    Map<TopicPartition, OffsetAndMetadata> process(List<ConsumerRecord<byte[], byte[]>> batch)
            throws SQLException, HoldException, java.util.concurrent.ExecutionException, InterruptedException {
        long started = System.nanoTime();
        OwnershipSnapshot snap = ownership.snapshot();
        List<RoutedEvent> writes = new ArrayList<>(batch.size());
        List<DeadLetterPublisher.DeadLetter> letters = new ArrayList<>();
        Map<TopicPartition, Long> next = new HashMap<>();
        Map<String, long[]> perTable = new HashMap<>();
        Set<String> holds = new HashSet<>();

        for (ConsumerRecord<byte[], byte[]> r : batch) {
            next.merge(new TopicPartition(r.topic(), r.partition()), r.offset() + 1, Math::max);
            String table = r.topic().substring(r.topic().lastIndexOf('.') + 1);
            long[] counts = perTable.computeIfAbsent(table, k -> new long[4]);
            counts[0]++;
            Optional<ChangeEvent> parsed;
            try {
                parsed = parser.parse(r.topic(), r.value());
            } catch (PoisonEventException e) {
                letters.add(new DeadLetterPublisher.DeadLetter(r, e.reason(), e.getMessage()));
                counts[3]++;
                continue;
            }
            if (parsed.isEmpty()) {
                counts[2]++;
                continue;
            }
            ChangeEvent event = parsed.get();
            Route route = snap.route(event.sourceId(), event.table());
            switch (route.target()) {
                case PROD, SHADOW -> {
                    writes.add(new RoutedEvent(event, route));
                    counts[1]++;
                }
                case SKIP -> counts[2]++;
                case HOLD -> holds.add(event.sourceId() + "/" + event.table());
                case UNKNOWN_SOURCE -> {
                    if ("skip".equalsIgnoreCase(config.unknownSourcePolicy())) {
                        counts[2]++;
                    } else {
                        letters.add(new DeadLetterPublisher.DeadLetter(
                                r, "UNKNOWN_SOURCE", "Source " + event.sourceId() + " is not registered"));
                        counts[3]++;
                    }
                }
                default -> throw new IllegalStateException("Unhandled route " + route.target());
            }
        }
        if (!holds.isEmpty()) {
            throw new HoldException("paused ownership for " + holds);
        }
        if (!letters.isEmpty()) {
            deadLetters.publish(letters);
        }
        List<PartitionOffset> offsets = new ArrayList<>(next.size());
        next.forEach((tp, off) -> offsets.add(new PartitionOffset(tp.topic(), tp.partition(), off)));
        List<WarehouseWriter.Applied> applied;
        try (Connection conn = dataSource.getConnection()) {
            applied = writer.write(conn, writes, offsets, config.groupId());
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
        return commit;
    }

    private final class Rebalance implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (buffer.isEmpty()) {
                return;
            }
            try {
                process(new ArrayList<>(buffer));
            } catch (Exception e) {
                LOG.warn("Could not flush batch on revocation; new owner will replay it", e);
            } finally {
                buffer.clear();
            }
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            buffer.clear();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            if (partitions.isEmpty()) {
                return;
            }
            Set<String> topics = new HashSet<>();
            partitions.forEach(tp -> topics.add(tp.topic()));
            Map<String, Long> stored = loadOffsetsWithRetry(topics);
            {
                List<TopicPartition> fromBeginning = new ArrayList<>();
                for (TopicPartition tp : partitions) {
                    Long offset = stored.get(tp.topic() + "#" + tp.partition());
                    if (offset != null) {
                        consumer.seek(tp, offset);
                    } else if (config.startFromEarliestWithoutDbOffset()) {
                        fromBeginning.add(tp);
                    }
                }
                if (!fromBeginning.isEmpty()) {
                    consumer.seekToBeginning(fromBeginning);
                }
                LOG.info("Assigned {} partitions ({} from warehouse offsets, {} from beginning)",
                        partitions.size(), partitions.size() - fromBeginning.size(), fromBeginning.size());
            }
        }

        private Map<String, Long> loadOffsetsWithRetry(Set<String> topics) {
            long backoff = 500;
            long deadline = System.currentTimeMillis() + Math.min(config.maxPollIntervalMs() / 2, 60_000);
            while (true) {
                try (Connection c = dataSource.getConnection()) {
                    return WarehouseWriter.loadOffsets(c, config.groupId(), topics);
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
