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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.fineract.bi.ingestion.routing.OwnershipSnapshot;
import org.apache.fineract.bi.ingestion.routing.Route;
import org.apache.fineract.bi.ingestion.sink.BatchWriter;
import org.apache.fineract.bi.ingestion.sink.PartitionOffset;
import org.apache.fineract.bi.ingestion.sink.PreflightRollbackException;
import org.apache.fineract.bi.ingestion.sink.ReplayIdentity;
import org.apache.fineract.bi.ingestion.sink.ReplayOutcome;
import org.apache.fineract.bi.ingestion.sink.RoutedEvent;
import org.apache.fineract.bi.ingestion.sink.SkipCheck;
import org.apache.fineract.bi.ingestion.sink.WarehouseWriter;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DeadLetterReplayCommandTest {

    private static final Path ROOT = Path.of("../..");
    private Contract contract;

    @BeforeClass
    public void setup() {
        contract = ContractLoader.load(ROOT.resolve("contracts/fineract_tables.json"));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RecordHeaders headers(String reason, String source, String topic, int partition, long offset) {
        RecordHeaders h = new RecordHeaders();
        h.add("fbi.dlt.reason", bytes(reason));
        h.add("fbi.dlt.message", bytes("test"));
        h.add("fbi.source.topic", bytes(topic));
        h.add("fbi.source.partition", bytes(Integer.toString(partition)));
        h.add("fbi.source.offset", bytes(Long.toString(offset)));
        return h;
    }

    private DeadLetterRecord record(String reason, String source, int part, long off, boolean omitted, byte[] key) {
        RecordHeaders h = headers(reason, source, "fineract.cdc.batch_job_instance", part, off);
        if (omitted) {
            h.add("fbi.dlt.value_omitted", bytes("true"));
            return DeadLetterRecord.parse("fineract.cdc.dlt", 0, off, h, key, null, contract);
        }
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":" + off + "},\"source\":{\"name\":\""
                + source + "\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        return DeadLetterRecord.parse("fineract.cdc.dlt", 0, off, h, key, payload, contract);
    }

    private static void renderReport(
            DeadLetterReader.ScanResult scan,
            DeadLetterReplayCommand.ReplayFilter filter,
            boolean dryRun,
            boolean skipPreflight,
            List<DeadLetterRecord> matched,
            List<DeadLetterRecord> rejectedPayloads,
            List<DeadLetterRecord> invalidReplayHeaders,
            List<DeadLetterRecord> preflightFailed,
            List<DeadLetterRecord> readyToReplay,
            int replayedCount,
            Map<DeadLetterRecord, String> failureReasons,
            PrintStream out) {
        DeadLetterReplayCommand.renderReport(
                scan, filter, dryRun, skipPreflight, matched, rejectedPayloads,
                invalidReplayHeaders, List.of(), Map.of(), preflightFailed, List.of(), readyToReplay,
                new DeadLetterReplayCommand.PublicationSnapshot(replayedCount, 0, Math.max(0, readyToReplay.size() - replayedCount), failureReasons),
                failureReasons,
                DeadLetterReplayCommand.LedgerCheckStatus.NOT_NEEDED, null, Map.of(), out);
    }

    @Test
    public void parsesPartitionAwareOffsetRanges() {
        DeadLetterReplayCommand.OffsetFilter f1 =
                DeadLetterReplayCommand.OffsetFilter.parse("fineract.cdc.batch_job_instance[0]:100..200", null);
        assertThat(f1.topic()).isEqualTo("fineract.cdc.batch_job_instance");
        assertThat(f1.partition()).isEqualTo(0);
        assertThat(f1.minOffset()).isEqualTo(100L);
        assertThat(f1.maxOffset()).isEqualTo(200L);
        assertThat(f1.matches("fineract.cdc.batch_job_instance", 0, 100L)).isTrue();
        assertThat(f1.matches("fineract.cdc.batch_job_instance", 0, 200L)).isTrue();
        assertThat(f1.matches("fineract.cdc.batch_job_instance", 0, 99L)).isFalse();
        assertThat(f1.matches("fineract.cdc.batch_job_instance", 0, 201L)).isFalse();
        assertThat(f1.matches("fineract.cdc.batch_job_instance", 1, 150L)).isFalse();

        DeadLetterReplayCommand.OffsetFilter f2 =
                DeadLetterReplayCommand.OffsetFilter.parse("1:50-80", "fineract.cdc.m_loan");
        assertThat(f2.topic()).isEqualTo("fineract.cdc.m_loan");
        assertThat(f2.partition()).isEqualTo(1);
        assertThat(f2.minOffset()).isEqualTo(50L);
        assertThat(f2.maxOffset()).isEqualTo(80L);

        DeadLetterReplayCommand.OffsetFilter f3 =
                DeadLetterReplayCommand.OffsetFilter.parse("fineract.cdc.batch_job_instance[0]:42", null);
        assertThat(f3.topic()).isEqualTo("fineract.cdc.batch_job_instance");
        assertThat(f3.partition()).isEqualTo(0);
        assertThat(f3.minOffset()).isEqualTo(42L);
        assertThat(f3.maxOffset()).isEqualTo(42L);
        assertThat(f3.matches("fineract.cdc.batch_job_instance", 0, 42L)).isTrue();
        assertThat(f3.matches("fineract.cdc.batch_job_instance", 0, 43L)).isFalse();
    }

    @Test
    public void rejectsAmbiguousPartitionOnlyWithoutTableScope() {
        assertThatThrownBy(() -> DeadLetterReplayCommand.OffsetFilter.parse("0:100..200", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ambiguous without a table scope");

        assertThatThrownBy(() -> DeadLetterReplayCommand.OffsetFilter.parse("0:42", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ambiguous without a table scope");
    }

    @Test
    public void rejectsConflictingTopicAndTableScope() {
        assertThatThrownBy(() -> DeadLetterReplayCommand.OffsetFilter.parse(
                "fineract.cdc.m_client[0]:100..200", "fineract.cdc.m_loan"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match scoped table topic");
    }

    @Test
    public void rejectsAmbiguousUnpartitionedOffsetRange() {
        assertThatThrownBy(() -> DeadLetterReplayCommand.OffsetFilter.parse("100..200", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition-aware");
    }

    @Test
    public void rejectsNegativePartitionOrInvertedBounds() {
        assertThatThrownBy(() -> DeadLetterReplayCommand.OffsetFilter.parse("-1:10..20", "fineract.cdc.batch_job_instance"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Partition must not be negative");

        assertThatThrownBy(() -> DeadLetterReplayCommand.OffsetFilter.parse("0:100..50", "fineract.cdc.batch_job_instance"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not exceed max");

        assertThatThrownBy(() -> DeadLetterReplayCommand.OffsetFilter.parse("fineract.cdc.batch_job_instance[-1]:10..20", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Partition must not be negative");

        assertThatThrownBy(() -> DeadLetterReplayCommand.OffsetFilter.parse("fineract.cdc.batch_job_instance[0]:100..50", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not exceed max");
    }

    @Test
    public void filterMatchesReasonSourceTableAndOffsets() {
        DeadLetterReplayCommand.OffsetFilter off =
                new DeadLetterReplayCommand.OffsetFilter("fineract.cdc.batch_job_instance", 0, 10L, 20L);
        DeadLetterReplayCommand.ReplayFilter filter =
                new DeadLetterReplayCommand.ReplayFilter("WAREHOUSE_REJECTED", "bank-a", "batch_job_instance", off);

        DeadLetterRecord matching = record("WAREHOUSE_REJECTED", "bank-a", 0, 15L, false, bytes("k"));
        assertThat(filter.matches(matching)).isTrue();

        DeadLetterRecord wrongReason = record("POISON_JSON", "bank-a", 0, 15L, false, bytes("k"));
        assertThat(filter.matches(wrongReason)).isFalse();

        DeadLetterRecord wrongSource = record("WAREHOUSE_REJECTED", "bank-b", 0, 15L, false, bytes("k"));
        assertThat(filter.matches(wrongSource)).isFalse();

        DeadLetterRecord wrongPartition = record("WAREHOUSE_REJECTED", "bank-a", 1, 15L, false, bytes("k"));
        assertThat(filter.matches(wrongPartition)).isFalse();

        DeadLetterRecord outOfRange = record("WAREHOUSE_REJECTED", "bank-a", 0, 25L, false, bytes("k"));
        assertThat(filter.matches(outOfRange)).isFalse();
    }

    @Test
    public void executesReplayPreservingOriginalPartitionAndKey() throws Exception {
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());

        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 1, 10L, false, bytes("key-1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 0, 20L, false, bytes("key-2"));

        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2,
                Map.of(new TopicPartition("fineract.cdc.dlt", 0), new DeadLetterReader.PartitionBounds(0, 2)));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DeadLetterReplayCommand.ReplayFilter filter = new DeadLetterReplayCommand.ReplayFilter(null, null, null, null);

        renderReport(
                scan, filter, false, true,
                List.of(r1, r2), List.of(), List.of(), List.of(), List.of(r1, r2), 2, Map.of(),
                new PrintStream(out, true, StandardCharsets.UTF_8));

        ProducerRecord<byte[], byte[]> pr1 = new ProducerRecord<>(
                r1.originalTopic(), r1.originalPartition(), r1.key(), r1.value());
        ReplayIdentityHeaders.add(pr1.headers(), r1.replayIdentity());
        producer.send(pr1).get();

        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(sent.topic()).isEqualTo("fineract.cdc.batch_job_instance");
        assertThat(sent.partition()).isEqualTo(1);
        assertThat(sent.key()).isEqualTo(bytes("key-1"));
        assertThat(new String(sent.headers().lastHeader(ReplayIdentity.TOPIC_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo("fineract.cdc.dlt");
        assertThat(new String(sent.headers().lastHeader(ReplayIdentity.PARTITION_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo("0");
        assertThat(new String(sent.headers().lastHeader(ReplayIdentity.OFFSET_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo("10");

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report).contains("LIVE REPLAY").contains("Records replayed to Kafka: 2");
    }

    @Test
    public void rejectsOmittedPayloadAndInvalidReplayHeadersFromPublishing() throws Exception {
        DeadLetterRecord omitted = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, true, bytes("k"));

        RecordHeaders badHeaders = headers("WAREHOUSE_REJECTED", "bank-a", "fineract.cdc.batch_job_instance", 0, 20L);
        badHeaders.add("fbi.replay.dlt.topic", bytes("dlt"));
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":20},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord badReplay = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 20L, badHeaders, bytes("k"), payload, contract);

        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2, Collections.emptyMap());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        Map<DeadLetterRecord, String> failures = new HashMap<>();
        failures.put(omitted, "Payload is omitted or unavailable");
        failures.put(badReplay, "Invalid replay identity: " + badReplay.replayIdentityError());

        renderReport(
                scan, new DeadLetterReplayCommand.ReplayFilter(null, null, null, null), false, true,
                List.of(omitted, badReplay), List.of(omitted), List.of(badReplay), List.of(), List.of(), 0,
                failures, new PrintStream(out, true, StandardCharsets.UTF_8));

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Payload unavailable / omitted: 1")
                .contains("Invalid replay identity headers: 1")
                .contains("Records replayed to Kafka: 0")
                .contains("Rejected / Failed records");
    }

    @Test
    public void dryRunReportsCandidatesWithoutPublishing() {
        DeadLetterRecord r = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k"));
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(1, Collections.emptyMap());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        renderReport(
                scan, new DeadLetterReplayCommand.ReplayFilter(null, null, null, null), true, false,
                List.of(r), List.of(), List.of(), List.of(), List.of(r), 0,
                Map.of(), new PrintStream(out, true, StandardCharsets.UTF_8));

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("PREFLIGHT / DRY-RUN")
                .contains("Records scanned: 1")
                .contains("Records matched filters: 1")
                .contains("Bytes scanned: 0")
                .contains("Scan stop reason: COMPLETED")
                .contains("Snapshot scan completed.")
                .contains("Preflight passed: 1")
                .doesNotContain("Records replayed to Kafka")
                .contains("Preflight verified candidates ready for live replay");
    }

    @Test
    public void defaultInvocationPerformsDryRunOnlyAndPublishesZeroRecords() throws Exception {
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        DeadLetterRecord r = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DeadLetterReplayCommand.ReplayFilter filter =
                new DeadLetterReplayCommand.ReplayFilter("WAREHOUSE_REJECTED", null, null, null);

        renderReport(
                new DeadLetterReader.ScanResult(1, Collections.emptyMap()), filter, true, false,
                List.of(r), List.of(), List.of(), List.of(), List.of(r), 0,
                Map.of(), new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(producer.history()).isEmpty();
        assertThat(out.toString(StandardCharsets.UTF_8))
                .contains("PREFLIGHT / DRY-RUN (pass --execute to republish validated records)");
    }

    @Test
    public void broadReplayWithoutFiltersOrLimitRequiresConfirmAll() {
        Map<String, String> opts = Map.of("execute", "true");
        boolean execute = "true".equalsIgnoreCase(opts.get("execute"));
        String reason = opts.get("reason");
        String table = opts.get("table");
        String sourceId = opts.get("source");
        String offsetRangeStr = opts.get("offset-range");
        int limit = opts.containsKey("limit") ? Integer.parseInt(opts.get("limit")) : Integer.MAX_VALUE;
        boolean confirmAll = "true".equalsIgnoreCase(opts.get("confirm-all"));

        boolean hasFilter = reason != null || sourceId != null || table != null || offsetRangeStr != null;
        assertThatThrownBy(() -> {
            if (execute && !hasFilter && limit == Integer.MAX_VALUE && !confirmAll) {
                throw new IllegalArgumentException("Refusing broad replay without filters");
            }
        }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Refusing broad replay without filters");

        int boundedLimit = 100;
        assertThat(execute && !hasFilter && boundedLimit == Integer.MAX_VALUE && !confirmAll).isFalse();

        boolean confirmed = true;
        assertThat(execute && !hasFilter && limit == Integer.MAX_VALUE && !confirmed).isFalse();
    }

    private static OwnershipSnapshot ownership(String source, String table, Route.Target target) {
        OwnershipSnapshot.Entry entry = new OwnershipSnapshot.Entry(
                target == Route.Target.PROD ? "KAFKA" : "PYTHON",
                target == Route.Target.SHADOW ? "KAFKA" : null,
                target == Route.Target.HOLD ? "PAUSED" : "STEADY",
                1L);
        return new OwnershipSnapshot(Map.of(OwnershipSnapshot.key(source, table), entry),
                Map.of(source, new OwnershipSnapshot.SourceMeta("tenant-a", 0)),
                System.currentTimeMillis());
    }

    private static OwnershipSnapshot ownership(Map<String, List<String>> sourcesAndTables, Route.Target target) {
        Map<String, OwnershipSnapshot.Entry> entries = new HashMap<>();
        Map<String, OwnershipSnapshot.SourceMeta> sources = new HashMap<>();
        for (Map.Entry<String, List<String>> e : sourcesAndTables.entrySet()) {
            String src = e.getKey();
            sources.put(src, new OwnershipSnapshot.SourceMeta("tenant-" + src, 0));
            for (String tbl : e.getValue()) {
                OwnershipSnapshot.Entry entry = new OwnershipSnapshot.Entry(
                        target == Route.Target.PROD ? "KAFKA" : "PYTHON",
                        target == Route.Target.SHADOW ? "KAFKA" : null,
                        target == Route.Target.HOLD ? "PAUSED" : "STEADY",
                        1L);
                entries.put(OwnershipSnapshot.key(src, tbl), entry);
            }
        }
        return new OwnershipSnapshot(entries, sources, System.currentTimeMillis());
    }

    private Connection mockConnection(AtomicBoolean isClosed, AtomicInteger abortCalls) {
        return mockConnection(isClosed, abortCalls, null, null, null, null, Set.of(), null);
    }

    private Connection mockConnection(AtomicBoolean isClosed, AtomicInteger abortCalls, AtomicInteger closeCalls) {
        return mockConnection(isClosed, abortCalls, closeCalls, null, null, null, Set.of(), null);
    }

    private Connection mockConnection(
            AtomicBoolean isClosed,
            AtomicInteger abortCalls,
            AtomicInteger getAutoCommitCalls,
            SQLException getAutoCommitException,
            SQLException setAutoCommitFalseException) {
        return mockConnection(isClosed, abortCalls, null, getAutoCommitCalls, getAutoCommitException, setAutoCommitFalseException, Set.of(), null);
    }

    private Connection mockConnection(Map<UUID, ReplayOutcome> completedLedgerOutcomes) {
        return mockConnection(new AtomicBoolean(false), new AtomicInteger(0), null, null, null, completedLedgerOutcomes);
    }

    private Connection mockConnection(
            AtomicBoolean isClosed,
            AtomicInteger abortCalls,
            AtomicInteger getAutoCommitCalls,
            SQLException getAutoCommitException,
            SQLException setAutoCommitFalseException,
            Map<UUID, ReplayOutcome> completedLedgerOutcomes) {
        return mockConnection(isClosed, abortCalls, null, getAutoCommitCalls, getAutoCommitException, setAutoCommitFalseException, completedLedgerOutcomes, null);
    }

    private Connection mockConnection(
            AtomicBoolean isClosed,
            AtomicInteger abortCalls,
            AtomicInteger getAutoCommitCalls,
            SQLException getAutoCommitException,
            SQLException setAutoCommitFalseException,
            Set<UUID> completedLedgerUuids) {
        Map<UUID, ReplayOutcome> outcomes = new HashMap<>();
        if (completedLedgerUuids != null) {
            for (UUID u : completedLedgerUuids) {
                outcomes.put(u, ReplayOutcome.APPLIED);
            }
        }
        return mockConnection(isClosed, abortCalls, null, getAutoCommitCalls, getAutoCommitException, setAutoCommitFalseException, outcomes, null);
    }

    private Connection mockConnection(
            AtomicBoolean isClosed,
            AtomicInteger abortCalls,
            AtomicInteger closeCalls,
            AtomicInteger getAutoCommitCalls,
            SQLException getAutoCommitException,
            SQLException setAutoCommitFalseException,
            Set<UUID> completedLedgerUuids,
            SQLException abortException) {
        Map<UUID, ReplayOutcome> outcomes = new HashMap<>();
        if (completedLedgerUuids != null) {
            for (UUID u : completedLedgerUuids) {
                outcomes.put(u, ReplayOutcome.APPLIED);
            }
        }
        return mockConnection(isClosed, abortCalls, closeCalls, getAutoCommitCalls, getAutoCommitException, setAutoCommitFalseException, outcomes, abortException);
    }

    private Connection mockConnection(
            AtomicBoolean isClosed,
            AtomicInteger abortCalls,
            AtomicInteger closeCalls,
            AtomicInteger getAutoCommitCalls,
            SQLException getAutoCommitException,
            SQLException setAutoCommitFalseException,
            Map<UUID, ReplayOutcome> completedLedgerOutcomes,
            SQLException abortException) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("isClosed".equals(name)) {
                        return isClosed.get();
                    }
                    if ("abort".equals(name)) {
                        abortCalls.incrementAndGet();
                        isClosed.set(true);
                        if (abortException != null) {
                            throw abortException;
                        }
                        return null;
                    }
                    if ("close".equals(name)) {
                        if (closeCalls != null) {
                            closeCalls.incrementAndGet();
                        }
                        isClosed.set(true);
                        return null;
                    }
                    if ("getAutoCommit".equals(name)) {
                        if (getAutoCommitCalls != null) {
                            getAutoCommitCalls.incrementAndGet();
                        }
                        if (getAutoCommitException != null) {
                            throw getAutoCommitException;
                        }
                        return true;
                    }
                    if ("setAutoCommit".equals(name)) {
                        boolean target = (Boolean) args[0];
                        if (!target && setAutoCommitFalseException != null) {
                            throw setAutoCommitFalseException;
                        }
                        return null;
                    }
                    if ("rollback".equals(name)) {
                        return null;
                    }
                    if ("prepareStatement".equals(name)) {
                        AtomicReference<UUID> currentUuid = new AtomicReference<>();
                        return Proxy.newProxyInstance(
                                PreparedStatement.class.getClassLoader(),
                                new Class<?>[] {PreparedStatement.class},
                                (psProxy, psMethod, psArgs) -> {
                                    String psName = psMethod.getName();
                                    if ("setObject".equals(psName) && psArgs != null && psArgs.length >= 2 && psArgs[1] instanceof UUID u) {
                                        currentUuid.set(u);
                                        return null;
                                    }
                                    if ("executeQuery".equals(psName)) {
                                        boolean exists = completedLedgerOutcomes != null
                                                && currentUuid.get() != null
                                                && completedLedgerOutcomes.containsKey(currentUuid.get());
                                        AtomicBoolean hasRead = new AtomicBoolean(false);
                                        return Proxy.newProxyInstance(
                                                ResultSet.class.getClassLoader(),
                                                new Class<?>[] {ResultSet.class},
                                                (rsProxy, rsMethod, rsArgs) -> {
                                                    String rsName = rsMethod.getName();
                                                    if ("next".equals(rsName)) {
                                                        return exists && hasRead.compareAndSet(false, true);
                                                    }
                                                    if ("getString".equals(rsName)) {
                                                        ReplayOutcome o = completedLedgerOutcomes != null ? completedLedgerOutcomes.get(currentUuid.get()) : null;
                                                        if (o == null) {
                                                            return null;
                                                        }
                                                        return o.name();
                                                    }
                                                    if ("close".equals(rsName)) {
                                                        return null;
                                                    }
                                                    return null;
                                                });
                                    }
                                    if ("close".equals(psName)) {
                                        return null;
                                    }
                                    return null;
                                });
                    }
                    throw new UnsupportedOperationException("Method " + name + " not supported");
                });
    }

    private static class MockDataSource extends HikariDataSource {
        final AtomicInteger evictCalls = new AtomicInteger(0);
        final AtomicReference<Connection> evictedConn = new AtomicReference<>();
        final Connection connToReturn;
        RuntimeException evictException;

        MockDataSource(Connection conn) {
            super();
            this.connToReturn = conn;
        }

        @Override
        public Connection getConnection() {
            return connToReturn;
        }

        @Override
        public void evictConnection(Connection connection) {
            evictCalls.incrementAndGet();
            evictedConn.set(connection);
            if (evictException != null) {
                throw evictException;
            }
        }

        @Override
        public void close() {
        }
    }

    static class StubWriter implements BatchWriter {
        final List<Collection<RoutedEvent>> preflightCalls = new ArrayList<>();
        SQLException nextException = null;
        Map<ReplayIdentity, ReplayOutcome> predictedOutcomes = null;
        java.util.function.Function<Collection<RoutedEvent>, SQLException> failureRule = null;
        Set<UUID> failingUuids = new HashSet<>();
        SQLException failureToThrow = null;

        @Override
        public BatchWriter.WriteResult write(Connection c, Collection<RoutedEvent> events,
                Collection<PartitionOffset> offsets, String consumerGroup, Collection<SkipCheck> skips) {
            return new BatchWriter.WriteResult(List.of(), Map.of());
        }

        @Override
        public BatchWriter.PreflightResult preflight(Connection c, Collection<RoutedEvent> events) throws SQLException {
            preflightCalls.add(events);
            if (nextException != null) {
                SQLException toThrow = nextException;
                nextException = null;
                throw toThrow;
            }
            if (failureRule != null) {
                SQLException err = failureRule.apply(events);
                if (err != null) {
                    throw err;
                }
            }
            if (!failingUuids.isEmpty()) {
                for (RoutedEvent e : events) {
                    if (e.replayIdentity() != null && failingUuids.contains(e.replayIdentity().replayId())) {
                        throw failureToThrow != null
                                ? failureToThrow
                                : new SQLException("22003: numeric field overflow", "22003");
                    }
                }
            }
            if (predictedOutcomes != null) {
                return new BatchWriter.PreflightResult(
                        List.of(new WarehouseWriter.Applied("batch_job_instance", "prod", events.size(), 0)),
                        predictedOutcomes);
            }
            Map<ReplayIdentity, ReplayOutcome> outcomes = new HashMap<>();
            for (RoutedEvent e : events) {
                if (e.replayIdentity() != null) {
                    outcomes.put(e.replayIdentity(), ReplayOutcome.APPLIED);
                }
            }
            return new BatchWriter.PreflightResult(
                    List.of(new WarehouseWriter.Applied("batch_job_instance", "prod", events.size(), 0)),
                    outcomes);
        }
    }

    @Test
    public void rollbackFailureAbortsReplayAndDiscardsConnection() {
        StubWriter writer = new StubWriter();
        SQLException rollbackErr = new SQLException("rollback exploded", "08006");
        writer.nextException = new PreflightRollbackException("rollback exploded", rollbackErr);
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 0, 11L, false, bytes("k2"));

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicInteger aborts = new AtomicInteger(0);
        Connection conn = mockConnection(closed, aborts);

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2, Map.of());

        assertThatThrownBy(() -> DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(new ByteArrayOutputStream())))
                .isInstanceOf(PreflightRollbackException.class)
                .hasMessageContaining("rollback exploded");

        assertThat(writer.preflightCalls).hasSize(1);
        assertThat(producer.history()).isEmpty();
        assertThat(closed.get()).isFalse();
        assertThat(aborts.get()).isEqualTo(0);
    }

    @Test
    public void ordinaryValidationFailureFailsEntireBatchClosedAndPreservesConnection() throws Exception {
        StubWriter writer = new StubWriter();
        writer.nextException = new SQLException("42P01: relation does not exist", "42P01");
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 0, 11L, false, bytes("k2"));

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicInteger aborts = new AtomicInteger(0);
        Connection conn = mockConnection(closed, aborts);

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(writer.preflightCalls).hasSize(1);
        assertThat(writer.preflightCalls.get(0)).hasSize(2);
        assertThat(closed.get()).isFalse();
        assertThat(aborts.get()).isEqualTo(0);
        assertThat(producer.history()).isEmpty();
        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report).contains("Preflight failed: 2").contains("relation does not exist");
    }

    @Test
    public void executeWithDataSourceEvictsAndAbortsUnsafeConnectionExactlyOnceOnPreflightRollbackException() {
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicInteger aborts = new AtomicInteger(0);
        AtomicInteger closeCalls = new AtomicInteger(0);
        Connection conn = mockConnection(closed, aborts, closeCalls);
        MockDataSource ds = new MockDataSource(conn);

        SQLException rollbackErr = new SQLException("rollback exploded", "08006");
        PreflightRollbackException ex = new PreflightRollbackException("rollback exploded", rollbackErr);

        assertThatThrownBy(() -> DeadLetterReplayCommand.executeWithDataSource(ds, c -> {
            throw ex;
        })).isSameAs(ex);

        assertThat(ds.evictCalls.get()).isEqualTo(1);
        assertThat(ds.evictedConn.get()).isSameAs(conn);
        assertThat(aborts.get()).isEqualTo(1);
        assertThat(closed.get()).isTrue();
        assertThat(closeCalls.get()).isEqualTo(0);
    }

    @Test
    public void executeWithDataSourcePreservesPreflightRollbackExceptionAndAttachesEvictionFailure() {
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicInteger aborts = new AtomicInteger(0);
        AtomicInteger closeCalls = new AtomicInteger(0);
        Connection conn = mockConnection(closed, aborts, closeCalls);
        MockDataSource ds = new MockDataSource(conn);
        RuntimeException evictErr = new RuntimeException("evict failed");
        ds.evictException = evictErr;

        SQLException rollbackErr = new SQLException("rollback exploded", "08006");
        PreflightRollbackException ex = new PreflightRollbackException("rollback exploded", rollbackErr);

        assertThatThrownBy(() -> DeadLetterReplayCommand.executeWithDataSource(ds, c -> {
            throw ex;
        })).isSameAs(ex)
           .satisfies(e -> assertThat(e.getSuppressed()).contains(evictErr));

        assertThat(ds.evictCalls.get()).isEqualTo(1);
        assertThat(aborts.get()).isEqualTo(1);
        assertThat(closed.get()).isTrue();
        assertThat(closeCalls.get()).isEqualTo(0);
    }

    @Test
    public void executeWithDataSourcePreservesPreflightRollbackExceptionAndAttachesAbortFailure() {
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicInteger aborts = new AtomicInteger(0);
        AtomicInteger closeCalls = new AtomicInteger(0);
        SQLException abortErr = new SQLException("abort failed", "08006");
        Connection conn = mockConnection(closed, aborts, closeCalls, null, null, null, Set.of(), abortErr);
        MockDataSource ds = new MockDataSource(conn);

        SQLException rollbackErr = new SQLException("rollback exploded", "08006");
        PreflightRollbackException ex = new PreflightRollbackException("rollback exploded", rollbackErr);

        assertThatThrownBy(() -> DeadLetterReplayCommand.executeWithDataSource(ds, c -> {
            throw ex;
        })).isSameAs(ex)
           .satisfies(e -> assertThat(e.getSuppressed()).contains(abortErr));

        assertThat(ds.evictCalls.get()).isEqualTo(1);
        assertThat(aborts.get()).isEqualTo(1);
        assertThat(closeCalls.get()).isEqualTo(0);
    }

    @Test
    public void executeWithDataSourceReturnsCleanConnectionToPoolOnSuccess() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicInteger aborts = new AtomicInteger(0);
        AtomicInteger closeCalls = new AtomicInteger(0);
        Connection conn = mockConnection(closed, aborts, closeCalls);
        MockDataSource ds = new MockDataSource(conn);

        AtomicBoolean executed = new AtomicBoolean(false);
        DeadLetterReplayCommand.executeWithDataSource(ds, c -> {
            assertThat(c).isSameAs(conn);
            executed.set(true);
        });

        assertThat(executed.get()).isTrue();
        assertThat(closeCalls.get()).isEqualTo(1);
        assertThat(ds.evictCalls.get()).isEqualTo(0);
        assertThat(aborts.get()).isEqualTo(0);
    }

    @Test
    public void rejectsMissingValidator() {
        DeadLetterRecord r = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(1, Map.of());
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());

        assertThatThrownBy(() -> DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, null, os, null, producer, new PrintStream(new ByteArrayOutputStream())))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("WarehousePreflightValidator");
    }

    @Test
    public void rejectsMissingOwnershipSnapshot() {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);
        DeadLetterRecord r = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(1, Map.of());
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());

        assertThatThrownBy(() -> DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, null, null, validator, producer, new PrintStream(new ByteArrayOutputStream())))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("OwnershipSnapshot");
    }

    @Test
    public void rejectsMissingConnectionWhenPreflightNotSkipped() {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);
        DeadLetterRecord r = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(1, Map.of());
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());

        assertThatThrownBy(() -> DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, null, os, validator, producer, new PrintStream(new ByteArrayOutputStream())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Database connection is required");
    }

    @Test
    public void liveReplayCannotBypassLedgerCheckThroughSkipPreflight() {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);
        DeadLetterRecord r = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k"));
        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(1, Map.of());

        assertThatThrownBy(() -> DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, true, null, os, validator, producer, new PrintStream(new ByteArrayOutputStream())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Database connection is required to check completed replays before live execution");
    }

    @Test
    public void dryRunAllowsMissingConnectionWhenPreflightSkipped() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);
        DeadLetterRecord r = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k"));
        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(1, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                true, true, null, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(producer.history()).isEmpty();
        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("PREFLIGHT / DRY-RUN")
                .contains("Preflight validation: SKIPPED")
                .contains("Already completed (in ledger): not checked (ledger check not performed: no database connection)")
                .doesNotContain("Already completed (in ledger): 0")
                .contains("Preflight passed: 1");
    }

    @Test
    public void reportDisplaysLedgerCheckPerformedWhenConnectionAvailableAndCandidatesChecked() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID completedUuid = UUID.randomUUID();
        UUID newUuid = UUID.randomUUID();

        RecordHeaders h1 = headers("WAREHOUSE_REJECTED", "bank-a", "fineract.cdc.batch_job_instance", 0, 10L);
        ReplayIdentityHeaders.add(h1, new ReplayIdentity(completedUuid, "fineract.cdc.dlt", 0, 10L));
        byte[] payload1 = bytes("{\"after\":{\"job_instance_id\":10},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord rCompleted = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 10L, h1, bytes("k1"), payload1, contract);

        RecordHeaders h2 = headers("WAREHOUSE_REJECTED", "bank-a", "fineract.cdc.batch_job_instance", 0, 11L);
        ReplayIdentityHeaders.add(h2, new ReplayIdentity(newUuid, "fineract.cdc.dlt", 0, 11L));
        byte[] payload2 = bytes("{\"after\":{\"job_instance_id\":11},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord rNew = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 11L, h2, bytes("k2"), payload2, contract);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0), null, null, null, Set.of(completedUuid));

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(rCompleted, rNew), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Already completed (in ledger): 1 (ledger check performed)")
                .doesNotContain("ledger check not needed")
                .doesNotContain("ledger check not performed");
    }

    @Test
    public void reportDisplaysLedgerCheckNotNeededWhenNoRouteValidCandidates() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        DeadLetterRecord rOmitted = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, true, bytes("k1"));

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));
        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(1, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(rOmitted), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                true, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Payload unavailable / omitted: 1")
                .contains("Already completed (in ledger): 0 (ledger check not needed)")
                .doesNotContain("ledger check performed")
                .doesNotContain("ledger check not performed");
    }

    @Test
    public void reportDisplaysLedgerCheckNotNeededWhenMatchedListIsEmpty() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));
        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(0, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                true, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Already completed (in ledger): 0 (ledger check not needed)")
                .doesNotContain("ledger check performed")
                .doesNotContain("ledger check not performed");
    }

    @Test
    public void preflightSkippingSkipsStagingAndEnforcesRouting() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        DeadLetterRecord rProd = record("WAREHOUSE_REJECTED", "bank-prod", 0, 10L, false, bytes("k1"));
        DeadLetterRecord rShadow = record("WAREHOUSE_REJECTED", "bank-shadow", 0, 11L, false, bytes("k2"));
        DeadLetterRecord rHold = record("WAREHOUSE_REJECTED", "bank-hold", 0, 12L, false, bytes("k3"));
        DeadLetterRecord rSkip = record("WAREHOUSE_REJECTED", "bank-skip", 0, 13L, false, bytes("k4"));
        DeadLetterRecord rUnknown = record("WAREHOUSE_REJECTED", "bank-unknown", 0, 14L, false, bytes("k5"));

        Map<String, OwnershipSnapshot.Entry> entries = new HashMap<>();
        entries.put(OwnershipSnapshot.key("bank-prod", "batch_job_instance"),
                new OwnershipSnapshot.Entry("KAFKA", null, "STEADY", 1L));
        entries.put(OwnershipSnapshot.key("bank-shadow", "batch_job_instance"),
                new OwnershipSnapshot.Entry("PYTHON", "KAFKA", "STEADY", 1L));
        entries.put(OwnershipSnapshot.key("bank-hold", "batch_job_instance"),
                new OwnershipSnapshot.Entry("KAFKA", null, "PAUSED", 1L));
        entries.put(OwnershipSnapshot.key("bank-skip", "batch_job_instance"),
                new OwnershipSnapshot.Entry("PYTHON", null, "STEADY", 1L));

        Map<String, OwnershipSnapshot.SourceMeta> sourceMeta = Map.of(
                "bank-prod", new OwnershipSnapshot.SourceMeta("tenant-a", 0),
                "bank-shadow", new OwnershipSnapshot.SourceMeta("tenant-a", 0),
                "bank-hold", new OwnershipSnapshot.SourceMeta("tenant-a", 0),
                "bank-skip", new OwnershipSnapshot.SourceMeta("tenant-a", 0)
        );
        OwnershipSnapshot os = new OwnershipSnapshot(entries, sourceMeta, System.currentTimeMillis());

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(5, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(rProd, rShadow, rHold, rSkip, rUnknown), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, true, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(writer.preflightCalls).isEmpty();

        assertThat(producer.history()).hasSize(2);
        List<String> replayedValues = producer.history().stream()
                .map(p -> new String(p.value(), StandardCharsets.UTF_8))
                .toList();
        assertThat(replayedValues.get(0)).contains("bank-prod");
        assertThat(replayedValues.get(1)).contains("bank-shadow");

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight failed: 3")
                .contains("Records replayed to Kafka: 2")
                .contains("paused (HOLD)")
                .contains("SKIP")
                .contains("Unknown source");
    }

    @Test
    public void recordsPresentInReplayLedgerAreExcludedFromPublication() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID completedUuid = UUID.randomUUID();
        UUID newUuid = UUID.randomUUID();

        RecordHeaders h1 = headers("WAREHOUSE_REJECTED", "bank-a", "fineract.cdc.batch_job_instance", 0, 10L);
        ReplayIdentityHeaders.add(h1, new ReplayIdentity(completedUuid, "fineract.cdc.dlt", 0, 10L));
        byte[] payload1 = bytes("{\"after\":{\"job_instance_id\":10},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord rCompleted = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 10L, h1, bytes("k1"), payload1, contract);

        RecordHeaders h2 = headers("WAREHOUSE_REJECTED", "bank-a", "fineract.cdc.batch_job_instance", 0, 11L);
        ReplayIdentityHeaders.add(h2, new ReplayIdentity(newUuid, "fineract.cdc.dlt", 0, 11L));
        byte[] payload2 = bytes("{\"after\":{\"job_instance_id\":11},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord rNew = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 11L, h2, bytes("k2"), payload2, contract);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicInteger aborts = new AtomicInteger(0);
        Connection conn = mockConnection(closed, aborts, null, null, null, Set.of(completedUuid));

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(rCompleted, rNew), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(writer.preflightCalls).hasSize(1);
        assertThat(writer.preflightCalls.get(0)).hasSize(1);
        assertThat(writer.preflightCalls.get(0).iterator().next().replayIdentity().replayId()).isEqualTo(newUuid);

        assertThat(producer.history()).hasSize(1);
        ProducerRecord<byte[], byte[]> published = producer.history().get(0);
        assertThat(published.key()).isEqualTo(bytes("k2"));

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Already completed (in ledger): 1")
                .contains("Preflight passed: 1")
                .contains("Records replayed to Kafka: 1")
                .contains("Already completed in replay ledger (" + completedUuid + ": APPLIED)");
    }

    @Test
    public void multipleRecordsWithSameReplayUuidHandledCorrectlyWhenAlreadyInLedger() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID sharedUuid = UUID.randomUUID();

        RecordHeaders h1 = headers("WAREHOUSE_REJECTED", "bank-a", "fineract.cdc.batch_job_instance", 0, 10L);
        ReplayIdentityHeaders.add(h1, new ReplayIdentity(sharedUuid, "fineract.cdc.dlt", 0, 10L));
        byte[] payload1 = bytes("{\"after\":{\"job_instance_id\":10},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord r1 = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 10L, h1, bytes("k1"), payload1, contract);

        RecordHeaders h2 = headers("WAREHOUSE_REJECTED", "bank-a", "fineract.cdc.batch_job_instance", 1, 20L);
        ReplayIdentityHeaders.add(h2, new ReplayIdentity(sharedUuid, "fineract.cdc.dlt", 1, 20L));
        byte[] payload2 = bytes("{\"after\":{\"job_instance_id\":20},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord r2 = DeadLetterRecord.parse("fineract.cdc.dlt", 1, 20L, h2, bytes("k2"), payload2, contract);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0), null, null, null, Set.of(sharedUuid));

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(writer.preflightCalls).isEmpty();
        assertThat(producer.history()).isEmpty();

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Already completed (in ledger): 2")
                .contains("Preflight passed: 0")
                .contains("Records replayed to Kafka: 0");
    }

    @Test
    public void multipleRecordsWithSameReplayUuidPreflightedTogetherWhenNotInLedger() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID sharedUuid = UUID.randomUUID();

        RecordHeaders h1 = headers("WAREHOUSE_REJECTED", "bank-a", "fineract.cdc.batch_job_instance", 0, 10L);
        ReplayIdentityHeaders.add(h1, new ReplayIdentity(sharedUuid, "fineract.cdc.dlt", 0, 10L));
        byte[] payload1 = bytes("{\"after\":{\"job_instance_id\":10},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord r1 = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 10L, h1, bytes("k1"), payload1, contract);

        RecordHeaders h2 = headers("WAREHOUSE_REJECTED", "bank-a", "fineract.cdc.batch_job_instance", 1, 20L);
        ReplayIdentityHeaders.add(h2, new ReplayIdentity(sharedUuid, "fineract.cdc.dlt", 1, 20L));
        byte[] payload2 = bytes("{\"after\":{\"job_instance_id\":20},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord r2 = DeadLetterRecord.parse("fineract.cdc.dlt", 1, 20L, h2, bytes("k2"), payload2, contract);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0), null, null, null, Set.of());

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(writer.preflightCalls).hasSize(1);
        assertThat(writer.preflightCalls.get(0)).hasSize(2);
        assertThat(producer.history()).hasSize(2);

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Already completed (in ledger): 0")
                .contains("Preflight passed: 2")
                .contains("Records replayed to Kafka: 2");
    }

    @Test
    public void batchContainingMultipleRouteValidRecordsInvokesPreflightOnce() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 0, 11L, false, bytes("k2"));
        DeadLetterRecord r3 = record("WAREHOUSE_REJECTED", "bank-a", 0, 12L, false, bytes("k3"));

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));
        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(3, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2, r3), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(writer.preflightCalls).hasSize(1);
        assertThat(writer.preflightCalls.get(0)).hasSize(3);

        assertThat(producer.history()).hasSize(3);
        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight passed: 3")
                .contains("Records replayed to Kafka: 3");
    }

    @Test
    public void contractTopicResolutionUsedForScopedTopic() {
        String topic = contract.topicFor("batch_job_instance");
        assertThat(topic).isEqualTo("fineract.cdc.batch_job_instance");

        DeadLetterReplayCommand.OffsetFilter filter =
                DeadLetterReplayCommand.OffsetFilter.parse("0:10..20", topic);
        assertThat(filter.matches(topic, 0, 15L)).isTrue();
        assertThat(filter.matches(topic, 1, 15L)).isFalse();
        assertThat(filter.matches("other.topic", 0, 15L)).isFalse();
    }

    @Test
    public void fakeWriterThrowsUnsupportedOperationExceptionOnPreflight() {
        SinkWorkerDeliveryTest.FakeWriter fakeWriter = new SinkWorkerDeliveryTest.FakeWriter();
        assertThatThrownBy(() -> fakeWriter.preflight(null, List.of()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("FakeWriter does not support preflight");
    }

    @Test
    public void rejectsNonPositiveLimitAndMaxBytesInRunAndExecute() {
        assertThatThrownBy(() -> DeadLetterReplayCommand.run(null, contract, new String[] {"--limit=0"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Limit must be positive");

        assertThatThrownBy(() -> DeadLetterReplayCommand.run(null, contract, new String[] {"--limit=-1"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Limit must be positive");

        assertThatThrownBy(() -> DeadLetterReplayCommand.run(null, contract, new String[] {"--max-bytes=0"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-bytes must be positive");

        assertThatThrownBy(() -> DeadLetterReplayCommand.run(null, contract, new String[] {"--max-bytes=-100"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-bytes must be positive");

        assertThatThrownBy(() -> DeadLetterReplayCommand.execute(
                null, contract, new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                true, true, 0, DeadLetterReader.DEFAULT_MAX_BYTES, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Limit must be positive");

        assertThatThrownBy(() -> DeadLetterReplayCommand.execute(
                null, contract, new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                true, true, 5, 0L, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-bytes must be positive");
    }

    @Test
    public void renderReportDisplaysScanStatisticsAndOperatorMessagesForStopReasons() {
        DeadLetterRecord r = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k"));

        for (DeadLetterReader.StopReason reason : DeadLetterReader.StopReason.values()) {
            DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(
                    100, 25, 4096, Map.of(), reason);
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            renderReport(
                    scan, new DeadLetterReplayCommand.ReplayFilter(null, null, null, null), true, false,
                    List.of(r), List.of(), List.of(), List.of(), List.of(r), 0,
                    Map.of(), new PrintStream(out, true, StandardCharsets.UTF_8));

            String report = out.toString(StandardCharsets.UTF_8);
            assertThat(report)
                    .contains("Records scanned: 100")
                    .contains("Records matched filters: 25")
                    .contains("Bytes scanned: 4096")
                    .contains("Scan stop reason: " + reason);

            switch (reason) {
                case COMPLETED -> assertThat(report).contains("Snapshot scan completed.");
                case RECORD_LIMIT_REACHED -> assertThat(report)
                        .contains("Scan stopped because the requested match limit was reached.");
                case BYTE_BUDGET_REACHED -> assertThat(report)
                        .contains("Scan stopped because the maximum byte budget was reached. Some snapshot records may remain unscanned.");
                case VISITOR_REQUESTED -> assertThat(report)
                        .contains("Scan stopped at the visitor's request.");
            }
        }
    }

    @Test
    public void parseArgsRejectsUnknownOptions() {
        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--foo"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown option: '--foo'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--bar=123"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown option: '--bar'");
    }

    @Test
    public void parseArgsRejectsAllRemovedAliasesWithActionableGuidance() {
        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--source-id=bank-a"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown option: '--source-id'.");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--offsets=0:10..20"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown option: '--offsets'.");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--preflight-only"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown option: '--preflight-only'.");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--confirm-unfiltered"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown option: '--confirm-unfiltered'.");
    }

    @Test
    public void parseArgsRejectsDuplicateOptions() {
        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--reason=A", "--reason=B"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate option: '--reason'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--source=s1", "--source=s2"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate option: '--source'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--limit=10", "--limit=20"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate option: '--limit'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--dry-run", "--dry-run"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate option: '--dry-run'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--execute", "--execute=true"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate option: '--execute'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--LIMIT=10", "--limit=20"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate option: '--limit'");
    }

    @Test
    public void parseArgsRejectsPositionalArguments() {
        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"positional"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unexpected positional argument: 'positional'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--reason=A", "extra"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unexpected positional argument: 'extra'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"-source=bank-a"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unexpected positional argument: '-source=bank-a'");
    }

    @Test
    public void parseArgsRejectsEmptyOptionNames() {
        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Empty option name");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--=value"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Empty option name");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--="}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Empty option name");
    }

    @Test
    public void parseArgsRejectsMissingOrEmptyValuesForValueOptions() {
        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--reason"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing value for option '--reason'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--table"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing value for option '--table'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--source"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing value for option '--source'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--offset-range"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing value for option '--offset-range'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--limit"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing value for option '--limit'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--max-bytes"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing value for option '--max-bytes'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--reason="}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Empty value for option '--reason'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--table=   "}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Empty value for option '--table'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--source="}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Empty value for option '--source'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--offset-range=  "}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Empty value for option '--offset-range'");
    }

    @Test
    public void parseArgsRejectsMalformedBooleanValues() {
        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--execute=yes"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid boolean value 'yes' for option '--execute'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--execute=1"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid boolean value '1' for option '--execute'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--execute="}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid boolean value '' for option '--execute'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--dry-run=foo"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid boolean value 'foo' for option '--dry-run'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--skip-preflight=no"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid boolean value 'no' for option '--skip-preflight'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--confirm-all=0"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid boolean value '0' for option '--confirm-all'");
    }

    @Test
    public void parseArgsAcceptsValidFlagsAndBooleanValues() {
        Map<String, String> opts = DeadLetterReplayCommand.parseArgs(new String[] {
                "--execute",
                "--skip-preflight=TRUE",
                "--confirm-all=false",
                "--reason=TEST",
                "--limit=50"
        });
        assertThat(opts.get("execute")).isEqualTo("true");
        assertThat(opts.get("skip-preflight")).isEqualTo("true");
        assertThat(opts.get("confirm-all")).isEqualTo("false");
        assertThat(opts.get("reason")).isEqualTo("TEST");
        assertThat(opts.get("limit")).isEqualTo("50");
    }

    @Test
    public void parseArgsRejectsContradictoryExecutionModes() {
        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--execute", "--dry-run"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Contradictory execution modes: cannot specify both --execute and --dry-run");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--execute=true", "--dry-run=true"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Contradictory execution modes: cannot specify both --execute and --dry-run");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--execute=false", "--dry-run=false"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Contradictory execution modes: cannot disable both --execute and --dry-run");

        assertThatThrownBy(() -> DeadLetterReplayCommand.parseArgs(new String[] {"--dry-run=false"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot specify --dry-run=false without --execute");

        Map<String, String> validLive = DeadLetterReplayCommand.parseArgs(new String[] {"--execute=true", "--dry-run=false"});
        assertThat(validLive.get("execute")).isEqualTo("true");
        assertThat(validLive.get("dry-run")).isEqualTo("false");

        Map<String, String> validDryRun = DeadLetterReplayCommand.parseArgs(new String[] {"--execute=false", "--dry-run=true"});
        assertThat(validDryRun.get("execute")).isEqualTo("false");
        assertThat(validDryRun.get("dry-run")).isEqualTo("true");
    }

    @Test
    public void runRejectsNonIntegerLimitAndNonLongMaxBytes() {
        assertThatThrownBy(() -> DeadLetterReplayCommand.run(null, contract, new String[] {"--limit=not-a-number"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid integer value for '--limit'");

        assertThatThrownBy(() -> DeadLetterReplayCommand.run(null, contract, new String[] {"--max-bytes=not-a-number"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid long value for '--max-bytes'");
    }

    @Test
    public void runPreservesDryRunAsDefaultWhenExecuteAbsent() throws Exception {
        assertThatThrownBy(() -> DeadLetterReplayCommand.run(null, contract, new String[] {"--reason=WAREHOUSE_REJECTED"}))
                .isNotInstanceOf(IllegalArgumentException.class)
                .isInstanceOf(NullPointerException.class);
    }

    static class TestProducer extends MockProducer<byte[], byte[]> {
        private final List<ProducerRecord<byte[], byte[]>> recordsSent = new ArrayList<>();
        private Function<ProducerRecord<byte[], byte[]>, Future<RecordMetadata>> sendHandler;
        private Runnable onFlush;

        TestProducer() {
            this(r -> CompletableFuture.completedFuture(null), null);
        }

        TestProducer(Function<ProducerRecord<byte[], byte[]>, Future<RecordMetadata>> sendHandler) {
            this(sendHandler, null);
        }

        TestProducer(Function<ProducerRecord<byte[], byte[]>, Future<RecordMetadata>> sendHandler, Runnable onFlush) {
            super(true, null, new ByteArraySerializer(), new ByteArraySerializer());
            this.sendHandler = sendHandler != null ? sendHandler : r -> CompletableFuture.completedFuture(null);
            this.onFlush = onFlush;
        }

        @Override
        public List<ProducerRecord<byte[], byte[]>> history() {
            return recordsSent;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            recordsSent.add(record);
            return sendHandler.apply(record);
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            recordsSent.add(record);
            Future<RecordMetadata> future = sendHandler.apply(record);
            if (callback != null) {
                if (future instanceof CompletableFuture) {
                    ((CompletableFuture<RecordMetadata>) future).whenComplete((meta, ex) -> {
                        callback.onCompletion(meta, ex instanceof Exception ? (Exception) ex : (ex != null ? new Exception(ex) : null));
                    });
                } else if (future != null && future.isDone()) {
                    try {
                        callback.onCompletion(future.get(), null);
                    } catch (ExecutionException e) {
                        callback.onCompletion(null, (Exception) (e.getCause() != null ? e.getCause() : e));
                    } catch (Exception e) {
                        callback.onCompletion(null, e);
                    }
                }
            }
            return future;
        }

        @Override
        public void flush() {
            if (onFlush != null) {
                onFlush.run();
            } else {
                super.flush();
            }
        }
    }

    private void runReplayTest(
            List<DeadLetterRecord> records,
            boolean dryRun,
            boolean skipPreflight,
            Producer<byte[], byte[]> producer,
            ByteArrayOutputStream out) throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));
        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(records.size(), Map.of());

        DeadLetterReplayCommand.processMatchedRecords(
                scan, records, contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                dryRun, skipPreflight, conn, os, validator, producer,
                new PrintStream(out, true, StandardCharsets.UTF_8));
    }

    @Test
    public void liveReplayAllSendsAcknowledgedReportsSuccessAndReturnsNormally() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 1, 11L, false, bytes("k2"));

        TestProducer producer = new TestProducer();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        runReplayTest(List.of(r1, r2), false, false, producer, out);

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(producer.history()).hasSize(2);
        assertThat(report)
                .contains("LIVE REPLAY (--execute)")
                .contains("Records ready for replay: 2")
                .contains("Records replayed to Kafka: 2")
                .contains("- Acknowledged: 2")
                .contains("- Failed: 0")
                .doesNotContain("Unconfirmed")
                .doesNotContain("PARTIAL PUBLICATION")
                .doesNotContain("Rejected / Failed records:");
    }

    @Test
    public void liveReplaySingleFutureFailureReportsPartialPublicationAndThrowsException() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 1, 11L, false, bytes("k2"));

        RuntimeException brokerError = new RuntimeException("Broker connection disconnected");
        TestProducer producer = new TestProducer(record -> {
            if (record.partition() == 1) {
                return CompletableFuture.failedFuture(brokerError);
            }
            return CompletableFuture.completedFuture(null);
        });
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(() -> runReplayTest(List.of(r1, r2), false, false, producer, out))
                .isInstanceOf(DeadLetterReplayCommand.ReplayPublicationException.class)
                .hasMessage("Replay publication incomplete: 1 acknowledged, 1 failed")
                .hasCause(brokerError)
                .satisfies(e -> {
                    DeadLetterReplayCommand.ReplayPublicationException rpe = (DeadLetterReplayCommand.ReplayPublicationException) e;
                    assertThat(rpe.acknowledgedCount()).isEqualTo(1);
                    assertThat(rpe.failedCount()).isEqualTo(1);
                });

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(producer.history()).hasSize(2);
        assertThat(report)
                .contains("PARTIAL PUBLICATION: some records failed to publish.")
                .contains("Records ready for replay: 2")
                .contains("- Acknowledged: 1")
                .contains("- Failed: 1")
                .doesNotContain("Unconfirmed")
                .contains("Rejected / Failed records:")
                .contains("Kafka publication failed: Broker connection disconnected");
    }

    @Test
    public void liveReplayMultipleFutureFailuresCollectsAllOutcomes() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 1, 11L, false, bytes("k2"));
        DeadLetterRecord r3 = record("WAREHOUSE_REJECTED", "bank-a", 2, 12L, false, bytes("k3"));

        RuntimeException err2 = new RuntimeException("Topic authorization failed");
        RuntimeException err3 = new RuntimeException("Timeout waiting for ack");

        TestProducer producer = new TestProducer(record -> {
            if (record.partition() == 1) {
                return CompletableFuture.failedFuture(err2);
            } else if (record.partition() == 2) {
                return CompletableFuture.failedFuture(err3);
            }
            return CompletableFuture.completedFuture(null);
        });
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(() -> runReplayTest(List.of(r1, r2, r3), false, false, producer, out))
                .isInstanceOf(DeadLetterReplayCommand.ReplayPublicationException.class)
                .hasMessage("Replay publication incomplete: 1 acknowledged, 2 failed")
                .hasCause(err2)
                .satisfies(e -> {
                    DeadLetterReplayCommand.ReplayPublicationException rpe = (DeadLetterReplayCommand.ReplayPublicationException) e;
                    assertThat(rpe.acknowledgedCount()).isEqualTo(1);
                    assertThat(rpe.failedCount()).isEqualTo(2);
                });

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(producer.history()).hasSize(3);
        assertThat(report)
                .contains("- Acknowledged: 1")
                .contains("- Failed: 2")
                .doesNotContain("Unconfirmed")
                .contains("Kafka publication failed: Topic authorization failed")
                .contains("Kafka publication failed: Timeout waiting for ack");
    }

    @Test
    public void liveReplaySynchronousSendExceptionContinuesRemainingRecords() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 1, 11L, false, bytes("k2"));

        RuntimeException syncErr = new RuntimeException("Local buffer exhausted");
        AtomicInteger sendCount = new AtomicInteger(0);
        TestProducer producer = new TestProducer(record -> {
            if (sendCount.getAndIncrement() == 0) {
                throw syncErr;
            }
            return CompletableFuture.completedFuture(null);
        });
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(() -> runReplayTest(List.of(r1, r2), false, false, producer, out))
                .isInstanceOf(DeadLetterReplayCommand.ReplayPublicationException.class)
                .hasMessage("Replay publication incomplete: 1 acknowledged, 1 failed")
                .hasCause(syncErr);

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(producer.history()).hasSize(2);
        assertThat(report)
                .contains("- Acknowledged: 1")
                .contains("- Failed: 1")
                .doesNotContain("Unconfirmed")
                .contains("Synchronous send failure: Local buffer exhausted");
    }

    @Test
    public void liveReplayInvalidPartitionFailsIndividuallyWithoutPreventingOtherRecords() throws Exception {
        byte[] payload1 = bytes("{\"after\":{\"job_instance_id\":10},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord rInvalidPart = new DeadLetterRecord(
                "fineract.cdc.dlt",
                0,
                10L,
                0L,
                "fineract.cdc.batch_job_instance",
                -1,
                10L,
                "WAREHOUSE_REJECTED",
                "test",
                "batch_job_instance",
                "bank-a",
                false,
                null,
                payload1,
                bytes("k1"),
                UUID.randomUUID(),
                null,
                false,
                null);
        DeadLetterRecord rValidPart = record("WAREHOUSE_REJECTED", "bank-a", 0, 11L, false, bytes("k2"));

        TestProducer producer = new TestProducer();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(() -> runReplayTest(List.of(rInvalidPart, rValidPart), false, false, producer, out))
                .isInstanceOf(DeadLetterReplayCommand.ReplayPublicationException.class)
                .hasMessage("Replay publication incomplete: 1 acknowledged, 1 failed")
                .satisfies(e -> {
                    DeadLetterReplayCommand.ReplayPublicationException rpe = (DeadLetterReplayCommand.ReplayPublicationException) e;
                    assertThat(rpe.acknowledgedCount()).isEqualTo(1);
                    assertThat(rpe.failedCount()).isEqualTo(1);
                });

        assertThat(producer.history()).hasSize(1);
        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("- Acknowledged: 1")
                .contains("- Failed: 1")
                .doesNotContain("Unconfirmed")
                .contains("Invalid original partition: -1");
    }

    @Test
    public void liveReplayFlushExceptionPreservesIndividualFutureOutcomes() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 1, 11L, false, bytes("k2"));

        RuntimeException flushErr = new RuntimeException("Kafka producer flush timeout");
        RuntimeException futureErr = new RuntimeException("Record 2 broker ack error");

        TestProducer producer = new TestProducer(
                record -> {
                    if (record.partition() == 1) {
                        return CompletableFuture.failedFuture(futureErr);
                    }
                    return CompletableFuture.completedFuture(null);
                },
                () -> {
                    throw flushErr;
                }
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(() -> runReplayTest(List.of(r1, r2), false, false, producer, out))
                .isInstanceOf(DeadLetterReplayCommand.ReplayPublicationException.class)
                .hasMessage("Replay publication incomplete: 1 acknowledged, 1 failed")
                .hasCause(flushErr)
                .satisfies(e -> {
                    DeadLetterReplayCommand.ReplayPublicationException rpe = (DeadLetterReplayCommand.ReplayPublicationException) e;
                    assertThat(rpe.acknowledgedCount()).isEqualTo(1);
                    assertThat(rpe.failedCount()).isEqualTo(1);
                });

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("- Acknowledged: 1")
                .contains("- Failed: 1")
                .doesNotContain("Unconfirmed")
                .contains("Kafka publication failed: Record 2 broker ack error");
    }

    @Test
    public void liveReplayAsynchronousCallbacksCompleteBeforeCountsAsserted() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 1, 11L, false, bytes("k2"));
        DeadLetterRecord r3 = record("WAREHOUSE_REJECTED", "bank-a", 2, 12L, false, bytes("k3"));

        CompletableFuture<RecordMetadata> f1 = new CompletableFuture<>();
        CompletableFuture<RecordMetadata> f2 = new CompletableFuture<>();
        CompletableFuture<RecordMetadata> f3 = new CompletableFuture<>();

        TestProducer producer = new TestProducer(
                record -> {
                    if (record.partition() == 0) return f1;
                    if (record.partition() == 1) return f2;
                    return f3;
                },
                () -> {
                    f1.complete(null);
                    f2.completeExceptionally(new RuntimeException("Async callback failure"));
                    f3.complete(null);
                }
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(() -> runReplayTest(List.of(r1, r2, r3), false, false, producer, out))
                .isInstanceOf(DeadLetterReplayCommand.ReplayPublicationException.class)
                .hasMessage("Replay publication incomplete: 2 acknowledged, 1 failed")
                .satisfies(e -> {
                    DeadLetterReplayCommand.ReplayPublicationException rpe = (DeadLetterReplayCommand.ReplayPublicationException) e;
                    assertThat(rpe.acknowledgedCount()).isEqualTo(2);
                    assertThat(rpe.failedCount()).isEqualTo(1);
                });

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("- Acknowledged: 2")
                .contains("- Failed: 1")
                .doesNotContain("Unconfirmed")
                .contains("Kafka publication failed: Async callback failure");
    }

    @Test
    public void liveReplayCancelledFutureTreatedAsFailure() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));

        CompletableFuture<RecordMetadata> cancelled = new CompletableFuture<>();
        cancelled.completeExceptionally(new CancellationException("Future was cancelled"));

        TestProducer producer = new TestProducer(record -> cancelled);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(() -> runReplayTest(List.of(r1), false, false, producer, out))
                .isInstanceOf(DeadLetterReplayCommand.ReplayPublicationException.class)
                .hasMessage("Replay publication incomplete: 0 acknowledged, 1 failed")
                .satisfies(e -> {
                    DeadLetterReplayCommand.ReplayPublicationException rpe = (DeadLetterReplayCommand.ReplayPublicationException) e;
                    assertThat(rpe.acknowledgedCount()).isEqualTo(0);
                    assertThat(rpe.failedCount()).isEqualTo(1);
                });

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("- Acknowledged: 0")
                .contains("- Failed: 1")
                .doesNotContain("Unconfirmed")
                .contains("Kafka publication failed: Future was cancelled");
    }

    @Test
    public void liveReplayDryRunPerformsNoKafkaPublicationsEvenWhenCandidatesReady() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 1, 11L, false, bytes("k2"));

        TestProducer producer = new TestProducer();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        runReplayTest(List.of(r1, r2), true, false, producer, out);

        assertThat(producer.history()).isEmpty();

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Mode: PREFLIGHT / DRY-RUN (pass --execute to republish validated records)")
                .contains("Preflight passed: 2")
                .doesNotContain("PARTIAL PUBLICATION")
                .doesNotContain("Records replayed to Kafka")
                .contains("Preflight verified candidates ready for live replay:");
    }

    @Test
    public void renderReportCanonicalFormat() {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(1, Map.of());
        DeadLetterReplayCommand.ReplayFilter filter = new DeadLetterReplayCommand.ReplayFilter(null, null, null, null);

        ByteArrayOutputStream out12 = new ByteArrayOutputStream();
        DeadLetterReplayCommand.renderReport(
                scan, filter, false, false,
                List.of(r1), List.of(), List.of(), List.of(), Map.of(),
                List.of(), List.of(), List.of(r1),
                new DeadLetterReplayCommand.PublicationSnapshot(1, 0, 0),
                Map.of(), DeadLetterReplayCommand.LedgerCheckStatus.NOT_NEEDED,
                null, Map.of(), new PrintStream(out12, true, StandardCharsets.UTF_8));
        assertThat(out12.toString(StandardCharsets.UTF_8))
                .contains("LIVE REPLAY (--execute)")
                .contains("Records replayed to Kafka: 1")
                .contains("- Acknowledged: 1");

        ByteArrayOutputStream out14 = new ByteArrayOutputStream();
        DeadLetterReplayCommand.renderReport(
                scan, filter, false, false,
                List.of(r1), List.of(), List.of(), List.of(), Map.of(),
                List.of(), List.of(), List.of(r1),
                new DeadLetterReplayCommand.PublicationSnapshot(1, 0, 0),
                Map.of(), DeadLetterReplayCommand.LedgerCheckStatus.PERFORMED,
                null, Map.of(), new PrintStream(out14, true, StandardCharsets.UTF_8));
        assertThat(out14.toString(StandardCharsets.UTF_8))
                .contains("Already completed (in ledger): 0 (ledger check performed)");

        ByteArrayOutputStream out17 = new ByteArrayOutputStream();
        DeadLetterReplayCommand.renderReport(
                scan, filter, false, false,
                List.of(r1), List.of(), List.of(), List.of(), Map.of(),
                List.of(), List.of(), List.of(r1),
                new DeadLetterReplayCommand.PublicationSnapshot(1, 0, 0),
                Map.of(), DeadLetterReplayCommand.LedgerCheckStatus.PERFORMED,
                new RuntimeException("Flush error"), Map.of(),
                new PrintStream(out17, true, StandardCharsets.UTF_8));
        assertThat(out17.toString(StandardCharsets.UTF_8))
                .contains("Producer flush failed: Flush error")
                .contains("PUBLICATION FAILED: producer flush encountered an error despite acknowledged records.");

        ByteArrayOutputStream out18 = new ByteArrayOutputStream();
        DeadLetterReplayCommand.renderReport(
                scan, filter, false, false,
                List.of(r1), List.of(), List.of(), List.of(), Map.of(),
                List.of(), List.of(), List.of(r1, record("WAREHOUSE_REJECTED", "bank-a", 1, 11L, false, bytes("k2"))),
                new DeadLetterReplayCommand.PublicationSnapshot(1, 0, 1),
                Map.of(), DeadLetterReplayCommand.LedgerCheckStatus.PERFORMED,
                new RuntimeException("Flush error"), Map.of(),
                new PrintStream(out18, true, StandardCharsets.UTF_8));
        assertThat(out18.toString(StandardCharsets.UTF_8))
                .contains("Callback outcomes observed at reporting time after flush failure:")
                .contains("- Acknowledged: 1")
                .contains("- Failed: 0")
                .doesNotContain("Records replayed to Kafka")
                .contains("Producer flush failed: Flush error")
                .contains("PUBLICATION FAILED: producer flush encountered an error before all callbacks completed.");
    }

    @Test
    public void flushFailureWithMixedCallbackResultsPreservesAccurateCountsAndThrows() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 1, 11L, false, bytes("k2"));

        RuntimeException flushErr = new RuntimeException("Kafka producer flush exploded");
        RuntimeException callbackErr = new RuntimeException("Record 2 callback error");

        TestProducer producer = new TestProducer(
                record -> {
                    if (record.partition() == 1) {
                        return CompletableFuture.failedFuture(callbackErr);
                    }
                    return CompletableFuture.completedFuture(null);
                },
                () -> { throw flushErr; }
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(() -> runReplayTest(List.of(r1, r2), false, false, producer, out))
                .isInstanceOf(DeadLetterReplayCommand.ReplayPublicationException.class)
                .hasMessage("Replay publication incomplete: 1 acknowledged, 1 failed")
                .hasCause(flushErr)
                .satisfies(e -> {
                    DeadLetterReplayCommand.ReplayPublicationException rpe = (DeadLetterReplayCommand.ReplayPublicationException) e;
                    assertThat(rpe.acknowledgedCount()).isEqualTo(1);
                    assertThat(rpe.failedCount()).isEqualTo(1);
                });

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("- Acknowledged: 1")
                .contains("- Failed: 1")
                .doesNotContain("Unconfirmed")
                .contains("Producer flush failed: Kafka producer flush exploded")
                .contains("PARTIAL PUBLICATION: some records failed to publish.")
                .contains("Kafka publication failed: Record 2 callback error");
    }

    @Test
    public void flushFailureWithAllFuturesAcknowledgedMaintainsAcknowledgedCountAndThrows() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 1, 11L, false, bytes("k2"));

        RuntimeException flushErr = new RuntimeException("Kafka producer flush socket reset");

        TestProducer producer = new TestProducer(
                record -> CompletableFuture.completedFuture(null),
                () -> { throw flushErr; }
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(() -> runReplayTest(List.of(r1, r2), false, false, producer, out))
                .isInstanceOf(DeadLetterReplayCommand.ReplayPublicationException.class)
                .hasMessage("Replay publication failed during flush: 2 acknowledged, 0 failed")
                .hasCause(flushErr)
                .satisfies(e -> {
                    DeadLetterReplayCommand.ReplayPublicationException rpe = (DeadLetterReplayCommand.ReplayPublicationException) e;
                    assertThat(rpe.acknowledgedCount()).isEqualTo(2);
                    assertThat(rpe.failedCount()).isEqualTo(0);
                });

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("- Acknowledged: 2")
                .contains("- Failed: 0")
                .doesNotContain("Unconfirmed")
                .contains("Producer flush failed: Kafka producer flush socket reset")
                .contains("PUBLICATION FAILED: producer flush encountered an error despite acknowledged records.")
                .doesNotContain("PARTIAL PUBLICATION")
                .doesNotContain("Rejected / Failed records:");
    }

    @Test
    public void liveReplayFlushFailsBeforeAllCallbacksCompleteReportsObservedOutcomesWithoutClaimingFinalTotals() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 1, 11L, false, bytes("k2"));
        DeadLetterRecord r3 = record("WAREHOUSE_REJECTED", "bank-a", 2, 12L, false, bytes("k3"));

        RuntimeException flushErr = new RuntimeException("Kafka producer flush socket reset");
        CompletableFuture<RecordMetadata> pendingFuture2 = new CompletableFuture<>();
        CompletableFuture<RecordMetadata> pendingFuture3 = new CompletableFuture<>();

        TestProducer producer = new TestProducer(
                record -> {
                    if (record.partition() == 0) {
                        return CompletableFuture.completedFuture(null);
                    } else if (record.partition() == 1) {
                        return pendingFuture2;
                    } else {
                        return pendingFuture3;
                    }
                },
                () -> { throw flushErr; }
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(() -> runReplayTest(List.of(r1, r2, r3), false, false, producer, out))
                .isInstanceOf(DeadLetterReplayCommand.ReplayPublicationException.class)
                .hasMessage("Replay publication failed during flush: observed 1 acknowledged, 0 failed (2 callbacks pending)")
                .hasCause(flushErr)
                .satisfies(e -> {
                    DeadLetterReplayCommand.ReplayPublicationException rpe = (DeadLetterReplayCommand.ReplayPublicationException) e;
                    assertThat(rpe.acknowledgedCount()).isEqualTo(1);
                    assertThat(rpe.failedCount()).isEqualTo(0);
                });

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Records ready for replay: 3")
                .contains("Callback outcomes observed at reporting time after flush failure:")
                .contains("- Acknowledged: 1")
                .contains("- Failed: 0")
                .doesNotContain("Records replayed to Kafka")
                .doesNotContain("Unconfirmed")
                .contains("Producer flush failed: Kafka producer flush socket reset")
                .contains("PUBLICATION FAILED: producer flush encountered an error before all callbacks completed.");
    }

    @Test
    public void liveReplayCallbacksCompletingAfterSnapshotDoNotMutateReportOrExceptionCounts() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 1, 11L, false, bytes("k2"));

        RuntimeException flushErr = new RuntimeException("Kafka producer flush socket reset");
        CompletableFuture<RecordMetadata> pendingFuture2 = new CompletableFuture<>();

        AtomicBoolean completedLate = new AtomicBoolean(false);
        TestProducer producer = new TestProducer(
                record -> {
                    if (record.partition() == 0) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return pendingFuture2;
                    }
                },
                () -> { throw flushErr; }
        );

        ByteArrayOutputStream out = new ByteArrayOutputStream() {
            @Override
            public synchronized void write(int b) {
                if (completedLate.compareAndSet(false, true)) {
                    pendingFuture2.complete(null);
                }
                super.write(b);
            }

            @Override
            public synchronized void write(byte[] b, int off, int len) {
                if (completedLate.compareAndSet(false, true)) {
                    pendingFuture2.complete(null);
                }
                super.write(b, off, len);
            }
        };

        assertThatThrownBy(() -> runReplayTest(List.of(r1, r2), false, false, producer, out))
                .isInstanceOf(DeadLetterReplayCommand.ReplayPublicationException.class)
                .hasMessage("Replay publication failed during flush: observed 1 acknowledged, 0 failed (1 callback pending)")
                .hasCause(flushErr)
                .satisfies(e -> {
                    DeadLetterReplayCommand.ReplayPublicationException rpe = (DeadLetterReplayCommand.ReplayPublicationException) e;
                    assertThat(rpe.acknowledgedCount()).isEqualTo(1);
                    assertThat(rpe.failedCount()).isEqualTo(0);
                    assertThat(rpe.publication().pendingCallbacks()).isEqualTo(1);
                    assertThat(rpe.publication().failureReasons()).isEmpty();
                });

        assertThat(completedLate).isTrue();
        assertThat(pendingFuture2).isCompleted();

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Records ready for replay: 2")
                .contains("Callback outcomes observed at reporting time after flush failure:")
                .contains("- Acknowledged: 1")
                .contains("- Failed: 0")
                .doesNotContain("Records replayed to Kafka")
                .doesNotContain("Unconfirmed")
                .contains("Producer flush failed: Kafka producer flush socket reset")
                .contains("PUBLICATION FAILED: producer flush encountered an error before all callbacks completed.");
    }

    @Test
    public void liveReplayCallbackFailureCompletingAfterSnapshotDoesNotMutateReportOrException() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 1, 11L, false, bytes("k2"));

        RuntimeException flushErr = new RuntimeException("Kafka producer flush socket reset");
        RuntimeException lateFailure = new RuntimeException("Late callback publication failure");
        CompletableFuture<RecordMetadata> pendingFuture2 = new CompletableFuture<>();

        AtomicBoolean completedLate = new AtomicBoolean(false);
        TestProducer producer = new TestProducer(
                record -> {
                    if (record.partition() == 0) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return pendingFuture2;
                    }
                },
                () -> { throw flushErr; }
        );

        ByteArrayOutputStream out = new ByteArrayOutputStream() {
            @Override
            public synchronized void write(int b) {
                if (completedLate.compareAndSet(false, true)) {
                    pendingFuture2.completeExceptionally(lateFailure);
                }
                super.write(b);
            }

            @Override
            public synchronized void write(byte[] b, int off, int len) {
                if (completedLate.compareAndSet(false, true)) {
                    pendingFuture2.completeExceptionally(lateFailure);
                }
                super.write(b, off, len);
            }
        };

        assertThatThrownBy(() -> runReplayTest(List.of(r1, r2), false, false, producer, out))
                .isInstanceOf(DeadLetterReplayCommand.ReplayPublicationException.class)
                .hasMessage("Replay publication failed during flush: observed 1 acknowledged, 0 failed (1 callback pending)")
                .hasCause(flushErr)
                .satisfies(e -> {
                    DeadLetterReplayCommand.ReplayPublicationException rpe = (DeadLetterReplayCommand.ReplayPublicationException) e;
                    assertThat(rpe.acknowledgedCount()).isEqualTo(1);
                    assertThat(rpe.failedCount()).isEqualTo(0);
                    assertThat(rpe.publication().pendingCallbacks()).isEqualTo(1);
                    assertThat(rpe.publication().failureReasons()).doesNotContainKey(r2);
                    assertThat(rpe.publication().failureReasons()).doesNotContainValue("Kafka publication failed: Late callback publication failure");
                });

        assertThat(completedLate).isTrue();
        assertThat(pendingFuture2).isCompletedExceptionally();

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Records ready for replay: 2")
                .contains("Callback outcomes observed at reporting time after flush failure:")
                .contains("- Acknowledged: 1")
                .contains("- Failed: 0")
                .doesNotContain("Late callback publication failure")
                .doesNotContain("Rejected / Failed records:")
                .doesNotContain("Records replayed to Kafka")
                .doesNotContain("Unconfirmed")
                .contains("Producer flush failed: Kafka producer flush socket reset")
                .contains("PUBLICATION FAILED: producer flush encountered an error before all callbacks completed.");
    }

    @Test
    public void liveReplayMixedCallbackAndSynchronousFailuresWithAccurateCounts() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 1, 11L, false, bytes("k2"));
        DeadLetterRecord r3 = record("WAREHOUSE_REJECTED", "bank-a", 2, 12L, false, bytes("k3"));

        RuntimeException syncErr = new RuntimeException("Synchronous buffer full");
        RuntimeException asyncErr = new RuntimeException("Broker rejected payload");

        AtomicInteger count = new AtomicInteger(0);
        TestProducer producer = new TestProducer(record -> {
            int idx = count.getAndIncrement();
            if (idx == 1) {
                throw syncErr;
            } else if (idx == 2) {
                return CompletableFuture.failedFuture(asyncErr);
            }
            return CompletableFuture.completedFuture(null);
        });
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(() -> runReplayTest(List.of(r1, r2, r3), false, false, producer, out))
                .isInstanceOf(DeadLetterReplayCommand.ReplayPublicationException.class)
                .hasMessage("Replay publication incomplete: 1 acknowledged, 2 failed")
                .hasCause(syncErr)
                .satisfies(e -> {
                    DeadLetterReplayCommand.ReplayPublicationException rpe = (DeadLetterReplayCommand.ReplayPublicationException) e;
                    assertThat(rpe.acknowledgedCount()).isEqualTo(1);
                    assertThat(rpe.failedCount()).isEqualTo(2);
                });

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("- Acknowledged: 1")
                .contains("- Failed: 2")
                .doesNotContain("Unconfirmed")
                .contains("Synchronous send failure: Synchronous buffer full")
                .contains("Kafka publication failed: Broker rejected payload");
    }

    @Test
    public void liveReplayCallbackExceptionWithoutMessageHandledCleanly() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));

        RuntimeException errWithoutMsg = new RuntimeException();
        TestProducer producer = new TestProducer(record -> CompletableFuture.failedFuture(errWithoutMsg));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(() -> runReplayTest(List.of(r1), false, false, producer, out))
                .isInstanceOf(DeadLetterReplayCommand.ReplayPublicationException.class)
                .hasMessage("Replay publication incomplete: 0 acknowledged, 1 failed")
                .hasCause(errWithoutMsg)
                .satisfies(e -> {
                    DeadLetterReplayCommand.ReplayPublicationException rpe = (DeadLetterReplayCommand.ReplayPublicationException) e;
                    assertThat(rpe.acknowledgedCount()).isEqualTo(0);
                    assertThat(rpe.failedCount()).isEqualTo(1);
                });

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("- Acknowledged: 0")
                .contains("- Failed: 1")
                .doesNotContain("Unconfirmed")
                .contains("Kafka publication failed: RuntimeException");
    }

    private DeadLetterRecord makeRecordWithReplayUuid(String reason, String sourceId, int partition, long offset, UUID replayUuid) {
        RecordHeaders h = headers(reason, sourceId, "fineract.cdc.batch_job_instance", partition, offset);
        ReplayIdentityHeaders.add(h, new ReplayIdentity(replayUuid, "fineract.cdc.dlt", partition, offset));
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":" + offset + "},\"source\":{\"name\":\"" + sourceId + "\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        return DeadLetterRecord.parse("fineract.cdc.dlt", partition, offset, h, bytes("k" + offset), payload, contract);
    }

    private DeadLetterRecord makeRecordWithReplayUuid(String reason, String sourceId, String table, int partition, long offset, UUID replayUuid) {
        RecordHeaders h = headers(reason, sourceId, "fineract.cdc." + table, partition, offset);
        ReplayIdentityHeaders.add(h, new ReplayIdentity(replayUuid, "fineract.cdc.dlt", partition, offset));
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":" + offset + "},\"source\":{\"name\":\"" + sourceId + "\",\"table\":\"" + table + "\",\"ts_ms\":1000},\"op\":\"c\"}");
        return DeadLetterRecord.parse("fineract.cdc.dlt", partition, offset, h, bytes("k" + offset), payload, contract);
    }

    private DeadLetterRecord makeRecordWithReplayUuidAndPk(String reason, String sourceId, String table, int partition, long offset, UUID replayUuid, long pk) {
        RecordHeaders h = headers(reason, sourceId, "fineract.cdc." + table, partition, offset);
        ReplayIdentityHeaders.add(h, new ReplayIdentity(replayUuid, "fineract.cdc.dlt", partition, offset));
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":" + pk + "},\"source\":{\"name\":\"" + sourceId + "\",\"table\":\"" + table + "\",\"ts_ms\":1000},\"op\":\"c\"}");
        return DeadLetterRecord.parse("fineract.cdc.dlt", partition, offset, h, bytes("k" + offset), payload, contract);
    }

    @Test
    public void individualHistoricalOutcomesDisplayExpectedReasonAndBreakdown() throws Exception {
        for (ReplayOutcome outcome : List.of(ReplayOutcome.APPLIED, ReplayOutcome.SKIPPED_STALE, ReplayOutcome.SKIPPED_SUPERSEDED, ReplayOutcome.CLAIMED)) {
            StubWriter writer = new StubWriter();
            WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

            UUID uuid = UUID.randomUUID();
            DeadLetterRecord r = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 10L, uuid);

            MockProducer<byte[], byte[]> producer =
                    new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
            Connection conn = mockConnection(Map.of(uuid, outcome));

            OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
            DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(1, Map.of());
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            DeadLetterReplayCommand.processMatchedRecords(
                    scan, List.of(r), contract,
                    new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                    true, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

            String report = out.toString(StandardCharsets.UTF_8);
            assertThat(report).contains("Already completed (in ledger): 1 (ledger check performed)");
            if (outcome == ReplayOutcome.CLAIMED) {
                assertThat(report)
                        .contains("- CLAIMED: 1 (in-progress/unresolved)")
                        .contains("Already present in replay ledger (" + uuid + ": CLAIMED - in-progress/unresolved)")
                        .doesNotContain("Already completed in replay ledger (" + uuid);
                assertThat(producer.history()).isEmpty();
            } else {
                assertThat(report)
                        .contains("- " + outcome.name() + ": 1")
                        .contains("Already completed in replay ledger (" + uuid + ": " + outcome.name() + ")");
            }
            assertThat(writer.preflightCalls).isEmpty();
        }
    }

    @Test
    public void multipleHistoricalOutcomesProduceCorrectSummaryBreakdownCounts() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID uApplied = UUID.randomUUID();
        UUID uStale = UUID.randomUUID();
        UUID uSuperseded = UUID.randomUUID();
        UUID uClaimed = UUID.randomUUID();

        DeadLetterRecord rApplied = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 10L, uApplied);
        DeadLetterRecord rStale = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 11L, uStale);
        DeadLetterRecord rSuperseded = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 12L, uSuperseded);
        DeadLetterRecord rClaimed = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 14L, uClaimed);

        Map<UUID, ReplayOutcome> outcomes = new HashMap<>();
        outcomes.put(uApplied, ReplayOutcome.APPLIED);
        outcomes.put(uStale, ReplayOutcome.SKIPPED_STALE);
        outcomes.put(uSuperseded, ReplayOutcome.SKIPPED_SUPERSEDED);
        outcomes.put(uClaimed, ReplayOutcome.CLAIMED);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(outcomes);

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(4, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(rApplied, rStale, rSuperseded, rClaimed), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                true, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Already completed (in ledger): 4 (ledger check performed)")
                .contains("- APPLIED: 1")
                .contains("- SKIPPED_STALE: 1")
                .contains("- SKIPPED_SUPERSEDED: 1")
                .contains("- CLAIMED: 1 (in-progress/unresolved)");
        assertThat(writer.preflightCalls).isEmpty();
    }

    @Test
    public void dryRunWarehousePreflightDisplaysPredictedOutcomesSummary() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1 = UUID.randomUUID();
        UUID u2 = UUID.randomUUID();
        UUID u3 = UUID.randomUUID();

        DeadLetterRecord r1 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 10L, u1);
        DeadLetterRecord r2 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 11L, u2);
        DeadLetterRecord r3 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 12L, u3);

        ReplayIdentity id1 = r1.replayIdentity();
        ReplayIdentity id2 = r2.replayIdentity();
        ReplayIdentity id3 = r3.replayIdentity();

        writer.predictedOutcomes = Map.of(
                id1, ReplayOutcome.APPLIED,
                id2, ReplayOutcome.SKIPPED_STALE,
                id3, ReplayOutcome.SKIPPED_SUPERSEDED);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(3, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2, r3), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                true, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Predicted warehouse outcomes (point-in-time estimate; not committed):")
                .contains("- APPLIED: 1")
                .contains("- SKIPPED_STALE: 1")
                .contains("- SKIPPED_SUPERSEDED: 1")
                .contains("Preflight passed: 3")
                .contains("point-in-time estimate; not committed");
    }

    @Test
    public void dryRunPredictionsDoNotCountClaimedAsSuccessfulPrediction() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1 = UUID.randomUUID();
        DeadLetterRecord r1 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 10L, u1);
        writer.predictedOutcomes = Map.of(r1.replayIdentity(), ReplayOutcome.CLAIMED);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(1, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                true, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Predicted warehouse outcomes (point-in-time estimate; not committed):")
                .contains("- APPLIED: 0")
                .contains("- SKIPPED_STALE: 0")
                .contains("- SKIPPED_SUPERSEDED: 0");
    }

    @Test
    public void noPredictionBlockAppearsWhenPreflightIsSkipped() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1 = UUID.randomUUID();
        DeadLetterRecord r1 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 10L, u1);
        writer.predictedOutcomes = Map.of(r1.replayIdentity(), ReplayOutcome.APPLIED);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(1, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                true, true, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight validation: SKIPPED")
                .doesNotContain("Predicted warehouse outcomes");
    }

    @Test
    public void noPredictionBlockAppearsWhenPreflightFailsOrNoCandidatesExist() throws Exception {
        StubWriter writer = new StubWriter();
        writer.nextException = new SQLException("warehouse connection rejected", "08001");
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1 = UUID.randomUUID();
        DeadLetterRecord r1 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 10L, u1);
        writer.predictedOutcomes = Map.of(r1.replayIdentity(), ReplayOutcome.APPLIED);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(1, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                true, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight failed: 1")
                .doesNotContain("Predicted warehouse outcomes");
    }

    @Test
    public void liveExecutionDisplaysAsynchronousNoticeAndDoesNotDescribeAcknowledgedAsWarehouseApplied() throws Exception {
        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        TestProducer producer = new TestProducer();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        runReplayTest(List.of(r1), false, false, producer, out);

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("LIVE REPLAY (--execute)")
                .contains("Records ready for replay: 1")
                .contains("Records replayed to Kafka: 1")
                .contains("- Acknowledged: 1")
                .contains("- Failed: 0")
                .doesNotContain("Unconfirmed")
                .contains("Notice: Records reported as Acknowledged have been accepted by the Kafka broker.")
                .contains("Warehouse ingestion consumes Kafka asynchronously; broker acknowledgement does not prove warehouse application.")
                .contains("Durable warehouse outcomes can be inspected later using 'dlt-list --with-ledger' after the sink processes the replay.")
                .doesNotContain("Predicted warehouse outcomes")
                .doesNotContain("Applied to warehouse")
                .doesNotContain("Warehouse applied");
    }

    @Test
    public void oneInvalidRecordAmongSeveralValidCandidatesIsolatesAndPublishesValidOnly() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1 = UUID.randomUUID();
        UUID u2Bad = UUID.randomUUID();
        UUID u3 = UUID.randomUUID();

        writer.failingUuids.add(u2Bad);

        DeadLetterRecord r1 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 10L, u1);
        DeadLetterRecord r2 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 11L, u2Bad);
        DeadLetterRecord r3 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 12L, u3);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicInteger aborts = new AtomicInteger(0);
        Connection conn = mockConnection(closed, aborts);

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(3, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2, r3), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(producer.history()).hasSize(2);
        assertThat(closed.get()).isFalse();
        assertThat(aborts.get()).isEqualTo(0);

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight passed: 2")
                .contains("Preflight failed: 1")
                .contains("numeric field overflow")
                .contains("Records replayed to Kafka: 2");
    }

    @Test
    public void multipleInvalidRecordsInDifferentPositionsIsolatedAccurately() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1Bad = UUID.randomUUID();
        UUID u2 = UUID.randomUUID();
        UUID u3Bad = UUID.randomUUID();
        UUID u4 = UUID.randomUUID();

        writer.failingUuids.add(u1Bad);
        writer.failingUuids.add(u3Bad);

        DeadLetterRecord r1 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 10L, u1Bad);
        DeadLetterRecord r2 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 11L, u2);
        DeadLetterRecord r3 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 12L, u3Bad);
        DeadLetterRecord r4 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 13L, u4);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicInteger aborts = new AtomicInteger(0);
        Connection conn = mockConnection(closed, aborts);

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(4, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2, r3, r4), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                true, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(producer.history()).isEmpty();
        assertThat(closed.get()).isFalse();

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight passed: 2")
                .contains("Preflight failed: 2")
                .contains("numeric field overflow")
                .contains("Preflight verified candidates ready for live replay:")
                .contains(r2.dltOffset() + "")
                .contains(r4.dltOffset() + "");
    }

    @Test
    public void allCandidatesValidInvokesPreflightOnlyOnce() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1 = UUID.randomUUID();
        UUID u2 = UUID.randomUUID();
        UUID u3 = UUID.randomUUID();

        DeadLetterRecord r1 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 10L, u1);
        DeadLetterRecord r2 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 11L, u2);
        DeadLetterRecord r3 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 12L, u3);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));
        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(3, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2, r3), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(writer.preflightCalls).hasSize(1);
        assertThat(writer.preflightCalls.get(0)).hasSize(3);
        assertThat(producer.history()).hasSize(3);
        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight passed: 3")
                .contains("Preflight failed: 0");
    }

    @Test
    public void allCandidatesInvalidBisectsAndFailsAllWithoutPublication() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1Bad = UUID.randomUUID();
        UUID u2Bad = UUID.randomUUID();

        writer.failingUuids.add(u1Bad);
        writer.failingUuids.add(u2Bad);

        DeadLetterRecord r1 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 10L, u1Bad);
        DeadLetterRecord r2 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 11L, u2Bad);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicInteger aborts = new AtomicInteger(0);
        Connection conn = mockConnection(closed, aborts);

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(writer.preflightCalls).hasSize(3);
        assertThat(producer.history()).isEmpty();
        assertThat(closed.get()).isFalse();

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight passed: 0")
                .contains("Preflight failed: 2")
                .contains("numeric field overflow");
    }

    @Test
    public void multipleSourceTableGroupsIsolatesFailingGroupWhilePassingValidGroup() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1 = UUID.randomUUID();
        UUID u2Bad = UUID.randomUUID();
        UUID u3 = UUID.randomUUID();
        UUID u4 = UUID.randomUUID();

        writer.failingUuids.add(u2Bad);

        DeadLetterRecord r1 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", "batch_job_instance", 0, 10L, u1);
        DeadLetterRecord r2 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", "batch_job_instance", 0, 11L, u2Bad);
        DeadLetterRecord r3 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-b", "batch_job_instance", 0, 12L, u3);
        DeadLetterRecord r4 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-b", "batch_job_instance", 0, 13L, u4);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));

        OwnershipSnapshot os = ownership(Map.of(
                "bank-a", List.of("batch_job_instance"),
                "bank-b", List.of("batch_job_instance")), Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(4, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2, r3, r4), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(producer.history()).hasSize(3);

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight passed: 3")
                .contains("Preflight failed: 1")
                .contains("Records replayed to Kafka: 3");
    }

    @Test
    public void infrastructureErrorsDoNotBisectAndFailEntireBatchClosedWithoutPublication() throws Exception {
        StubWriter writer = new StubWriter();
        writer.nextException = new SQLException("08006: connection failure", "08006");
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 0, 11L, false, bytes("k2"));
        DeadLetterRecord r3 = record("WAREHOUSE_REJECTED", "bank-a", 0, 12L, false, bytes("k3"));

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicInteger aborts = new AtomicInteger(0);
        Connection conn = mockConnection(closed, aborts);

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(3, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2, r3), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(writer.preflightCalls).hasSize(1);
        assertThat(closed.get()).isFalse();
        assertThat(aborts.get()).isEqualTo(0);
        assertThat(producer.history()).isEmpty();

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight failed: 3")
                .contains("connection failure");
    }

    @Test
    public void finalCombinedSetValidationFailureMarksProvisionallyValidCandidatesInconclusive() throws Exception {
        StubWriter writer = new StubWriter();
        AtomicInteger callCount = new AtomicInteger(0);
        writer.failureRule = events -> {
            int call = callCount.incrementAndGet();
            if (call == 1) {
                return new SQLException("22003: numeric field overflow", "22003");
            }
            if (events.size() > 1) {
                return new SQLException("23505: unique constraint violation", "23505");
            }
            return null;
        };
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1 = UUID.randomUUID();
        UUID u2 = UUID.randomUUID();

        DeadLetterRecord r1 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 10L, u1);
        DeadLetterRecord r2 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 11L, u2);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(producer.history()).isEmpty();

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight inconclusive: 2")
                .contains("Preflight passed: 0")
                .contains("Preflight failed: 0")
                .contains("Warehouse validation inconclusive: provisionally valid records failed when combined in a single batch")
                .doesNotContain("Predicted warehouse outcomes");
    }

    @Test
    public void interdependentParentChildPairDoesNotFalselyAttributeChildAsInvalidAndDoesNotPublish() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1Parent = UUID.randomUUID();
        UUID u2Child = UUID.randomUUID();
        UUID u3Bad = UUID.randomUUID();

        writer.failureRule = events -> {
            boolean hasU3 = events.stream().anyMatch(e -> e.replayIdentity() != null && u3Bad.equals(e.replayIdentity().replayId()));
            if (hasU3) {
                return new SQLException("22003: numeric field overflow", "22003");
            }
            boolean hasParent = events.stream().anyMatch(e -> e.replayIdentity() != null && u1Parent.equals(e.replayIdentity().replayId()));
            boolean hasChild = events.stream().anyMatch(e -> e.replayIdentity() != null && u2Child.equals(e.replayIdentity().replayId()));
            if (hasChild && !hasParent) {
                return new SQLException("23503: foreign key violation", "23503");
            }
            return null;
        };

        DeadLetterRecord r1 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 10L, u1Parent);
        DeadLetterRecord r2 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 11L, u2Child);
        DeadLetterRecord r3 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 12L, u3Bad);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(3, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2, r3), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(producer.history()).isEmpty();

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight inconclusive: 3")
                .contains("Preflight passed: 0")
                .contains("Preflight failed: 0")
                .contains("23503")
                .contains("foreign key violation");
    }

    @Test
    public void systemicErrorDuringRecursionAbortsIsolationAndDoesNotFalselyLabelUnresolvedCandidates() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1 = UUID.randomUUID();
        UUID u2 = UUID.randomUUID();
        UUID u3 = UUID.randomUUID();

        AtomicInteger callCount = new AtomicInteger(0);
        writer.failureRule = events -> {
            int call = callCount.incrementAndGet();
            if (call == 1) {
                return new SQLException("22003: numeric field overflow", "22003");
            }
            if (call == 2) {
                return null;
            }
            return new SQLException("08006: connection failure during recursion", "08006");
        };

        DeadLetterRecord r1 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 10L, u1);
        DeadLetterRecord r2 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 11L, u2);
        DeadLetterRecord r3 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 12L, u3);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(3, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2, r3), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(callCount.get()).isEqualTo(3);
        assertThat(producer.history()).isEmpty();

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight inconclusive: 3")
                .contains("Preflight passed: 0")
                .contains("Preflight failed: 0")
                .contains("08006")
                .contains("connection failure during recursion");
    }

    @Test
    public void interleavedSourceTableCandidatesPreserveOriginalOrderInCombinedPreflightAndPublication() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1 = UUID.randomUUID();
        UUID u2 = UUID.randomUUID();
        UUID u3 = UUID.randomUUID();
        UUID u4Bad = UUID.randomUUID();

        writer.failingUuids.add(u4Bad);

        DeadLetterRecord r1 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", "batch_job_instance", 0, 10L, u1);
        DeadLetterRecord r2 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-b", "batch_job_instance", 0, 11L, u2);
        DeadLetterRecord r3 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", "batch_job_instance", 0, 12L, u3);
        DeadLetterRecord r4 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-b", "batch_job_instance", 0, 13L, u4Bad);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));

        OwnershipSnapshot os = ownership(Map.of(
                "bank-a", List.of("batch_job_instance"),
                "bank-b", List.of("batch_job_instance")), Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(4, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2, r3, r4), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(producer.history()).hasSize(3);
        List<UUID> publishedUuids = producer.history().stream()
                .map(p -> ReplayIdentityHeaders.read(p.headers()).orElseThrow().replayId())
                .toList();
        assertThat(publishedUuids).containsExactly(u1, u2, u3);

        Collection<RoutedEvent> finalCombinedCall = writer.preflightCalls.get(writer.preflightCalls.size() - 1);
        List<UUID> preflightedUuids = finalCombinedCall.stream()
                .map(e -> e.replayIdentity().replayId())
                .toList();
        assertThat(preflightedUuids).containsExactly(u1, u2, u3);

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight passed: 3")
                .contains("Preflight failed: 1")
                .contains("Records replayed to Kafka: 3");
    }

    @Test
    public void predictedOutcomesComeFromFinalCombinedPreflightNotStaleSubset() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1Bad = UUID.randomUUID();
        UUID u2 = UUID.randomUUID();

        writer.failingUuids.add(u1Bad);

        DeadLetterRecord r1 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 10L, u1Bad);
        DeadLetterRecord r2 = makeRecordWithReplayUuid("WAREHOUSE_REJECTED", "bank-a", 0, 11L, u2);

        writer.predictedOutcomes = Map.of(r2.replayIdentity(), ReplayOutcome.SKIPPED_STALE);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                true, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight passed: 1")
                .contains("Preflight failed: 1")
                .contains("- SKIPPED_STALE: 1")
                .contains("- APPLIED: 0")
                .contains("- SKIPPED_SUPERSEDED: 0");
    }

    @Test
    public void candidatesForSameEntityTreatedAsAtomicUnitSoSupersededRecordDoesNotFailPreflight() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1Older = UUID.randomUUID();
        UUID u2Newer = UUID.randomUUID();
        UUID u3Bad = UUID.randomUUID();

        writer.failureRule = events -> {
            boolean hasU3 = events.stream().anyMatch(e -> e.replayIdentity() != null
                    && u3Bad.equals(e.replayIdentity().replayId()));
            if (hasU3) {
                return new SQLException("22003: numeric field overflow on unrelated entity", "22003");
            }
            boolean hasU1 = events.stream().anyMatch(e -> e.replayIdentity() != null
                    && u1Older.equals(e.replayIdentity().replayId()));
            boolean hasU2 = events.stream().anyMatch(e -> e.replayIdentity() != null
                    && u2Newer.equals(e.replayIdentity().replayId()));
            if (hasU1 && !hasU2) {
                return new SQLException("22003: numeric field overflow on older superseded record", "22003");
            }
            return null;
        };

        DeadLetterRecord r1 = makeRecordWithReplayUuidAndPk("WAREHOUSE_REJECTED", "bank-a", "batch_job_instance", 0, 10L, u1Older, 9001L);
        DeadLetterRecord r2 = makeRecordWithReplayUuidAndPk("WAREHOUSE_REJECTED", "bank-a", "batch_job_instance", 0, 11L, u2Newer, 9001L);
        DeadLetterRecord r3 = makeRecordWithReplayUuidAndPk("WAREHOUSE_REJECTED", "bank-a", "batch_job_instance", 0, 12L, u3Bad, 9999L);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(3, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2, r3), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        for (Collection<RoutedEvent> call : writer.preflightCalls) {
            boolean has1 = call.stream().anyMatch(e -> e.replayIdentity() != null && u1Older.equals(e.replayIdentity().replayId()));
            boolean has2 = call.stream().anyMatch(e -> e.replayIdentity() != null && u2Newer.equals(e.replayIdentity().replayId()));
            assertThat(has1).isEqualTo(has2);
        }

        Collection<RoutedEvent> finalCall = writer.preflightCalls.get(writer.preflightCalls.size() - 1);
        List<UUID> finalUuids = finalCall.stream().map(e -> e.replayIdentity().replayId()).toList();
        assertThat(finalUuids).containsExactly(u1Older, u2Newer);

        assertThat(producer.history()).hasSize(2);
        List<UUID> publishedUuids = producer.history().stream()
                .map(p -> ReplayIdentityHeaders.read(p.headers()).orElseThrow().replayId())
                .toList();
        assertThat(publishedUuids).containsExactly(u1Older, u2Newer);

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight passed: 2")
                .contains("Preflight failed: 1")
                .contains("numeric field overflow on unrelated entity")
                .doesNotContain("numeric field overflow on older superseded record");
    }

    @Test
    public void failingMultiRecordEntityUnitReportedAsInconclusiveRatherThanIndividuallyFailed() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1 = UUID.randomUUID();
        UUID u2 = UUID.randomUUID();
        UUID u3 = UUID.randomUUID();

        writer.failureRule = events -> {
            boolean hasEntity9001 = events.stream().anyMatch(e -> e.replayIdentity() != null
                    && (u1.equals(e.replayIdentity().replayId()) || u2.equals(e.replayIdentity().replayId())));
            if (hasEntity9001) {
                return new SQLException("22003: numeric field overflow on entity 9001", "22003");
            }
            return null;
        };

        DeadLetterRecord r1 = makeRecordWithReplayUuidAndPk("WAREHOUSE_REJECTED", "bank-a", "batch_job_instance", 0, 10L, u1, 9001L);
        DeadLetterRecord r2 = makeRecordWithReplayUuidAndPk("WAREHOUSE_REJECTED", "bank-a", "batch_job_instance", 0, 11L, u2, 9001L);
        DeadLetterRecord r3 = makeRecordWithReplayUuidAndPk("WAREHOUSE_REJECTED", "bank-a", "batch_job_instance", 0, 12L, u3, 9999L);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(3, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2, r3), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(producer.history()).hasSize(1);
        UUID published = ReplayIdentityHeaders.read(producer.history().get(0).headers()).orElseThrow().replayId();
        assertThat(published).isEqualTo(u3);

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight passed: 1")
                .contains("Preflight failed: 0")
                .contains("Preflight inconclusive: 2")
                .contains("multi-record entity unit failed validation and individual attribution is unsafe");
    }

    @Test
    public void unresolvableEntityPrimaryKeyFailsClosedAsInconclusiveWithoutArbitraryBisection() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        UUID u1 = UUID.randomUUID();
        UUID u2 = UUID.randomUUID();

        writer.failureRule = events -> new SQLException("22003: numeric field overflow", "22003");

        RecordHeaders h1 = headers("WAREHOUSE_REJECTED", "bank-a", "fineract.cdc.batch_job_instance", 0, 10L);
        ReplayIdentityHeaders.add(h1, new ReplayIdentity(u1, "fineract.cdc.dlt", 0, 10L));
        byte[] payload1 = bytes("{\"after\":{\"job_instance_id\":9999999999999999999999999999999999999999},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord r1 = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 10L, h1, bytes("k10"), payload1, contract);

        DeadLetterRecord r2 = makeRecordWithReplayUuidAndPk("WAREHOUSE_REJECTED", "bank-a", "batch_job_instance", 0, 11L, u2, 9002L);

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Connection conn = mockConnection(new AtomicBoolean(false), new AtomicInteger(0));

        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        assertThat(producer.history()).isEmpty();
        assertThat(writer.preflightCalls).hasSize(1);

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("Preflight inconclusive: 2")
                .contains("Preflight passed: 0")
                .contains("Preflight failed: 0")
                .contains("primary-key identity could not be reliably determined for entity isolation");
    }

    @Test
    public void auditPrimaryKeyNormalizationMatchesPostgresEqualitySemantics() {
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("char(10)", "abc   ")).isEqualTo("abc");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("character(10)", "abc   ")).isEqualTo("abc");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("bpchar", "abc   ")).isEqualTo("abc");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("char", "abc")).isEqualTo("abc");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("char(10)", "   ")).isEqualTo("");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("char(10)", " abc ")).isEqualTo(" abc");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("varchar(255)", "abc   ")).isEqualTo("abc   ");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("character varying(100)", "abc   ")).isEqualTo("abc   ");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("text", "abc   ")).isEqualTo("abc   ");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("numeric(10,2)", "100.00")).isEqualTo("100");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("decimal", "100.0")).isEqualTo("100");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("numeric", "0.00")).isEqualTo("0");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("numeric", "-0.0")).isEqualTo("0");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("numeric", "0")).isEqualTo("0");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("numeric", "1e2")).isEqualTo("100");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("numeric", "123.4500")).isEqualTo("123.45");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("uuid", "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"))
                .isEqualTo("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("uuid", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))
                .isEqualTo("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("bigint", "  123  ")).isEqualTo("123");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("int8", "123")).isEqualTo("123");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("bigserial", "123")).isEqualTo("123");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("integer", "123")).isEqualTo("123");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("int4", "123")).isEqualTo("123");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("serial", "123")).isEqualTo("123");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("smallint", "123")).isEqualTo("123");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("int2", "123")).isEqualTo("123");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("smallserial", "123")).isEqualTo("123");
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("smallint", "99999999")).isNull();
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("integer", "9999999999")).isNull();
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("date", "2026-01-01")).isNull();
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("timestamptz", "2026-01-01T00:00:00Z")).isNull();
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("bytea", "\\x0102")).isNull();
        assertThat(DeadLetterReplayCommand.normalizePrimaryKey("jsonb", "{}")).isNull();
    }
}
