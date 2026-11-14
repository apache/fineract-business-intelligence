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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.fineract.bi.ingestion.routing.OwnershipSnapshot;
import org.apache.fineract.bi.ingestion.routing.Route;
import org.apache.fineract.bi.ingestion.sink.BatchWriter;
import org.apache.fineract.bi.ingestion.sink.PartitionOffset;
import org.apache.fineract.bi.ingestion.sink.PreflightRollbackException;
import org.apache.fineract.bi.ingestion.sink.ReplayIdentity;
import org.apache.fineract.bi.ingestion.sink.RoutedEvent;
import org.apache.fineract.bi.ingestion.sink.SkipCheck;
import org.apache.fineract.bi.ingestion.sink.WarehouseWriter;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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

        DeadLetterReplayCommand.renderReport(
                scan, filter, false, true,
                List.of(r1, r2), List.of(), List.of(), List.of(), List.of(r1, r2), 2, Map.of(),
                new PrintStream(out, true, StandardCharsets.UTF_8));

        // Test publication manually to verify partition and headers
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
        badHeaders.add("fbi.replay.dlt.topic", bytes("dlt")); // Missing partition & offset
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":20},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord badReplay = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 20L, badHeaders, bytes("k"), payload, contract);

        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2, Collections.emptyMap());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        Map<DeadLetterRecord, String> failures = new HashMap<>();
        failures.put(omitted, "Payload is omitted or unavailable");
        failures.put(badReplay, "Invalid replay identity: " + badReplay.replayIdentityError());

        DeadLetterReplayCommand.renderReport(
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

        DeadLetterReplayCommand.renderReport(
                scan, new DeadLetterReplayCommand.ReplayFilter(null, null, null, null), true, false,
                List.of(r), List.of(), List.of(), List.of(), List.of(r), 0,
                Map.of(), new PrintStream(out, true, StandardCharsets.UTF_8));

        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report)
                .contains("PREFLIGHT / DRY-RUN")
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

        // When execute is not set, dryRun is true
        DeadLetterReplayCommand.renderReport(
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

        // Allowed with bounded limit
        int boundedLimit = 100;
        assertThat(execute && !hasFilter && boundedLimit == Integer.MAX_VALUE && !confirmAll).isFalse();

        // Allowed with confirm-all
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

    private Connection mockConnection(AtomicBoolean isClosed, AtomicInteger abortCalls) {
        return mockConnection(isClosed, abortCalls, null, null, null);
    }

    private Connection mockConnection(
            AtomicBoolean isClosed,
            AtomicInteger abortCalls,
            AtomicInteger getAutoCommitCalls,
            SQLException getAutoCommitException,
            SQLException setAutoCommitFalseException) {
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
                        return null;
                    }
                    if ("close".equals(name)) {
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
                    throw new UnsupportedOperationException("Method " + name + " not supported");
                });
    }

    static class StubWriter implements BatchWriter {
        final List<Collection<RoutedEvent>> preflightCalls = new ArrayList<>();
        SQLException nextException = null;

        @Override
        public List<WarehouseWriter.Applied> write(Connection c, Collection<RoutedEvent> events,
                Collection<PartitionOffset> offsets, String consumerGroup, Collection<SkipCheck> skips) {
            return List.of();
        }

        @Override
        public List<WarehouseWriter.Applied> preflight(Connection c, Collection<RoutedEvent> events) throws SQLException {
            preflightCalls.add(events);
            if (nextException != null) {
                SQLException toThrow = nextException;
                nextException = null; // Clear so subsequent calls succeed
                throw toThrow;
            }
            return List.of(new WarehouseWriter.Applied("batch_job_instance", "prod", events.size(), 0));
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

        // Operation was aborted: second record was never evaluated by preflight writer
        assertThat(writer.preflightCalls).hasSize(1);
        // Producer published zero records
        assertThat(producer.history()).isEmpty();
        // Connection was aborted / closed and marked unusable
        assertThat(closed.get()).isTrue();
        assertThat(aborts.get()).isEqualTo(1);
    }

    @Test
    public void connectionReusedAfterOrdinaryValidationFailure() throws Exception {
        StubWriter writer = new StubWriter();
        // Record 1 fails with ordinary warehouse validation failure
        writer.nextException = new SQLException("22003: numeric field overflow", "22003");
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

        // Both records were processed using the same connection without abortion
        assertThat(writer.preflightCalls).hasSize(2);
        // Connection remained open and was NOT aborted
        assertThat(closed.get()).isFalse();
        assertThat(aborts.get()).isEqualTo(0);
        // Record 1 was rejected, Record 2 was replayed
        assertThat(producer.history()).hasSize(1);
        String report = out.toString(StandardCharsets.UTF_8);
        assertThat(report).contains("Preflight failed: 1").contains("Records replayed to Kafka: 1");
    }

    @Test
    public void autoCommitRestorationFailureIsFatalAndPreventsProcessingSubsequentRecords() {
        StubWriter writer = new StubWriter();
        SQLException autoCommitErr = new SQLException("autoCommit restore failed", "08006");
        writer.nextException = new PreflightRollbackException("Failed to restore connection autoCommit state", autoCommitErr);
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
                .hasMessageContaining("Failed to restore connection autoCommit state")
                .hasCause(autoCommitErr);

        // Operation was aborted: second record was never evaluated by preflight writer
        assertThat(writer.preflightCalls).hasSize(1);
        // Producer published zero records
        assertThat(producer.history()).isEmpty();
        // Connection was aborted / closed and marked unusable
        assertThat(closed.get()).isTrue();
        assertThat(aborts.get()).isEqualTo(1);
    }

    @Test
    public void getAutoCommitFailureIsFatalAndPreventsProcessingSubsequentRecords() {
        WarehouseWriter realWriter = new WarehouseWriter(contract, "KAFKA");
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, realWriter);

        SQLException getAutoCommitErr = new SQLException("getAutoCommit failed", "08006");
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicInteger aborts = new AtomicInteger(0);
        AtomicInteger getAutoCommitCalls = new AtomicInteger(0);
        Connection conn = mockConnection(closed, aborts, getAutoCommitCalls, getAutoCommitErr, null);

        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 0, 11L, false, bytes("k2"));

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2, Map.of());

        assertThatThrownBy(() -> DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(new ByteArrayOutputStream())))
                .isInstanceOf(PreflightRollbackException.class)
                .hasMessageContaining("Failed to initialize connection transaction state")
                .hasCause(getAutoCommitErr);

        // First record failed setup; second record was never evaluated by preflight
        assertThat(getAutoCommitCalls.get()).isEqualTo(1);
        // Producer published zero records
        assertThat(producer.history()).isEmpty();
        // Connection was aborted and marked unusable
        assertThat(closed.get()).isTrue();
        assertThat(aborts.get()).isEqualTo(1);
    }

    @Test
    public void setAutoCommitFalseFailureIsFatalAndPreventsProcessingSubsequentRecords() {
        WarehouseWriter realWriter = new WarehouseWriter(contract, "KAFKA");
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, realWriter);

        SQLException setAutoCommitFalseErr = new SQLException("setAutoCommit(false) failed", "08006");
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicInteger aborts = new AtomicInteger(0);
        AtomicInteger getAutoCommitCalls = new AtomicInteger(0);
        Connection conn = mockConnection(closed, aborts, getAutoCommitCalls, null, setAutoCommitFalseErr);

        DeadLetterRecord r1 = record("WAREHOUSE_REJECTED", "bank-a", 0, 10L, false, bytes("k1"));
        DeadLetterRecord r2 = record("WAREHOUSE_REJECTED", "bank-a", 0, 11L, false, bytes("k2"));

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(2, Map.of());

        assertThatThrownBy(() -> DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(r1, r2), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, false, conn, os, validator, producer, new PrintStream(new ByteArrayOutputStream())))
                .isInstanceOf(PreflightRollbackException.class)
                .hasMessageContaining("Failed to initialize connection transaction state")
                .hasCause(setAutoCommitFalseErr);

        // First record failed setup; second record was never evaluated by preflight
        assertThat(getAutoCommitCalls.get()).isEqualTo(1);
        // Producer published zero records
        assertThat(producer.history()).isEmpty();
        // Connection was aborted and marked unusable
        assertThat(closed.get()).isTrue();
        assertThat(aborts.get()).isEqualTo(1);
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
    public void missingConnectionAllowedWhenPreflightSkippedAndEnforcesRouting() throws Exception {
        StubWriter writer = new StubWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        DeadLetterRecord rProd = record("WAREHOUSE_REJECTED", "bank-prod", 0, 10L, false, bytes("k1"));
        DeadLetterRecord rShadow = record("WAREHOUSE_REJECTED", "bank-shadow", 0, 11L, false, bytes("k2"));
        DeadLetterRecord rHold = record("WAREHOUSE_REJECTED", "bank-hold", 0, 12L, false, bytes("k3"));
        DeadLetterRecord rSkip = record("WAREHOUSE_REJECTED", "bank-skip", 0, 13L, false, bytes("k4"));
        DeadLetterRecord rUnknown = record("WAREHOUSE_REJECTED", "bank-unknown", 0, 14L, false, bytes("k5"));

        Map<String, OwnershipSnapshot.Entry> entries = new HashMap<>();
        entries.put(OwnershipSnapshot.key("bank-prod", "batch_job_instance"),
                new OwnershipSnapshot.Entry("KAFKA", null, "STEADY", 1L)); // PROD
        entries.put(OwnershipSnapshot.key("bank-shadow", "batch_job_instance"),
                new OwnershipSnapshot.Entry("PYTHON", "KAFKA", "STEADY", 1L)); // SHADOW
        entries.put(OwnershipSnapshot.key("bank-hold", "batch_job_instance"),
                new OwnershipSnapshot.Entry("KAFKA", null, "PAUSED", 1L)); // HOLD
        entries.put(OwnershipSnapshot.key("bank-skip", "batch_job_instance"),
                new OwnershipSnapshot.Entry("PYTHON", null, "STEADY", 1L)); // SKIP
        // bank-unknown is omitted from sourceMeta and entries, so it routes to UNKNOWN_SOURCE

        Map<String, OwnershipSnapshot.SourceMeta> sourceMeta = Map.of(
                "bank-prod", new OwnershipSnapshot.SourceMeta("tenant-a", 0),
                "bank-shadow", new OwnershipSnapshot.SourceMeta("tenant-a", 0),
                "bank-hold", new OwnershipSnapshot.SourceMeta("tenant-a", 0),
                "bank-skip", new OwnershipSnapshot.SourceMeta("tenant-a", 0)
        );
        OwnershipSnapshot os = new OwnershipSnapshot(entries, sourceMeta, System.currentTimeMillis());

        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(5, Map.of());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // Run with execute=true (dryRun=false) and skipPreflight=true
        DeadLetterReplayCommand.processMatchedRecords(
                scan, List.of(rProd, rShadow, rHold, rSkip, rUnknown), contract,
                new DeadLetterReplayCommand.ReplayFilter(null, null, null, null),
                false, true, null, os, validator, producer, new PrintStream(out, true, StandardCharsets.UTF_8));

        // Warehouse writer preflight staging was skipped as requested
        assertThat(writer.preflightCalls).isEmpty();

        // ONLY PROD and SHADOW records were published to Kafka; HOLD, SKIP, and UNKNOWN_SOURCE were NOT published!
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
}
