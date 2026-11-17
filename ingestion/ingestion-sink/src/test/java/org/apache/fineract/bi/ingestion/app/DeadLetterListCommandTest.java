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

import com.zaxxer.hikari.HikariDataSource;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.fineract.bi.ingestion.config.Env;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.fineract.bi.ingestion.sink.ReplayIdentity;
import org.apache.fineract.bi.ingestion.sink.ReplayOutcome;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

public class DeadLetterListCommandTest {

    private static final String DLT = "fineract.cdc.dlt";
    private static final Contract CONTRACT =
            ContractLoader.load(Path.of("../../contracts/fineract_tables.json"));

    private static SinkConfig testConfig() {
        Map<String, String> env = Map.of(
                "SINK_BOOTSTRAP_SERVERS", "localhost:9092",
                "SINK_DLT_TOPIC", DLT,
                "SINK_WRITER_ID", "test-list-writer",
                "SINK_DB_URL", "jdbc:postgresql://localhost:5432/analytics",
                "SINK_DB_USER", "postgres",
                "SINK_DB_PASSWORD", "postgres"
        );
        return SinkConfig.from(Env.of(env), env);
    }

    private static ConsumerRecord<byte[], byte[]> createConsumerRecord(
            String dltTopic,
            int dltPartition,
            long dltOffset,
            String sourceId,
            String table,
            long sourceOffset,
            UUID replayId,
            boolean invalidReplayHeader) {

        byte[] payload = ("{\"source\":{\"name\":\"" + sourceId + "\",\"table\":\"" + table + "\"},\"op\":\"c\"}")
                .getBytes(StandardCharsets.UTF_8);

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(dltTopic, dltPartition, dltOffset, null, payload);

        record.headers().add("fbi.dlt.reason", "WAREHOUSE_REJECTED".getBytes(StandardCharsets.UTF_8));
        record.headers().add("fbi.dlt.message", "test rejection".getBytes(StandardCharsets.UTF_8));
        record.headers().add("fbi.source.topic", ("fineract.cdc." + table).getBytes(StandardCharsets.UTF_8));
        record.headers().add("fbi.source.partition", "0".getBytes(StandardCharsets.UTF_8));
        record.headers().add("fbi.source.offset", Long.toString(sourceOffset).getBytes(StandardCharsets.UTF_8));

        if (invalidReplayHeader) {
            record.headers().add(ReplayIdentity.ID_HEADER, "invalid-uuid".getBytes(StandardCharsets.UTF_8));
            record.headers().add(ReplayIdentity.TOPIC_HEADER, dltTopic.getBytes(StandardCharsets.UTF_8));
            record.headers().add(ReplayIdentity.PARTITION_HEADER, Integer.toString(dltPartition).getBytes(StandardCharsets.UTF_8));
            record.headers().add(ReplayIdentity.OFFSET_HEADER, Long.toString(dltOffset).getBytes(StandardCharsets.UTF_8));
        } else if (replayId != null) {
            record.headers().add(ReplayIdentity.ID_HEADER, replayId.toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add(ReplayIdentity.TOPIC_HEADER, dltTopic.getBytes(StandardCharsets.UTF_8));
            record.headers().add(ReplayIdentity.PARTITION_HEADER, Integer.toString(dltPartition).getBytes(StandardCharsets.UTF_8));
            record.headers().add(ReplayIdentity.OFFSET_HEADER, Long.toString(dltOffset).getBytes(StandardCharsets.UTF_8));
        }

        return record;
    }

    private static MockConsumer<byte[], byte[]> createMockConsumer(
            String dltTopic,
            List<ConsumerRecord<byte[], byte[]>> records) {

        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        TopicPartition tp0 = new TopicPartition(dltTopic, 0);

        consumer.updatePartitions(dltTopic, List.of(new PartitionInfo(dltTopic, 0, null, null, null)));
        if (records.isEmpty()) {
            consumer.updateBeginningOffsets(Map.of(tp0, 0L));
            consumer.updateEndOffsets(Map.of(tp0, 0L));
        } else {
            long minOffset = records.stream().mapToLong(ConsumerRecord::offset).min().orElse(0L);
            long maxOffset = records.stream().mapToLong(ConsumerRecord::offset).max().orElse(0L);
            consumer.updateBeginningOffsets(Map.of(tp0, minOffset));
            consumer.updateEndOffsets(Map.of(tp0, maxOffset + 1));
            consumer.schedulePollTask(() -> {
                for (ConsumerRecord<byte[], byte[]> r : records) {
                    consumer.addRecord(r);
                }
            });
        }
        return consumer;
    }

    private static class MockDataSource extends HikariDataSource {
        final AtomicInteger getConnectionCalls = new AtomicInteger(0);
        final AtomicInteger evictCalls = new AtomicInteger(0);
        final AtomicBoolean isClosed = new AtomicBoolean(false);
        final Connection conn;
        SQLException getConnectionException;

        MockDataSource(Connection conn) {
            super();
            this.conn = conn;
        }

        @Override
        public Connection getConnection() throws SQLException {
            getConnectionCalls.incrementAndGet();
            if (getConnectionException != null) {
                throw getConnectionException;
            }
            return conn;
        }

        @Override
        public void evictConnection(Connection connection) {
            evictCalls.incrementAndGet();
        }

        @Override
        public void close() {
            isClosed.set(true);
        }

        @Override
        public boolean isClosed() {
            return isClosed.get();
        }
    }

    private static Connection mockConnection(
            Map<UUID, ReplayOutcome> outcomes,
            List<UUID> queriedUuids,
            AtomicInteger closeCalls,
            SQLException queryException) {

        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("close".equals(name)) {
                        if (closeCalls != null) {
                            closeCalls.incrementAndGet();
                        }
                        return null;
                    }
                    if ("isClosed".equals(name)) {
                        return false;
                    }
                    if ("prepareStatement".equals(name)) {
                        if (queryException != null) {
                            throw queryException;
                        }
                        AtomicReference<UUID> currentUuid = new AtomicReference<>();
                        return Proxy.newProxyInstance(
                                PreparedStatement.class.getClassLoader(),
                                new Class<?>[] {PreparedStatement.class},
                                (psProxy, psMethod, psArgs) -> {
                                    String psName = psMethod.getName();
                                    if ("setObject".equals(psName) && psArgs != null && psArgs.length >= 2 && psArgs[1] instanceof UUID u) {
                                        currentUuid.set(u);
                                        if (queriedUuids != null) {
                                            queriedUuids.add(u);
                                        }
                                        return null;
                                    }
                                    if ("executeQuery".equals(psName)) {
                                        if (queryException != null) {
                                            throw queryException;
                                        }
                                        UUID uuid = currentUuid.get();
                                        boolean exists = outcomes != null && outcomes.containsKey(uuid);
                                        ReplayOutcome outcome = exists ? outcomes.get(uuid) : null;
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
                                                        int col = (Integer) rsArgs[0];
                                                        if (col == 2) {
                                                            return outcome != null ? outcome.name() : null;
                                                        }
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

    private static String render(
            DeadLetterReader.ScanResult scan,
            List<DeadLetterSummary.Group> groups) {

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            DeadLetterListCommand.render(DLT, scan, groups, out);
        }

        return bytes.toString(StandardCharsets.UTF_8);
    }

    @Test
    public void displaysGroupedCountsAndPartitionSpecificOffsetRanges() {
        TopicPartition dlt0 = new TopicPartition(DLT, 0);

        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(
                2,
                Map.of(dlt0, new DeadLetterReader.PartitionBounds(10, 12)));

        DeadLetterSummary.Group group = new DeadLetterSummary.Group(
                new DeadLetterSummary.Key("WAREHOUSE_REJECTED", "batch_job_instance", "bank-a"),
                2,
                2,
                0,
                List.of(
                        new DeadLetterSummary.OffsetRange(
                                "fineract.cdc.batch_job_instance", 0, 100, 105),
                        new DeadLetterSummary.OffsetRange(
                                "fineract.cdc.batch_job_instance", 1, 200, 200)),
                List.of(new DeadLetterSummary.OffsetRange(DLT, 0, 10, 11)));

        String output = render(scan, List.of(group));

        assertThat(output)
                .contains("Dead-letter inspection")
                .contains("Records scanned: 2")
                .contains("Partitions scanned: 1")
                .contains("WAREHOUSE_REJECTED")
                .contains("batch_job_instance")
                .contains("bank-a")
                .contains("Original offsets")
                .contains("fineract.cdc.batch_job_instance[0]:100..105")
                .contains("fineract.cdc.batch_job_instance[1]:200..200")
                .contains("fineract.cdc.dlt[0]:10..11");
    }

    @Test
    public void displaysEmptyDltMessage() {
        DeadLetterReader.ScanResult scan =
                new DeadLetterReader.ScanResult(0, Map.of());

        String output = render(scan, List.of());

        assertThat(output)
                .contains("Records scanned: 0")
                .contains("No dead-lettered records found.");
    }

    @Test
    public void escapesControlCharactersInDisplayedMetadata() {
        DeadLetterReader.ScanResult scan =
                new DeadLetterReader.ScanResult(1, Map.of());

        DeadLetterSummary.Group group = new DeadLetterSummary.Group(
                new DeadLetterSummary.Key("BAD\nREASON", "batch_job_instance", "bank-a"),
                1,
                1,
                0,
                List.of(),
                List.of());

        String output = render(scan, List.of(group));

        assertThat(output)
                .contains("BAD\\nREASON")
                .doesNotContain("BAD\nREASON");
    }

    @Test
    public void displaysWarningWhenByteBudgetReached() {
        TopicPartition dlt0 = new TopicPartition(DLT, 0);
        TopicPartition dlt1 = new TopicPartition(DLT, 1);

        DeadLetterReader.ScanResult scan = new DeadLetterReader.ScanResult(
                10,
                10,
                512,
                Map.of(
                        dlt0, new DeadLetterReader.PartitionBounds(0, 100),
                        dlt1, new DeadLetterReader.PartitionBounds(0, 100)),
                DeadLetterReader.StopReason.BYTE_BUDGET_REACHED);

        String output = render(scan, List.of());

        assertThat(output)
                .contains("Dead-letter inspection")
                .contains("Records scanned: 10")
                .contains("Records matched: 10")
                .contains("Bytes scanned: 512")
                .contains("Stop reason: BYTE_BUDGET_REACHED")
                .contains("Warning: Listing stopped early because the byte budget was reached; not all partitions were fully scanned.")
                .contains("Partitions in snapshot: 2 (scan stopped early: BYTE_BUDGET_REACHED)");
    }

    @Test
    public void defaultListingDoesNotCreateDatasourceOrAccessPostgres() throws Exception {
        ConsumerRecord<byte[], byte[]> r = createConsumerRecord(DLT, 0, 10L, "bank-a", "batch_job_instance", 100L, UUID.randomUUID(), false);
        MockConsumer<byte[], byte[]> consumer = createMockConsumer(DLT, List.of(r));
        DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT);

        AtomicInteger closeCalls = new AtomicInteger(0);
        Connection conn = mockConnection(Map.of(), new ArrayList<>(), closeCalls, null);
        MockDataSource ds = new MockDataSource(conn);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DeadLetterListCommand.run(testConfig(), CONTRACT, new String[0], new PrintStream(out, true, StandardCharsets.UTF_8), ds, reader);

        String result = out.toString(StandardCharsets.UTF_8);
        assertThat(result)
                .contains("Records scanned: 1")
                .contains("batch_job_instance")
                .doesNotContain("Historical replay ledger outcomes");

        assertThat(ds.getConnectionCalls.get()).isZero();
        assertThat(consumer.closed()).isTrue();
    }

    @Test
    public void withLedgerDisplaysAppliedOutcome() throws Exception {
        UUID appliedUuid = UUID.randomUUID();
        ConsumerRecord<byte[], byte[]> r = createConsumerRecord(DLT, 0, 10L, "bank-a", "batch_job_instance", 100L, appliedUuid, false);
        MockConsumer<byte[], byte[]> consumer = createMockConsumer(DLT, List.of(r));
        DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT);

        List<UUID> queriedUuids = new ArrayList<>();
        Connection conn = mockConnection(Map.of(appliedUuid, ReplayOutcome.APPLIED), queriedUuids, new AtomicInteger(0), null);
        MockDataSource ds = new MockDataSource(conn);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DeadLetterListCommand.run(testConfig(), CONTRACT, new String[]{"--with-ledger"},
                new PrintStream(out, true, StandardCharsets.UTF_8), ds, reader);

        String result = out.toString(StandardCharsets.UTF_8);
        assertThat(result)
                .contains("Historical replay ledger outcomes (meta.replay_ledger):")
                .contains("- APPLIED: 1")
                .contains("- SKIPPED_STALE: 0")
                .contains("- No ledger entry: 0")
                .contains("Historical record outcomes:")
                .contains(appliedUuid.toString() + "\tAPPLIED");

        assertThat(queriedUuids).containsExactly(appliedUuid);
        assertThat(ds.getConnectionCalls.get()).isEqualTo(1);
        assertThat(consumer.closed()).isTrue();
    }

    @Test
    public void withLedgerDisplaysSkippedStaleAndSkippedSuperseded() throws Exception {
        UUID staleUuid = UUID.randomUUID();
        UUID supersededUuid = UUID.randomUUID();

        ConsumerRecord<byte[], byte[]> r1 = createConsumerRecord(DLT, 0, 10L, "bank-a", "batch_job_instance", 100L, staleUuid, false);
        ConsumerRecord<byte[], byte[]> r2 = createConsumerRecord(DLT, 0, 11L, "bank-a", "batch_job_instance", 101L, supersededUuid, false);
        MockConsumer<byte[], byte[]> consumer = createMockConsumer(DLT, List.of(r1, r2));
        DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT);

        Connection conn = mockConnection(
                Map.of(staleUuid, ReplayOutcome.SKIPPED_STALE, supersededUuid, ReplayOutcome.SKIPPED_SUPERSEDED),
                new ArrayList<>(), new AtomicInteger(0), null);
        MockDataSource ds = new MockDataSource(conn);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DeadLetterListCommand.run(testConfig(), CONTRACT, new String[]{"--with-ledger"},
                new PrintStream(out, true, StandardCharsets.UTF_8), ds, reader);

        String result = out.toString(StandardCharsets.UTF_8);
        assertThat(result)
                .contains("- APPLIED: 0")
                .contains("- SKIPPED_STALE: 1")
                .contains("- SKIPPED_SUPERSEDED: 1")
                .contains(staleUuid.toString() + "\tSKIPPED_STALE")
                .contains(supersededUuid.toString() + "\tSKIPPED_SUPERSEDED");
    }

    @Test
    public void withLedgerDisplaysLegacyUnknownAndNotAsApplied() throws Exception {
        UUID legacyUuid = UUID.randomUUID();

        ConsumerRecord<byte[], byte[]> r = createConsumerRecord(DLT, 0, 10L, "bank-a", "batch_job_instance", 100L, legacyUuid, false);
        MockConsumer<byte[], byte[]> consumer = createMockConsumer(DLT, List.of(r));
        DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT);

        Connection conn = mockConnection(Map.of(legacyUuid, ReplayOutcome.LEGACY_UNKNOWN),
                new ArrayList<>(), new AtomicInteger(0), null);
        MockDataSource ds = new MockDataSource(conn);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DeadLetterListCommand.run(testConfig(), CONTRACT, new String[]{"--with-ledger"},
                new PrintStream(out, true, StandardCharsets.UTF_8), ds, reader);

        String result = out.toString(StandardCharsets.UTF_8);
        assertThat(result)
                .contains("- APPLIED: 0")
                .contains("- LEGACY_UNKNOWN: 1")
                .contains(legacyUuid.toString() + "\tLEGACY_UNKNOWN")
                .doesNotContain(legacyUuid.toString() + "\tAPPLIED");
    }

    @Test
    public void withLedgerDisplaysNoLedgerEntryNotUnprocessed() throws Exception {
        UUID absentUuid = UUID.randomUUID();

        ConsumerRecord<byte[], byte[]> r = createConsumerRecord(DLT, 0, 10L, "bank-a", "batch_job_instance", 100L, absentUuid, false);
        MockConsumer<byte[], byte[]> consumer = createMockConsumer(DLT, List.of(r));
        DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT);

        Connection conn = mockConnection(Map.of(), new ArrayList<>(), new AtomicInteger(0), null);
        MockDataSource ds = new MockDataSource(conn);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DeadLetterListCommand.run(testConfig(), CONTRACT, new String[]{"--with-ledger"},
                new PrintStream(out, true, StandardCharsets.UTF_8), ds, reader);

        String result = out.toString(StandardCharsets.UTF_8);
        assertThat(result)
                .contains("- No ledger entry: 1")
                .contains(absentUuid.toString() + "\tNo ledger entry")
                .doesNotContain("Unprocessed");
    }

    @Test
    public void withLedgerHandlesMissingOrMalformedReplayIdentityExplicitly() throws Exception {
        ConsumerRecord<byte[], byte[]> invalidRecord = createConsumerRecord(
                DLT, 0, 10L, "bank-a", "batch_job_instance", 100L, null, true);
        MockConsumer<byte[], byte[]> consumer = createMockConsumer(DLT, List.of(invalidRecord));
        DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT);

        List<UUID> queriedUuids = new ArrayList<>();
        Connection conn = mockConnection(Map.of(), queriedUuids, new AtomicInteger(0), null);
        MockDataSource ds = new MockDataSource(conn);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DeadLetterListCommand.run(testConfig(), CONTRACT, new String[]{"--with-ledger"},
                new PrintStream(out, true, StandardCharsets.UTF_8), ds, reader);

        String result = out.toString(StandardCharsets.UTF_8);
        assertThat(result)
                .contains("Warning: Found 1 record(s) with invalid replay identity metadata")
                .contains("- Identity unavailable: 1")
                .contains("- No ledger entry: 0")
                .contains("Identity unavailable (Invalid UUID in replay identity header fbi.replay.id)");

        assertThat(queriedUuids).isEmpty();
    }

    @Test
    public void withLedgerHandlesDuplicateReplayIdentitiesCorrectly() throws Exception {
        UUID sharedUuid = UUID.randomUUID();

        ConsumerRecord<byte[], byte[]> r1 = createConsumerRecord(DLT, 0, 10L, "bank-a", "batch_job_instance", 100L, sharedUuid, false);
        ConsumerRecord<byte[], byte[]> r2 = createConsumerRecord(DLT, 0, 11L, "bank-a", "batch_job_instance", 101L, sharedUuid, false);
        MockConsumer<byte[], byte[]> consumer = createMockConsumer(DLT, List.of(r1, r2));
        DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT);

        List<UUID> queriedUuids = new ArrayList<>();
        Connection conn = mockConnection(Map.of(sharedUuid, ReplayOutcome.APPLIED), queriedUuids, new AtomicInteger(0), null);
        MockDataSource ds = new MockDataSource(conn);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DeadLetterListCommand.run(testConfig(), CONTRACT, new String[]{"--with-ledger"},
                new PrintStream(out, true, StandardCharsets.UTF_8), ds, reader);

        String result = out.toString(StandardCharsets.UTF_8);
        assertThat(result)
                .contains("- APPLIED: 2")
                .contains("fineract.cdc.dlt[0]@10\tfineract.cdc.batch_job_instance[0]@100\t" + sharedUuid + "\tAPPLIED")
                .contains("fineract.cdc.dlt[0]@11\tfineract.cdc.batch_job_instance[0]@101\t" + sharedUuid + "\tAPPLIED");

        assertThat(queriedUuids).containsExactly(sharedUuid);
    }

    @Test
    public void withLedgerEmptyDltScanWorksCleanly() throws Exception {
        MockConsumer<byte[], byte[]> consumer = createMockConsumer(DLT, List.of());
        DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT);

        Connection conn = mockConnection(Map.of(), new ArrayList<>(), new AtomicInteger(0), null);
        MockDataSource ds = new MockDataSource(conn);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DeadLetterListCommand.run(testConfig(), CONTRACT, new String[]{"--with-ledger"},
                new PrintStream(out, true, StandardCharsets.UTF_8), ds, reader);

        String result = out.toString(StandardCharsets.UTF_8);
        assertThat(result)
                .contains("Records scanned: 0")
                .contains("No dead-lettered records found.");

        assertThat(consumer.closed()).isTrue();
    }

    @Test
    public void withLedgerQueryFailureProducesClearError() throws Exception {
        UUID uuid = UUID.randomUUID();
        ConsumerRecord<byte[], byte[]> r = createConsumerRecord(DLT, 0, 10L, "bank-a", "batch_job_instance", 100L, uuid, false);
        MockConsumer<byte[], byte[]> consumer = createMockConsumer(DLT, List.of(r));
        DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT);

        SQLException dbErr = new SQLException("PostgreSQL connection refused", "08001");
        Connection conn = mockConnection(Map.of(), new ArrayList<>(), new AtomicInteger(0), dbErr);
        MockDataSource ds = new MockDataSource(conn);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertThatThrownBy(() -> DeadLetterListCommand.run(
                testConfig(), CONTRACT, new String[]{"--with-ledger"},
                new PrintStream(out, true, StandardCharsets.UTF_8), ds, reader))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to inspect replay ledger")
                .hasCause(dbErr);

        assertThat(consumer.closed()).isTrue();
    }

    @Test
    public void withLedgerCleansUpKafkaConsumerAndDatabaseResourcesOnSuccessAndFailure() throws Exception {
        UUID uuid = UUID.randomUUID();
        AtomicInteger closeCalls = new AtomicInteger(0);

        ConsumerRecord<byte[], byte[]> r1 = createConsumerRecord(DLT, 0, 10L, "bank-a", "batch_job_instance", 100L, uuid, false);
        MockConsumer<byte[], byte[]> consumer1 = createMockConsumer(DLT, List.of(r1));
        DeadLetterReader reader1 = new DeadLetterReader(consumer1, DLT, CONTRACT);

        Connection conn1 = mockConnection(Map.of(uuid, ReplayOutcome.APPLIED), new ArrayList<>(), closeCalls, null);
        MockDataSource ds1 = new MockDataSource(conn1);

        DeadLetterListCommand.run(testConfig(), CONTRACT, new String[]{"--with-ledger"},
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8), ds1, reader1);

        assertThat(consumer1.closed()).isTrue();
        assertThat(closeCalls.get()).isEqualTo(1);
        assertThat(ds1.evictCalls.get()).isZero();

        closeCalls.set(0);
        ConsumerRecord<byte[], byte[]> r2 = createConsumerRecord(DLT, 0, 11L, "bank-a", "batch_job_instance", 101L, uuid, false);
        MockConsumer<byte[], byte[]> consumer2 = createMockConsumer(DLT, List.of(r2));
        DeadLetterReader reader2 = new DeadLetterReader(consumer2, DLT, CONTRACT);

        SQLException queryFailure = new SQLException("Table meta.replay_ledger does not exist", "42P01");
        Connection conn2 = mockConnection(Map.of(), new ArrayList<>(), closeCalls, queryFailure);
        MockDataSource ds2 = new MockDataSource(conn2);

        assertThatThrownBy(() -> DeadLetterListCommand.run(
                testConfig(), CONTRACT, new String[]{"--with-ledger"},
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8), ds2, reader2))
                .isInstanceOf(IllegalStateException.class);

        assertThat(consumer2.closed()).isTrue();
        assertThat(closeCalls.get()).isEqualTo(1);
        assertThat(ds2.evictCalls.get()).isEqualTo(1);
    }

    @Test
    public void cliArgumentParsingInvariants() {
        assertThatThrownBy(() -> DeadLetterListCommand.parseArgs(new String[]{"--foo"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown option: '--foo'.");

        assertThatThrownBy(() -> DeadLetterListCommand.parseArgs(new String[]{"positional"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unexpected positional argument: 'positional'. Options must start with '--'.");

        assertThatThrownBy(() -> DeadLetterListCommand.parseArgs(new String[]{"--with-ledger", "--with-ledger"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate option: '--with-ledger'.");

        assertThatThrownBy(() -> DeadLetterListCommand.parseArgs(new String[]{"--"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Empty option name: '--'.");

        assertThatThrownBy(() -> DeadLetterListCommand.parseArgs(new String[]{"--with-ledger=maybe"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid boolean value 'maybe' for option '--with-ledger'. Expected 'true' or 'false'.");

        Map<String, String> opts = DeadLetterListCommand.parseArgs(new String[]{"--with-ledger=false"});
        assertThat(opts.get("with-ledger")).isEqualTo("false");

        Map<String, String> opts2 = DeadLetterListCommand.parseArgs(new String[]{"--with-ledger"});
        assertThat(opts2.get("with-ledger")).isEqualTo("true");

        ConsumerRecord<byte[], byte[]> r = createConsumerRecord(DLT, 0, 10L, "bank-a", "batch_job_instance", 100L, UUID.randomUUID(), false);
        MockConsumer<byte[], byte[]> consumer = createMockConsumer(DLT, List.of(r));
        DeadLetterReader reader = new DeadLetterReader(consumer, DLT, CONTRACT);
        assertThatThrownBy(() -> DeadLetterListCommand.run(
                testConfig(), CONTRACT, new String[]{"--limit=0"},
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8), null, reader))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Limit must be positive: 0");

        MockConsumer<byte[], byte[]> consumer2 = createMockConsumer(DLT, List.of(r));
        DeadLetterReader reader2 = new DeadLetterReader(consumer2, DLT, CONTRACT);
        assertThatThrownBy(() -> DeadLetterListCommand.run(
                testConfig(), CONTRACT, new String[]{"--max-bytes=-10"},
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8), null, reader2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("max-bytes must be positive: -10");
    }
}
