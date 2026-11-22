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

import com.zaxxer.hikari.HikariConfig;
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
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.fineract.bi.ingestion.cdc.ChangeEvent;
import org.apache.fineract.bi.ingestion.cdc.DebeziumEnvelopeParser;
import org.apache.fineract.bi.ingestion.config.Env;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.fineract.bi.ingestion.contract.ContractVerifier;
import org.apache.fineract.bi.ingestion.routing.OwnershipCache;
import org.apache.fineract.bi.ingestion.routing.Route;
import org.apache.fineract.bi.ingestion.sink.BatchWriter;
import org.apache.fineract.bi.ingestion.sink.PreflightRollbackException;
import org.apache.fineract.bi.ingestion.sink.ReplayIdentity;
import org.apache.fineract.bi.ingestion.sink.ReplayOutcome;
import org.apache.fineract.bi.ingestion.sink.RoutedEvent;
import org.apache.fineract.bi.ingestion.sink.WarehouseWriter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SinkKafkaPostgresTest {

    private static final String TABLE = "batch_job_instance";
    private static final String TOPIC = "fineract.cdc.batch_job_instance";
    private static final String DLT = "fineract.cdc.dlt";
    private static final String SOURCE = "bank-a";
    private static final int PARTITIONS = 2;
    private static final Path ROOT = Path.of("../..");

    private PostgreSQLContainer postgres;
    private KafkaContainer kafka;
    private HikariDataSource engineSource;
    private Contract contract;
    private KafkaProducer<byte[], byte[]> producer;
    private long nextId = 1;

    @BeforeClass
    public void startContainers() throws Exception {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new SkipException("Docker is not available");
        }
        contract = ContractLoader.load(ROOT.resolve("contracts/fineract_tables.json"));
        postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:18.3"))
                .withDatabaseName("analytics")
                .withUsername("analytics_admin")
                .withPassword("ci-only")
                .withCopyFileToContainer(MountableFile.forHostPath(ROOT.resolve("warehouse/schema")), "/schema")
                .withCopyFileToContainer(MountableFile.forHostPath(ROOT.resolve("contracts")), "/contracts");
        kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.1"));
        postgres.start();
        kafka.start();
        bootstrapWarehouse();

        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1), new NewTopic(DLT, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
        Properties p = new Properties();
        p.put("bootstrap.servers", kafka.getBootstrapServers());
        p.put("key.serializer", ByteArraySerializer.class.getName());
        p.put("value.serializer", ByteArraySerializer.class.getName());
        p.put("acks", "all");
        producer = new KafkaProducer<>(p);

        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(postgres.getJdbcUrl());
        hc.setUsername("analytics_engine");
        hc.setPassword("engine-only");
        hc.setMaximumPoolSize(8);
        hc.setAutoCommit(false);
        engineSource = new HikariDataSource(hc);
    }

    @AfterClass(alwaysRun = true)
    public void stopContainers() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
        if (engineSource != null) {
            engineSource.close();
        }
        if (kafka != null) {
            kafka.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
    }

    private void bootstrapWarehouse() throws Exception {
        admin("psql -U analytics_admin -d analytics -v ON_ERROR_STOP=1 -q "
                + "-c \"CREATE ROLE analytics_engine LOGIN PASSWORD 'engine-only'\"");
        admin("cd /schema && psql -U analytics_admin -d analytics -v ON_ERROR_STOP=1 -q "
                + "-f platform.sql -f access_control.sql");
        admin("psql -U analytics_admin -d analytics -v ON_ERROR_STOP=1 -q "
                + "-c \"SELECT meta.load_table_contract(pg_read_file('/contracts/fineract_tables.json')::jsonb)\" "
                + "-c \"SELECT meta.provision_raw_tables()\"");
        admin("psql -U analytics_admin -d analytics -v ON_ERROR_STOP=1 -q -c \""
                + "GRANT USAGE ON SCHEMA raw, raw_shadow, meta TO analytics_engine; "
                + "GRANT TEMPORARY ON DATABASE analytics TO analytics_engine; "
                + "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA raw, raw_shadow TO analytics_engine; "
                + "GRANT SELECT ON ALL TABLES IN SCHEMA meta TO analytics_engine; "
                + "GRANT INSERT, UPDATE, DELETE ON meta.consumer_offsets, meta.extract_watermarks, "
                + "meta.ingestion_progress, meta.ingestion_audit, meta.pipeline_state TO analytics_engine; "
                + "GRANT INSERT ON meta.entity_change_log TO analytics_engine; "
                + "GRANT SELECT, INSERT, UPDATE (outcome) ON meta.replay_ledger TO analytics_engine; "
                + "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA meta TO analytics_engine\"");
        update("INSERT INTO meta.source_registry (source_id, tenant_id, lifecycle_state) VALUES ('" + SOURCE
                + "', 'tenant-a', 'ACTIVE')");
        update("INSERT INTO meta.ingestion_ownership (source_id, table_name, engine) VALUES ('" + SOURCE + "', '"
                + TABLE + "', 'KAFKA')");
    }

    private void admin(String command) throws Exception {
        ExecResult r = postgres.execInContainer("sh", "-c", command);
        if (r.getExitCode() != 0) {
            throw new IllegalStateException("warehouse bootstrap failed: " + command + "\n" + r.getStderr());
        }
    }

    private long scalar(String sql) throws SQLException {
        try (Connection c = postgres.createConnection(""); Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private void update(String sql) throws SQLException {
        try (Connection c = postgres.createConnection(""); Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    private long rows() throws SQLException {
        return scalar("SELECT count(*) FROM raw.raw_" + TABLE + " WHERE source_id = '" + SOURCE + "'");
    }

    private long storedOffsets(String group) throws SQLException {
        return scalar("SELECT coalesce(sum(next_offset), 0) FROM meta.consumer_offsets WHERE topic = '" + TOPIC
                + "' AND consumer_group = '" + group + "'");
    }

    private long eventsConsumed() throws SQLException {
        return scalar("SELECT coalesce(sum(events_consumed), 0) FROM meta.ingestion_audit WHERE table_name = '"
                + TABLE + "'");
    }

    private void produce(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            long id = nextId++;
            produceRaw((int) (id % PARTITIONS), event(id));
        }
        producer.flush();
    }

    private void produceRaw(int partition, String json) throws Exception {
        producer.send(new ProducerRecord<>(TOPIC, partition, null, json.getBytes(StandardCharsets.UTF_8))).get();
    }

    private static String event(long id) {
        return "{\"after\":{\"job_instance_id\":" + id + ",\"job_name\":\"j" + id
                + "\",\"job_key\":\"k\"},\"source\":{\"name\":\"" + SOURCE + "\",\"table\":\"" + TABLE
                + "\",\"lsn\":" + (1000 + id) + ",\"ts_ms\":1767261600000},\"op\":\"c\"}";
    }

    interface Condition {
        boolean holds() throws Exception;
    }

    static void await(String what, Condition condition) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.holds()) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Timed out waiting for " + what);
    }

    private final class Running implements AutoCloseable {
        final SinkWorker worker;
        final Thread thread;
        final OwnershipCache ownership;
        final ScheduledExecutorService timers = Executors.newSingleThreadScheduledExecutor();
        final DeadLetterPublisher letters;
        final ProgressReporter progress;

        Running(String group, String writer, boolean autoRefresh) throws Exception {
            Map<String, String> env = new HashMap<>();
            env.put("SINK_BOOTSTRAP_SERVERS", kafka.getBootstrapServers());
            env.put("SINK_GROUP_ID", group);
            env.put("SINK_MAX_BATCH_WAIT_MS", "50");
            env.put("SINK_POLL_TIMEOUT_MS", "100");
            env.put("SINK_FETCH_MIN_BYTES", "1");
            env.put("SINK_MAX_BACKOFF_MS", "500");
            env.put("SINK_WRITER_ID", writer);
            env.put("SINK_START_FROM_EARLIEST_WITHOUT_DB_OFFSET", "true");
            SinkConfig config = SinkConfig.from(Env.of(env), env);
            ownership = new OwnershipCache(engineSource, 0);
            ownership.refresh(true);
            if (autoRefresh) {
                timers.scheduleWithFixedDelay(ownership::refreshQuietly, 200, 200, TimeUnit.MILLISECONDS);
            }
            letters = new DeadLetterPublisher(config);
            progress = new ProgressReporter(engineSource, writer);
            worker = new SinkWorker(0, config, engineSource::getConnection, ownership,
                    new DebeziumEnvelopeParser(contract), new WarehouseWriter(contract, "KAFKA"), letters,
                    new SinkMetrics(), progress);
            thread = new Thread(worker, "it-" + writer);
            thread.start();
        }

        @Override
        public void close() throws Exception {
            worker.shutdown();
            thread.join(30_000);
            progress.flush();
            timers.shutdownNow();
            letters.close();
        }
    }

    private List<String> deadLetters() {
        Properties p = new Properties();
        p.put("bootstrap.servers", kafka.getBootstrapServers());
        p.put("group.id", "dlt-reader-" + UUID.randomUUID());
        p.put("auto.offset.reset", "earliest");
        List<String> values = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> c =
                new KafkaConsumer<>(p, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            c.subscribe(List.of(DLT));
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline && values.isEmpty()) {
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500))) {
                    values.add(new String(r.value() == null ? new byte[0] : r.value(), StandardCharsets.UTF_8));
                }
            }
        }
        return values;
    }

    private ConsumerRecord<byte[], byte[]> awaitDltRecord(
            java.util.function.Predicate<ConsumerRecord<byte[], byte[]>> predicate, long timeoutMs) throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", kafka.getBootstrapServers());
        p.put("group.id", "dlt-await-" + UUID.randomUUID());
        p.put("auto.offset.reset", "earliest");
        try (KafkaConsumer<byte[], byte[]> c =
                new KafkaConsumer<>(p, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            c.subscribe(List.of(DLT));
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(300))) {
                    if (predicate.test(r)) {
                        return r;
                    }
                }
            }
        }
        throw new AssertionError("Timed out waiting for matching DLT record");
    }

    @Test
    public void everyEventIsAppliedOnceAndTheOffsetsLiveInTheWarehouse() throws Exception {
        try (Running sink = new Running("g-basic", "w1", true)) {
            produce(200);
            await("200 rows", () -> rows() == 200);
            await("offsets stored", () -> storedOffsets("g-basic") == 200);
        }
        assertThat(rows()).isEqualTo(200);
        assertThat(storedOffsets("g-basic")).isEqualTo(200);
        assertThat(eventsConsumed()).as("no event was consumed twice").isEqualTo(200);
    }

    @Test(dependsOnMethods = "everyEventIsAppliedOnceAndTheOffsetsLiveInTheWarehouse")
    public void aRestartedSinkResumesFromTheWarehouseOffsetsWithoutReplaying() throws Exception {
        produce(100);
        try (Running sink = new Running("g-basic", "w1", true)) {
            await("300 rows", () -> rows() == 300);
            await("offsets stored", () -> storedOffsets("g-basic") == 300);
        }
        assertThat(eventsConsumed()).as("the restarted sink read only the new events").isEqualTo(300);
    }

    @Test(dependsOnMethods = "aRestartedSinkResumesFromTheWarehouseOffsetsWithoutReplaying")
    public void aSecondSinkJoiningTheGroupRebalancesWithoutLossOrDuplicates() throws Exception {
        try (Running first = new Running("g-rebalance", "w-a", true)) {
            produce(100);
            await("400 rows", () -> rows() == 400);
            try (Running second = new Running("g-rebalance", "w-b", true)) {
                produce(300);
                await("700 rows", () -> rows() == 700);
                await("offsets stored", () -> storedOffsets("g-rebalance") == 700);
            }
            produce(100);
            await("800 rows", () -> rows() == 800);
        }
        assertThat(rows()).isEqualTo(800);
        assertThat(storedOffsets("g-rebalance")).isEqualTo(800);
    }

    @Test(dependsOnMethods = "aSecondSinkJoiningTheGroupRebalancesWithoutLossOrDuplicates")
    public void aPoisonEventIsDeadLetteredBeforeTheOffsetMovesPastIt() throws Exception {
        long before = rows();
        long offsetsBefore = storedOffsets("g-rebalance");
        try (Running sink = new Running("g-rebalance", "w-c", true)) {
            produceRaw(0, event(nextId++));
            produceRaw(0, "{this is not json");
            produceRaw(0, event(nextId++));
            producer.flush();
            await("two good rows", () -> rows() == before + 2);
            await("offsets past the poison event", () -> storedOffsets("g-rebalance") == offsetsBefore + 3);
        }
        assertThat(deadLetters()).isNotEmpty();
    }

    @Test(dependsOnMethods = "aPoisonEventIsDeadLetteredBeforeTheOffsetMovesPastIt")
    public void pausedOwnershipHoldsEventsAndNothingIsLostWhenItResumes() throws Exception {
        long before = rows();
        long offsetsBefore = storedOffsets("g-rebalance");
        update("UPDATE meta.ingestion_ownership SET cutover_state = 'PAUSED' WHERE source_id = '" + SOURCE + "'");
        try (Running sink = new Running("g-rebalance", "w-d", true)) {
            produce(20);
            Thread.sleep(3_000);
            assertThat(rows()).as("a paused table receives nothing").isEqualTo(before);
            assertThat(storedOffsets("g-rebalance")).as("the held offsets do not move").isEqualTo(offsetsBefore);
            update("UPDATE meta.ingestion_ownership SET cutover_state = 'STEADY' WHERE source_id = '" + SOURCE + "'");
            await("held events applied", () -> rows() == before + 20);
            await("offsets advanced", () -> storedOffsets("g-rebalance") == offsetsBefore + 20);
        }
    }

    @Test(dependsOnMethods = "pausedOwnershipHoldsEventsAndNothingIsLostWhenItResumes")
    public void aStaleEpochIsFencedByTheDatabaseAndTheBatchIsRetriedWithTheNewOne() throws Exception {
        long before = rows();
        try (Running sink = new Running("g-fence", "w-e", false)) {
            await("group caught up", () -> rows() >= before);
            update("UPDATE meta.ingestion_ownership SET epoch = epoch + 1 WHERE source_id = '" + SOURCE + "'");
            produce(10);
            await("fenced batch re-routed and applied", () -> rows() == before + 10);
        }
        assertThat(scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "'"))
                .isGreaterThan(1L);
    }

    @Test
    public void theStartupContractCheckCatchesDriftBetweenTheSinkAndTheWarehouse() throws Exception {
        String original;
        try (Connection c = postgres.createConnection(""); Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT columns::text FROM meta.table_contract WHERE table_name = '"
                        + TABLE + "'")) {
            rs.next();
            original = rs.getString(1);
        }
        try (Connection c = postgres.createConnection("")) {
            assertThat(ContractVerifier.differences(c, contract)).isEmpty();
            update("UPDATE meta.table_contract SET columns = jsonb_set(columns, '{1,type}', '\"bigint\"') "
                    + "WHERE table_name = '" + TABLE + "'");
            assertThat(ContractVerifier.differences(c, contract))
                    .anyMatch(d -> d.startsWith(TABLE + ".") && d.contains("type bigint in the warehouse"));
            assertThatThrownBy(() -> ContractVerifier.verify(c, contract)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("does not match meta.table_contract");
        } finally {
            try (Connection c = postgres.createConnection("");
                    java.sql.PreparedStatement ps = c.prepareStatement(
                            "UPDATE meta.table_contract SET columns = ?::jsonb WHERE table_name = ?")) {
                ps.setString(1, original);
                ps.setString(2, TABLE);
                ps.executeUpdate();
            }
        }
    }

    @Test(dependsOnMethods = "aStaleEpochIsFencedByTheDatabaseAndTheBatchIsRetriedWithTheNewOne")
    public void transactionRollbackAfterWarehouseRejectionAndSuccessfulRetry() throws Exception {
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);
        UUID replayUuid = UUID.randomUUID();
        ReplayIdentity replayId = new ReplayIdentity(replayUuid, DLT, 0, 5555L);
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");

        ChangeEvent badEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {"99999999999999999999999999999999999999999999999999", "bad_job", "bad_key"},
                List.of(), 5001L, System.currentTimeMillis() * 1000L);
        RoutedEvent badRouted = new RoutedEvent(badEvent, route, replayId);

        try (Connection c = engineSource.getConnection()) {
            assertThatThrownBy(() -> writer.write(c, List.of(badRouted), List.of(), "g-rb-test", List.of()))
                    .isInstanceOf(SQLException.class);
        }

        try (Connection c = postgres.createConnection("");
                PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM meta.replay_ledger WHERE replay_id = ?")) {
            ps.setObject(1, replayUuid);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isZero();
            }
        }
        assertThat(scalar("SELECT count(*) FROM raw.raw_" + TABLE + " WHERE job_instance_id = 99999999999999999999999999999999999999999999999999")).isZero();

        ChangeEvent goodEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {"55555", "fixed_job", "fixed_key"},
                List.of(), 5002L, System.currentTimeMillis() * 1000L);
        RoutedEvent goodRouted = new RoutedEvent(goodEvent, route, replayId);

        try (Connection c = engineSource.getConnection()) {
            writer.write(c, List.of(goodRouted), List.of(), "g-rb-test", List.of());
        }

        try (Connection c = postgres.createConnection("");
                PreparedStatement ps = c.prepareStatement(
                        "SELECT dlt_topic, dlt_partition, dlt_offset, source_id, table_name, outcome FROM meta.replay_ledger WHERE replay_id = ?")) {
            ps.setObject(1, replayUuid);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("dlt_topic")).isEqualTo(DLT);
                assertThat(rs.getInt("dlt_partition")).isEqualTo(0);
                assertThat(rs.getLong("dlt_offset")).isEqualTo(5555L);
                assertThat(rs.getString("source_id")).isEqualTo(SOURCE);
                assertThat(rs.getString("table_name")).isEqualTo(TABLE);
                assertThat(rs.getString("outcome")).isEqualTo("APPLIED");
                assertThat(rs.next()).isFalse();
            }
        }
        assertThat(scalar("SELECT count(*) FROM raw.raw_" + TABLE + " WHERE job_instance_id = 55555")).isEqualTo(1L);
    }

    @Test(dependsOnMethods = "transactionRollbackAfterWarehouseRejectionAndSuccessfulRetry")
    public void replayUuidDeduplicatesDifferentDltCoordinatesAndPersistsMetadata() throws Exception {
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);
        UUID sharedUuid = UUID.randomUUID();
        ReplayIdentity firstId = new ReplayIdentity(sharedUuid, DLT, 0, 8888L);
        ReplayIdentity secondId = new ReplayIdentity(sharedUuid, DLT, 1, 9999L);
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");

        ChangeEvent firstEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {"88888", "job_8888", "key_8888"},
                List.of(), 8001L, System.currentTimeMillis() * 1000L);
        RoutedEvent firstRouted = new RoutedEvent(firstEvent, route, firstId);

        ChangeEvent secondEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {"99999", "job_9999", "key_9999"},
                List.of(), 8002L, System.currentTimeMillis() * 1000L);
        RoutedEvent secondRouted = new RoutedEvent(secondEvent, route, secondId);

        try (Connection c = engineSource.getConnection()) {
            writer.write(c, List.of(firstRouted), List.of(), "g-dedup-test-1", List.of());
        }
        try (Connection c = engineSource.getConnection()) {
            writer.write(c, List.of(secondRouted), List.of(), "g-dedup-test-2", List.of());
        }

        assertThat(scalar("SELECT count(*) FROM raw.raw_" + TABLE + " WHERE job_instance_id = 88888")).isEqualTo(1L);
        assertThat(scalar("SELECT count(*) FROM raw.raw_" + TABLE + " WHERE job_instance_id = 99999")).isZero();

        try (Connection c = postgres.createConnection("");
                PreparedStatement ps = c.prepareStatement(
                        "SELECT replay_id, dlt_topic, dlt_partition, dlt_offset, source_id, table_name, outcome FROM meta.replay_ledger WHERE replay_id = ?")) {
            ps.setObject(1, sharedUuid);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getObject("replay_id", UUID.class)).isEqualTo(sharedUuid);
                assertThat(rs.getString("dlt_topic")).isEqualTo(DLT);
                assertThat(rs.getInt("dlt_partition")).isEqualTo(0);
                assertThat(rs.getLong("dlt_offset")).isEqualTo(8888L);
                assertThat(rs.getString("source_id")).isEqualTo(SOURCE);
                assertThat(rs.getString("table_name")).isEqualTo(TABLE);
                assertThat(rs.getString("outcome")).isEqualTo("APPLIED");
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test(dependsOnMethods = "replayUuidDeduplicatesDifferentDltCoordinatesAndPersistsMetadata")
    public void concurrentDuplicateReplaysDeduplicateAtDatabaseLevel() throws Exception {
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);
        ReplayIdentity replayId = new ReplayIdentity(DLT, 0, 6666L);
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");

        ChangeEvent event = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {"66666", "concurrent_job", "c_key"},
                List.of(), 6001L, System.currentTimeMillis() * 1000L);
        RoutedEvent routed = new RoutedEvent(event, route, replayId);

        int threads = 4;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(exec.submit(() -> {
                try (Connection c = engineSource.getConnection()) {
                    writer.write(c, List.of(routed), List.of(), "g-concurrent", List.of());
                    return null;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        exec.shutdown();

        assertThat(scalar("SELECT count(*) FROM meta.replay_ledger WHERE dlt_topic = '" + DLT + "' AND dlt_offset = 6666")).isEqualTo(1L);
        assertThat(scalar("SELECT count(*) FROM raw.raw_" + TABLE + " WHERE job_instance_id = 66666")).isEqualTo(1L);
    }

    @Test(dependsOnMethods = "concurrentDuplicateReplaysDeduplicateAtDatabaseLevel")
    public void sequentialDuplicateReplayAdvancesKafkaOffsetsWithoutDoubleApplying() throws Exception {
        long eventId = 77777L;
        ReplayIdentity replayId = new ReplayIdentity(DLT, 0, 7777L);

        try (Running sink = new Running("g-seq-replay", "w-seq", true)) {
            ProducerRecord<byte[], byte[]> pr1 = new ProducerRecord<>(TOPIC, 0, null, event(eventId).getBytes(StandardCharsets.UTF_8));
            ReplayIdentityHeaders.add(pr1.headers(), replayId);
            producer.send(pr1).get();

            await("first replay row applied", () -> scalar("SELECT count(*) FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + eventId) == 1L);
            await("first replay receipt recorded", () -> scalar("SELECT count(*) FROM meta.replay_ledger WHERE dlt_offset = 7777") == 1L);
            await("stored offset exists", () -> storedOffsets("g-seq-replay") >= 1);
            long off1 = storedOffsets("g-seq-replay");

            ProducerRecord<byte[], byte[]> pr2 = new ProducerRecord<>(TOPIC, 0, null, event(eventId).getBytes(StandardCharsets.UTF_8));
            ReplayIdentityHeaders.add(pr2.headers(), replayId);
            producer.send(pr2).get();

            await("offset advanced past duplicate", () -> storedOffsets("g-seq-replay") == off1 + 1);

            assertThat(scalar("SELECT count(*) FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + eventId)).isEqualTo(1L);
            assertThat(scalar("SELECT count(*) FROM meta.replay_ledger WHERE dlt_offset = 7777")).isEqualTo(1L);
        }
    }

    @Test(dependsOnMethods = "sequentialDuplicateReplayAdvancesKafkaOffsetsWithoutDoubleApplying")
    public void replayIdentitySurvivesMultipleDltCyclesWithKeyAndPartitionPreserved() throws Exception {
        try (Running sink = new Running("g-dlt-cycle", "w-cycle", true)) {
            byte[] key = "test-key-cycle".getBytes(StandardCharsets.UTF_8);
            producer.send(new ProducerRecord<>(TOPIC, 1, key, "{not_valid_json_cycle_1".getBytes(StandardCharsets.UTF_8))).get();

            ConsumerRecord<byte[], byte[]> dlt1 = awaitDltRecord(
                    r -> r.value() != null && new String(r.value(), StandardCharsets.UTF_8).contains("not_valid_json_cycle_1"), 15_000);

            assertThat(dlt1.key()).isEqualTo(key);
            assertThat(new String(dlt1.headers().lastHeader("fbi.source.partition").value(), StandardCharsets.UTF_8)).isEqualTo("1");
            assertThat(ReplayIdentityHeaders.read(dlt1.headers())).isEmpty();

            ReplayIdentity replayId = new ReplayIdentity(DLT, dlt1.partition(), dlt1.offset());

            ProducerRecord<byte[], byte[]> pr2 = new ProducerRecord<>(TOPIC, 1, key, "{not_valid_json_cycle_2".getBytes(StandardCharsets.UTF_8));
            ReplayIdentityHeaders.add(pr2.headers(), replayId);
            producer.send(pr2).get();

            ConsumerRecord<byte[], byte[]> dlt2 = awaitDltRecord(
                    r -> r.value() != null && new String(r.value(), StandardCharsets.UTF_8).contains("not_valid_json_cycle_2"), 15_000);

            assertThat(dlt2.key()).as("key preserved across cycles").isEqualTo(key);
            assertThat(new String(dlt2.headers().lastHeader("fbi.source.partition").value(), StandardCharsets.UTF_8))
                    .as("original partition preserved").isEqualTo("1");
            assertThat(ReplayIdentityHeaders.read(dlt2.headers()))
                    .as("replay identity survived second DLT cycle")
                    .contains(replayId);
        }
    }

    @Test(dependsOnMethods = "replayIdentitySurvivesMultipleDltCyclesWithKeyAndPartitionPreserved")
    public void dltReplayCommandEndToEndWithPreflight() throws Exception {
        String unregSource = "bank-unregistered-replay";
        long cmdJobId = 88001L;
        String json = "{\"after\":{\"job_instance_id\":" + cmdJobId + ",\"job_name\":\"cmd_job\",\"job_key\":\"k\"},"
                + "\"source\":{\"name\":\"" + unregSource + "\",\"table\":\"" + TABLE + "\",\"lsn\":8001,\"ts_ms\":1767261600000},\"op\":\"c\"}";

        try (Running sink = new Running("g-cmd-replay", "w-cmd", true)) {
            org.apache.kafka.clients.producer.RecordMetadata rm = producer.send(
                    new ProducerRecord<>(TOPIC, 0, "k-cmd".getBytes(StandardCharsets.UTF_8), json.getBytes(StandardCharsets.UTF_8))).get();
            long origOffset = rm.offset();

            ConsumerRecord<byte[], byte[]> dltRecord = awaitDltRecord(
                    r -> r.value() != null && new String(r.value(), StandardCharsets.UTF_8).contains("bank-unregistered-replay"), 15_000);

            assertThat(new String(dltRecord.headers().lastHeader("fbi.dlt.reason").value(), StandardCharsets.UTF_8)).isEqualTo("UNKNOWN_SOURCE");
            long dltOffset = dltRecord.offset();

            Map<String, String> env = new HashMap<>();
            env.put("SINK_BOOTSTRAP_SERVERS", kafka.getBootstrapServers());
            env.put("SINK_DB_URL", postgres.getJdbcUrl());
            env.put("SINK_DB_USER", "analytics_engine");
            env.put("SINK_DB_PASSWORD", "engine-only");
            env.put("SINK_DLT_TOPIC", DLT);
            SinkConfig cmdConfig = SinkConfig.from(Env.of(env), env);

            DeadLetterReplayCommand.run(cmdConfig, contract, new String[] {
                    "--reason=UNKNOWN_SOURCE",
                    "--source=" + unregSource,
                    "--table=" + TABLE,
                    "--offset-range=" + TOPIC + "[0]:" + origOffset + ".." + origOffset,
                    "--dry-run"
            });

            update("INSERT INTO meta.source_registry (source_id, tenant_id, lifecycle_state) VALUES ('" + unregSource + "', 'tenant-cmd', 'ACTIVE')");
            update("INSERT INTO meta.ingestion_ownership (source_id, table_name, engine) VALUES ('" + unregSource + "', '" + TABLE + "', 'KAFKA')");
            sink.ownership.refresh(true);

            DeadLetterReplayCommand.run(cmdConfig, contract, new String[] {
                    "--reason=UNKNOWN_SOURCE",
                    "--source=" + unregSource,
                    "--table=" + TABLE,
                    "--offset-range=" + TOPIC + "[0]:" + origOffset + ".." + origOffset,
                    "--execute"
            });

            await("replayed row in warehouse", () -> scalar("SELECT count(*) FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + cmdJobId) == 1L);
            await("replay receipt in ledger", () -> scalar("SELECT count(*) FROM meta.replay_ledger WHERE dlt_offset = " + dltOffset) == 1L);
        }
    }

    @Test(dependsOnMethods = "dltReplayCommandEndToEndWithPreflight")
    public void replayOutcomesDistinguishAppliedFromStaleAndDeletes() throws Exception {
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");

        long entityId = 123450L;
        ChangeEvent baseEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityId), "job_initial", "key_initial"},
                List.of(), 1000L, System.currentTimeMillis() * 1000L);
        try (Connection c = engineSource.getConnection()) {
            writer.write(c, List.of(new RoutedEvent(baseEvent, route, null)), List.of(), "g-outcome-test", List.of());
        }

        UUID replayAppliedUuid = UUID.randomUUID();
        ReplayIdentity replayAppliedId = new ReplayIdentity(replayAppliedUuid, DLT, 0, 1100L);
        ChangeEvent appliedEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityId), "job_applied", "key_applied"},
                List.of(), 1100L, System.currentTimeMillis() * 1000L);
        try (Connection c = engineSource.getConnection()) {
            BatchWriter.WriteResult res = writer.write(c, List.of(new RoutedEvent(appliedEvent, route, replayAppliedId)), List.of(), "g-outcome-test", List.of());
            assertThat(res.replayOutcomes().get(replayAppliedId)).isEqualTo(ReplayOutcome.APPLIED);
        }
        assertThat(queryLedgerOutcome(replayAppliedUuid)).isEqualTo("APPLIED");

        UUID replayStaleUuid = UUID.randomUUID();
        ReplayIdentity replayStaleId = new ReplayIdentity(replayStaleUuid, DLT, 0, 900L);
        ChangeEvent staleEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityId), "job_stale", "key_stale"},
                List.of(), 900L, System.currentTimeMillis() * 1000L);
        try (Connection c = engineSource.getConnection()) {
            BatchWriter.WriteResult res = writer.write(c, List.of(new RoutedEvent(staleEvent, route, replayStaleId)), List.of(), "g-outcome-test", List.of());
            assertThat(res.replayOutcomes().get(replayStaleId)).isEqualTo(ReplayOutcome.SKIPPED_STALE);
        }
        assertThat(queryLedgerOutcome(replayStaleUuid)).isEqualTo("SKIPPED_STALE");
        try (Connection c = postgres.createConnection("");
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT job_name, _is_deleted FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + entityId)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("job_applied");
            assertThat(rs.getBoolean(2)).isFalse();
        }

        UUID staleDeleteUuid = UUID.randomUUID();
        ReplayIdentity staleDeleteId = new ReplayIdentity(staleDeleteUuid, DLT, 0, 800L);
        ChangeEvent staleDeleteEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.DELETE,
                new String[] {String.valueOf(entityId), null, null},
                List.of(), 800L, System.currentTimeMillis() * 1000L);
        try (Connection c = engineSource.getConnection()) {
            BatchWriter.WriteResult res = writer.write(c, List.of(new RoutedEvent(staleDeleteEvent, route, staleDeleteId)), List.of(), "g-outcome-test", List.of());
            assertThat(res.replayOutcomes().get(staleDeleteId)).isEqualTo(ReplayOutcome.SKIPPED_STALE);
        }
        assertThat(queryLedgerOutcome(staleDeleteUuid)).isEqualTo("SKIPPED_STALE");
        assertThat(scalar("SELECT count(*) FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + entityId + " AND _is_deleted = false")).isEqualTo(1L);

        UUID freshDeleteUuid = UUID.randomUUID();
        ReplayIdentity freshDeleteId = new ReplayIdentity(freshDeleteUuid, DLT, 0, 1200L);
        ChangeEvent freshDeleteEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.DELETE,
                new String[] {String.valueOf(entityId), null, null},
                List.of(), 1200L, System.currentTimeMillis() * 1000L);
        try (Connection c = engineSource.getConnection()) {
            BatchWriter.WriteResult res = writer.write(c, List.of(new RoutedEvent(freshDeleteEvent, route, freshDeleteId)), List.of(), "g-outcome-test", List.of());
            assertThat(res.replayOutcomes().get(freshDeleteId)).isEqualTo(ReplayOutcome.APPLIED);
        }
        assertThat(queryLedgerOutcome(freshDeleteUuid)).isEqualTo("APPLIED");
        assertThat(scalar("SELECT count(*) FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + entityId + " AND _is_deleted = true")).isEqualTo(1L);
    }

    @Test(dependsOnMethods = "replayOutcomesDistinguishAppliedFromStaleAndDeletes")
    public void intraBatchDuplicatesMarkNewerAppliedAndOlderSuperseded() throws Exception {
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");

        long entityId = 234560L;
        UUID olderUuid = UUID.randomUUID();
        ReplayIdentity olderId = new ReplayIdentity(olderUuid, DLT, 0, 2001L);
        ChangeEvent olderEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityId), "job_older", "key_older"},
                List.of(), 2001L, System.currentTimeMillis() * 1000L);

        UUID newerUuid = UUID.randomUUID();
        ReplayIdentity newerId = new ReplayIdentity(newerUuid, DLT, 0, 2002L);
        ChangeEvent newerEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityId), "job_newer", "key_newer"},
                List.of(), 2002L, System.currentTimeMillis() * 1000L);

        try (Connection c = engineSource.getConnection()) {
            BatchWriter.WriteResult res = writer.write(c,
                    List.of(new RoutedEvent(olderEvent, route, olderId), new RoutedEvent(newerEvent, route, newerId)),
                    List.of(), "g-batch-dup", List.of());
            assertThat(res.replayOutcomes().get(newerId)).isEqualTo(ReplayOutcome.APPLIED);
            assertThat(res.replayOutcomes().get(olderId)).isEqualTo(ReplayOutcome.SKIPPED_SUPERSEDED);
        }

        assertThat(queryLedgerOutcome(newerUuid)).isEqualTo("APPLIED");
        assertThat(queryLedgerOutcome(olderUuid)).isEqualTo("SKIPPED_SUPERSEDED");

        try (Connection c = postgres.createConnection("");
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT job_name, job_key FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + entityId)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("job_name")).isEqualTo("job_newer");
            assertThat(rs.getString("job_key")).isEqualTo("key_newer");
        }
    }

    @Test(dependsOnMethods = "intraBatchDuplicatesMarkNewerAppliedAndOlderSuperseded")
    public void unavailableToastMergedReplayMarksContributingRowApplied() throws Exception {
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");

        long entityId = 345670L;
        UUID contributingUuid = UUID.randomUUID();
        ReplayIdentity contributingId = new ReplayIdentity(contributingUuid, DLT, 0, 3001L);
        ChangeEvent contribEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityId), "name_toast_contributed", "key_orig"},
                List.of(), 3001L, System.currentTimeMillis() * 1000L);

        UUID newerToastUuid = UUID.randomUUID();
        ReplayIdentity newerToastId = new ReplayIdentity(newerToastUuid, DLT, 0, 3002L);
        ChangeEvent toastEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityId), null, "key_toast_updated"},
                List.of("job_name"), 3002L, System.currentTimeMillis() * 1000L);

        try (Connection c = engineSource.getConnection()) {
            BatchWriter.WriteResult res = writer.write(c,
                    List.of(new RoutedEvent(contribEvent, route, contributingId), new RoutedEvent(toastEvent, route, newerToastId)),
                    List.of(), "g-toast-merge", List.of());
            assertThat(res.replayOutcomes().get(contributingId)).isEqualTo(ReplayOutcome.APPLIED);
            assertThat(res.replayOutcomes().get(newerToastId)).isEqualTo(ReplayOutcome.APPLIED);
        }

        assertThat(queryLedgerOutcome(contributingUuid)).isEqualTo("APPLIED");
        assertThat(queryLedgerOutcome(newerToastUuid)).isEqualTo("APPLIED");

        try (Connection c = postgres.createConnection("");
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT job_name, job_key FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + entityId)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("job_name")).isEqualTo("name_toast_contributed");
            assertThat(rs.getString("job_key")).isEqualTo("key_toast_updated");
        }
    }

    @Test(dependsOnMethods = "unavailableToastMergedReplayMarksContributingRowApplied")
    public void unavailableToastNonReplayBatchAndSubsequentReplayBatch() throws Exception {
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");

        long entityIdNonReplay = 345671L;
        ChangeEvent contribNonReplay = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityIdNonReplay), "name_nonreplay_contrib", "key_orig_nonreplay"},
                List.of(), 3010L, System.currentTimeMillis() * 1000L);

        ChangeEvent toastNonReplay = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityIdNonReplay), null, "key_toast_updated_nonreplay"},
                List.of("job_name"), 3011L, System.currentTimeMillis() * 1000L);

        try (Connection c = engineSource.getConnection()) {
            BatchWriter.WriteResult res = writer.write(c,
                    List.of(new RoutedEvent(contribNonReplay, route, null), new RoutedEvent(toastNonReplay, route, null)),
                    List.of(), "g-toast-nonreplay", List.of());
            assertThat(res.replayOutcomes()).isEmpty();
        }

        try (Connection c = postgres.createConnection("");
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT job_name, job_key FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + entityIdNonReplay)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("job_name")).isEqualTo("name_nonreplay_contrib");
            assertThat(rs.getString("job_key")).isEqualTo("key_toast_updated_nonreplay");
        }

        long entityIdReplay = 345672L;
        UUID replayUuid1 = UUID.randomUUID();
        ReplayIdentity replayId1 = new ReplayIdentity(replayUuid1, DLT, 0, 3020L);
        ChangeEvent contribReplay = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityIdReplay), "name_subsequent_replay_contrib", "key_orig_replay"},
                List.of(), 3020L, System.currentTimeMillis() * 1000L);

        UUID replayUuid2 = UUID.randomUUID();
        ReplayIdentity replayId2 = new ReplayIdentity(replayUuid2, DLT, 0, 3021L);
        ChangeEvent toastReplay = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityIdReplay), null, "key_toast_updated_replay"},
                List.of("job_name"), 3021L, System.currentTimeMillis() * 1000L);

        try (Connection c = engineSource.getConnection()) {
            BatchWriter.WriteResult res = writer.write(c,
                    List.of(new RoutedEvent(contribReplay, route, replayId1), new RoutedEvent(toastReplay, route, replayId2)),
                    List.of(), "g-toast-subsequent-replay", List.of());
            assertThat(res.replayOutcomes().get(replayId1)).isEqualTo(ReplayOutcome.APPLIED);
            assertThat(res.replayOutcomes().get(replayId2)).isEqualTo(ReplayOutcome.APPLIED);
        }

        assertThat(queryLedgerOutcome(replayUuid1)).isEqualTo("APPLIED");
        assertThat(queryLedgerOutcome(replayUuid2)).isEqualTo("APPLIED");

        try (Connection c = postgres.createConnection("");
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT job_name, job_key FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + entityIdReplay)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("job_name")).isEqualTo("name_subsequent_replay_contrib");
            assertThat(rs.getString("job_key")).isEqualTo("key_toast_updated_replay");
        }
    }

    @Test(dependsOnMethods = "unavailableToastNonReplayBatchAndSubsequentReplayBatch")
    public void pythonExtractorStagingCompatibility() throws Exception {
        long pyJobId = 456780L;
        String pySource = "bank-py-test";
        update("INSERT INTO meta.source_registry (source_id, tenant_id, lifecycle_state) VALUES ('" + pySource + "', 'tenant-py', 'ACTIVE') ON CONFLICT (source_id) DO NOTHING");
        update("INSERT INTO meta.ingestion_ownership (source_id, table_name, engine, epoch) VALUES ('" + pySource + "', '" + TABLE + "', 'PYTHON', 1) "
                + "ON CONFLICT (source_id, table_name) DO UPDATE SET engine = 'PYTHON', epoch = 1");

        try (Connection c = engineSource.getConnection()) {
            c.setAutoCommit(false);
            String stage;
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT meta.create_stage_table('" + TABLE + "')")) {
                assertThat(rs.next()).isTrue();
                stage = rs.getString(1);
            }
            String copySql = "COPY pg_temp.\"" + stage + "\" (source_id, tenant_id, \"job_instance_id\", \"job_name\", \"job_key\", "
                    + "_source_generation, _source_lsn, _source_ts, _op, _unavailable, _owner_epoch, _target) FROM STDIN WITH (FORMAT csv)";
            String csvRow = "\"" + pySource + "\",\"tenant-py\"," + pyJobId + ",\"py_job\",\"py_key\",0,9999,\"2026-01-01T00:00:00Z\",u,,1,prod\n";
            org.postgresql.PGConnection pgConn = c.unwrap(org.postgresql.PGConnection.class);
            pgConn.getCopyAPI().copyIn(copySql, new java.io.StringReader(csvRow));

            try (PreparedStatement ps = c.prepareStatement("SELECT target, rows_applied, rows_deleted FROM meta.apply_staged_batch(?, 'PYTHON')")) {
                ps.setString(1, TABLE);
                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getString(1)).isEqualTo("prod");
                    assertThat(rs.getLong(2)).isEqualTo(1L);
                    assertThat(rs.getLong(3)).isZero();
                }
            }
            c.commit();
        }
        assertThat(scalar("SELECT count(*) FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + pyJobId)).isEqualTo(1L);
    }

    @Test(dependsOnMethods = "pythonExtractorStagingCompatibility")
    public void concurrentTransactionOrderingGuardBlocksReplayAndMarksStale() throws Exception {
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);
        long entityId = 678910L;

        update("INSERT INTO raw.raw_" + TABLE + " (source_id, tenant_id, job_instance_id, job_name, job_key, _source_generation, _source_lsn, _source_ts, _ingested_at, _ingest_engine, _is_deleted) "
                + "VALUES ('" + SOURCE + "', 'tenant-a', " + entityId + ", 'job_lsn200', 'key_lsn200', 0, 200, NOW(), NOW(), 'KAFKA', false)");

        CountDownLatch tx1UpdatedLatch = new CountDownLatch(1);
        CountDownLatch tx1CommitLatch = new CountDownLatch(1);

        CompletableFuture<Void> tx1Future = CompletableFuture.runAsync(() -> {
            try (Connection c1 = engineSource.getConnection()) {
                c1.setAutoCommit(false);
                try (PreparedStatement ps = c1.prepareStatement(
                        "UPDATE raw.raw_" + TABLE + " SET job_name = 'job_lsn300', job_key = 'key_lsn300', _source_lsn = 300, _source_ts = NOW() "
                                + "WHERE source_id = ? AND tenant_id = ? AND job_instance_id = ?")) {
                    ps.setString(1, SOURCE);
                    ps.setString(2, "tenant-a");
                    ps.setLong(3, entityId);
                    int updated = ps.executeUpdate();
                    assertThat(updated).isEqualTo(1);
                }
                tx1UpdatedLatch.countDown();
                assertThat(tx1CommitLatch.await(30, TimeUnit.SECONDS)).isTrue();
                c1.commit();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(tx1UpdatedLatch.await(10, TimeUnit.SECONDS)).isTrue();

        UUID replayUuid = UUID.randomUUID();
        ReplayIdentity replayId = new ReplayIdentity(replayUuid, DLT, 0, 100L);
        ChangeEvent replayEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityId), "job_stale_lsn100", "key_stale_lsn100"},
                List.of(), 100L, System.currentTimeMillis() * 1000L);
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");

        CountDownLatch tx2PidCapturedLatch = new CountDownLatch(1);
        AtomicLong tx2PidHolder = new AtomicLong();

        CompletableFuture<BatchWriter.WriteResult> tx2Future = CompletableFuture.supplyAsync(() -> {
            try (Connection c2 = engineSource.getConnection()) {
                long pid;
                try (Statement s = c2.createStatement();
                     ResultSet rs = s.executeQuery("SELECT pg_backend_pid()")) {
                    rs.next();
                    pid = rs.getLong(1);
                }
                tx2PidHolder.set(pid);
                tx2PidCapturedLatch.countDown();

                return writer.write(c2,
                        List.of(new RoutedEvent(replayEvent, route, replayId)),
                        List.of(), "g-concurrent-guard", List.of());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(tx2PidCapturedLatch.await(10, TimeUnit.SECONDS))
                .as("Transaction 2 should capture its backend PID").isTrue();
        long tx2Pid = tx2PidHolder.get();

        await("Transaction 2 (pid " + tx2Pid + ") waiting on ungranted row lock in pg_locks", () ->
                scalar("SELECT count(*) FROM pg_locks WHERE NOT granted AND pid = " + tx2Pid) > 0);

        tx1CommitLatch.countDown();
        tx1Future.get(15, TimeUnit.SECONDS);

        BatchWriter.WriteResult res = tx2Future.get(15, TimeUnit.SECONDS);

        assertThat(res.replayOutcomes().get(replayId)).isEqualTo(ReplayOutcome.SKIPPED_STALE);
        assertThat(queryLedgerOutcome(replayUuid)).isEqualTo("SKIPPED_STALE");

        try (Connection c = postgres.createConnection("");
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT job_name, job_key, _source_lsn FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + entityId)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("job_name")).isEqualTo("job_lsn300");
            assertThat(rs.getString("job_key")).isEqualTo("key_lsn300");
            assertThat(rs.getLong("_source_lsn")).isEqualTo(300L);
        }
    }

    @Test(dependsOnMethods = "concurrentTransactionOrderingGuardBlocksReplayAndMarksStale")
    public void unavailableToastMergedReplayLosingOrderingGuardMarksContributingStale() throws Exception {
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");

        long entityId = 789012L;
        update("INSERT INTO raw.raw_" + TABLE + " (source_id, tenant_id, job_instance_id, job_name, job_key, _source_generation, _source_lsn, _source_ts, _ingested_at, _ingest_engine, _is_deleted) "
                + "VALUES ('" + SOURCE + "', 'tenant-a', " + entityId + ", 'job_newer_5000', 'key_newer_5000', 0, 5000, NOW(), NOW(), 'KAFKA', false)");

        UUID olderNonContribUuid = UUID.randomUUID();
        ReplayIdentity olderNonContribId = new ReplayIdentity(olderNonContribUuid, DLT, 0, 4000L);
        ChangeEvent olderNonContribEvent = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityId), "job_older_unused", "key_older_unused"},
                List.of(), 4000L, System.currentTimeMillis() * 1000L);

        UUID contrib1Uuid = UUID.randomUUID();
        ReplayIdentity contrib1Id = new ReplayIdentity(contrib1Uuid, DLT, 0, 4001L);
        ChangeEvent contrib1Event = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityId), "job_contributed_name", null},
                List.of("job_key"), 4001L, System.currentTimeMillis() * 1000L);

        UUID contrib2Uuid = UUID.randomUUID();
        ReplayIdentity contrib2Id = new ReplayIdentity(contrib2Uuid, DLT, 0, 4002L);
        ChangeEvent contrib2Event = new ChangeEvent(
                SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(entityId), null, "key_contributed_key"},
                List.of("job_name"), 4002L, System.currentTimeMillis() * 1000L);

        try (Connection c = engineSource.getConnection()) {
            BatchWriter.WriteResult res = writer.write(c,
                    List.of(
                            new RoutedEvent(olderNonContribEvent, route, olderNonContribId),
                            new RoutedEvent(contrib1Event, route, contrib1Id),
                            new RoutedEvent(contrib2Event, route, contrib2Id)),
                    List.of(), "g-toast-stale", List.of());

            assertThat(res.replayOutcomes().get(contrib1Id)).isEqualTo(ReplayOutcome.SKIPPED_STALE);
            assertThat(res.replayOutcomes().get(contrib2Id)).isEqualTo(ReplayOutcome.SKIPPED_STALE);
            assertThat(res.replayOutcomes().get(olderNonContribId)).isEqualTo(ReplayOutcome.SKIPPED_SUPERSEDED);
        }

        assertThat(queryLedgerOutcome(contrib1Uuid)).isEqualTo("SKIPPED_STALE");
        assertThat(queryLedgerOutcome(contrib2Uuid)).isEqualTo("SKIPPED_STALE");
        assertThat(queryLedgerOutcome(olderNonContribUuid)).isEqualTo("SKIPPED_SUPERSEDED");

        try (Connection c = postgres.createConnection("");
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM meta.replay_ledger WHERE outcome = 'CLAIMED'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isZero();
        }

        try (Connection c = postgres.createConnection("");
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT job_name, job_key, _source_lsn FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + entityId)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("job_name")).isEqualTo("job_newer_5000");
            assertThat(rs.getString("job_key")).isEqualTo("key_newer_5000");
            assertThat(rs.getLong("_source_lsn")).isEqualTo(5000L);
        }
    }

    @Test(dependsOnMethods = "unavailableToastMergedReplayLosingOrderingGuardMarksContributingStale")
    public void unavailableToastPartialMergeLosingOrderingGuardMarksContributingStale() throws Exception {
        String execTable = "batch_job_execution";
        update("INSERT INTO meta.ingestion_ownership (source_id, table_name, engine, epoch) VALUES ('" + SOURCE + "', '"
                + execTable + "', 'KAFKA', 1) ON CONFLICT (source_id, table_name) DO UPDATE SET engine = 'KAFKA', epoch = 1");
        Route route = new Route(Route.Target.PROD, 1L, "tenant-a", 0, null);
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");

        long execId = 890124L;
        update("INSERT INTO raw.raw_" + execTable + " (source_id, tenant_id, job_execution_id, job_instance_id, status, exit_code, _source_generation, _source_lsn, _source_ts, _ingested_at, _ingest_engine, _is_deleted) "
                + "VALUES ('" + SOURCE + "', 'tenant-a', " + execId + ", 1, 'EXISTING_STATUS', '0', 0, 6000, NOW(), NOW(), 'KAFKA', false)");

        UUID olderNonContribUuid = UUID.randomUUID();
        ReplayIdentity olderNonContribId = new ReplayIdentity(olderNonContribUuid, DLT, 0, 4500L);
        ChangeEvent olderNonContribEvent = new ChangeEvent(
                SOURCE, execTable, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(execId), "1", "OLD_STATUS", null, null, "OLD_CODE", null, null, null},
                List.of("start_time", "end_time", "exit_message", "created_on_utc", "last_modified_on_utc"),
                4500L, System.currentTimeMillis() * 1000L);

        UUID contrib1Uuid = UUID.randomUUID();
        ReplayIdentity contrib1Id = new ReplayIdentity(contrib1Uuid, DLT, 0, 4501L);
        ChangeEvent contrib1Event = new ChangeEvent(
                SOURCE, execTable, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(execId), "1", "NEW_STATUS", null, null, null, null, null, null},
                List.of("exit_code", "start_time", "end_time", "exit_message", "created_on_utc", "last_modified_on_utc"),
                4501L, System.currentTimeMillis() * 1000L);

        UUID contrib2Uuid = UUID.randomUUID();
        ReplayIdentity contrib2Id = new ReplayIdentity(contrib2Uuid, DLT, 0, 4502L);
        ChangeEvent contrib2Event = new ChangeEvent(
                SOURCE, execTable, ChangeEvent.Op.UPSERT,
                new String[] {String.valueOf(execId), "1", null, null, null, "NEW_CODE", null, null, null},
                List.of("status", "start_time", "end_time", "exit_message", "created_on_utc", "last_modified_on_utc"),
                4502L, System.currentTimeMillis() * 1000L);

        try (Connection c = engineSource.getConnection()) {
            BatchWriter.WriteResult res = writer.write(c,
                    List.of(
                            new RoutedEvent(olderNonContribEvent, route, olderNonContribId),
                            new RoutedEvent(contrib1Event, route, contrib1Id),
                            new RoutedEvent(contrib2Event, route, contrib2Id)),
                    List.of(), "g-toast-partial-stale", List.of());

            assertThat(res.replayOutcomes().get(contrib1Id)).isEqualTo(ReplayOutcome.SKIPPED_STALE);
            assertThat(res.replayOutcomes().get(contrib2Id)).isEqualTo(ReplayOutcome.SKIPPED_STALE);
            assertThat(res.replayOutcomes().get(olderNonContribId)).isEqualTo(ReplayOutcome.SKIPPED_SUPERSEDED);
        }

        assertThat(queryLedgerOutcome(contrib1Uuid)).isEqualTo("SKIPPED_STALE");
        assertThat(queryLedgerOutcome(contrib2Uuid)).isEqualTo("SKIPPED_STALE");
        assertThat(queryLedgerOutcome(olderNonContribUuid)).isEqualTo("SKIPPED_SUPERSEDED");

        try (Connection c = postgres.createConnection("");
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM meta.replay_ledger WHERE outcome = 'CLAIMED'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isZero();
        }

        try (Connection c = postgres.createConnection("");
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT status, exit_code, _source_lsn FROM raw.raw_" + execTable + " WHERE job_execution_id = " + execId)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("status")).isEqualTo("EXISTING_STATUS");
            assertThat(rs.getString("exit_code")).isEqualTo("0");
            assertThat(rs.getLong("_source_lsn")).isEqualTo(6000L);
        }
    }

    @Test
    public void preflightPredictsTerminalOutcomesAndRollsBackAllDatabaseState() throws Exception {
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);

        long idStaleEntity = 9101L;
        long idDedupEntity = 9102L;
        update("DELETE FROM raw.raw_" + TABLE + " WHERE job_instance_id IN (" + idStaleEntity + ", " + idDedupEntity + ")");

        try {
            update("INSERT INTO raw.raw_" + TABLE
                    + " (source_id, tenant_id, job_instance_id, job_name, job_key, _source_generation, _source_lsn, _source_ts, _ingested_at, _ingest_engine, _is_deleted) "
                    + "VALUES ('" + SOURCE + "', 'tenant-a', " + idStaleEntity + ", 'existing-job', 'existing-key', 0, 500, NOW(), NOW(), 'KAFKA', false) "
                    + "ON CONFLICT (source_id, tenant_id, job_instance_id) DO UPDATE SET _source_lsn = 500, job_name = 'existing-job'");

            UUID uuidStale = UUID.randomUUID();
            ReplayIdentity idStale = new ReplayIdentity(uuidStale, DLT, 0, 5001L);
            ChangeEvent eventStale = new ChangeEvent(SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                    new String[] {String.valueOf(idStaleEntity), "stale-job", "stale-key"},
                    List.of(), 200L, System.currentTimeMillis() * 1000L);

            UUID uuidOlder = UUID.randomUUID();
            ReplayIdentity idOlder = new ReplayIdentity(uuidOlder, DLT, 0, 5002L);
            ChangeEvent eventOlder = new ChangeEvent(SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                    new String[] {String.valueOf(idDedupEntity), "older-job", "older-key"},
                    List.of(), 100L, System.currentTimeMillis() * 1000L);

            UUID uuidNewer = UUID.randomUUID();
            ReplayIdentity idNewer = new ReplayIdentity(uuidNewer, DLT, 0, 5003L);
            ChangeEvent eventNewer = new ChangeEvent(SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                    new String[] {String.valueOf(idDedupEntity), "newer-job", "newer-key"},
                    List.of(), 200L, System.currentTimeMillis() * 1000L);

            BatchWriter.PreflightResult res;
            try (Connection c = engineSource.getConnection()) {
                c.setAutoCommit(true);
                res = writer.preflight(c, List.of(
                        new RoutedEvent(eventOlder, route, idOlder),
                        new RoutedEvent(eventNewer, route, idNewer),
                        new RoutedEvent(eventStale, route, idStale)));

                assertThat(c.getAutoCommit()).isTrue();
            }

            assertThat(res.predictedOutcomes()).containsEntry(idStale, ReplayOutcome.SKIPPED_STALE);
            assertThat(res.predictedOutcomes()).containsEntry(idNewer, ReplayOutcome.APPLIED);
            assertThat(res.predictedOutcomes()).containsEntry(idOlder, ReplayOutcome.SKIPPED_SUPERSEDED);

            assertThat(queryLedgerOutcome(uuidStale)).isNull();
            assertThat(queryLedgerOutcome(uuidNewer)).isNull();
            assertThat(queryLedgerOutcome(uuidOlder)).isNull();

            try (Connection c = postgres.createConnection("");
                 Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*) FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + idDedupEntity)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isZero();
            }

            try (Connection c = postgres.createConnection("");
                 Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT job_name, _source_lsn FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + idStaleEntity)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("job_name")).isEqualTo("existing-job");
                assertThat(rs.getLong("_source_lsn")).isEqualTo(500L);
            }
        } finally {
            update("DELETE FROM raw.raw_" + TABLE + " WHERE job_instance_id IN (" + idStaleEntity + ", " + idDedupEntity + ")");
        }
    }

    @Test
    public void preflightEntityAtomicPairResolvesOutcomesAndRollsBackWithoutMutatingRawTable() throws Exception {
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);

        long entityPk = 9201L;
        update("DELETE FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + entityPk);

        try {
            UUID uuidOlder = UUID.randomUUID();
            ReplayIdentity idOlder = new ReplayIdentity(uuidOlder, DLT, 0, 6001L);
            ChangeEvent eventOlder = new ChangeEvent(SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                    new String[] {String.valueOf(entityPk), "order-1", "key-1"},
                    List.of(), 100L, 1000L);

            UUID uuidNewer = UUID.randomUUID();
            ReplayIdentity idNewer = new ReplayIdentity(uuidNewer, DLT, 0, 6002L);
            ChangeEvent eventNewer = new ChangeEvent(SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                    new String[] {String.valueOf(entityPk), "order-2", "key-2"},
                    List.of(), 200L, 2000L);

            BatchWriter.PreflightResult res;
            try (Connection c = engineSource.getConnection()) {
                c.setAutoCommit(true);
                res = writer.preflight(c, List.of(
                        new RoutedEvent(eventOlder, route, idOlder),
                        new RoutedEvent(eventNewer, route, idNewer)));
                assertThat(c.getAutoCommit()).isTrue();
            }

            assertThat(res.predictedOutcomes()).containsEntry(idOlder, ReplayOutcome.SKIPPED_SUPERSEDED);
            assertThat(res.predictedOutcomes()).containsEntry(idNewer, ReplayOutcome.APPLIED);

            assertThat(queryLedgerOutcome(uuidOlder)).isNull();
            assertThat(queryLedgerOutcome(uuidNewer)).isNull();

            try (Connection c = postgres.createConnection("");
                 Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*) FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + entityPk)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isZero();
            }
        } finally {
            update("DELETE FROM raw.raw_" + TABLE + " WHERE job_instance_id = " + entityPk);
        }
    }

    @Test
    public void preflightFailureDuringStagingRollsBackTransientLedgerClaims() throws Exception {
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);

        UUID failUuid = UUID.randomUUID();
        ReplayIdentity failId = new ReplayIdentity(failUuid, DLT, 0, 5010L);
        ChangeEvent poisonEvent = new ChangeEvent(SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {"INVALID_NON_NUMERIC_ID", "fail-job", "fail-key"},
                List.of(), 100L, System.currentTimeMillis() * 1000L);

        try (Connection c = engineSource.getConnection()) {
            c.setAutoCommit(true);
            assertThatThrownBy(() -> writer.preflight(c, List.of(new RoutedEvent(poisonEvent, route, failId))))
                    .isInstanceOf(SQLException.class);

            assertThat(c.getAutoCommit()).isTrue();
        }

        assertThat(queryLedgerOutcome(failUuid)).isNull();
    }

    @Test
    public void preflightRejectsWhenConnectionAutoCommitIsAlreadyFalseWithoutMutatingCallerTransaction() throws Exception {
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);

        UUID uuid = UUID.randomUUID();
        ReplayIdentity id = new ReplayIdentity(uuid, DLT, 0, 5020L);
        ChangeEvent event = new ChangeEvent(SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {"9999", "test-job", "test-key"},
                List.of(), 100L, System.currentTimeMillis() * 1000L);

        try (Connection c = engineSource.getConnection()) {
            c.setAutoCommit(false);
            assertThatThrownBy(() -> writer.preflight(c, List.of(new RoutedEvent(event, route, id))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Connection must have autoCommit enabled before entering preflight");

            assertThat(c.getAutoCommit()).isFalse();
            c.rollback();
        }
    }

    @Test
    public void preflightRollbackFailureAbortsWithoutCallingSetAutoCommitTrue() throws Exception {
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);

        UUID uuid = UUID.randomUUID();
        ReplayIdentity id = new ReplayIdentity(uuid, DLT, 0, 5030L);
        ChangeEvent event = new ChangeEvent(SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {"8888", "rollback-job", "rollback-key"},
                List.of(), 100L, System.currentTimeMillis() * 1000L);

        try (Connection realConn = engineSource.getConnection()) {
            realConn.setAutoCommit(true);

            AtomicBoolean autoCommitTrueCalled = new AtomicBoolean(false);
            SQLException forcedRollbackError = new SQLException("forced rollback error", "08006");
            Connection proxy = (Connection) Proxy.newProxyInstance(
                    realConn.getClass().getClassLoader(),
                    new Class<?>[] {Connection.class, org.postgresql.PGConnection.class},
                    (p, method, args) -> {
                        if ("rollback".equals(method.getName())) {
                            throw forcedRollbackError;
                        }
                        if ("setAutoCommit".equals(method.getName()) && args != null && args.length > 0 && Boolean.TRUE.equals(args[0])) {
                            autoCommitTrueCalled.set(true);
                        }
                        if ("unwrap".equals(method.getName())) {
                            Class<?> target = (Class<?>) args[0];
                            if (target.isAssignableFrom(Connection.class)) {
                                return p;
                            }
                            return realConn.unwrap(target);
                        }
                        if ("isWrapperFor".equals(method.getName())) {
                            Class<?> target = (Class<?>) args[0];
                            return target.isAssignableFrom(Connection.class) || realConn.isWrapperFor(target);
                        }
                        return method.invoke(realConn, args);
                    });

            assertThatThrownBy(() -> writer.preflight(proxy, List.of(new RoutedEvent(event, route, id))))
                    .isInstanceOf(PreflightRollbackException.class)
                    .hasCause(forcedRollbackError);

            assertThat(autoCommitTrueCalled.get()).isFalse();

            realConn.rollback();
            realConn.setAutoCommit(true);
        }

        assertThat(queryLedgerOutcome(uuid)).isNull();
    }

    @Test
    public void preflightAutoCommitRestoreFailureFailsClosedAfterSuccessfulRollback() throws Exception {
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");
        long epoch = scalar("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + SOURCE + "' AND table_name = '" + TABLE + "'");
        Route route = new Route(Route.Target.PROD, epoch, "tenant-a", 0, null);

        UUID uuid = UUID.randomUUID();
        ReplayIdentity id = new ReplayIdentity(uuid, DLT, 0, 5031L);
        ChangeEvent event = new ChangeEvent(SOURCE, TABLE, ChangeEvent.Op.UPSERT,
                new String[] {"8889", "autocommit-job", "autocommit-key"},
                List.of(), 100L, System.currentTimeMillis() * 1000L);

        try (Connection realConn = engineSource.getConnection()) {
            realConn.setAutoCommit(true);

            AtomicBoolean rollbackCalled = new AtomicBoolean(false);
            SQLException forcedAutoCommitError = new SQLException("forced autoCommit error", "08006");
            Connection proxy = (Connection) Proxy.newProxyInstance(
                    realConn.getClass().getClassLoader(),
                    new Class<?>[] {Connection.class, org.postgresql.PGConnection.class},
                    (p, method, args) -> {
                        if ("rollback".equals(method.getName())) {
                            rollbackCalled.set(true);
                            return method.invoke(realConn, args);
                        }
                        if ("setAutoCommit".equals(method.getName()) && args != null && args.length > 0 && Boolean.TRUE.equals(args[0])) {
                            throw forcedAutoCommitError;
                        }
                        if ("unwrap".equals(method.getName())) {
                            Class<?> target = (Class<?>) args[0];
                            if (target.isAssignableFrom(Connection.class)) {
                                return p;
                            }
                            return realConn.unwrap(target);
                        }
                        if ("isWrapperFor".equals(method.getName())) {
                            Class<?> target = (Class<?>) args[0];
                            return target.isAssignableFrom(Connection.class) || realConn.isWrapperFor(target);
                        }
                        return method.invoke(realConn, args);
                    });

            assertThatThrownBy(() -> writer.preflight(proxy, List.of(new RoutedEvent(event, route, id))))
                    .isInstanceOf(PreflightRollbackException.class)
                    .hasCause(forcedAutoCommitError);

            assertThat(rollbackCalled.get()).isTrue();

            realConn.setAutoCommit(true);
        }

        assertThat(queryLedgerOutcome(uuid)).isNull();
    }

    @Test
    public void dltListWithLedgerDisplaysHistoricalReplayOutcomesAgainstRealPostgres() throws Exception {
        UUID appliedUuid = UUID.randomUUID();
        UUID staleUuid = UUID.randomUUID();
        UUID unrecordedUuid = UUID.randomUUID();

        try (Connection c = postgres.createConnection("");
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO meta.replay_ledger (replay_id, dlt_topic, dlt_partition, dlt_offset, source_id, table_name, outcome) "
                             + "VALUES (?, ?, 0, 1, ?, ?, ?)")) {
            ps.setObject(1, appliedUuid);
            ps.setString(2, DLT);
            ps.setString(3, SOURCE);
            ps.setString(4, TABLE);
            ps.setString(5, "APPLIED");
            ps.executeUpdate();

            ps.setObject(1, staleUuid);
            ps.setString(2, DLT);
            ps.setString(3, SOURCE);
            ps.setString(4, TABLE);
            ps.setString(5, "SKIPPED_STALE");
            ps.executeUpdate();
        }

        for (UUID uuid : List.of(appliedUuid, staleUuid, unrecordedUuid)) {
            ProducerRecord<byte[], byte[]> pr = new ProducerRecord<>(
                    DLT, 0, null, ("{\"source\":{\"name\":\"" + SOURCE + "\",\"table\":\"" + TABLE + "\"},\"op\":\"c\"}").getBytes(StandardCharsets.UTF_8));
            pr.headers().add("fbi.dlt.reason", "WAREHOUSE_REJECTED".getBytes(StandardCharsets.UTF_8));
            pr.headers().add("fbi.dlt.message", "integration test".getBytes(StandardCharsets.UTF_8));
            pr.headers().add("fbi.source.topic", TOPIC.getBytes(StandardCharsets.UTF_8));
            pr.headers().add("fbi.source.partition", "0".getBytes(StandardCharsets.UTF_8));
            pr.headers().add("fbi.source.offset", "100".getBytes(StandardCharsets.UTF_8));
            ReplayIdentityHeaders.add(pr.headers(), new ReplayIdentity(uuid, DLT, 0, 100L));
            producer.send(pr).get(10, TimeUnit.SECONDS);
        }
        producer.flush();

        Map<String, String> env = Map.of(
                "SINK_BOOTSTRAP_SERVERS", kafka.getBootstrapServers(),
                "SINK_DLT_TOPIC", DLT,
                "SINK_WRITER_ID", "dlt-list-it",
                "SINK_DB_URL", postgres.getJdbcUrl(),
                "SINK_DB_USER", postgres.getUsername(),
                "SINK_DB_PASSWORD", postgres.getPassword()
        );
        SinkConfig config = SinkConfig.from(Env.of(env), env);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DeadLetterListCommand.run(config, contract, new String[]{"--with-ledger"},
                new PrintStream(out, true, StandardCharsets.UTF_8), null, null);

        String output = out.toString(StandardCharsets.UTF_8);
        assertThat(output)
                .contains("Historical replay ledger outcomes (meta.replay_ledger):")
                .contains("- APPLIED:")
                .contains("- SKIPPED_STALE:")
                .contains("- No ledger entry:")
                .contains(appliedUuid + "\tAPPLIED")
                .contains(staleUuid + "\tSKIPPED_STALE")
                .contains(unrecordedUuid + "\tNo ledger entry");
    }

    private String queryLedgerOutcome(UUID replayId) throws Exception {
        try (Connection c = postgres.createConnection("");
             PreparedStatement ps = c.prepareStatement("SELECT outcome FROM meta.replay_ledger WHERE replay_id = ?")) {
            ps.setObject(1, replayId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
                return null;
            }
        }
    }
}
