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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.fineract.bi.ingestion.cdc.DebeziumEnvelopeParser;
import org.apache.fineract.bi.ingestion.config.Env;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.fineract.bi.ingestion.routing.OwnershipCache;
import org.apache.fineract.bi.ingestion.sink.WarehouseWriter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
}
