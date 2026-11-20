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

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.fineract.bi.ingestion.sink.ReplayIdentity;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ReplayLedgerMigrationPostgresTest {

    private static final Path ROOT = Path.of("../..");
    private PostgreSQLContainer postgres;

    @BeforeClass
    public void startPostgres() {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new SkipException("Docker is not available");
        }
        postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:18.3"))
                .withDatabaseName("analytics")
                .withUsername("analytics_admin")
                .withPassword("ci-only")
                .withCopyFileToContainer(MountableFile.forHostPath(ROOT.resolve("warehouse/schema")), "/schema");
        postgres.start();
    }

    @AfterClass(alwaysRun = true)
    public void stopPostgres() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    private void runPlatformSql() throws Exception {
        ExecResult r = postgres.execInContainer("psql", "-U", "analytics_admin", "-d", "analytics",
                "-v", "ON_ERROR_STOP=1", "-q", "-f", "/schema/platform.sql");
        if (r.getExitCode() != 0) {
            throw new IllegalStateException("platform.sql execution failed:\n" + r.getStderr());
        }
    }

    private Connection connect() throws Exception {
        return DriverManager.getConnection(postgres.getJdbcUrl(), "analytics_admin", "ci-only");
    }

    @Test(priority = 1)
    public void freshSchemaCreationProvisionsTargetSchemaAndIndexes() throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS meta CASCADE");
            s.execute("CREATE SCHEMA meta");
        }

        runPlatformSql();

        try (Connection c = connect()) {
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT to_regclass('meta.replay_ledger') IS NOT NULL")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getBoolean(1)).isTrue();
            }

            List<String> pkColumns = queryPrimaryKeyColumns(c);
            assertThat(pkColumns).containsExactly("replay_id");

            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT indexname FROM pg_indexes WHERE schemaname = 'meta' AND tablename = 'replay_ledger' ORDER BY indexname")) {
                List<String> indexes = new ArrayList<>();
                while (rs.next()) {
                    indexes.add(rs.getString(1));
                }
                assertThat(indexes).contains(
                        "replay_ledger_dlt_coords_idx",
                        "replay_ledger_pkey",
                        "replay_ledger_replayed_at_idx",
                        "replay_ledger_source_id_idx");
            }
        }
    }

    @Test(priority = 2)
    public void migrationFromOldCompositeKeySchemaPreservesRowsAndMatchesJavaUuids() throws Exception {
        String topic1 = "fineract.cdc.dlt";
        int part1 = 0;
        long off1 = 0L;
        UUID expectedUuid1 = ReplayIdentity.legacyLineageId(topic1, part1, off1);
        assertThat(expectedUuid1).isEqualTo(UUID.fromString("8db3f0e5-3c71-35db-b979-bf97bc9ada55"));

        String topic2 = "fineract.cdc.dlt";
        int part2 = 1;
        long off2 = 42L;
        UUID expectedUuid2 = ReplayIdentity.legacyLineageId(topic2, part2, off2);
        assertThat(expectedUuid2).isEqualTo(UUID.fromString("3a2640f5-5bec-35fb-821c-4ad8161d1b01"));

        String topic3 = "fineract-dlt-events";
        int part3 = 3;
        long off3 = 100500L;
        UUID expectedUuid3 = ReplayIdentity.legacyLineageId(topic3, part3, off3);
        assertThat(expectedUuid3).isEqualTo(UUID.fromString("e2b70e37-5c34-3134-b37e-af9786e6adf6"));

        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS meta.replay_ledger CASCADE");
            s.execute("CREATE TABLE meta.replay_ledger ("
                    + "dlt_topic TEXT NOT NULL, "
                    + "dlt_partition INTEGER NOT NULL, "
                    + "dlt_offset BIGINT NOT NULL, "
                    + "replayed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                    + "PRIMARY KEY (dlt_topic, dlt_partition, dlt_offset))");

            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO meta.replay_ledger (dlt_topic, dlt_partition, dlt_offset) VALUES (?, ?, ?)")) {
                ps.setString(1, topic1);
                ps.setInt(2, part1);
                ps.setLong(3, off1);
                ps.executeUpdate();

                ps.setString(1, topic2);
                ps.setInt(2, part2);
                ps.setLong(3, off2);
                ps.executeUpdate();

                ps.setString(1, topic3);
                ps.setInt(2, part3);
                ps.setLong(3, off3);
                ps.executeUpdate();
            }
        }

        runPlatformSql();

        try (Connection c = connect()) {
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*) FROM meta.replay_ledger")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).as("All 3 legacy rows must be preserved").isEqualTo(3L);
            }

            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT replay_id, source_id, table_name, outcome FROM meta.replay_ledger "
                            + "WHERE dlt_topic = ? AND dlt_partition = ? AND dlt_offset = ?")) {
                ps.setString(1, topic1);
                ps.setInt(2, part1);
                ps.setLong(3, off1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getObject("replay_id", UUID.class)).isEqualTo(expectedUuid1);
                    assertThat(rs.getString("source_id")).isEqualTo("<legacy>");
                    assertThat(rs.getString("table_name")).isEqualTo("<legacy>");
                    assertThat(rs.getString("outcome")).isEqualTo("LEGACY_UNKNOWN");
                }

                ps.setString(1, topic2);
                ps.setInt(2, part2);
                ps.setLong(3, off2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getObject("replay_id", UUID.class)).isEqualTo(expectedUuid2);
                    assertThat(rs.getString("source_id")).isEqualTo("<legacy>");
                    assertThat(rs.getString("table_name")).isEqualTo("<legacy>");
                    assertThat(rs.getString("outcome")).isEqualTo("LEGACY_UNKNOWN");
                }

                ps.setString(1, topic3);
                ps.setInt(2, part3);
                ps.setLong(3, off3);
                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getObject("replay_id", UUID.class)).isEqualTo(expectedUuid3);
                    assertThat(rs.getString("source_id")).isEqualTo("<legacy>");
                    assertThat(rs.getString("table_name")).isEqualTo("<legacy>");
                    assertThat(rs.getString("outcome")).isEqualTo("LEGACY_UNKNOWN");
                }
            }

            List<String> pkColumns = queryPrimaryKeyColumns(c);
            assertThat(pkColumns).containsExactly("replay_id");

            verifyNotNull(c, "replay_id");
            verifyNotNull(c, "source_id");
            verifyNotNull(c, "table_name");
            verifyNotNull(c, "outcome");
        }
    }

    public record FullLedgerEntry(
            UUID replayId,
            String dltTopic,
            int dltPartition,
            long dltOffset,
            String sourceId,
            String tableName,
            String outcome,
            Instant replayedAt) {}

    private static List<FullLedgerEntry> queryFullLedger(Connection c) throws Exception {
        String sql = "SELECT replay_id, dlt_topic, dlt_partition, dlt_offset, source_id, table_name, outcome, replayed_at "
                + "FROM meta.replay_ledger ORDER BY dlt_topic, dlt_partition, dlt_offset";
        List<FullLedgerEntry> list = new ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                list.add(new FullLedgerEntry(
                        rs.getObject("replay_id", UUID.class),
                        rs.getString("dlt_topic"),
                        rs.getInt("dlt_partition"),
                        rs.getLong("dlt_offset"),
                        rs.getString("source_id"),
                        rs.getString("table_name"),
                        rs.getString("outcome"),
                        rs.getTimestamp("replayed_at").toInstant()));
            }
        }
        return list;
    }

    @Test(priority = 3)
    public void reapplyingMigrationIsSafeAndIdempotent() throws Exception {
        List<FullLedgerEntry> beforeReapplication;
        try (Connection c = connect()) {
            beforeReapplication = queryFullLedger(c);
            assertThat(beforeReapplication).hasSize(3);
        }

        runPlatformSql();

        try (Connection c = connect()) {
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*) FROM meta.replay_ledger")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(3L);
            }

            List<FullLedgerEntry> afterReapplication = queryFullLedger(c);
            assertThat(afterReapplication)
                    .as("All existing ledger records and values (replay_id, coordinates, source, table, replayed_at) must remain unchanged")
                    .isEqualTo(beforeReapplication);

            List<String> pkColumns = queryPrimaryKeyColumns(c);
            assertThat(pkColumns).containsExactly("replay_id");
        }
    }

    @Test(priority = 4)
    public void partiallyMigratedTableWithMissingBackfillCompletesMigration() throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS meta.replay_ledger CASCADE");
            s.execute("CREATE TABLE meta.replay_ledger ("
                    + "replay_id UUID, "
                    + "dlt_topic TEXT NOT NULL, "
                    + "dlt_partition INTEGER NOT NULL, "
                    + "dlt_offset BIGINT NOT NULL, "
                    + "source_id TEXT, "
                    + "table_name TEXT, "
                    + "replayed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                    + "PRIMARY KEY (dlt_topic, dlt_partition, dlt_offset))");

            s.execute("INSERT INTO meta.replay_ledger (dlt_topic, dlt_partition, dlt_offset) "
                    + "VALUES ('partial.topic', 2, 888)");
        }

        runPlatformSql();

        try (Connection c = connect()) {
            UUID expectedUuid = ReplayIdentity.legacyLineageId("partial.topic", 2, 888L);
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT replay_id, source_id, table_name FROM meta.replay_ledger")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getObject("replay_id", UUID.class)).isEqualTo(expectedUuid);
                assertThat(rs.getString("source_id")).isEqualTo("<legacy>");
                assertThat(rs.getString("table_name")).isEqualTo("<legacy>");
            }

            List<String> pkColumns = queryPrimaryKeyColumns(c);
            assertThat(pkColumns).containsExactly("replay_id");
            verifyNotNull(c, "replay_id");
            verifyNotNull(c, "source_id");
            verifyNotNull(c, "table_name");
        }
    }

    @Test(priority = 5)
    public void migrationDoesNotOverwriteExistingNonNullReplayId() throws Exception {
        UUID customUuid = UUID.randomUUID();
        String customSource = "custom-tenant";
        String customTable = "m_loan";

        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS meta.replay_ledger CASCADE");
            s.execute("CREATE TABLE meta.replay_ledger ("
                    + "replay_id UUID, "
                    + "dlt_topic TEXT NOT NULL, "
                    + "dlt_partition INTEGER NOT NULL, "
                    + "dlt_offset BIGINT NOT NULL, "
                    + "source_id TEXT, "
                    + "table_name TEXT, "
                    + "replayed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                    + "PRIMARY KEY (dlt_topic, dlt_partition, dlt_offset))");

            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO meta.replay_ledger (replay_id, dlt_topic, dlt_partition, dlt_offset, source_id, table_name) "
                            + "VALUES (?, ?, ?, ?, ?, ?)")) {
                ps.setObject(1, customUuid);
                ps.setString(2, "custom.topic");
                ps.setInt(3, 1);
                ps.setLong(4, 100L);
                ps.setString(5, customSource);
                ps.setString(6, customTable);
                ps.executeUpdate();
            }

            s.execute("INSERT INTO meta.replay_ledger (dlt_topic, dlt_partition, dlt_offset) "
                    + "VALUES ('legacy.topic', 1, 200)");
        }

        runPlatformSql();

        try (Connection c = connect()) {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT replay_id, source_id, table_name FROM meta.replay_ledger WHERE dlt_offset = 100")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getObject("replay_id", UUID.class)).as("Existing replay_id must not be overwritten").isEqualTo(customUuid);
                    assertThat(rs.getString("source_id")).isEqualTo(customSource);
                    assertThat(rs.getString("table_name")).isEqualTo(customTable);
                }
            }

            UUID expectedLegacyUuid = ReplayIdentity.legacyLineageId("legacy.topic", 1, 200L);
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT replay_id, source_id, table_name FROM meta.replay_ledger WHERE dlt_offset = 200")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getObject("replay_id", UUID.class)).isEqualTo(expectedLegacyUuid);
                    assertThat(rs.getString("source_id")).isEqualTo("<legacy>");
                    assertThat(rs.getString("table_name")).isEqualTo("<legacy>");
                }
            }

            List<String> pkColumns = queryPrimaryKeyColumns(c);
            assertThat(pkColumns).containsExactly("replay_id");
        }
    }

    @Test(priority = 6)
    public void engineRoleReceivesConditionalGrantsWhenRoleExists() throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("DO $$ BEGIN "
                    + "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analytics_engine') THEN "
                    + "    CREATE ROLE analytics_engine LOGIN PASSWORD 'test-engine-pass'; "
                    + "END IF; "
                    + "END $$; "
                    + "GRANT USAGE ON SCHEMA meta TO analytics_engine;");
        }

        runPlatformSql();

        try (Connection c = connect(); Statement s = c.createStatement()) {
            try (ResultSet rs = s.executeQuery(
                    "SELECT has_table_privilege('analytics_engine', 'meta.replay_ledger', 'SELECT'), "
                            + "has_table_privilege('analytics_engine', 'meta.replay_ledger', 'INSERT'), "
                            + "has_column_privilege('analytics_engine', 'meta.replay_ledger', 'outcome', 'UPDATE'), "
                            + "has_column_privilege('analytics_engine', 'meta.replay_ledger', 'replay_id', 'UPDATE'), "
                            + "has_column_privilege('analytics_engine', 'meta.replay_ledger', 'source_id', 'UPDATE'), "
                            + "has_table_privilege('analytics_engine', 'meta.replay_ledger', 'DELETE')")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getBoolean(1)).as("analytics_engine must have SELECT on meta.replay_ledger").isTrue();
                assertThat(rs.getBoolean(2)).as("analytics_engine must have INSERT on meta.replay_ledger").isTrue();
                assertThat(rs.getBoolean(3)).as("analytics_engine must have UPDATE on outcome column").isTrue();
                assertThat(rs.getBoolean(4)).as("analytics_engine must NOT have UPDATE on replay_id column").isFalse();
                assertThat(rs.getBoolean(5)).as("analytics_engine must NOT have UPDATE on source_id column").isFalse();
                assertThat(rs.getBoolean(6)).as("analytics_engine must NOT have DELETE on meta.replay_ledger").isFalse();
            }

            s.execute("SET ROLE analytics_engine");
            s.execute("UPDATE meta.replay_ledger SET outcome = 'APPLIED' WHERE 1 = 0");
            assertThatThrownBy(() -> s.execute("UPDATE meta.replay_ledger SET source_id = 'denied' WHERE 1 = 0"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("permission denied");
            assertThatThrownBy(() -> s.execute("DELETE FROM meta.replay_ledger WHERE 1 = 0"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("permission denied");
        }
    }

    @Test(priority = 7)
    public void outcomeCheckConstraintRejectsInvalidOutcome() throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            assertThatThrownBy(() -> s.execute(
                    "INSERT INTO meta.replay_ledger (replay_id, dlt_topic, dlt_partition, dlt_offset, source_id, table_name, outcome) "
                            + "VALUES ('" + UUID.randomUUID() + "', 'topic', 0, 9999, 'src', 'tbl', 'INVALID_OUTCOME')"))
                    .hasMessageContaining("chk_replay_outcome");
        }
    }

    private static List<String> queryPrimaryKeyColumns(Connection c) throws Exception {
        String sql = "SELECT kcu.column_name "
                + "FROM information_schema.table_constraints tc "
                + "JOIN information_schema.key_column_usage kcu "
                + "  ON tc.constraint_name = kcu.constraint_name "
                + " AND tc.table_schema = kcu.table_schema "
                + " AND tc.table_name = kcu.table_name "
                + "WHERE tc.table_schema = 'meta' "
                + "  AND tc.table_name = 'replay_ledger' "
                + "  AND tc.constraint_type = 'PRIMARY KEY' "
                + "ORDER BY kcu.ordinal_position";
        List<String> columns = new ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                columns.add(rs.getString(1));
            }
        }
        return columns;
    }

    private static void verifyNotNull(Connection c, String columnName) throws Exception {
        String sql = "SELECT is_nullable FROM information_schema.columns "
                + "WHERE table_schema = 'meta' AND table_name = 'replay_ledger' AND column_name = '" + columnName + "'";
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).as(columnName + " must be NOT NULL").isEqualTo("NO");
        }
    }
}
