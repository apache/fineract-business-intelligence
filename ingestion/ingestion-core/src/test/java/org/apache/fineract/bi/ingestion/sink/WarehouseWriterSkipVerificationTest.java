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
package org.apache.fineract.bi.ingestion.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class WarehouseWriterSkipVerificationTest {

    private static final String TABLE = "batch_job_instance";

    private String source;

    private static Connection open() throws SQLException {
        String url = System.getenv().getOrDefault("FBI_TEST_DB_URL", "jdbc:postgresql://localhost:5434/analytics");
        String user = System.getenv().getOrDefault("FBI_TEST_DB_USER", "analytics_loader");
        String password = System.getenv().getOrDefault("FBI_TEST_DB_PASSWORD", "analytics_loader_dev_only");
        return DriverManager.getConnection(url, user, password);
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    @BeforeMethod
    public void register() throws SQLException {
        source = "it-" + UUID.randomUUID().toString().substring(0, 8);
        try (Connection c = open()) {
            exec(c, "INSERT INTO meta.source_registry (source_id, tenant_id, lifecycle_state) VALUES ('" + source
                    + "', 't', 'ACTIVE')");
            exec(c, "INSERT INTO meta.ingestion_ownership (source_id, table_name, engine) VALUES ('" + source
                    + "', '" + TABLE + "', 'NONE')");
        } catch (SQLException e) {
            throw new SkipException("warehouse database not reachable or not initialised: " + e.getMessage());
        }
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws SQLException {
        if (source == null) {
            return;
        }
        try (Connection c = open()) {
            exec(c, "DELETE FROM meta.source_registry WHERE source_id = '" + source + "'");
        } catch (SQLException ignored) {
        }
    }

    private long epoch(Connection c) throws SQLException {
        try (Statement st = c.createStatement();
                var rs = st.executeQuery("SELECT epoch FROM meta.ingestion_ownership WHERE source_id = '" + source
                        + "' AND table_name = '" + TABLE + "'")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    public void aSkipDecisionMadeAtTheCurrentEpochPasses() throws Exception {
        try (Connection c = open()) {
            c.setAutoCommit(false);
            long epoch = epoch(c);
            WarehouseWriter.verifySkips(c, List.of(new SkipCheck(source, TABLE, epoch)));
            c.rollback();
        }
    }

    @Test
    public void aSkipDecisionFromAStaleSnapshotIsRejectedAsFenced() throws Exception {
        try (Connection c = open()) {
            c.setAutoCommit(false);
            long stale = epoch(c);
            exec(c, "UPDATE meta.ingestion_ownership SET engine = 'KAFKA', epoch = epoch + 1 WHERE source_id = '"
                    + source + "'");
            c.commit();
            assertThatThrownBy(() -> WarehouseWriter.verifySkips(c, List.of(new SkipCheck(source, TABLE, stale))))
                    .isInstanceOf(SQLException.class)
                    .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("FBI01"));
            c.rollback();
        }
    }

    @Test
    public void aSourceThatWasSkippedAsUnregisteredMustStillBeUnregistered() throws Exception {
        try (Connection c = open()) {
            c.setAutoCommit(false);
            assertThatThrownBy(() -> WarehouseWriter.verifySkips(
                    c, List.of(new SkipCheck(source, "*", SkipCheck.MUST_BE_UNREGISTERED))))
                    .isInstanceOf(SQLException.class);
            c.rollback();
            WarehouseWriter.verifySkips(c, List.of(new SkipCheck("it-never-registered", "*",
                    SkipCheck.MUST_BE_UNREGISTERED)));
            c.rollback();
        }
    }

    @Test(timeOut = 60_000)
    public void verificationWaitsForAnOwnershipTransitionThatIsInFlight() throws Exception {
        try (Connection control = open(); Connection sink = open()) {
            control.setAutoCommit(false);
            sink.setAutoCommit(false);
            long stale = epoch(control);
            exec(control, "SELECT meta.lock_ownership_exclusive('" + source + "', '" + TABLE + "')");
            exec(control, "UPDATE meta.ingestion_ownership SET engine = 'KAFKA', epoch = epoch + 1 WHERE source_id = '"
                    + source + "'");
            CompletableFuture<Throwable> verifying = CompletableFuture.supplyAsync(() -> {
                try {
                    WarehouseWriter.verifySkips(sink, List.of(new SkipCheck(source, TABLE, stale)));
                    return null;
                } catch (Throwable t) {
                    return t;
                }
            });
            Thread.sleep(1_000);
            assertThat(verifying.isDone()).as("the sink must wait for the transition instead of racing it").isFalse();
            control.commit();
            Throwable failure = verifying.get(30, TimeUnit.SECONDS);
            assertThat(failure).isInstanceOf(SQLException.class);
            assertThat(((SQLException) failure).getSQLState()).isEqualTo("FBI01");
            sink.rollback();
        }
    }
}
