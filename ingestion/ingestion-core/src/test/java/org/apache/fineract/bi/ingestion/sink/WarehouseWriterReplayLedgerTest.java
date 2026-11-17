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

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.fineract.bi.ingestion.cdc.ChangeEvent;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.fineract.bi.ingestion.routing.Route;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class WarehouseWriterReplayLedgerTest {

    private Contract contract;
    private WarehouseWriter writer;

    @BeforeClass
    public void setup() {
        contract = ContractLoader.load(Path.of("../../contracts/fineract_tables.json"));
        writer = new WarehouseWriter(contract, "KAFKA");
    }

    private RoutedEvent createReplayEvent(String sourceId, String table, String idVal, ReplayIdentity replayIdentity) {
        String[] values = new String[] {idVal, "test-job-" + idVal, null};
        ChangeEvent e = new ChangeEvent(sourceId, table, ChangeEvent.Op.UPSERT, values,
                List.of(), 100L, 1_700_000_000_000_000L);
        Route route = new Route(Route.Target.PROD, 1, "tenant-a", 1, null);
        return new RoutedEvent(e, route, replayIdentity);
    }

    private RoutedEvent createOrdinaryEvent(String sourceId, String table, String idVal) {
        String[] values = new String[] {idVal, "test-job-" + idVal, null};
        ChangeEvent e = new ChangeEvent(sourceId, table, ChangeEvent.Op.UPSERT, values,
                List.of(), 100L, 1_700_000_000_000_000L);
        Route route = new Route(Route.Target.PROD, 1, "tenant-a", 1, null);
        return new RoutedEvent(e, route, null);
    }

    private static class LedgerRow {
        final UUID replayId;
        final String dltTopic;
        final int dltPartition;
        final long dltOffset;
        final String sourceId;
        final String tableName;

        LedgerRow(UUID replayId, String dltTopic, int dltPartition, long dltOffset, String sourceId, String tableName) {
            this.replayId = replayId;
            this.dltTopic = dltTopic;
            this.dltPartition = dltPartition;
            this.dltOffset = dltOffset;
            this.sourceId = sourceId;
            this.tableName = tableName;
        }
    }

    private static class TestDatabase {
        final Map<UUID, LedgerRow> committedLedger = new HashMap<>();
        final Map<UUID, LedgerRow> txLedger = new HashMap<>();
        final List<LedgerRow> insertedParams = new ArrayList<>();
        final List<UUID> selectQueries = new ArrayList<>();
        final AtomicInteger insertExecutionCount = new AtomicInteger();
        final AtomicInteger commitCount = new AtomicInteger();
        final AtomicInteger rollbackCount = new AtomicInteger();
        final AtomicBoolean failStaging = new AtomicBoolean(false);
        final Map<UUID, ReplayOutcome> outcomeOverrides = new HashMap<>();
        final List<String> preparedStatements = new ArrayList<>();

        Connection createConnection() {
            AtomicBoolean autoCommit = new AtomicBoolean(true);
            return (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[] {Connection.class},
                    (proxy, method, args) -> {
                        String name = method.getName();
                        if ("setAutoCommit".equals(name)) {
                            autoCommit.set((Boolean) args[0]);
                            return null;
                        }
                        if ("getAutoCommit".equals(name)) {
                            return autoCommit.get();
                        }
                        if ("commit".equals(name)) {
                            commitCount.incrementAndGet();
                            committedLedger.putAll(txLedger);
                            txLedger.clear();
                            return null;
                        }
                        if ("rollback".equals(name)) {
                            rollbackCount.incrementAndGet();
                            txLedger.clear();
                            return null;
                        }
                        if ("prepareStatement".equals(name)) {
                            String sql = (String) args[0];
                            preparedStatements.add(sql);
                            if (sql.contains("INSERT INTO meta.replay_ledger")) {
                                return createInsertPreparedStatement();
                            }
                            if (sql.contains("meta.replay_ledger") && sql.contains("SELECT")) {
                                return createSelectPreparedStatement();
                            }
                            if (sql.contains("INSERT INTO meta.consumer_offsets")) {
                                return createDummyBatchPreparedStatement();
                            }
                            if (sql.contains("meta.apply_staged_batch")) {
                                if (failStaging.get()) {
                                    throw new SQLException("Forced staging failure for test", "42P01");
                                }
                                return createApplyStagedBatchPreparedStatement();
                            }
                            return createDummyPreparedStatement();
                        }
                        if ("createStatement".equals(name)) {
                            return createDummyStatement();
                        }
                        if ("unwrap".equals(name)) {
                            Class<?> target = (Class<?>) args[0];
                            if (failStaging.get()) {
                                throw new SQLException("Forced staging failure on unwrap", "42P01");
                            }
                            if (target.isAssignableFrom(Connection.class)) {
                                return proxy;
                            }
                            if (target.isAssignableFrom(org.postgresql.PGConnection.class)) {
                                org.postgresql.core.BaseConnection baseConn =
                                        (org.postgresql.core.BaseConnection) Proxy.newProxyInstance(
                                                org.postgresql.core.BaseConnection.class.getClassLoader(),
                                                new Class<?>[] {org.postgresql.core.BaseConnection.class},
                                                (bProxy, bMethod, bArgs) -> {
                                                    if ("getEncoding".equals(bMethod.getName())) {
                                                        return org.postgresql.core.Encoding.defaultEncoding();
                                                    }
                                                    return null;
                                                });
                                org.postgresql.copy.CopyManager copyManager = new org.postgresql.copy.CopyManager(baseConn) {
                                    @Override
                                    public long copyIn(String sql, java.io.Reader from) {
                                        return 1L;
                                    }
                                };
                                return (org.postgresql.PGConnection) Proxy.newProxyInstance(
                                        org.postgresql.PGConnection.class.getClassLoader(),
                                        new Class<?>[] {org.postgresql.PGConnection.class},
                                        (pgProxy, pgMethod, pgArgs) -> {
                                            if ("getCopyAPI".equals(pgMethod.getName())) {
                                                return copyManager;
                                            }
                                            return null;
                                        });
                            }
                            throw new SQLException("Unwrap target not supported: " + target.getName());
                        }
                        if ("isWrapperFor".equals(name)) {
                            return true;
                        }
                        if ("close".equals(name)) {
                            return null;
                        }
                        return null;
                    });
        }

        private PreparedStatement createInsertPreparedStatement() {
            Object[] params = new Object[7];
            return (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(),
                    new Class<?>[] {PreparedStatement.class},
                    (proxy, method, args) -> {
                        String name = method.getName();
                        if ("setObject".equals(name)) {
                            params[(Integer) args[0]] = args[1];
                            return null;
                        }
                        if ("setString".equals(name)) {
                            params[(Integer) args[0]] = args[1];
                            return null;
                        }
                        if ("setInt".equals(name)) {
                            params[(Integer) args[0]] = args[1];
                            return null;
                        }
                        if ("setLong".equals(name)) {
                            params[(Integer) args[0]] = args[1];
                            return null;
                        }
                        if ("executeQuery".equals(name)) {
                            insertExecutionCount.incrementAndGet();
                            UUID replayId = (UUID) params[1];
                            String dltTopic = (String) params[2];
                            int dltPartition = (Integer) params[3];
                            long dltOffset = (Long) params[4];
                            String sourceId = (String) params[5];
                            String tableName = (String) params[6];

                            LedgerRow row = new LedgerRow(replayId, dltTopic, dltPartition, dltOffset, sourceId, tableName);
                            insertedParams.add(row);

                            boolean alreadyExists = committedLedger.containsKey(replayId) || txLedger.containsKey(replayId);
                            if (!alreadyExists) {
                                txLedger.put(replayId, row);
                            }
                            boolean hasRow = !alreadyExists;
                            boolean[] returned = {false};
                            return (ResultSet) Proxy.newProxyInstance(
                                    ResultSet.class.getClassLoader(),
                                    new Class<?>[] {ResultSet.class},
                                    (rProxy, rMethod, rArgs) -> {
                                        if ("next".equals(rMethod.getName())) {
                                            if (hasRow && !returned[0]) {
                                                returned[0] = true;
                                                return true;
                                            }
                                            return false;
                                        }
                                        if ("close".equals(rMethod.getName())) {
                                            return null;
                                        }
                                        return null;
                                    });
                        }
                        if ("close".equals(name)) {
                            return null;
                        }
                        return null;
                    });
        }

        private PreparedStatement createSelectPreparedStatement() {
            UUID[] queried = new UUID[1];
            return (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(),
                    new Class<?>[] {PreparedStatement.class},
                    (proxy, method, args) -> {
                        String name = method.getName();
                        if ("setObject".equals(name)) {
                            if (args[1] instanceof UUID u) {
                                queried[0] = u;
                                selectQueries.add(u);
                            }
                            return null;
                        }
                        if ("executeQuery".equals(name)) {
                            boolean exists = committedLedger.containsKey(queried[0]) || txLedger.containsKey(queried[0]);
                            ReplayOutcome outcome = outcomeOverrides.getOrDefault(queried[0], ReplayOutcome.APPLIED);
                            boolean[] returned = {false};
                            return (ResultSet) Proxy.newProxyInstance(
                                     ResultSet.class.getClassLoader(),
                                     new Class<?>[] {ResultSet.class},
                                     (rProxy, rMethod, rArgs) -> {
                                         String rName = rMethod.getName();
                                         if ("next".equals(rName)) {
                                             if (exists && !returned[0]) {
                                                 returned[0] = true;
                                                 return true;
                                             }
                                             return false;
                                         }
                                         if ("getString".equals(rName)) {
                                             return outcome.name();
                                         }
                                         if ("getObject".equals(rName)) {
                                             return queried[0];
                                         }
                                         if ("close".equals(rName)) {
                                             return null;
                                         }
                                         return null;
                                     });
                        }
                        if ("close".equals(name)) {
                            return null;
                        }
                        return null;
                    });
        }

        private PreparedStatement createApplyStagedBatchPreparedStatement() {
            return (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(),
                    new Class<?>[] {PreparedStatement.class},
                    (proxy, method, args) -> {
                        String name = method.getName();
                        if ("setString".equals(name)) {
                            return null;
                        }
                        if ("executeQuery".equals(name)) {
                            boolean[] returned = {false};
                            return (ResultSet) Proxy.newProxyInstance(
                                    ResultSet.class.getClassLoader(),
                                    new Class<?>[] {ResultSet.class},
                                    (rProxy, rMethod, rArgs) -> {
                                        if ("next".equals(rMethod.getName())) {
                                            if (!returned[0]) {
                                                returned[0] = true;
                                                return true;
                                            }
                                            return false;
                                        }
                                        if ("getString".equals(rMethod.getName())) {
                                            return "prod";
                                        }
                                        if ("getLong".equals(rMethod.getName())) {
                                            int idx = (Integer) rArgs[0];
                                            return idx == 2 ? 1L : 0L;
                                        }
                                        if ("close".equals(rMethod.getName())) {
                                            return null;
                                        }
                                        return null;
                                    });
                        }
                        if ("close".equals(name)) {
                            return null;
                        }
                        return null;
                    });
        }

        private PreparedStatement createDummyBatchPreparedStatement() {
            return (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(),
                    new Class<?>[] {PreparedStatement.class},
                    (proxy, method, args) -> {
                        if ("executeBatch".equals(method.getName())) {
                            return new int[] {1};
                        }
                        return null;
                    });
        }

        private PreparedStatement createDummyPreparedStatement() {
            return (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(),
                    new Class<?>[] {PreparedStatement.class},
                    (proxy, method, args) -> {
                        String name = method.getName();
                        if ("execute".equals(name)) {
                            return false;
                        }
                        if ("executeUpdate".equals(name)) {
                            return 0;
                        }
                        if ("close".equals(name)) {
                            return null;
                        }
                        return null;
                    });
        }

        private java.sql.Statement createDummyStatement() {
            return (java.sql.Statement) Proxy.newProxyInstance(
                    java.sql.Statement.class.getClassLoader(),
                    new Class<?>[] {java.sql.Statement.class},
                    (proxy, method, args) -> {
                        if ("execute".equals(method.getName())) {
                            return false;
                        }
                        if ("executeUpdate".equals(method.getName())) {
                            return 0;
                        }
                        if ("close".equals(method.getName())) {
                            return null;
                        }
                        return null;
                    });
        }
    }

    @Test
    public void newUuidClaimedSuccessfullyWithSourceIdAndTableNameFromRoutedEvent() throws Exception {
        TestDatabase db = new TestDatabase();
        Connection c = db.createConnection();

        UUID replayUuid = UUID.randomUUID();
        ReplayIdentity id = new ReplayIdentity(replayUuid, "fineract.cdc.dlt", 2, 75L);
        RoutedEvent event = createReplayEvent("bank-xyz", "batch_job_instance", "10", id);

        Set<ReplayIdentity> claimed = WarehouseWriter.claimReplays(c, List.of(event));

        assertThat(claimed).containsExactly(id);
        assertThat(db.insertedParams).hasSize(1);
        LedgerRow inserted = db.insertedParams.get(0);
        assertThat(inserted.replayId).isEqualTo(replayUuid);
        assertThat(inserted.dltTopic).isEqualTo("fineract.cdc.dlt");
        assertThat(inserted.dltPartition).isEqualTo(2);
        assertThat(inserted.dltOffset).isEqualTo(75L);
        assertThat(inserted.sourceId).isEqualTo("bank-xyz");
        assertThat(inserted.tableName).isEqualTo("batch_job_instance");
    }

    @Test
    public void completedUuidSkippedInFindCompletedReplaysAndClaimReplays() throws Exception {
        TestDatabase db = new TestDatabase();
        Connection c = db.createConnection();

        UUID completedUuid = UUID.randomUUID();
        LedgerRow existingRow = new LedgerRow(completedUuid, "orig.dlt", 0, 10L, "bank-a", "batch_job_instance");
        db.committedLedger.put(completedUuid, existingRow);

        ReplayIdentity queryId = new ReplayIdentity(completedUuid, "different.dlt", 3, 999L);
        Set<ReplayIdentity> completed = WarehouseWriter.findCompletedReplays(c, List.of(queryId));

        assertThat(completed).contains(queryId);
        assertThat(db.selectQueries).contains(completedUuid);

        RoutedEvent event = createReplayEvent("bank-a", "batch_job_instance", "11", queryId);
        Set<ReplayIdentity> claimed = WarehouseWriter.claimReplays(c, List.of(event));
        assertThat(claimed).isEmpty();
    }

    @Test
    public void twoIdentitiesWithSameUuidDifferentCoordinatesDeduplicated() throws Exception {
        TestDatabase db = new TestDatabase();
        Connection c = db.createConnection();

        UUID sharedUuid = UUID.randomUUID();
        ReplayIdentity id1 = new ReplayIdentity(sharedUuid, "dlt-topic-1", 0, 100L);
        ReplayIdentity id2 = new ReplayIdentity(sharedUuid, "dlt-topic-2", 1, 200L);

        RoutedEvent e1 = createReplayEvent("bank-a", "batch_job_instance", "1", id1);
        RoutedEvent e2 = createReplayEvent("bank-a", "batch_job_instance", "2", id2);

        Set<ReplayIdentity> claimed = WarehouseWriter.claimReplays(c, List.of(e1, e2));

        assertThat(claimed).hasSize(1);
        assertThat(claimed).contains(id1);
        assertThat(claimed).contains(id2);
        assertThat(db.insertExecutionCount.get()).isEqualTo(1);
        assertThat(db.insertedParams).hasSize(1);
        assertThat(db.insertedParams.get(0).replayId).isEqualTo(sharedUuid);
    }

    @Test
    public void transactionRollbackAllowsSameReplayUuidToBeRetried() throws Exception {
        TestDatabase db = new TestDatabase();
        Connection c = db.createConnection();

        UUID retryUuid = UUID.randomUUID();
        ReplayIdentity id = new ReplayIdentity(retryUuid, "fineract.cdc.dlt", 0, 50L);
        RoutedEvent badEvent = createReplayEvent("bank-a", "batch_job_instance", "1", id);

        db.failStaging.set(true);

        assertThatThrownBy(() -> writer.write(c, List.of(badEvent), List.of(), "test-group", List.of()))
                .isInstanceOf(SQLException.class);

        assertThat(db.rollbackCount.get()).isEqualTo(1);
        assertThat(db.committedLedger).doesNotContainKey(retryUuid);
        assertThat(db.txLedger).isEmpty();

        db.failStaging.set(false);
        RoutedEvent fixedEvent = createReplayEvent("bank-a", "batch_job_instance", "1", id);

        writer.write(c, List.of(fixedEvent), List.of(), "test-group", List.of());

        assertThat(db.commitCount.get()).isEqualTo(1);
        assertThat(db.committedLedger).containsKey(retryUuid);
        assertThat(db.committedLedger.get(retryUuid).sourceId).isEqualTo("bank-a");
    }

    @Test
    public void ordinaryNonReplayWritesRemainUnchangedWithoutQueryingLedger() throws Exception {
        TestDatabase db = new TestDatabase();
        Connection c = db.createConnection();

        RoutedEvent ordinaryEvent = createOrdinaryEvent("bank-a", "batch_job_instance", "1");
        assertThat(ordinaryEvent.isReplay()).isFalse();

        Set<ReplayIdentity> claimed = WarehouseWriter.claimReplays(c, List.of(ordinaryEvent));
        assertThat(claimed).isEmpty();
        assertThat(db.insertExecutionCount.get()).isZero();

        writer.write(c, List.of(ordinaryEvent), List.of(), "test-group", List.of());

        assertThat(db.commitCount.get()).isEqualTo(1);
        assertThat(db.insertExecutionCount.get()).isZero();
        assertThat(db.selectQueries).isEmpty();
        assertThat(db.committedLedger).isEmpty();
        assertThat(db.preparedStatements.stream().noneMatch(s -> s.contains("meta.replay_ledger"))).isTrue();
    }

    @Test
    public void unresolvedClaimedStatusThrowsAndRollsBack() throws Exception {
        TestDatabase db = new TestDatabase();
        Connection c = db.createConnection();

        UUID unresolvedUuid = UUID.randomUUID();
        ReplayIdentity id = new ReplayIdentity(unresolvedUuid, "fineract.cdc.dlt", 0, 50L);
        RoutedEvent event = createReplayEvent("bank-a", "batch_job_instance", "1", id);

        db.outcomeOverrides.put(unresolvedUuid, ReplayOutcome.CLAIMED);

        assertThatThrownBy(() -> writer.write(c, List.of(event), List.of(), "test-group", List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("did not receive a terminal outcome");

        assertThat(db.rollbackCount.get()).isEqualTo(1);
        assertThat(db.committedLedger).doesNotContainKey(unresolvedUuid);
        assertThat(db.commitCount.get()).isZero();
    }

    @Test
    public void preflightPredictsAppliedOutcomeAndRollsBackCleanly() throws Exception {
        TestDatabase db = new TestDatabase();
        Connection c = db.createConnection();

        UUID uuid = UUID.randomUUID();
        ReplayIdentity id = new ReplayIdentity(uuid, "fineract.cdc.dlt", 0, 100L);
        RoutedEvent event = createReplayEvent("bank-a", "batch_job_instance", "1", id);
        db.outcomeOverrides.put(uuid, ReplayOutcome.APPLIED);

        BatchWriter.PreflightResult result = writer.preflight(c, List.of(event));

        assertThat(result.predictedOutcomes()).containsEntry(id, ReplayOutcome.APPLIED);
        assertThat(result.applied()).isNotEmpty();

        assertThat(db.rollbackCount.get()).isEqualTo(1);
        assertThat(db.commitCount.get()).isZero();
        assertThat(db.txLedger).isEmpty();
        assertThat(db.committedLedger).isEmpty();
        assertThat(c.getAutoCommit()).isTrue();
    }

    @Test
    public void preflightPredictsSkippedStaleAndSkippedSupersededOutcomes() throws Exception {
        TestDatabase db = new TestDatabase();
        Connection c = db.createConnection();

        UUID uuidStale = UUID.randomUUID();
        ReplayIdentity idStale = new ReplayIdentity(uuidStale, "fineract.cdc.dlt", 0, 101L);
        RoutedEvent eventStale = createReplayEvent("bank-a", "batch_job_instance", "1", idStale);

        UUID uuidSuperseded = UUID.randomUUID();
        ReplayIdentity idSuperseded = new ReplayIdentity(uuidSuperseded, "fineract.cdc.dlt", 0, 102L);
        RoutedEvent eventSuperseded = createReplayEvent("bank-a", "batch_job_instance", "2", idSuperseded);

        db.outcomeOverrides.put(uuidStale, ReplayOutcome.SKIPPED_STALE);
        db.outcomeOverrides.put(uuidSuperseded, ReplayOutcome.SKIPPED_SUPERSEDED);

        BatchWriter.PreflightResult result = writer.preflight(c, List.of(eventStale, eventSuperseded));

        assertThat(result.predictedOutcomes()).containsEntry(idStale, ReplayOutcome.SKIPPED_STALE);
        assertThat(result.predictedOutcomes()).containsEntry(idSuperseded, ReplayOutcome.SKIPPED_SUPERSEDED);

        assertThat(db.rollbackCount.get()).isEqualTo(1);
        assertThat(db.commitCount.get()).isZero();
        assertThat(db.txLedger).isEmpty();
        assertThat(db.committedLedger).isEmpty();
        assertThat(c.getAutoCommit()).isTrue();
    }

    @Test
    public void preflightWithMultipleReplaysAndOrdinaryEventsInSameBatch() throws Exception {
        TestDatabase db = new TestDatabase();
        Connection c = db.createConnection();

        UUID uuid1 = UUID.randomUUID();
        ReplayIdentity id1 = new ReplayIdentity(uuid1, "fineract.cdc.dlt", 0, 100L);
        RoutedEvent replay1 = createReplayEvent("bank-a", "batch_job_instance", "1", id1);

        RoutedEvent ordinary = createOrdinaryEvent("bank-a", "batch_job_instance", "2");

        db.outcomeOverrides.put(uuid1, ReplayOutcome.APPLIED);

        BatchWriter.PreflightResult result = writer.preflight(c, List.of(replay1, ordinary));

        assertThat(result.predictedOutcomes()).hasSize(1);
        assertThat(result.predictedOutcomes()).containsEntry(id1, ReplayOutcome.APPLIED);
        assertThat(result.applied()).isNotEmpty();

        assertThat(db.rollbackCount.get()).isEqualTo(1);
        assertThat(db.commitCount.get()).isZero();
        assertThat(db.txLedger).isEmpty();
        assertThat(db.committedLedger).isEmpty();
    }

    @Test
    public void preflightFailsClosedWhenOutcomeRemainsClaimedOrMissing() throws Exception {
        TestDatabase db = new TestDatabase();
        Connection c = db.createConnection();

        UUID claimedUuid = UUID.randomUUID();
        ReplayIdentity id = new ReplayIdentity(claimedUuid, "fineract.cdc.dlt", 0, 103L);
        RoutedEvent event = createReplayEvent("bank-a", "batch_job_instance", "1", id);

        db.outcomeOverrides.put(claimedUuid, ReplayOutcome.CLAIMED);

        assertThatThrownBy(() -> writer.preflight(c, List.of(event)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("did not receive a terminal predicted outcome");

        assertThat(db.rollbackCount.get()).isEqualTo(1);
        assertThat(db.commitCount.get()).isZero();
        assertThat(db.txLedger).isEmpty();
        assertThat(c.getAutoCommit()).isTrue();
    }

    @Test
    public void preflightRollsBackTransientClaimsOnStagingFailure() throws Exception {
        TestDatabase db = new TestDatabase();
        Connection c = db.createConnection();

        UUID uuid = UUID.randomUUID();
        ReplayIdentity id = new ReplayIdentity(uuid, "fineract.cdc.dlt", 0, 104L);
        RoutedEvent event = createReplayEvent("bank-a", "batch_job_instance", "1", id);

        db.failStaging.set(true);

        assertThatThrownBy(() -> writer.preflight(c, List.of(event)))
                .isInstanceOf(SQLException.class);

        assertThat(db.rollbackCount.get()).isEqualTo(1);
        assertThat(db.commitCount.get()).isZero();
        assertThat(db.txLedger).isEmpty();
        assertThat(c.getAutoCommit()).isTrue();
    }

    @Test
    public void preflightRejectsWhenConnectionAutoCommitIsAlreadyFalse() throws Exception {
        TestDatabase db = new TestDatabase();
        Connection c = db.createConnection();
        c.setAutoCommit(false);

        UUID uuid = UUID.randomUUID();
        ReplayIdentity id = new ReplayIdentity(uuid, "fineract.cdc.dlt", 0, 105L);
        RoutedEvent event = createReplayEvent("bank-a", "batch_job_instance", "1", id);

        assertThatThrownBy(() -> writer.preflight(c, List.of(event)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Connection must have autoCommit enabled before entering preflight");

        assertThat(db.rollbackCount.get()).isZero();
        assertThat(db.commitCount.get()).isZero();
        assertThat(c.getAutoCommit()).isFalse();
    }
}
