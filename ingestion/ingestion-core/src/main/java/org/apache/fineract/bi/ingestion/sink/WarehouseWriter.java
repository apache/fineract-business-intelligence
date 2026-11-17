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

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.fineract.bi.ingestion.cdc.ChangeEvent;
import org.apache.fineract.bi.ingestion.contract.ColumnSpec;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.TableContract;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

public final class WarehouseWriter implements BatchWriter {

    private static final String UPSERT_OFFSET =
            "INSERT INTO meta.consumer_offsets (consumer_group, topic, kafka_partition, next_offset, updated_at) "
                    + "VALUES (?, ?, ?, ?, NOW()) ON CONFLICT (consumer_group, topic, kafka_partition) DO UPDATE "
                    + "SET next_offset = GREATEST(meta.consumer_offsets.next_offset, EXCLUDED.next_offset), "
                    + "updated_at = NOW()";

    private final Contract contract;
    private final String engine;
    private final Map<String, String> copySql = new ConcurrentHashMap<>();

    public WarehouseWriter(Contract contract, String engine) {
        this.contract = contract;
        this.engine = engine;
    }

    public record Applied(String table, String target, long rows, long deletes) {
    }

    public WriteResult write(Connection c, Collection<RoutedEvent> events, Collection<PartitionOffset> offsets,
            String consumerGroup) throws SQLException {
        return write(c, events, offsets, consumerGroup, List.of());
    }

    @Override
    public WriteResult write(Connection c, Collection<RoutedEvent> events, Collection<PartitionOffset> offsets,
            String consumerGroup, Collection<SkipCheck> skips) throws SQLException {
        c.setAutoCommit(false);
        try {
            Set<ReplayIdentity> claimed = claimReplays(c, events);

            Map<String, List<RoutedEvent>> byTable = new TreeMap<>();
            Set<ReplayIdentity> batchSeen = new HashSet<>();
            for (RoutedEvent e : events) {
                if (e.isReplay()) {
                    ReplayIdentity id = e.replayIdentity();
                    if (!claimed.contains(id) || !batchSeen.add(id)) {
                        continue;
                    }
                }
                byTable.computeIfAbsent(e.event().table(), k -> new ArrayList<>()).add(e);
            }

            verifySkips(c, skips);
            List<Applied> applied = stageAndApply(c, byTable);

            Map<ReplayIdentity, ReplayOutcome> replayOutcomes = new LinkedHashMap<>();
            if (!claimed.isEmpty()) {
                replayOutcomes = findCompletedReplayOutcomes(c, claimed);
                for (ReplayIdentity id : claimed) {
                    ReplayOutcome outcome = replayOutcomes.get(id);
                    if (outcome == null || outcome == ReplayOutcome.CLAIMED) {
                        throw new IllegalStateException("Replay identity " + id.replayId()
                                + " did not receive a terminal outcome (outcome=" + outcome + ")");
                    }
                }
            }

            if (!offsets.isEmpty()) {
                try (PreparedStatement ps = c.prepareStatement(UPSERT_OFFSET)) {
                    for (PartitionOffset o : offsets) {
                        ps.setString(1, consumerGroup);
                        ps.setString(2, o.topic());
                        ps.setInt(3, o.partition());
                        ps.setLong(4, o.nextOffset());
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
            }
            c.commit();
            return new WriteResult(applied, replayOutcomes);
        } catch (SQLException e) {
            rollbackQuietly(c);
            if (FencedWriteException.SQLSTATE.equals(e.getSQLState())) {
                throw new FencedWriteException(e);
            }
            throw e;
        } catch (IOException e) {
            rollbackQuietly(c);
            throw new SQLException("COPY stream failed", "08006", e);
        } catch (RuntimeException e) {
            rollbackQuietly(c);
            throw e;
        }
    }

    public List<Applied> stageAndApply(Connection c, Map<String, List<RoutedEvent>> byTable)
            throws SQLException, IOException {
        List<Applied> applied = new ArrayList<>();
        if (byTable.isEmpty()) {
            return applied;
        }
        CopyManager copy = c.unwrap(PGConnection.class).getCopyAPI();
        for (Map.Entry<String, List<RoutedEvent>> entry : byTable.entrySet()) {
            TableContract table = contract.requireTable(entry.getKey());
            createStage(c, table);
            copy.copyIn(copySql(table), new StringReader(encode(table, entry.getValue())));
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT target, rows_applied, rows_deleted FROM meta.apply_staged_batch(?, ?)")) {
                ps.setString(1, table.tableName());
                ps.setString(2, engine);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        applied.add(new Applied(table.tableName(), rs.getString(1), rs.getLong(2), rs.getLong(3)));
                    }
                }
            }
        }
        return applied;
    }

    @Override
    public PreflightResult preflight(Connection c, Collection<RoutedEvent> events) throws SQLException {
        boolean origAutoCommit;
        try {
            origAutoCommit = c.getAutoCommit();
        } catch (SQLException | RuntimeException setupEx) {
            String msg = "Failed to initialize connection transaction state for preflight: "
                    + setupEx.getMessage();
            throw new PreflightRollbackException(msg, setupEx);
        }

        if (!origAutoCommit) {
            throw new IllegalStateException("Connection must have autoCommit enabled before entering preflight; "
                    + "existing transaction boundary owned by caller must not be mutated or rolled back");
        }

        try {
            c.setAutoCommit(false);
        } catch (SQLException | RuntimeException setupEx) {
            String msg = "Failed to initialize connection transaction state for preflight: "
                    + setupEx.getMessage();
            throw new PreflightRollbackException(msg, setupEx);
        }

        List<Applied> result = null;
        Map<ReplayIdentity, ReplayOutcome> predictedOutcomes = Map.of();
        Exception validationFailure = null;
        try {
            Set<ReplayIdentity> claimed = claimReplays(c, events);

            Map<String, List<RoutedEvent>> byTable = new TreeMap<>();
            Set<ReplayIdentity> batchSeen = new HashSet<>();
            for (RoutedEvent e : events) {
                if (e != null && e.isReplay()) {
                    ReplayIdentity id = e.replayIdentity();
                    if (!claimed.contains(id) || !batchSeen.add(id)) {
                        continue;
                    }
                }
                if (e != null) {
                    byTable.computeIfAbsent(e.event().table(), k -> new ArrayList<>()).add(e);
                }
            }

            result = stageAndApply(c, byTable);

            if (!claimed.isEmpty()) {
                predictedOutcomes = findCompletedReplayOutcomes(c, claimed);
                for (ReplayIdentity id : claimed) {
                    ReplayOutcome outcome = predictedOutcomes.get(id);
                    if (outcome == null || outcome == ReplayOutcome.CLAIMED) {
                        throw new IllegalStateException("Replay identity " + id.replayId()
                                + " did not receive a terminal predicted outcome (outcome=" + outcome + ")");
                    }
                }
            }
        } catch (IOException e) {
            validationFailure = new SQLException("COPY stream failed during preflight", "08006", e);
        } catch (Exception e) {
            validationFailure = e;
        }

        boolean rollbackSucceeded = false;
        try {
            c.rollback();
            rollbackSucceeded = true;
        } catch (SQLException | RuntimeException rollbackEx) {
            String msg = "Preflight transaction rollback failed; connection may be in an unresolved state: "
                    + rollbackEx.getMessage();
            throw new PreflightRollbackException(msg, rollbackEx, validationFailure);
        }

        if (origAutoCommit && rollbackSucceeded) {
            try {
                c.setAutoCommit(true);
            } catch (SQLException | RuntimeException autoCommitEx) {
                String msg = "Failed to restore connection autoCommit state after preflight rollback: "
                        + autoCommitEx.getMessage();
                throw new PreflightRollbackException(msg, autoCommitEx, validationFailure);
            }
        }

        if (validationFailure != null) {
            if (validationFailure instanceof SQLException sqlEx) {
                throw sqlEx;
            }
            if (validationFailure instanceof RuntimeException rtEx) {
                throw rtEx;
            }
            throw new SQLException("Preflight validation failed", "XX000", validationFailure);
        }

        return new PreflightResult(result != null ? result : List.of(), predictedOutcomes);
    }

    public static Set<ReplayIdentity> claimReplays(Connection c, Collection<RoutedEvent> events)
            throws SQLException {
        if (c == null || events == null || events.isEmpty()) {
            return Set.of();
        }
        Map<UUID, RoutedEvent> firstByReplayId = new LinkedHashMap<>();
        for (RoutedEvent e : events) {
            if (e != null && e.isReplay()) {
                firstByReplayId.putIfAbsent(e.replayIdentity().replayId(), e);
            }
        }
        if (firstByReplayId.isEmpty()) {
            return Set.of();
        }
        Set<ReplayIdentity> claimed = new HashSet<>();
        String sql = "INSERT INTO meta.replay_ledger (replay_id, dlt_topic, dlt_partition, dlt_offset, source_id, table_name, outcome) "
                + "VALUES (?, ?, ?, ?, ?, ?, 'CLAIMED') ON CONFLICT (replay_id) DO NOTHING RETURNING replay_id";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (RoutedEvent e : firstByReplayId.values()) {
                ReplayIdentity id = e.replayIdentity();
                ps.setObject(1, id.replayId());
                ps.setString(2, id.dltTopic());
                ps.setInt(3, id.dltPartition());
                ps.setLong(4, id.dltOffset());
                ps.setString(5, e.event().sourceId());
                ps.setString(6, e.event().table());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        claimed.add(id);
                    }
                }
            }
        }
        return claimed;
    }

    public static Map<ReplayIdentity, ReplayOutcome> findCompletedReplayOutcomes(Connection c,
            Collection<ReplayIdentity> identities) throws SQLException {
        if (c == null || identities == null || identities.isEmpty()) {
            return Map.of();
        }
        Map<UUID, ReplayOutcome> outcomeByUuid = new HashMap<>();
        Map<ReplayIdentity, ReplayOutcome> result = new LinkedHashMap<>();
        String sql = "SELECT replay_id, outcome FROM meta.replay_ledger WHERE replay_id = ?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (ReplayIdentity id : identities) {
                if (id == null || id.replayId() == null) {
                    continue;
                }
                UUID uuid = id.replayId();
                if (!outcomeByUuid.containsKey(uuid)) {
                    ps.setObject(1, uuid);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            String outcomeStr = rs.getString(2);
                            ReplayOutcome outcome = outcomeStr != null ? ReplayOutcome.valueOf(outcomeStr)
                                    : ReplayOutcome.LEGACY_UNKNOWN;
                            outcomeByUuid.put(uuid, outcome);
                        } else {
                            outcomeByUuid.put(uuid, null);
                        }
                    }
                }
                ReplayOutcome outcome = outcomeByUuid.get(uuid);
                if (outcome != null) {
                    result.put(id, outcome);
                }
            }
        }
        return result;
    }

    public static Set<ReplayIdentity> findCompletedReplays(Connection c, Collection<ReplayIdentity> identities)
            throws SQLException {
        return findCompletedReplayOutcomes(c, identities).keySet();
    }

    static void verifySkips(Connection c, Collection<SkipCheck> skips) throws SQLException {
        if (skips == null || skips.isEmpty()) {
            return;
        }
        Map<String, SkipCheck> distinct = new TreeMap<>();
        for (SkipCheck s : skips) {
            distinct.put(s.sourceId() + '\u0000' + s.table(), s);
        }
        String[] sources = new String[distinct.size()];
        String[] tables = new String[distinct.size()];
        Long[] epochs = new Long[distinct.size()];
        int i = 0;
        for (SkipCheck s : distinct.values()) {
            sources[i] = s.sourceId();
            tables[i] = s.table();
            epochs[i] = s.epoch();
            i++;
        }
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT pg_advisory_xact_lock_shared(k) FROM (SELECT DISTINCT meta.ownership_lock_key(s, t) AS k "
                        + "FROM unnest(?::text[], ?::text[]) AS x(s, t) ORDER BY k) q")) {
            ps.setArray(1, c.createArrayOf("text", sources));
            ps.setArray(2, c.createArrayOf("text", tables));
            ps.execute();
        }
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT x.s, x.t, x.e, o.epoch, (r.source_id IS NOT NULL) AS registered "
                        + "FROM unnest(?::text[], ?::text[], ?::bigint[]) AS x(s, t, e) "
                        + "LEFT JOIN meta.ingestion_ownership o ON o.source_id = x.s AND o.table_name = x.t "
                        + "LEFT JOIN meta.source_registry r ON r.source_id = x.s")) {
            ps.setArray(1, c.createArrayOf("text", sources));
            ps.setArray(2, c.createArrayOf("text", tables));
            ps.setArray(3, c.createArrayOf("bigint", epochs));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long expected = rs.getLong(3);
                    boolean stale;
                    if (expected == SkipCheck.MUST_BE_UNREGISTERED) {
                        stale = rs.getBoolean(5);
                    } else {
                        long actual = rs.getObject(4) == null ? 0L : rs.getLong(4);
                        stale = actual != expected;
                    }
                    if (stale) {
                        throw new SQLException("Skip decision for " + rs.getString(1) + "/" + rs.getString(2)
                                + " is stale (ownership changed since the snapshot)", FencedWriteException.SQLSTATE);
                    }
                }
            }
        }
    }

    public static Map<String, Long> loadOffsets(Connection c, String consumerGroup, Collection<String> topics)
            throws SQLException {
        Map<String, Long> result = new HashMap<>();
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT topic, kafka_partition, next_offset FROM meta.consumer_offsets "
                        + "WHERE consumer_group = ? AND topic = ANY(?)")) {
            ps.setString(1, consumerGroup);
            ps.setArray(2, c.createArrayOf("text", topics.toArray()));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.put(rs.getString(1) + "#" + rs.getInt(2), rs.getLong(3));
                }
            }
        }
        if (!c.getAutoCommit()) {
            c.commit();
        }
        return result;
    }

    String encode(TableContract table, List<RoutedEvent> events) {
        StringBuilder sb = new StringBuilder(events.size() * 256);
        List<ColumnSpec> columns = table.columns();
        for (RoutedEvent re : events) {
            ChangeEvent e = re.event();
            CsvEncoder.append(sb, e.sourceId());
            sb.append(',');
            CsvEncoder.append(sb, re.route().tenantId());
            String[] values = e.values();
            for (int i = 0; i < columns.size(); i++) {
                sb.append(',');
                CsvEncoder.append(sb, values[i]);
            }
            sb.append(',').append(re.route().generation());
            sb.append(',');
            if (e.lsn() != null) {
                sb.append(e.lsn());
            }
            sb.append(',');
            CsvEncoder.append(sb, microsToIso(e.sourceTsMicros()));
            sb.append(',').append(e.op() == ChangeEvent.Op.DELETE ? 'd' : 'u');
            sb.append(',');
            CsvEncoder.append(sb, CsvEncoder.identifierArray(e.unavailable()));
            sb.append(',').append(re.route().epoch());
            sb.append(',').append(re.route().stageTarget());
            sb.append(',');
            if (re.isReplay() && re.replayIdentity() != null && re.replayIdentity().replayId() != null) {
                sb.append(re.replayIdentity().replayId());
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private static String microsToIso(long micros) {
        return Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L)
                .toString();
    }

    private static void createStage(Connection c, TableContract table) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT meta.create_stage_table(?)")) {
            ps.setString(1, table.tableName());
            ps.execute();
        }
    }

    private String copySql(TableContract table) {
        return copySql.computeIfAbsent(table.tableName(), k -> {
            StringBuilder cols = new StringBuilder("source_id, tenant_id");
            for (ColumnSpec col : table.columns()) {
                cols.append(", \"").append(col.name()).append('"');
            }
            cols.append(", _source_generation, _source_lsn, _source_ts, _op, _unavailable, _owner_epoch, _target, _replay_id");
            return "COPY pg_temp.\"stage_" + table.rawTable() + "\" (" + cols + ") FROM STDIN WITH (FORMAT csv)";
        });
    }

    private static void rollbackQuietly(Connection c) {
        try {
            c.rollback();
        } catch (SQLException ignored) {
        }
    }
}
