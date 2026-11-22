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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.sink.ReplayIdentity;
import org.apache.fineract.bi.ingestion.sink.ReplayOutcome;
import org.apache.fineract.bi.ingestion.sink.WarehouseWriter;
import org.apache.kafka.common.TopicPartition;

public final class DeadLetterListCommand {

    public static final String STATUS_NO_LEDGER_ENTRY = "No ledger entry";
    public static final String STATUS_IDENTITY_UNAVAILABLE = "Identity unavailable";

    public record RecordLedgerStatus(
            DeadLetterRecord record,
            ReplayIdentity replayIdentity,
            String status,
            String detail) {
        public RecordLedgerStatus {
            Objects.requireNonNull(record, "record");
            Objects.requireNonNull(status, "status");
        }
    }

    public record LedgerInspectionResult(
            Map<String, Long> outcomeCounts,
            List<RecordLedgerStatus> recordStatuses) {
        public LedgerInspectionResult {
            outcomeCounts = Map.copyOf(outcomeCounts);
            recordStatuses = List.copyOf(recordStatuses);
        }
    }

    private static final Set<String> VALUE_OPTIONS = Set.of(
            "limit", "max-bytes"
    );

    private static final Set<String> BOOLEAN_OPTIONS = Set.of(
            "with-ledger"
    );

    private DeadLetterListCommand() {
    }

    static Map<String, String> parseArgs(String[] args) {
        return parseArgs(args, VALUE_OPTIONS, BOOLEAN_OPTIONS);
    }

    static Map<String, String> parseArgs(String[] args, Set<String> valueOptions, Set<String> booleanOptions) {
        Map<String, String> values = new HashMap<>();
        if (args == null || args.length == 0) {
            return values;
        }
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a == null) {
                throw new IllegalArgumentException("Argument at index " + i + " must not be null.");
            }
            if (!a.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected positional argument: '" + a + "'. Options must start with '--'.");
            }
            if (a.equals("--")) {
                throw new IllegalArgumentException("Empty option name: '--'.");
            }
            int eq = a.indexOf('=');
            if (eq == 2) {
                throw new IllegalArgumentException("Empty option name in '" + a + "'.");
            }

            String rawName = eq > 0 ? a.substring(2, eq) : a.substring(2);
            String optName = rawName.trim().toLowerCase(Locale.ROOT);
            if (optName.isEmpty()) {
                throw new IllegalArgumentException("Empty option name in '" + a + "'.");
            }

            if (!valueOptions.contains(optName) && !booleanOptions.contains(optName)) {
                throw new IllegalArgumentException("Unknown option: '--" + optName + "'.");
            }

            if (values.containsKey(optName)) {
                throw new IllegalArgumentException("Duplicate option: '--" + optName + "'.");
            }

            if (valueOptions.contains(optName)) {
                if (eq < 0) {
                    throw new IllegalArgumentException(
                            "Missing value for option '--" + optName + "'. Expected '--" + optName + "=<value>'.");
                }
                String val = a.substring(eq + 1).trim();
                if (val.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Empty value for option '--" + optName + "'. Expected '--" + optName + "=<value>'.");
                }
                values.put(optName, val);
            } else {
                if (eq < 0) {
                    values.put(optName, "true");
                } else {
                    String val = a.substring(eq + 1).trim();
                    if ("true".equalsIgnoreCase(val)) {
                        values.put(optName, "true");
                    } else if ("false".equalsIgnoreCase(val)) {
                        values.put(optName, "false");
                    } else {
                        throw new IllegalArgumentException(
                                "Invalid boolean value '" + val + "' for option '--" + optName + "'. Expected 'true' or 'false'.");
                    }
                }
            }
        }
        return values;
    }

    public static void run(SinkConfig config, Contract contract) throws Exception {
        run(config, contract, new String[0]);
    }

    public static void run(SinkConfig config, Contract contract, String[] args) throws Exception {
        run(config, contract, args, System.out, null, null);
    }

    static void run(
            SinkConfig config,
            Contract contract,
            String[] args,
            PrintStream out,
            HikariDataSource dsOverride,
            DeadLetterReader readerOverride) throws Exception {

        if (args != null && args.length > 0 && "dlt-list".equalsIgnoreCase(args[0])) {
            args = Arrays.copyOfRange(args, 1, args.length);
        }
        Map<String, String> opts = parseArgs(args);

        long limit = Long.MAX_VALUE;
        if (opts.containsKey("limit")) {
            try {
                limit = Long.parseLong(opts.get("limit"));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid long value for '--limit': " + opts.get("limit"));
            }
            if (limit <= 0) {
                throw new IllegalArgumentException("Limit must be positive: " + limit);
            }
        }

        boolean withLedger = "true".equals(opts.get("with-ledger"));

        if (!withLedger && opts.containsKey("max-bytes")) {
            throw new IllegalArgumentException("Cannot specify --max-bytes without --with-ledger.");
        }

        long maxBytes = withLedger ? DeadLetterReader.DEFAULT_MAX_BYTES : Long.MAX_VALUE;
        if (opts.containsKey("max-bytes")) {
            try {
                maxBytes = Long.parseLong(opts.get("max-bytes"));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid long value for '--max-bytes': " + opts.get("max-bytes"));
            }
            if (maxBytes <= 0) {
                throw new IllegalArgumentException("max-bytes must be positive: " + maxBytes);
            }
        }

        DeadLetterReader.ScanOptions scanOptions = new DeadLetterReader.ScanOptions(limit, maxBytes);

        DeadLetterSummary.Accumulator accumulator = new DeadLetterSummary.Accumulator();
        List<DeadLetterRecord> records = withLedger ? new ArrayList<>() : null;
        DeadLetterReader.ScanResult scan;

        try (DeadLetterReader reader = readerOverride != null ? readerOverride : new DeadLetterReader(config, contract)) {
            scan = reader.scan(scanOptions, record -> {
                accumulator.add(record);
                if (withLedger) {
                    records.add(record);
                }
                return new DeadLetterReader.VisitResult(true, false);
            });
        }

        LedgerInspectionResult ledgerResult = null;
        if (withLedger) {
            ledgerResult = inspectLedger(config, records, dsOverride);
        }

        render(config.dltTopic(), scan, accumulator.snapshot(), accumulator.invalidReplayRecordCount(), ledgerResult, out);
    }

    static LedgerInspectionResult inspectLedger(
            SinkConfig config,
            List<DeadLetterRecord> records,
            HikariDataSource dsOverride) throws Exception {

        Map<DeadLetterRecord, String> unresolvable = new HashMap<>();
        Map<DeadLetterRecord, ReplayIdentity> resolvable = new HashMap<>();
        Set<ReplayIdentity> distinctIdentities = new LinkedHashSet<>();

        for (DeadLetterRecord r : records) {
            if (r.hasInvalidReplayIdentity()) {
                unresolvable.put(r, r.replayIdentityError());
            } else {
                try {
                    ReplayIdentity id = r.replayIdentity();
                    if (id != null) {
                        resolvable.put(r, id);
                        distinctIdentities.add(id);
                    } else {
                        unresolvable.put(r, "Replay identity could not be constructed");
                    }
                } catch (Exception e) {
                    unresolvable.put(r, e.getMessage());
                }
            }
        }

        Map<ReplayIdentity, ReplayOutcome> outcomes = new HashMap<>();
        if (dsOverride != null) {
            try {
                outcomes.putAll(queryLedgerWithDataSource(dsOverride, distinctIdentities));
            } catch (Exception e) {
                throw new IllegalStateException("Failed to inspect replay ledger: " + e.getMessage(), e);
            }
        } else {
            HikariConfig hc = new HikariConfig();
            hc.setJdbcUrl(config.dbUrl());
            hc.setUsername(config.dbUser());
            hc.setPassword(config.dbPassword());
            hc.setMaximumPoolSize(1);
            hc.setAutoCommit(true);
            hc.setPoolName("fbi-dlt-list");
            hc.setConnectionTimeout(5000);

            try (HikariDataSource ds = new HikariDataSource(hc)) {
                outcomes.putAll(queryLedgerWithDataSource(ds, distinctIdentities));
            } catch (Exception e) {
                throw new IllegalStateException("Failed to inspect replay ledger: " + e.getMessage(), e);
            }
        }

        Map<String, Long> outcomeCounts = new LinkedHashMap<>();
        outcomeCounts.put(ReplayOutcome.APPLIED.name(), 0L);
        outcomeCounts.put(ReplayOutcome.SKIPPED_STALE.name(), 0L);
        outcomeCounts.put(ReplayOutcome.SKIPPED_SUPERSEDED.name(), 0L);
        outcomeCounts.put(ReplayOutcome.CLAIMED.name(), 0L);
        outcomeCounts.put(STATUS_NO_LEDGER_ENTRY, 0L);
        outcomeCounts.put(STATUS_IDENTITY_UNAVAILABLE, 0L);

        List<RecordLedgerStatus> statuses = new ArrayList<>(records.size());
        for (DeadLetterRecord r : records) {
            if (unresolvable.containsKey(r)) {
                String detail = unresolvable.get(r);
                statuses.add(new RecordLedgerStatus(r, null, STATUS_IDENTITY_UNAVAILABLE, detail));
                outcomeCounts.merge(STATUS_IDENTITY_UNAVAILABLE, 1L, Long::sum);
            } else {
                ReplayIdentity id = resolvable.get(r);
                ReplayOutcome outcome = outcomes.get(id);
                String status = outcome != null ? outcome.name() : STATUS_NO_LEDGER_ENTRY;
                statuses.add(new RecordLedgerStatus(r, id, status, null));
                outcomeCounts.merge(status, 1L, Long::sum);
            }
        }

        return new LedgerInspectionResult(outcomeCounts, statuses);
    }

    static Map<ReplayIdentity, ReplayOutcome> queryLedgerWithDataSource(
            HikariDataSource ds,
            Set<ReplayIdentity> distinctIdentities) throws Exception {

        Connection conn = null;
        try {
            conn = ds.getConnection();
            return WarehouseWriter.findCompletedReplayOutcomes(conn, distinctIdentities);
        } catch (Exception e) {
            if (conn != null) {
                try {
                    ds.evictConnection(conn);
                } catch (Exception ex) {
                    e.addSuppressed(ex);
                }
            }
            throw e;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    static void render(
            String topic,
            DeadLetterReader.ScanResult scan,
            List<DeadLetterSummary.Group> groups,
            PrintStream out) {
        render(topic, scan, groups, 0L, null, out);
    }

    static void render(
            String topic,
            DeadLetterReader.ScanResult scan,
            List<DeadLetterSummary.Group> groups,
            long invalidReplayRecordCount,
            PrintStream out) {
        render(topic, scan, groups, invalidReplayRecordCount, null, out);
    }

    static void render(
            String topic,
            DeadLetterReader.ScanResult scan,
            List<DeadLetterSummary.Group> groups,
            long invalidReplayRecordCount,
            LedgerInspectionResult ledgerResult,
            PrintStream out) {

        out.println("Dead-letter inspection");
        out.println("======================");
        out.println("Topic: " + escape(topic));
        out.println("Records scanned: " + scan.recordsScanned());
        out.println("Records matched: " + scan.recordsMatched());
        out.println("Bytes scanned: " + scan.bytesScanned());
        out.println("Stop reason: " + scan.stopReason());
        if (scan.stopReason() == DeadLetterReader.StopReason.BYTE_BUDGET_REACHED) {
            out.println("Warning: Listing stopped early because the byte budget was reached; not all partitions were fully scanned.");
        }
        if (scan.stopReason() == DeadLetterReader.StopReason.COMPLETED) {
            out.println("Partitions scanned: " + scan.partitions().size());
        } else {
            out.println("Partitions in snapshot: " + scan.partitions().size() + " (scan stopped early: " + scan.stopReason() + ")");
        }

        scan.partitions().entrySet().stream()
                .sorted(Map.Entry.comparingByKey(
                        Comparator.comparing(TopicPartition::topic)
                                .thenComparingInt(TopicPartition::partition)))
                .forEach(entry -> {
                    TopicPartition partition = entry.getKey();
                    DeadLetterReader.PartitionBounds bounds = entry.getValue();

                    out.printf("  %s[%d]: offsets [%d, %d)%n",
                            escape(partition.topic()),
                            partition.partition(),
                            bounds.beginningOffset(),
                            bounds.endOffset());
                });

        out.println();

        if (groups.isEmpty()) {
            out.println("No dead-lettered records found.");
            return;
        }

        out.println("Reason\tTable\tSource\tCount\tPayload available\tPayload omitted"
                + "\tOriginal offsets\tDLT offsets");

        for (DeadLetterSummary.Group group : groups) {
            DeadLetterSummary.Key key = group.key();

            out.println(String.join("\t",
                    escape(key.reason()),
                    escape(key.table()),
                    escape(key.sourceId()),
                    Long.toString(group.count()),
                    Long.toString(group.payloadAvailable()),
                    Long.toString(group.omittedPayloads()),
                    formatRanges(group.originalOffsets()),
                    formatRanges(group.dltOffsets())));
        }

        if (invalidReplayRecordCount > 0) {
            out.println();
            out.println("Warning: Found " + invalidReplayRecordCount
                    + " record(s) with invalid replay identity metadata (refused by dlt-replay).");
        }

        if (ledgerResult != null) {
            out.println();
            out.println("Historical replay ledger outcomes (meta.replay_ledger):");
            for (Map.Entry<String, Long> entry : ledgerResult.outcomeCounts().entrySet()) {
                out.printf("  - %s: %d%n", entry.getKey(), entry.getValue());
            }
            out.println("Note: Historical outcomes reflect current rows in meta.replay_ledger. 'No ledger entry' indicates no current ledger row was found (records may be un-replayed or older entries may have been purged by retention).");

            if (!ledgerResult.recordStatuses().isEmpty()) {
                out.println();
                out.println("Historical record outcomes:");
                out.println("DLT Coordinate\tOriginal Target\tReplay ID\tHistorical Outcome");
                for (RecordLedgerStatus s : ledgerResult.recordStatuses()) {
                    DeadLetterRecord r = s.record();
                    String dltCoord = String.format("%s[%d]@%d", escape(r.dltTopic()), r.dltPartition(), r.dltOffset());
                    String origCoord = String.format("%s[%d]@%d", escape(r.originalTopic()), r.originalPartition(), r.originalOffset());
                    String replayId = s.replayIdentity() != null ? s.replayIdentity().replayId().toString() : "-";
                    String outcomeStr = s.status();
                    if (STATUS_IDENTITY_UNAVAILABLE.equals(s.status()) && s.detail() != null && !s.detail().isBlank()) {
                        outcomeStr = "Identity unavailable (" + escape(s.detail()) + ")";
                    }
                    out.printf("%s\t%s\t%s\t%s%n", dltCoord, origCoord, replayId, outcomeStr);
                }
            }
        }
    }

    private static String formatRanges(List<DeadLetterSummary.OffsetRange> ranges) {
        if (ranges.isEmpty()) {
            return "-";
        }

        return ranges.stream()
                .map(range -> escape(range.topic()) + "[" + range.partition() + "]:"
                        + range.oldestOffset() + ".." + range.newestOffset())
                .collect(Collectors.joining(", "));
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }

        return value.replace("\\", "\\\\")
                .replace("\t", "\\t")
                .replace("\r", "\\r")
                .replace("\n", "\\n");
    }
}
