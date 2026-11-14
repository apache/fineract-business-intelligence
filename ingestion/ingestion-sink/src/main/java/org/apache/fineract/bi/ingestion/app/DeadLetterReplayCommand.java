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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.routing.OwnershipCache;
import org.apache.fineract.bi.ingestion.routing.OwnershipSnapshot;
import org.apache.fineract.bi.ingestion.sink.PreflightRollbackException;
import org.apache.fineract.bi.ingestion.sink.WarehouseWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class DeadLetterReplayCommand {

    private DeadLetterReplayCommand() {
    }

    public record OffsetFilter(String topic, int partition, long minOffset, long maxOffset) {
        public boolean matches(String recordTopic, int recordPartition, long recordOffset) {
            if (topic != null && !topic.equals(recordTopic)) {
                return false;
            }
            if (partition != recordPartition) {
                return false;
            }
            return recordOffset >= minOffset && recordOffset <= maxOffset;
        }

        public static OffsetFilter parse(String spec, String scopedTopic) {
            if (spec == null || spec.isBlank()) {
                return null;
            }
            String s = spec.trim();
            String topic = scopedTopic;
            int partition;
            String rangePart;

            int bracketOpen = s.indexOf('[');
            int bracketClose = s.indexOf(']');
            int colon = s.indexOf(':');

            if (bracketOpen >= 0 && bracketClose > bracketOpen && colon > bracketClose) {
                String explicitTopic = s.substring(0, bracketOpen).trim();
                if (explicitTopic.isEmpty()) {
                    throw new IllegalArgumentException("Topic name in offset filter must not be empty: " + s);
                }
                if (scopedTopic != null && !scopedTopic.equalsIgnoreCase(explicitTopic)) {
                    throw new IllegalArgumentException(
                            "Offset filter topic '" + explicitTopic + "' does not match scoped table topic '" + scopedTopic + "'");
                }
                topic = explicitTopic;
                partition = Integer.parseInt(s.substring(bracketOpen + 1, bracketClose).trim());
                rangePart = s.substring(colon + 1).trim();
            } else if (colon >= 0 && bracketOpen < 0 && bracketClose < 0) {
                if (scopedTopic == null || scopedTopic.isBlank()) {
                    throw new IllegalArgumentException(
                            "Partition-only offset filter '" + s + "' is ambiguous without a table scope. "
                                    + "Specify --table=<table> or provide a fully-qualified filter '<topic>[<partition>]:<min>..<max>'");
                }
                partition = Integer.parseInt(s.substring(0, colon).trim());
                rangePart = s.substring(colon + 1).trim();
            } else {
                throw new IllegalArgumentException(
                        "Original offset filter must be partition-aware, e.g. '<partition>:<min>..<max>' (with --table) "
                                + "or '<topic>[<partition>]:<min>..<max>'");
            }

            if (partition < 0) {
                throw new IllegalArgumentException("Partition must not be negative: " + partition);
            }

            long min;
            long max;
            if (rangePart.contains("..")) {
                String[] parts = rangePart.split("\\.\\.", 2);
                min = Long.parseLong(parts[0].trim());
                max = Long.parseLong(parts[1].trim());
            } else if (rangePart.contains("-")) {
                String[] parts = rangePart.split("-", 2);
                min = Long.parseLong(parts[0].trim());
                max = Long.parseLong(parts[1].trim());
            } else {
                min = Long.parseLong(rangePart.trim());
                max = min;
            }

            if (min < 0 || max < 0) {
                throw new IllegalArgumentException("Offset bounds must not be negative");
            }
            if (min > max) {
                throw new IllegalArgumentException("Offset min (" + min + ") must not exceed max (" + max + ")");
            }

            return new OffsetFilter(topic, partition, min, max);
        }
    }

    public record ReplayFilter(String reason, String sourceId, String table, OffsetFilter offsetFilter) {
        public boolean matches(DeadLetterRecord r) {
            if (reason != null && !reason.equalsIgnoreCase(r.reason())) {
                return false;
            }
            if (sourceId != null && !sourceId.equals(r.sourceId())) {
                return false;
            }
            if (table != null && !table.equalsIgnoreCase(r.table())) {
                return false;
            }
            if (offsetFilter != null
                    && !offsetFilter.matches(r.originalTopic(), r.originalPartition(), r.originalOffset())) {
                return false;
            }
            return true;
        }
    }

    public static void run(SinkConfig config, Contract contract, String[] args) throws Exception {
        Map<String, String> opts = parseArgs(args);

        String reason = opts.get("reason");
        String table = opts.get("table");
        String sourceId = opts.getOrDefault("source", opts.get("source-id"));
        String offsetRangeStr = opts.getOrDefault("offset-range", opts.get("offsets"));
        boolean execute = "true".equalsIgnoreCase(opts.get("execute"));
        boolean dryRun = !execute || "true".equalsIgnoreCase(opts.getOrDefault("dry-run", opts.get("preflight-only")));
        boolean skipPreflight = "true".equalsIgnoreCase(opts.get("skip-preflight"));
        int limit = opts.containsKey("limit") ? Integer.parseInt(opts.get("limit")) : Integer.MAX_VALUE;
        boolean confirmAll = "true".equalsIgnoreCase(opts.getOrDefault("confirm-all", opts.get("confirm-unfiltered")));

        boolean hasFilter = reason != null || sourceId != null || table != null || offsetRangeStr != null;
        if (execute && !hasFilter && limit == Integer.MAX_VALUE && !confirmAll) {
            throw new IllegalArgumentException(
                    "Refusing broad replay without filters. Specify at least one filter (--reason, --source, --table, --offset-range), "
                            + "a bounded --limit=<n>, or pass --confirm-all to replay all records.");
        }

        String scopedTopic = table != null ? contract.table(table).map(t -> "fineract.cdc." + t.tableName()).orElse(null) : null;
        OffsetFilter offsetFilter = OffsetFilter.parse(offsetRangeStr, scopedTopic);
        ReplayFilter filter = new ReplayFilter(reason, sourceId, table, offsetFilter);

        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(config.dbUrl());
        hc.setUsername(config.dbUser());
        hc.setPassword(config.dbPassword());
        hc.setMaximumPoolSize(2);
        hc.setAutoCommit(true);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.writerId() + "-dlt-replay");
        props.putAll(config.producerOverrides());

        try (HikariDataSource ds = new HikariDataSource(hc);
                Producer<byte[], byte[]> producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
            OwnershipCache ownershipCache = new OwnershipCache(ds, 1000);
            ownershipCache.refresh(true);
            WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");
            WarehousePreflightValidator preflightValidator = new WarehousePreflightValidator(contract, writer);

            Connection conn = null;
            try {
                conn = ds.getConnection();
                execute(config, contract, filter, dryRun, skipPreflight, limit,
                        conn, ownershipCache.snapshot(), preflightValidator, producer, System.out);
            } catch (PreflightRollbackException e) {
                if (conn != null) {
                    try {
                        ds.evictConnection(conn);
                    } catch (Throwable ignored) {
                    }
                    try {
                        if (!conn.isClosed()) {
                            conn.abort(Runnable::run);
                        }
                    } catch (Throwable ignored) {
                    }
                    conn = null;
                }
                throw e;
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException ignored) {
                    }
                }
            }
        }
    }

    static void execute(
            SinkConfig config,
            Contract contract,
            ReplayFilter filter,
            boolean dryRun,
            boolean skipPreflight,
            int limit,
            Connection conn,
            OwnershipSnapshot ownership,
            WarehousePreflightValidator preflightValidator,
            Producer<byte[], byte[]> producer,
            PrintStream out) throws Exception {

        List<DeadLetterRecord> matched = new ArrayList<>();
        DeadLetterReader.ScanResult scan;

        try (DeadLetterReader reader = new DeadLetterReader(config, contract)) {
            scan = reader.scan(record -> {
                if (filter.matches(record) && matched.size() < limit) {
                    matched.add(record);
                }
            });
        }

        processMatchedRecords(scan, matched, contract, filter, dryRun, skipPreflight,
                conn, ownership, preflightValidator, producer, out);
    }

    static void processMatchedRecords(
            DeadLetterReader.ScanResult scan,
            List<DeadLetterRecord> matched,
            Contract contract,
            ReplayFilter filter,
            boolean dryRun,
            boolean skipPreflight,
            Connection conn,
            OwnershipSnapshot ownership,
            WarehousePreflightValidator preflightValidator,
            Producer<byte[], byte[]> producer,
            PrintStream out) throws Exception {

        Objects.requireNonNull(preflightValidator, "WarehousePreflightValidator must not be null");
        Objects.requireNonNull(ownership, "OwnershipSnapshot must not be null");
        if (!skipPreflight && conn == null) {
            throw new IllegalArgumentException(
                    "Database connection is required for preflight validation unless --skip-preflight is enabled");
        }

        List<DeadLetterRecord> rejectedPayloads = new ArrayList<>();
        List<DeadLetterRecord> invalidReplayHeaders = new ArrayList<>();
        List<DeadLetterRecord> preflightFailed = new ArrayList<>();
        Map<DeadLetterRecord, String> failureReasons = new HashMap<>();
        List<DeadLetterRecord> readyToReplay = new ArrayList<>();

        for (DeadLetterRecord r : matched) {
            // Payload parsing and ownership routing checks must NEVER be bypassed
            WarehousePreflightValidator.ValidationResult routeResult = preflightValidator.validateRoute(ownership, r);

            if (!routeResult.passed()) {
                if (!r.payloadAvailable()) {
                    rejectedPayloads.add(r);
                } else if (r.hasInvalidReplayIdentity()) {
                    invalidReplayHeaders.add(r);
                } else {
                    preflightFailed.add(r);
                }
                failureReasons.put(r, routeResult.reason());
                continue;
            }

            // Real database warehouse preflight validation against actual write semantics
            if (!skipPreflight) {
                try {
                    WarehousePreflightValidator.ValidationResult warehouseResult =
                            preflightValidator.validateWarehouse(conn, routeResult.routedEvent());
                    if (!warehouseResult.passed()) {
                        preflightFailed.add(r);
                        failureReasons.put(r, warehouseResult.reason());
                        continue;
                    }
                } catch (PreflightRollbackException e) {
                    // Transaction setup, rollback, or cleanup failed during preflight validation; transaction state is unrecoverable.
                    // Immediately abort operation and ensure connection is closed/discarded.
                    if (conn != null) {
                        try {
                            if (!conn.isClosed()) {
                                conn.abort(Runnable::run);
                            }
                        } catch (Throwable ignored) {
                        }
                    }
                    throw e;
                }
            }

            readyToReplay.add(r);
        }

        int replayedCount = 0;
        if (!dryRun && !readyToReplay.isEmpty()) {
            List<Future<RecordMetadata>> futures = new ArrayList<>(readyToReplay.size());
            for (DeadLetterRecord r : readyToReplay) {
                if (r.originalPartition() < 0) {
                    throw new IllegalStateException("Original partition is negative or invalid: " + r.originalPartition());
                }
                ProducerRecord<byte[], byte[]> pr = new ProducerRecord<>(
                        r.originalTopic(),
                        r.originalPartition(),
                        r.key(),
                        r.value());
                ReplayIdentityHeaders.add(pr.headers(), r.replayIdentity());
                futures.add(producer.send(pr));
            }
            producer.flush();
            for (Future<RecordMetadata> f : futures) {
                f.get();
                replayedCount++;
            }
        }

        renderReport(scan, filter, dryRun, skipPreflight, matched, rejectedPayloads,
                invalidReplayHeaders, preflightFailed, readyToReplay, replayedCount, failureReasons, out);
    }

    static void renderReport(
            DeadLetterReader.ScanResult scan,
            ReplayFilter filter,
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

        out.println("Dead-letter replay");
        out.println("==================");
        out.printf("Mode: %s%n", dryRun ? "PREFLIGHT / DRY-RUN (pass --execute to republish validated records)" : "LIVE REPLAY (--execute)");
        out.printf("Preflight validation: %s%n", skipPreflight ? "SKIPPED" : "ENABLED");
        out.printf("Records scanned: %d%n", scan.recordsRead());
        out.printf("Records matched filters: %d%n", matched.size());
        out.printf("  - Payload unavailable / omitted: %d%n", rejectedPayloads.size());
        out.printf("  - Invalid replay identity headers: %d%n", invalidReplayHeaders.size());
        out.printf("  - Preflight failed: %d%n", preflightFailed.size());
        out.printf("  - Preflight passed: %d%n", readyToReplay.size());
        if (!dryRun) {
            out.printf("Records replayed to Kafka: %d%n", replayedCount);
        }
        out.println();

        if (!failureReasons.isEmpty()) {
            out.println("Rejected / Failed records:");
            out.println("DLT Coordinate\tOriginal Target\tReason");
            for (Map.Entry<DeadLetterRecord, String> entry : failureReasons.entrySet()) {
                DeadLetterRecord r = entry.getKey();
                out.printf("%s[%d]@%d\t%s[%d]\t%s%n",
                        r.dltTopic(), r.dltPartition(), r.dltOffset(),
                        r.originalTopic(), r.originalPartition(),
                        entry.getValue());
            }
            out.println();
        }

        if (dryRun && !readyToReplay.isEmpty()) {
            out.println("Preflight verified candidates ready for live replay:");
            out.println("DLT Coordinate\tOriginal Target\tTable\tSource");
            for (DeadLetterRecord r : readyToReplay) {
                out.printf("%s[%d]@%d\t%s[%d]\t%s\t%s%n",
                        r.dltTopic(), r.dltPartition(), r.dltOffset(),
                        r.originalTopic(), r.originalPartition(),
                        r.table(), r.sourceId());
            }
        }
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> values = new HashMap<>();
        if (args == null) {
            return values;
        }
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.startsWith("--")) {
                int eq = a.indexOf('=');
                if (eq > 0) {
                    values.put(a.substring(2, eq).trim().toLowerCase(), a.substring(eq + 1).trim());
                } else {
                    values.put(a.substring(2).trim().toLowerCase(), "true");
                }
            }
        }
        return values;
    }
}
