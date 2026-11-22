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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.fineract.bi.ingestion.cdc.ChangeEvent;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.TableContract;
import org.apache.fineract.bi.ingestion.routing.OwnershipCache;
import org.apache.fineract.bi.ingestion.routing.OwnershipSnapshot;
import org.apache.fineract.bi.ingestion.routing.Route;
import org.apache.fineract.bi.ingestion.sink.PreflightRollbackException;
import org.apache.fineract.bi.ingestion.sink.ReplayIdentity;
import org.apache.fineract.bi.ingestion.sink.ReplayOutcome;
import org.apache.fineract.bi.ingestion.sink.RoutedEvent;
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

    record Candidate(int index, DeadLetterRecord record, RoutedEvent routedEvent) {}
    record GroupKey(String sourceId, String table) {}
    record EntityKey(String sourceId, String table, String tenantId, String normalizedPk) {}

    static EntityKey extractEntityKey(Contract contract, Candidate c) {
        if (contract == null || c == null) {
            return null;
        }
        RoutedEvent re = c.routedEvent();
        if (re == null || re.event() == null) {
            return null;
        }
        ChangeEvent e = re.event();
        String sourceId = e.sourceId();
        String table = e.table();
        if (sourceId == null || table == null) {
            return null;
        }
        Optional<TableContract> optTable = contract.table(table);
        if (optTable.isEmpty()) {
            return null;
        }
        TableContract tc = optTable.get();
        int pkIdx = tc.primaryKeyIndex();
        String[] values = e.values();
        if (pkIdx < 0 || values == null || pkIdx >= values.length) {
            return null;
        }
        String rawVal = values[pkIdx];
        if (rawVal == null) {
            return null;
        }
        String pkType = tc.column(tc.primaryKey()).type();
        String normalizedPk = normalizePrimaryKey(pkType, rawVal);
        if (normalizedPk == null) {
            return null;
        }
        String tenantId = re.route() != null && re.route().tenantId() != null ? re.route().tenantId() : "";
        return new EntityKey(sourceId, table, tenantId, normalizedPk);
    }

    static String normalizePrimaryKey(String type, String raw) {
        if (type == null || raw == null) {
            return null;
        }
        String base = type.trim().toLowerCase(Locale.ROOT);
        int paren = base.indexOf('(');
        String typeName = paren >= 0 ? base.substring(0, paren).trim() : base;
        switch (typeName) {
            case "bigint":
            case "int8":
            case "bigserial":
                try {
                    return Long.toString(Long.parseLong(raw.trim()));
                } catch (NumberFormatException e) {
                    return null;
                }
            case "integer":
            case "int":
            case "int4":
            case "serial":
                try {
                    return Integer.toString(Integer.parseInt(raw.trim()));
                } catch (NumberFormatException e) {
                    return null;
                }
            case "smallint":
            case "int2":
            case "smallserial":
                try {
                    return Short.toString(Short.parseShort(raw.trim()));
                } catch (NumberFormatException e) {
                    return null;
                }
            case "uuid":
                try {
                    return UUID.fromString(raw.trim()).toString();
                } catch (IllegalArgumentException e) {
                    return null;
                }
            case "text":
            case "varchar":
            case "character varying":
                return raw;
            case "char":
            case "character":
            case "bpchar":
                int end = raw.length();
                while (end > 0 && raw.charAt(end - 1) == ' ') {
                    end--;
                }
                return raw.substring(0, end);
            case "numeric":
            case "decimal":
                try {
                    java.math.BigDecimal bd = new java.math.BigDecimal(raw.trim());
                    if (bd.compareTo(java.math.BigDecimal.ZERO) == 0) {
                        return "0";
                    }
                    return bd.stripTrailingZeros().toPlainString();
                } catch (Exception e) {
                    return null;
                }
            default:
                return null;
        }
    }

    static final class IsolationState {
        private final boolean[] provisionallyValid;
        private final Set<Integer> individuallyInvalid = new HashSet<>();
        private final Map<Integer, String> invalidReasons = new HashMap<>();
        private final Set<Integer> inconclusive = new HashSet<>();
        private final Map<Integer, String> inconclusiveReasons = new HashMap<>();
        private boolean aborted;
        private String abortReason;

        IsolationState(int size) {
            this.provisionallyValid = new boolean[size];
        }

        boolean isAborted() {
            return aborted;
        }

        void abort(String reason) {
            this.aborted = true;
            this.abortReason = reason;
        }

        String abortReason() {
            return abortReason;
        }

        void markProvisionallyValid(int index) {
            provisionallyValid[index] = true;
        }

        boolean isProvisionallyValid(int index) {
            return provisionallyValid[index];
        }

        void markIndividuallyInvalid(int index, String reason) {
            individuallyInvalid.add(index);
            invalidReasons.put(index, reason);
        }

        boolean isIndividuallyInvalid(int index) {
            return individuallyInvalid.contains(index);
        }

        String invalidReason(int index) {
            return invalidReasons.get(index);
        }

        void markInconclusive(int index, String reason) {
            inconclusive.add(index);
            inconclusiveReasons.put(index, reason);
        }

        boolean isInconclusive(int index) {
            return inconclusive.contains(index);
        }

        String inconclusiveReason(int index) {
            return inconclusiveReasons.get(index);
        }
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
        if (args != null && args.length > 0 && "dlt-replay".equalsIgnoreCase(args[0])) {
            args = Arrays.copyOfRange(args, 1, args.length);
        }
        Map<String, String> opts = parseArgs(args);

        String reason = opts.get("reason");
        String table = opts.get("table");
        String sourceId = opts.get("source");
        String offsetRangeStr = opts.get("offset-range");
        boolean execute = "true".equals(opts.get("execute"));
        boolean dryRun = !execute;
        boolean skipPreflight = "true".equals(opts.get("skip-preflight"));
        final int limit;
        if (opts.containsKey("limit")) {
            try {
                limit = Integer.parseInt(opts.get("limit"));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid integer value for '--limit': " + opts.get("limit"));
            }
        } else {
            limit = Integer.MAX_VALUE;
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive: " + limit);
        }

        final long maxBytes;
        if (opts.containsKey("max-bytes")) {
            try {
                maxBytes = Long.parseLong(opts.get("max-bytes"));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid long value for '--max-bytes': " + opts.get("max-bytes"));
            }
        } else {
            maxBytes = DeadLetterReader.DEFAULT_MAX_BYTES;
        }
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("max-bytes must be positive: " + maxBytes);
        }

        boolean confirmAll = "true".equals(opts.get("confirm-all"));

        boolean hasFilter = reason != null || sourceId != null || table != null || offsetRangeStr != null;
        if (execute && !hasFilter && limit == Integer.MAX_VALUE && !confirmAll) {
            throw new IllegalArgumentException(
                    "Refusing broad replay without filters. Specify at least one filter (--reason, --source, --table, --offset-range), "
                            + "a bounded --limit=<n>, or pass --confirm-all to replay all records.");
        }

        String scopedTopic = table != null
                ? contract.table(table).map(t -> contract.topicFor(t.tableName())).orElseGet(() -> contract.topicFor(table))
                : null;
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

            executeWithDataSource(ds, conn -> execute(config, contract, filter, dryRun, skipPreflight, limit, maxBytes,
                    conn, ownershipCache.snapshot(), preflightValidator, producer, System.out));
        }
    }

    @FunctionalInterface
    interface ReplayExecutor {
        void execute(Connection conn) throws Exception;
    }

    static void executeWithDataSource(HikariDataSource ds, ReplayExecutor executor) throws Exception {
        Connection conn = null;
        try {
            conn = ds.getConnection();
            executor.execute(conn);
        } catch (PreflightRollbackException e) {
            if (conn != null) {
                Connection toDispose = conn;
                conn = null;
                try {
                    ds.evictConnection(toDispose);
                } catch (Exception ex) {
                    e.addSuppressed(ex);
                }
                try {
                    if (!toDispose.isClosed()) {
                        toDispose.abort(Runnable::run);
                    }
                } catch (Exception ex) {
                    e.addSuppressed(ex);
                }
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

    static void execute(
            SinkConfig config,
            Contract contract,
            ReplayFilter filter,
            boolean dryRun,
            boolean skipPreflight,
            int limit,
            long maxBytes,
            Connection conn,
            OwnershipSnapshot ownership,
            WarehousePreflightValidator preflightValidator,
            Producer<byte[], byte[]> producer,
            PrintStream out) throws Exception {

        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive: " + limit);
        }
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("max-bytes must be positive: " + maxBytes);
        }

        List<DeadLetterRecord> matched = new ArrayList<>();
        DeadLetterReader.ScanOptions options = new DeadLetterReader.ScanOptions(limit, maxBytes);
        DeadLetterReader.ScanResult scan;

        try (DeadLetterReader reader = new DeadLetterReader(config, contract)) {
            scan = reader.scan(options, record -> {
                if (filter.matches(record)) {
                    matched.add(record);
                    return new DeadLetterReader.VisitResult(true, false);
                }
                return new DeadLetterReader.VisitResult(false, false);
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
        if (!dryRun && conn == null) {
            throw new IllegalArgumentException(
                    "Database connection is required to check completed replays before live execution");
        }
        if (!skipPreflight && conn == null) {
            throw new IllegalArgumentException(
                    "Database connection is required for preflight validation unless --skip-preflight is enabled");
        }

        List<DeadLetterRecord> rejectedPayloads = new ArrayList<>();
        List<DeadLetterRecord> invalidReplayHeaders = new ArrayList<>();
        List<DeadLetterRecord> alreadyCompleted = new ArrayList<>();
        List<DeadLetterRecord> preflightFailed = new ArrayList<>();
        List<DeadLetterRecord> preflightInconclusive = new ArrayList<>();
        Map<DeadLetterRecord, String> failureReasons = new ConcurrentHashMap<>();
        List<DeadLetterRecord> readyToReplay = new ArrayList<>();

        List<Candidate> routeValidCandidates = new ArrayList<>();

        for (DeadLetterRecord r : matched) {
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

            routeValidCandidates.add(new Candidate(-1, r, routeResult.routedEvent()));
        }

        List<Candidate> eligibleCandidates = new ArrayList<>();
        Map<DeadLetterRecord, ReplayOutcome> alreadyCompletedOutcomes = new LinkedHashMap<>();
        LedgerCheckStatus ledgerCheckStatus;

        if (routeValidCandidates.isEmpty()) {
            ledgerCheckStatus = LedgerCheckStatus.NOT_NEEDED;
        } else if (conn == null) {
            ledgerCheckStatus = LedgerCheckStatus.NOT_PERFORMED;
            int idx = 0;
            for (Candidate c : routeValidCandidates) {
                eligibleCandidates.add(new Candidate(idx++, c.record(), c.routedEvent()));
            }
        } else {
            ledgerCheckStatus = LedgerCheckStatus.PERFORMED;
            List<ReplayIdentity> candidateIdentities = routeValidCandidates.stream()
                    .map(c -> c.routedEvent().replayIdentity())
                    .filter(Objects::nonNull)
                    .toList();
            Map<ReplayIdentity, ReplayOutcome> completedOutcomes =
                    WarehouseWriter.findCompletedReplayOutcomes(conn, candidateIdentities);

            int idx = 0;
            for (Candidate c : routeValidCandidates) {
                ReplayIdentity id = c.routedEvent().replayIdentity();
                if (id != null && completedOutcomes.containsKey(id)) {
                    ReplayOutcome outcome = completedOutcomes.get(id);
                    alreadyCompleted.add(c.record());
                    alreadyCompletedOutcomes.put(c.record(), outcome);
                    if (outcome == ReplayOutcome.CLAIMED) {
                        failureReasons.put(c.record(),
                                "Already present in replay ledger (" + id.replayId() + ": CLAIMED - in-progress/unresolved)");
                    } else {
                        failureReasons.put(c.record(),
                                "Already completed in replay ledger (" + id.replayId() + ": " + outcome + ")");
                    }
                } else {
                    eligibleCandidates.add(new Candidate(idx++, c.record(), c.routedEvent()));
                }
            }
        }

        Map<ReplayIdentity, ReplayOutcome> predictedOutcomes = null;

        if (!skipPreflight && !eligibleCandidates.isEmpty()) {
            List<RoutedEvent> routedEvents = eligibleCandidates.stream()
                    .map(Candidate::routedEvent)
                    .toList();
            WarehousePreflightValidator.BatchValidationResult batchResult =
                    preflightValidator.validateWarehouse(conn, routedEvents);
            if (batchResult.passed()) {
                for (Candidate c : eligibleCandidates) {
                    readyToReplay.add(c.record());
                }
                predictedOutcomes = batchResult.predictedOutcomes();
            } else if (!batchResult.isRecordLevelError()) {
                for (Candidate c : eligibleCandidates) {
                    preflightFailed.add(c.record());
                    failureReasons.put(c.record(), batchResult.reason());
                }
            } else {
                IsolationState state = new IsolationState(eligibleCandidates.size());
                isolateCandidates(conn, preflightValidator, contract, eligibleCandidates, batchResult.reason(), state);

                if (state.isAborted()) {
                    for (Candidate c : eligibleCandidates) {
                        if (state.isIndividuallyInvalid(c.index())) {
                            preflightFailed.add(c.record());
                            failureReasons.put(c.record(), state.invalidReason(c.index()));
                        } else if (state.isInconclusive(c.index())) {
                            preflightInconclusive.add(c.record());
                            failureReasons.put(c.record(), state.inconclusiveReason(c.index()));
                        } else {
                            preflightInconclusive.add(c.record());
                            failureReasons.put(c.record(),
                                    "Warehouse validation inconclusive: non-isolatable error encountered during isolation ("
                                            + state.abortReason() + ")");
                        }
                    }
                } else {
                    for (Candidate c : eligibleCandidates) {
                        if (state.isIndividuallyInvalid(c.index())) {
                            preflightFailed.add(c.record());
                            failureReasons.put(c.record(), state.invalidReason(c.index()));
                        } else if (state.isInconclusive(c.index())) {
                            preflightInconclusive.add(c.record());
                            failureReasons.put(c.record(), state.inconclusiveReason(c.index()));
                        } else if (!state.isProvisionallyValid(c.index())) {
                            preflightInconclusive.add(c.record());
                            failureReasons.put(c.record(),
                                    "Warehouse validation inconclusive: candidate was not resolved during isolation");
                        }
                    }

                    List<Candidate> provisionallyValid = new ArrayList<>();
                    for (Candidate c : eligibleCandidates) {
                        if (state.isProvisionallyValid(c.index())) {
                            provisionallyValid.add(c);
                        }
                    }

                    if (!provisionallyValid.isEmpty()) {
                        List<RoutedEvent> combinedEvents = provisionallyValid.stream()
                                .map(Candidate::routedEvent)
                                .toList();
                        WarehousePreflightValidator.BatchValidationResult combinedResult =
                                preflightValidator.validateWarehouse(conn, combinedEvents);
                        if (combinedResult.passed()) {
                            for (Candidate c : provisionallyValid) {
                                readyToReplay.add(c.record());
                            }
                            predictedOutcomes = combinedResult.predictedOutcomes();
                        } else {
                            String inconclusiveReason = "Warehouse validation inconclusive: provisionally valid records failed when combined in a single batch ("
                                    + combinedResult.reason() + ")";
                            for (Candidate c : provisionallyValid) {
                                preflightInconclusive.add(c.record());
                                failureReasons.put(c.record(), inconclusiveReason);
                            }
                        }
                    }
                }
            }
        } else if (skipPreflight) {
            for (Candidate c : eligibleCandidates) {
                readyToReplay.add(c.record());
            }
        }

        Object publicationLock = new Object();
        AtomicInteger acknowledgedCount = new AtomicInteger();
        AtomicInteger failedCount = new AtomicInteger();
        AtomicReference<Throwable> firstFailureCause = new AtomicReference<>();
        Throwable flushError = null;

        if (!dryRun && !readyToReplay.isEmpty()) {
            for (DeadLetterRecord r : readyToReplay) {
                if (r.originalPartition() < 0) {
                    String msg = "Invalid original partition: " + r.originalPartition();
                    synchronized (publicationLock) {
                        failedCount.incrementAndGet();
                        failureReasons.put(r, msg);
                        firstFailureCause.compareAndSet(null, new IllegalArgumentException(msg));
                    }
                    continue;
                }

                try {
                    ProducerRecord<byte[], byte[]> pr = new ProducerRecord<>(
                            r.originalTopic(),
                            r.originalPartition(),
                            r.key(),
                            r.value());
                    ReplayIdentityHeaders.add(pr.headers(), r.replayIdentity());
                    producer.send(pr, (metadata, exception) -> {
                        if (exception == null) {
                            synchronized (publicationLock) {
                                acknowledgedCount.incrementAndGet();
                            }
                        } else {
                            String msg = "Kafka publication failed: " + (exception.getMessage() != null ? exception.getMessage() : exception.getClass().getSimpleName());
                            synchronized (publicationLock) {
                                failedCount.incrementAndGet();
                                failureReasons.put(r, msg);
                                firstFailureCause.compareAndSet(null, exception);
                            }
                        }
                    });
                } catch (Exception e) {
                    String msg = "Synchronous send failure: " + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
                    synchronized (publicationLock) {
                        failedCount.incrementAndGet();
                        failureReasons.put(r, msg);
                        firstFailureCause.compareAndSet(null, e);
                    }
                }
            }

            try {
                producer.flush();
            } catch (Exception e) {
                flushError = e;
                synchronized (publicationLock) {
                    firstFailureCause.compareAndSet(null, e);
                }
            }
        }

        PublicationSnapshot publication;
        Throwable publicationFailureCause;
        synchronized (publicationLock) {
            int ack = acknowledgedCount.get();
            int failed = failedCount.get();
            int pending = Math.max(0, readyToReplay.size() - (ack + failed));
            publication = new PublicationSnapshot(ack, failed, pending, failureReasons);
            publicationFailureCause = firstFailureCause.get();
        }

        renderReport(scan, filter, dryRun, skipPreflight, matched, rejectedPayloads,
                invalidReplayHeaders, alreadyCompleted, alreadyCompletedOutcomes,
                preflightFailed, preflightInconclusive, readyToReplay, publication,
                publication.failureReasons(), ledgerCheckStatus, flushError, predictedOutcomes, out);

        if (!dryRun && (publication.failedCount() > 0 || flushError != null)) {
            String message;
            if (flushError != null && publication.pendingCallbacks() > 0) {
                int pendingCallbacks = publication.pendingCallbacks();
                String pendingDesc = pendingCallbacks == 1 ? "1 callback pending" : pendingCallbacks + " callbacks pending";
                message = String.format(
                        "Replay publication failed during flush: observed %d acknowledged, %d failed (%s)",
                        publication.acknowledgedCount(), publication.failedCount(), pendingDesc);
            } else if (publication.failedCount() > 0) {
                message = String.format(
                        "Replay publication incomplete: %d acknowledged, %d failed",
                        publication.acknowledgedCount(), publication.failedCount());
            } else {
                message = String.format(
                        "Replay publication failed during flush: %d acknowledged, %d failed",
                        publication.acknowledgedCount(), publication.failedCount());
            }
            Throwable cause = flushError != null ? flushError : publicationFailureCause;
            throw new ReplayPublicationException(message, cause, publication);
        }
    }

    static void isolateCandidates(
            Connection conn,
            WarehousePreflightValidator validator,
            Contract contract,
            List<Candidate> candidates,
            String initialReason,
            IsolationState state) throws SQLException {

        Map<GroupKey, List<Candidate>> groups = new LinkedHashMap<>();
        for (Candidate c : candidates) {
            GroupKey key = new GroupKey(c.routedEvent().event().sourceId(), c.routedEvent().event().table());
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(c);
        }

        for (List<Candidate> groupCandidates : groups.values()) {
            if (state.isAborted()) {
                break;
            }

            Map<EntityKey, List<Candidate>> entityMap = new LinkedHashMap<>();
            boolean resolvable = true;
            for (Candidate c : groupCandidates) {
                EntityKey eKey = extractEntityKey(contract, c);
                if (eKey == null) {
                    resolvable = false;
                    break;
                }
                entityMap.computeIfAbsent(eKey, k -> new ArrayList<>()).add(c);
            }

            if (!resolvable) {
                for (Candidate c : groupCandidates) {
                    state.markInconclusive(c.index(),
                            "Warehouse validation inconclusive: primary-key identity could not be reliably determined for entity isolation");
                }
                continue;
            }

            List<List<Candidate>> entityUnits = new ArrayList<>(entityMap.values());
            if (groups.size() == 1) {
                if (entityUnits.size() == 1) {
                    List<Candidate> singleUnit = entityUnits.get(0);
                    if (singleUnit.size() == 1) {
                        Candidate c = singleUnit.get(0);
                        state.markIndividuallyInvalid(c.index(), initialReason);
                    } else {
                        for (Candidate c : singleUnit) {
                            state.markInconclusive(c.index(),
                                    "Warehouse validation inconclusive: multi-record entity unit failed validation and individual attribution is unsafe ("
                                            + initialReason + ")");
                        }
                    }
                } else {
                    int mid = entityUnits.size() / 2;
                    List<List<Candidate>> left = entityUnits.subList(0, mid);
                    List<List<Candidate>> right = entityUnits.subList(mid, entityUnits.size());
                    bisectEntitySlice(conn, validator, left, state);
                    if (!state.isAborted()) {
                        bisectEntitySlice(conn, validator, right, state);
                    }
                }
            } else {
                bisectEntitySlice(conn, validator, entityUnits, state);
            }
        }
    }

    static void bisectEntitySlice(
            Connection conn,
            WarehousePreflightValidator validator,
            List<List<Candidate>> entityUnits,
            IsolationState state) throws SQLException {

        if (state.isAborted() || entityUnits == null || entityUnits.isEmpty()) {
            return;
        }

        List<Candidate> sliceCandidates = new ArrayList<>();
        for (List<Candidate> unit : entityUnits) {
            sliceCandidates.addAll(unit);
        }
        List<RoutedEvent> sliceEvents = sliceCandidates.stream().map(Candidate::routedEvent).toList();
        WarehousePreflightValidator.BatchValidationResult sliceResult =
                validator.validateWarehouse(conn, sliceEvents);

        if (sliceResult.passed()) {
            for (Candidate c : sliceCandidates) {
                state.markProvisionallyValid(c.index());
            }
        } else if (!sliceResult.isRecordLevelError()) {
            state.abort(sliceResult.reason());
        } else if (entityUnits.size() == 1) {
            List<Candidate> singleUnit = entityUnits.get(0);
            if (singleUnit.size() == 1) {
                Candidate c = singleUnit.get(0);
                state.markIndividuallyInvalid(c.index(), sliceResult.reason());
            } else {
                for (Candidate c : singleUnit) {
                    state.markInconclusive(c.index(),
                            "Warehouse validation inconclusive: multi-record entity unit failed validation and individual attribution is unsafe ("
                                    + sliceResult.reason() + ")");
                }
            }
        } else {
            int mid = entityUnits.size() / 2;
            List<List<Candidate>> left = entityUnits.subList(0, mid);
            List<List<Candidate>> right = entityUnits.subList(mid, entityUnits.size());
            bisectEntitySlice(conn, validator, left, state);
            if (!state.isAborted()) {
                bisectEntitySlice(conn, validator, right, state);
            }
        }
    }

    public record PublicationSnapshot(
            int acknowledgedCount,
            int failedCount,
            int pendingCallbacks,
            Map<DeadLetterRecord, String> failureReasons) {

        public PublicationSnapshot(
                int acknowledgedCount,
                int failedCount,
                int pendingCallbacks,
                Map<DeadLetterRecord, String> failureReasons) {
            this.acknowledgedCount = acknowledgedCount;
            this.failedCount = failedCount;
            this.pendingCallbacks = pendingCallbacks;
            this.failureReasons = failureReasons != null
                    ? Collections.unmodifiableMap(new LinkedHashMap<>(failureReasons))
                    : Map.of();
        }

        public PublicationSnapshot(int acknowledgedCount, int failedCount, int pendingCallbacks) {
            this(acknowledgedCount, failedCount, pendingCallbacks, Map.of());
        }

        public PublicationSnapshot(int acknowledgedCount, int failedCount) {
            this(acknowledgedCount, failedCount, 0, Map.of());
        }
    }

    public static class ReplayPublicationException extends IllegalStateException {
        private final PublicationSnapshot publication;

        public ReplayPublicationException(
                String message,
                Throwable cause,
                PublicationSnapshot publication) {
            super(message, cause);
            this.publication = publication != null ? publication : new PublicationSnapshot(0, 0, 0, Map.of());
        }

        public ReplayPublicationException(
                String message,
                Throwable cause,
                int acknowledgedCount,
                int failedCount) {
            this(message, cause, new PublicationSnapshot(acknowledgedCount, failedCount, 0, Map.of()));
        }

        public int acknowledgedCount() {
            return publication.acknowledgedCount();
        }

        public int failedCount() {
            return publication.failedCount();
        }

        public PublicationSnapshot publication() {
            return publication;
        }
    }

    public enum LedgerCheckStatus {
        PERFORMED,
        NOT_NEEDED,
        NOT_PERFORMED
    }

    static void renderReport(
            DeadLetterReader.ScanResult scan,
            ReplayFilter filter,
            boolean dryRun,
            boolean skipPreflight,
            List<DeadLetterRecord> matched,
            List<DeadLetterRecord> rejectedPayloads,
            List<DeadLetterRecord> invalidReplayHeaders,
            List<DeadLetterRecord> alreadyCompleted,
            Map<DeadLetterRecord, ReplayOutcome> alreadyCompletedOutcomes,
            List<DeadLetterRecord> preflightFailed,
            List<DeadLetterRecord> preflightInconclusive,
            List<DeadLetterRecord> readyToReplay,
            PublicationSnapshot publication,
            Map<DeadLetterRecord, String> failureReasons,
            LedgerCheckStatus ledgerCheckStatus,
            Throwable flushError,
            Map<ReplayIdentity, ReplayOutcome> predictedOutcomes,
            PrintStream out) {

        out.println("Dead-letter replay");
        out.println("==================");
        out.printf("Mode: %s%n", dryRun ? "PREFLIGHT / DRY-RUN (pass --execute to republish validated records)" : "LIVE REPLAY (--execute)");
        out.printf("Preflight validation: %s%n", skipPreflight ? "SKIPPED" : "ENABLED");
        out.printf("Records scanned: %d%n", scan.recordsScanned());
        out.printf("Records matched filters: %d%n", scan.recordsMatched());
        out.printf("Bytes scanned: %d%n", scan.bytesScanned());
        out.printf("Scan stop reason: %s%n", scan.stopReason());
        switch (scan.stopReason()) {
            case COMPLETED -> out.println("Snapshot scan completed.");
            case RECORD_LIMIT_REACHED -> out.println("Scan stopped because the requested match limit was reached.");
            case BYTE_BUDGET_REACHED -> out.println("Scan stopped because the maximum byte budget was reached. Some snapshot records may remain unscanned.");
            case VISITOR_REQUESTED -> out.println("Scan stopped at the visitor's request.");
        }
        out.printf("  - Payload unavailable / omitted: %d%n", rejectedPayloads.size());
        out.printf("  - Invalid replay identity headers: %d%n", invalidReplayHeaders.size());
        LedgerCheckStatus status = ledgerCheckStatus != null ? ledgerCheckStatus :
                (alreadyCompleted.isEmpty() ? LedgerCheckStatus.NOT_NEEDED : LedgerCheckStatus.PERFORMED);
        switch (status) {
            case PERFORMED -> {
                out.printf("  - Already completed (in ledger): %d (ledger check performed)%n", alreadyCompleted.size());
                if (!alreadyCompleted.isEmpty()) {
                    long applied = 0;
                    long stale = 0;
                    long superseded = 0;
                    long claimed = 0;
                    if (alreadyCompletedOutcomes != null) {
                        for (DeadLetterRecord r : alreadyCompleted) {
                            ReplayOutcome o = alreadyCompletedOutcomes.get(r);
                            if (o == ReplayOutcome.APPLIED) {
                                applied++;
                            } else if (o == ReplayOutcome.SKIPPED_STALE) {
                                stale++;
                            } else if (o == ReplayOutcome.SKIPPED_SUPERSEDED) {
                                superseded++;
                            } else if (o == ReplayOutcome.CLAIMED) {
                                claimed++;
                            }
                        }
                    }
                    out.printf("      - APPLIED: %d%n", applied);
                    out.printf("      - SKIPPED_STALE: %d%n", stale);
                    out.printf("      - SKIPPED_SUPERSEDED: %d%n", superseded);
                    if (claimed > 0) {
                        out.printf("      - CLAIMED: %d (in-progress/unresolved)%n", claimed);
                    }
                }
            }
            case NOT_NEEDED -> out.println("  - Already completed (in ledger): 0 (ledger check not needed)");
            case NOT_PERFORMED -> out.println("  - Already completed (in ledger): not checked (ledger check not performed: no database connection)");
        }
        out.printf("  - Preflight failed: %d%n", preflightFailed.size());
        if (preflightInconclusive != null && !preflightInconclusive.isEmpty()) {
            out.printf("  - Preflight inconclusive: %d%n", preflightInconclusive.size());
        }
        out.printf("  - Preflight passed: %d%n", readyToReplay.size());
        if (dryRun && !skipPreflight && predictedOutcomes != null && !predictedOutcomes.isEmpty()) {
            long predictedApplied = 0;
            long predictedStale = 0;
            long predictedSuperseded = 0;
            for (ReplayOutcome outcome : predictedOutcomes.values()) {
                if (outcome == ReplayOutcome.APPLIED) {
                    predictedApplied++;
                } else if (outcome == ReplayOutcome.SKIPPED_STALE) {
                    predictedStale++;
                } else if (outcome == ReplayOutcome.SKIPPED_SUPERSEDED) {
                    predictedSuperseded++;
                }
            }
            out.println();
            out.println("Predicted warehouse outcomes (point-in-time estimate; not committed):");
            out.printf("  - APPLIED: %d%n", predictedApplied);
            out.printf("  - SKIPPED_STALE: %d%n", predictedStale);
            out.printf("  - SKIPPED_SUPERSEDED: %d%n", predictedSuperseded);
        }
        if (!dryRun) {
            out.printf("Records ready for replay: %d%n", readyToReplay.size());
            int replayedCount = publication != null ? publication.acknowledgedCount() : 0;
            int failedCount = publication != null ? publication.failedCount() : 0;
            int pendingCallbacks = publication != null ? publication.pendingCallbacks() : 0;
            if (flushError != null && pendingCallbacks > 0) {
                out.println("Callback outcomes observed at reporting time after flush failure:");
                out.printf("  - Acknowledged: %d%n", replayedCount);
                out.printf("  - Failed: %d%n", failedCount);
                out.printf("Producer flush failed: %s%n", flushError.getMessage());
                out.println("PUBLICATION FAILED: producer flush encountered an error before all callbacks completed.");
            } else {
                out.printf("Records replayed to Kafka: %d%n", replayedCount);
                out.printf("  - Acknowledged: %d%n", replayedCount);
                out.printf("  - Failed: %d%n", failedCount);
                if (flushError != null) {
                    out.printf("Producer flush failed: %s%n", flushError.getMessage());
                }
                if (failedCount > 0 || flushError != null) {
                    if (failedCount > 0) {
                        out.println("PARTIAL PUBLICATION: some records failed to publish.");
                    } else {
                        out.println("PUBLICATION FAILED: producer flush encountered an error despite acknowledged records.");
                    }
                }
            }
            out.println();
            out.println("Notice: Records reported as Acknowledged have been accepted by the Kafka broker.");
            out.println("Warehouse ingestion consumes Kafka asynchronously; broker acknowledgement does not prove warehouse application.");
            out.println("Durable warehouse outcomes can be inspected later using 'dlt-list --with-ledger' after the sink processes the replay.");
        }
        out.println();

        Map<DeadLetterRecord, String> reportedFailureReasons = publication != null && publication.failureReasons() != null && (!publication.failureReasons().isEmpty() || failureReasons == null || failureReasons.isEmpty())
                ? publication.failureReasons()
                : failureReasons;

        if (reportedFailureReasons != null && !reportedFailureReasons.isEmpty()) {
            out.println("Rejected / Failed records:");
            out.println("DLT Coordinate\tOriginal Target\tReason");
            for (Map.Entry<DeadLetterRecord, String> entry : reportedFailureReasons.entrySet()) {
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

    private static final Set<String> VALUE_OPTIONS = Set.of(
            "reason", "table", "source", "offset-range", "limit", "max-bytes"
    );

    private static final Set<String> BOOLEAN_OPTIONS = Set.of(
            "execute", "dry-run", "skip-preflight", "confirm-all"
    );

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> values = DeadLetterListCommand.parseArgs(args, VALUE_OPTIONS, BOOLEAN_OPTIONS);

        boolean hasExecute = "true".equals(values.get("execute"));
        boolean hasDryRun = "true".equals(values.get("dry-run"));

        if (hasExecute && hasDryRun) {
            throw new IllegalArgumentException(
                    "Contradictory execution modes: cannot specify both --execute and --dry-run.");
        }
        if ("false".equals(values.get("execute")) && "false".equals(values.get("dry-run"))) {
            throw new IllegalArgumentException(
                    "Contradictory execution modes: cannot disable both --execute and --dry-run.");
        }
        if ("false".equals(values.get("dry-run")) && !hasExecute) {
            throw new IllegalArgumentException("Cannot specify --dry-run=false without --execute.");
        }

        return values;
    }
}
