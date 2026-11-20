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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.routing.OwnershipCache;
import org.apache.fineract.bi.ingestion.routing.OwnershipSnapshot;
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
            Connection conn,
            OwnershipSnapshot ownership,
            WarehousePreflightValidator preflightValidator,
            Producer<byte[], byte[]> producer,
            PrintStream out) throws Exception {
        execute(config, contract, filter, dryRun, skipPreflight, limit, DeadLetterReader.DEFAULT_MAX_BYTES,
                conn, ownership, preflightValidator, producer, out);
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
        Map<DeadLetterRecord, String> failureReasons = new HashMap<>();
        List<DeadLetterRecord> readyToReplay = new ArrayList<>();

        record Candidate(DeadLetterRecord record, RoutedEvent routedEvent) {}
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

            routeValidCandidates.add(new Candidate(r, routeResult.routedEvent()));
        }

        List<Candidate> eligibleCandidates = new ArrayList<>();
        Map<DeadLetterRecord, ReplayOutcome> alreadyCompletedOutcomes = new LinkedHashMap<>();
        LedgerCheckStatus ledgerCheckStatus;

        if (routeValidCandidates.isEmpty()) {
            ledgerCheckStatus = LedgerCheckStatus.NOT_NEEDED;
        } else if (conn == null) {
            ledgerCheckStatus = LedgerCheckStatus.NOT_PERFORMED;
            eligibleCandidates.addAll(routeValidCandidates);
        } else {
            ledgerCheckStatus = LedgerCheckStatus.PERFORMED;
            List<ReplayIdentity> candidateIdentities = routeValidCandidates.stream()
                    .map(c -> c.routedEvent().replayIdentity())
                    .filter(Objects::nonNull)
                    .toList();
            Map<ReplayIdentity, ReplayOutcome> completedOutcomes =
                    WarehouseWriter.findCompletedReplayOutcomes(conn, candidateIdentities);

            for (Candidate c : routeValidCandidates) {
                ReplayIdentity id = c.routedEvent().replayIdentity();
                if (id != null && completedOutcomes.containsKey(id)) {
                    ReplayOutcome outcome = completedOutcomes.get(id);
                    if (outcome == null) {
                        outcome = ReplayOutcome.LEGACY_UNKNOWN;
                    }
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
                    eligibleCandidates.add(c);
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
            } else {
                for (Candidate c : eligibleCandidates) {
                    preflightFailed.add(c.record());
                    failureReasons.put(c.record(), batchResult.reason());
                }
            }
        } else if (skipPreflight) {
            for (Candidate c : eligibleCandidates) {
                readyToReplay.add(c.record());
            }
        }

        List<PublicationAttempt> publicationAttempts = new ArrayList<>();
        int acknowledgedCount = 0;
        int failedCount = 0;
        int unconfirmedCount = 0;
        Throwable firstFailureCause = null;
        Throwable flushError = null;

        if (!dryRun && !readyToReplay.isEmpty()) {
            for (DeadLetterRecord r : readyToReplay) {
                PublicationAttempt attempt = new PublicationAttempt(r);
                publicationAttempts.add(attempt);

                if (r.originalPartition() < 0) {
                    attempt.attempted = false;
                    attempt.status = PublicationStatus.FAILED;
                    attempt.errorMessage = "Invalid original partition: " + r.originalPartition();
                    attempt.errorCause = new IllegalArgumentException(attempt.errorMessage);
                    failureReasons.put(r, attempt.errorMessage);
                    if (firstFailureCause == null) {
                        firstFailureCause = attempt.errorCause;
                    }
                    continue;
                }

                attempt.attempted = true;
                try {
                    ProducerRecord<byte[], byte[]> pr = new ProducerRecord<>(
                            r.originalTopic(),
                            r.originalPartition(),
                            r.key(),
                            r.value());
                    ReplayIdentityHeaders.add(pr.headers(), r.replayIdentity());
                    attempt.future = producer.send(pr);
                } catch (Exception e) {
                    attempt.status = PublicationStatus.FAILED;
                    attempt.errorMessage = "Synchronous send failure: " + e.getMessage();
                    attempt.errorCause = e;
                    failureReasons.put(r, attempt.errorMessage);
                    if (firstFailureCause == null) {
                        firstFailureCause = e;
                    }
                }
            }

            try {
                producer.flush();
            } catch (Exception e) {
                flushError = e;
                if (firstFailureCause == null) {
                    firstFailureCause = e;
                }
            }

            boolean interrupted = false;
            for (int i = 0; i < publicationAttempts.size(); i++) {
                PublicationAttempt attempt = publicationAttempts.get(i);
                if (attempt.status == PublicationStatus.FAILED) {
                    continue;
                }

                if (interrupted) {
                    attempt.status = PublicationStatus.UNCONFIRMED;
                    attempt.errorMessage = flushError != null
                            ? "Wait cancelled due to thread interruption (producer flush failed: " + flushError.getMessage() + ")"
                            : "Wait cancelled due to thread interruption";
                    if (attempt.errorCause == null) {
                        attempt.errorCause = flushError;
                    }
                    failureReasons.put(attempt.record, attempt.errorMessage);
                    continue;
                }

                if (attempt.future == null) {
                    attempt.status = PublicationStatus.FAILED;
                    attempt.errorMessage = "No future returned from Kafka producer";
                    attempt.errorCause = new IllegalStateException(attempt.errorMessage);
                    failureReasons.put(attempt.record, attempt.errorMessage);
                    if (firstFailureCause == null) {
                        firstFailureCause = attempt.errorCause;
                    }
                    continue;
                }

                if (flushError != null && !attempt.future.isDone()) {
                    attempt.status = PublicationStatus.UNCONFIRMED;
                    attempt.errorMessage = "Producer flush failed with unfinished future: " + flushError.getMessage();
                    attempt.errorCause = flushError;
                    failureReasons.put(attempt.record, attempt.errorMessage);
                    continue;
                }

                try {
                    if (attempt.future.isCancelled()) {
                        attempt.status = PublicationStatus.FAILED;
                        attempt.errorMessage = "Future was cancelled";
                        attempt.errorCause = new CancellationException(attempt.errorMessage);
                        failureReasons.put(attempt.record, attempt.errorMessage);
                        if (firstFailureCause == null) {
                            firstFailureCause = attempt.errorCause;
                        }
                    } else {
                        attempt.future.get();
                        attempt.status = PublicationStatus.ACKNOWLEDGED;
                    }
                } catch (ExecutionException e) {
                    attempt.status = PublicationStatus.FAILED;
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    attempt.errorMessage = "Kafka publication failed: " + cause.getMessage();
                    attempt.errorCause = cause;
                    failureReasons.put(attempt.record, attempt.errorMessage);
                    if (firstFailureCause == null) {
                        firstFailureCause = cause;
                    }
                } catch (CancellationException e) {
                    attempt.status = PublicationStatus.FAILED;
                    attempt.errorMessage = "Future was cancelled: " + e.getMessage();
                    attempt.errorCause = e;
                    failureReasons.put(attempt.record, attempt.errorMessage);
                    if (firstFailureCause == null) {
                        firstFailureCause = e;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    interrupted = true;
                    attempt.status = PublicationStatus.UNCONFIRMED;
                    attempt.errorMessage = flushError != null
                            ? "Wait interrupted while awaiting Kafka acknowledgement (producer flush failed: " + flushError.getMessage() + ")"
                            : "Wait interrupted while awaiting Kafka acknowledgement";
                    attempt.errorCause = e;
                    failureReasons.put(attempt.record, attempt.errorMessage);
                    if (firstFailureCause == null) {
                        firstFailureCause = e;
                    }
                } catch (Exception e) {
                    attempt.status = PublicationStatus.FAILED;
                    attempt.errorMessage = "Kafka future inspection error: " + e.getMessage();
                    attempt.errorCause = e;
                    failureReasons.put(attempt.record, attempt.errorMessage);
                    if (firstFailureCause == null) {
                        firstFailureCause = e;
                    }
                }
            }

            if (flushError != null) {
                for (PublicationAttempt attempt : publicationAttempts) {
                    if (attempt.status == PublicationStatus.UNCONFIRMED) {
                        if (attempt.errorCause == null) {
                            attempt.errorCause = flushError;
                        }
                        if (attempt.errorMessage == null) {
                            attempt.errorMessage = "Producer flush failed: " + flushError.getMessage();
                            failureReasons.put(attempt.record, attempt.errorMessage);
                        }
                    }
                }
            }

            for (PublicationAttempt attempt : publicationAttempts) {
                switch (attempt.status) {
                    case ACKNOWLEDGED -> acknowledgedCount++;
                    case FAILED -> failedCount++;
                    case UNCONFIRMED -> unconfirmedCount++;
                }
            }
        }

        renderReport(scan, filter, dryRun, skipPreflight, matched, rejectedPayloads,
                invalidReplayHeaders, alreadyCompleted, alreadyCompletedOutcomes,
                preflightFailed, readyToReplay, acknowledgedCount, failedCount,
                unconfirmedCount, failureReasons, ledgerCheckStatus, flushError,
                predictedOutcomes, out);

        if (!dryRun && (failedCount > 0 || unconfirmedCount > 0 || flushError != null)) {
            String message;
            if (failedCount > 0 || unconfirmedCount > 0) {
                message = String.format(
                        "Replay publication incomplete: %d acknowledged, %d failed, %d unconfirmed",
                        acknowledgedCount, failedCount, unconfirmedCount);
            } else {
                message = String.format(
                        "Replay publication failed during flush: %d acknowledged, %d failed, %d unconfirmed",
                        acknowledgedCount, failedCount, unconfirmedCount);
            }
            throw new ReplayPublicationException(message, firstFailureCause, acknowledgedCount, failedCount, unconfirmedCount);
        }
    }

    public enum PublicationStatus {
        ACKNOWLEDGED,
        FAILED,
        UNCONFIRMED
    }

    public static class ReplayPublicationException extends IllegalStateException {
        private final int acknowledgedCount;
        private final int failedCount;
        private final int unconfirmedCount;

        public ReplayPublicationException(
                String message,
                Throwable cause,
                int acknowledgedCount,
                int failedCount,
                int unconfirmedCount) {
            super(message, cause);
            this.acknowledgedCount = acknowledgedCount;
            this.failedCount = failedCount;
            this.unconfirmedCount = unconfirmedCount;
        }

        public int acknowledgedCount() {
            return acknowledgedCount;
        }

        public int failedCount() {
            return failedCount;
        }

        public int unconfirmedCount() {
            return unconfirmedCount;
        }
    }

    public static final class PublicationAttempt {
        private final DeadLetterRecord record;
        private boolean attempted;
        private Future<RecordMetadata> future;
        private PublicationStatus status;
        private String errorMessage;
        private Throwable errorCause;

        PublicationAttempt(DeadLetterRecord record) {
            this.record = record;
            this.attempted = false;
            this.future = null;
            this.status = PublicationStatus.UNCONFIRMED;
        }

        public DeadLetterRecord record() { return record; }
        public boolean attempted() { return attempted; }
        public Future<RecordMetadata> future() { return future; }
        public PublicationStatus status() { return status; }
        public String errorMessage() { return errorMessage; }
        public Throwable errorCause() { return errorCause; }

        public boolean isAcknowledged() { return status == PublicationStatus.ACKNOWLEDGED; }
        public boolean isFailed() { return status == PublicationStatus.FAILED; }
        public boolean isUnconfirmed() { return status == PublicationStatus.UNCONFIRMED; }
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
            List<DeadLetterRecord> preflightFailed,
            List<DeadLetterRecord> readyToReplay,
            int replayedCount,
            Map<DeadLetterRecord, String> failureReasons,
            PrintStream out) {
        renderReport(scan, filter, dryRun, skipPreflight, matched, rejectedPayloads,
                invalidReplayHeaders, List.of(), Map.of(), preflightFailed, readyToReplay,
                replayedCount, 0, 0, failureReasons, LedgerCheckStatus.NOT_NEEDED, null, Map.of(), out);
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
            List<DeadLetterRecord> preflightFailed,
            List<DeadLetterRecord> readyToReplay,
            int replayedCount,
            Map<DeadLetterRecord, String> failureReasons,
            PrintStream out) {
        renderReport(scan, filter, dryRun, skipPreflight, matched, rejectedPayloads,
                invalidReplayHeaders, alreadyCompleted, Map.of(), preflightFailed, readyToReplay,
                replayedCount, 0, 0, failureReasons,
                alreadyCompleted.isEmpty() ? LedgerCheckStatus.NOT_NEEDED : LedgerCheckStatus.PERFORMED,
                null, Map.of(), out);
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
            List<DeadLetterRecord> preflightFailed,
            List<DeadLetterRecord> readyToReplay,
            int replayedCount,
            Map<DeadLetterRecord, String> failureReasons,
            LedgerCheckStatus ledgerCheckStatus,
            PrintStream out) {
        renderReport(scan, filter, dryRun, skipPreflight, matched, rejectedPayloads,
                invalidReplayHeaders, alreadyCompleted, Map.of(), preflightFailed, readyToReplay,
                replayedCount, 0, 0, failureReasons, ledgerCheckStatus, null, Map.of(), out);
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
            List<DeadLetterRecord> preflightFailed,
            List<DeadLetterRecord> readyToReplay,
            int replayedCount,
            int failedCount,
            int unconfirmedCount,
            Map<DeadLetterRecord, String> failureReasons,
            LedgerCheckStatus ledgerCheckStatus,
            PrintStream out) {
        renderReport(scan, filter, dryRun, skipPreflight, matched, rejectedPayloads,
                invalidReplayHeaders, alreadyCompleted, Map.of(), preflightFailed, readyToReplay,
                replayedCount, failedCount, unconfirmedCount, failureReasons,
                ledgerCheckStatus, null, Map.of(), out);
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
            List<DeadLetterRecord> preflightFailed,
            List<DeadLetterRecord> readyToReplay,
            int replayedCount,
            int failedCount,
            int unconfirmedCount,
            Map<DeadLetterRecord, String> failureReasons,
            LedgerCheckStatus ledgerCheckStatus,
            Throwable flushError,
            PrintStream out) {
        renderReport(scan, filter, dryRun, skipPreflight, matched, rejectedPayloads,
                invalidReplayHeaders, alreadyCompleted, Map.of(), preflightFailed, readyToReplay,
                replayedCount, failedCount, unconfirmedCount, failureReasons,
                ledgerCheckStatus, flushError, Map.of(), out);
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
            List<DeadLetterRecord> readyToReplay,
            int replayedCount,
            int failedCount,
            int unconfirmedCount,
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
                    long unknown = 0;
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
                            } else if (o == ReplayOutcome.LEGACY_UNKNOWN) {
                                unknown++;
                            } else if (o == ReplayOutcome.CLAIMED) {
                                claimed++;
                            }
                        }
                    }
                    out.printf("      - APPLIED: %d%n", applied);
                    out.printf("      - SKIPPED_STALE: %d%n", stale);
                    out.printf("      - SKIPPED_SUPERSEDED: %d%n", superseded);
                    if (unknown > 0) {
                        out.printf("      - LEGACY_UNKNOWN: %d%n", unknown);
                    }
                    if (claimed > 0) {
                        out.printf("      - CLAIMED: %d (in-progress/unresolved)%n", claimed);
                    }
                }
            }
            case NOT_NEEDED -> out.println("  - Already completed (in ledger): 0 (ledger check not needed)");
            case NOT_PERFORMED -> out.println("  - Already completed (in ledger): not checked (ledger check not performed: no database connection)");
        }
        out.printf("  - Preflight failed: %d%n", preflightFailed.size());
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
            out.printf("Records replayed to Kafka: %d%n", replayedCount);
            out.printf("  - Acknowledged: %d%n", replayedCount);
            out.printf("  - Failed: %d%n", failedCount);
            out.printf("  - Unconfirmed: %d%n", unconfirmedCount);
            if (flushError != null) {
                out.printf("Producer flush failed: %s%n", flushError.getMessage());
            }
            if (failedCount > 0 || unconfirmedCount > 0 || flushError != null) {
                if (failedCount > 0 || unconfirmedCount > 0) {
                    out.println("PARTIAL PUBLICATION: some records failed to publish or remain unconfirmed.");
                } else {
                    out.println("PUBLICATION FAILED: producer flush encountered an error despite acknowledged futures.");
                }
            }
            out.println();
            out.println("Notice: Records reported as Acknowledged have been accepted by the Kafka broker.");
            out.println("Warehouse ingestion consumes Kafka asynchronously; broker acknowledgement does not prove warehouse application.");
            out.println("Durable warehouse outcomes can be inspected later using 'dlt-list --with-ledger' after the sink processes the replay.");
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

    private static final Set<String> VALUE_OPTIONS = Set.of(
            "reason", "table", "source", "offset-range", "limit", "max-bytes"
    );

    private static final Set<String> BOOLEAN_OPTIONS = Set.of(
            "execute", "dry-run", "skip-preflight", "confirm-all"
    );

    private static final Map<String, String> REMOVED_ALIASES = Map.of(
            "source-id", "source",
            "offsets", "offset-range",
            "preflight-only", "dry-run",
            "confirm-unfiltered", "confirm-all"
    );

    static Map<String, String> parseArgs(String[] args) {
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

            if (REMOVED_ALIASES.containsKey(optName)) {
                String replacement = REMOVED_ALIASES.get(optName);
                throw new IllegalArgumentException(
                        "Option '--" + optName + "' is no longer supported. Use '--" + replacement + "' instead.");
            }

            if (!VALUE_OPTIONS.contains(optName) && !BOOLEAN_OPTIONS.contains(optName)) {
                throw new IllegalArgumentException("Unknown option: '--" + optName + "'.");
            }

            if (values.containsKey(optName)) {
                throw new IllegalArgumentException("Duplicate option: '--" + optName + "'.");
            }

            if (VALUE_OPTIONS.contains(optName)) {
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
