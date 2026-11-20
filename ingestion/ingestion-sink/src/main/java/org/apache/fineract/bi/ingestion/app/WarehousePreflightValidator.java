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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.fineract.bi.ingestion.cdc.ChangeEvent;
import org.apache.fineract.bi.ingestion.cdc.DebeziumEnvelopeParser;
import org.apache.fineract.bi.ingestion.cdc.PoisonEventException;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.routing.OwnershipSnapshot;
import org.apache.fineract.bi.ingestion.routing.Route;
import org.apache.fineract.bi.ingestion.sink.BatchWriter;
import org.apache.fineract.bi.ingestion.sink.PreflightRollbackException;
import org.apache.fineract.bi.ingestion.sink.ReplayIdentity;
import org.apache.fineract.bi.ingestion.sink.ReplayOutcome;
import org.apache.fineract.bi.ingestion.sink.RoutedEvent;

public final class WarehousePreflightValidator {

    public record ValidationResult(boolean passed, String reason, RoutedEvent routedEvent) {
    }

    public record BatchValidationResult(
            boolean passed,
            String reason,
            Collection<RoutedEvent> routedEvents,
            Map<ReplayIdentity, ReplayOutcome> predictedOutcomes) {

        public BatchValidationResult(boolean passed, String reason, Collection<RoutedEvent> routedEvents) {
            this(passed, reason, routedEvents, Map.of());
        }

        public BatchValidationResult {
            routedEvents = routedEvents == null ? List.of() : List.copyOf(routedEvents);
            predictedOutcomes = predictedOutcomes == null ? Map.of() : Map.copyOf(predictedOutcomes);
        }
    }

    private final Contract contract;
    private final DebeziumEnvelopeParser parser;
    private final BatchWriter writer;

    public WarehousePreflightValidator(Contract contract, BatchWriter writer) {
        this.contract = Objects.requireNonNull(contract, "contract");
        this.parser = new DebeziumEnvelopeParser(contract);
        this.writer = Objects.requireNonNull(writer, "writer");
    }

    public ValidationResult validateRoute(OwnershipSnapshot ownership, DeadLetterRecord record) {
        Objects.requireNonNull(ownership, "OwnershipSnapshot is required for route validation");
        Objects.requireNonNull(record, "DeadLetterRecord is required for route validation");
        if (!record.payloadAvailable()) {
            return new ValidationResult(false, "Payload is omitted or unavailable", null);
        }

        if (record.hasInvalidReplayIdentity()) {
            return new ValidationResult(
                    false, "Invalid replay identity headers: " + record.replayIdentityError(), null);
        }

        Optional<ChangeEvent> parsed;
        try {
            parsed = parser.parse(record.originalTopic(), record.value());
        } catch (PoisonEventException e) {
            return new ValidationResult(false, "Poison event: " + e.getMessage(), null);
        } catch (Exception e) {
            return new ValidationResult(false, "Failed to parse CDC envelope: " + e.getMessage(), null);
        }

        if (parsed.isEmpty()) {
            return new ValidationResult(false, "CDC event parsed to empty (unsupported or ignored)", null);
        }

        ChangeEvent event = parsed.get();
        Route route = ownership.route(event.sourceId(), event.table());

        if (route.target() == Route.Target.HOLD) {
            return new ValidationResult(
                    false, "Route is paused (HOLD) for source " + event.sourceId() + " table " + event.table(), null);
        }
        if (route.target() == Route.Target.SKIP) {
            return new ValidationResult(
                    false, "Route is SKIP for source " + event.sourceId() + " table " + event.table() + " (not owned by CDC)", null);
        }
        if (route.target() == Route.Target.UNKNOWN_SOURCE) {
            return new ValidationResult(
                    false, "Unknown source: " + event.sourceId() + " is not registered", null);
        }
        if (route.target() != Route.Target.PROD && route.target() != Route.Target.SHADOW) {
            return new ValidationResult(
                    false, "Unhandled route target: " + route.target(), null);
        }

        RoutedEvent routed = new RoutedEvent(event, route, record.replayIdentity());
        return new ValidationResult(true, null, routed);
    }

    public BatchValidationResult validateWarehouse(Connection connection, Collection<RoutedEvent> routedEvents)
            throws PreflightRollbackException {
        Objects.requireNonNull(connection, "Database connection is required for warehouse preflight");
        Objects.requireNonNull(routedEvents, "RoutedEvents collection is required for warehouse preflight");
        if (routedEvents.isEmpty()) {
            return new BatchValidationResult(true, null, List.of(), Map.of());
        }
        try {
            BatchWriter.PreflightResult preflightResult = writer.preflight(connection, routedEvents);
            return new BatchValidationResult(true, null, routedEvents, preflightResult.predictedOutcomes());
        } catch (PreflightRollbackException e) {
            throw e;
        } catch (SQLException e) {
            return new BatchValidationResult(
                    false, "Warehouse validation failed: [" + e.getSQLState() + "] " + e.getMessage(), routedEvents, Map.of());
        } catch (Exception e) {
            return new BatchValidationResult(
                    false, "Warehouse validation failed: " + e.getMessage(), routedEvents, Map.of());
        }
    }

    public ValidationResult validateWarehouse(Connection connection, RoutedEvent routed)
            throws PreflightRollbackException {
        Objects.requireNonNull(routed, "RoutedEvent is required for warehouse preflight");
        BatchValidationResult batchResult = validateWarehouse(connection, List.of(routed));
        return new ValidationResult(batchResult.passed(), batchResult.reason(), routed);
    }

    public ValidationResult validate(Connection connection, OwnershipSnapshot ownership, DeadLetterRecord record)
            throws PreflightRollbackException {
        ValidationResult routeResult = validateRoute(ownership, record);
        if (!routeResult.passed()) {
            return routeResult;
        }
        return validateWarehouse(connection, routeResult.routedEvent());
    }
}
