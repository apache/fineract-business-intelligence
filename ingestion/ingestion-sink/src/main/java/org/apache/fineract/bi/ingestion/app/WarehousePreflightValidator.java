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
import java.util.List;
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
import org.apache.fineract.bi.ingestion.sink.RoutedEvent;

public final class WarehousePreflightValidator {

    public record ValidationResult(boolean passed, String reason, RoutedEvent routedEvent) {
    }

    private final Contract contract;
    private final DebeziumEnvelopeParser parser;
    private final BatchWriter writer;

    public WarehousePreflightValidator(Contract contract, BatchWriter writer) {
        this.contract = Objects.requireNonNull(contract, "contract");
        this.parser = new DebeziumEnvelopeParser(contract);
        this.writer = Objects.requireNonNull(writer, "writer");
    }

    /**
     * Validates CDC payload availability, replay identity headers, Debezium envelope parsing,
     * and current ownership routing.
     * <p>
     * Rejects records with omitted/unavailable payloads, invalid replay headers, poison JSON,
     * or routes that are HOLD, SKIP, or UNKNOWN_SOURCE. Only records routed to PROD or SHADOW pass.
     */
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

    /**
     * Executes warehouse preflight staging and apply_staged_batch validation inside a transaction
     * that is unconditionally rolled back.
     * <p>
     * Ordinary warehouse validation rejections return a failed {@link ValidationResult}.
     * If preflight setup, rollback, or connection restoration fails, {@link PreflightRollbackException}
     * is propagated immediately to abort the replay operation and prevent connection reuse.
     */
    public ValidationResult validateWarehouse(Connection connection, RoutedEvent routed)
            throws PreflightRollbackException {
        Objects.requireNonNull(connection, "Database connection is required for warehouse preflight");
        Objects.requireNonNull(routed, "RoutedEvent is required for warehouse preflight");
        try {
            writer.preflight(connection, List.of(routed));
            return new ValidationResult(true, null, routed);
        } catch (PreflightRollbackException e) {
            throw e;
        } catch (SQLException e) {
            return new ValidationResult(
                    false, "Warehouse validation failed: [" + e.getSQLState() + "] " + e.getMessage(), routed);
        }
    }

    /**
     * Validates both payload routing and warehouse staging against the database.
     */
    public ValidationResult validate(Connection connection, OwnershipSnapshot ownership, DeadLetterRecord record)
            throws PreflightRollbackException {
        ValidationResult routeResult = validateRoute(ownership, record);
        if (!routeResult.passed()) {
            return routeResult;
        }
        return validateWarehouse(connection, routeResult.routedEvent());
    }
}
