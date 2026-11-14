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

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.fineract.bi.ingestion.routing.OwnershipSnapshot;
import org.apache.fineract.bi.ingestion.routing.Route;
import org.apache.fineract.bi.ingestion.sink.BatchWriter;
import org.apache.fineract.bi.ingestion.sink.PartitionOffset;
import org.apache.fineract.bi.ingestion.sink.PreflightRollbackException;
import org.apache.fineract.bi.ingestion.sink.RoutedEvent;
import org.apache.fineract.bi.ingestion.sink.SkipCheck;
import org.apache.fineract.bi.ingestion.sink.WarehouseWriter;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class WarehousePreflightValidatorTest {

    private static final Path ROOT = Path.of("../..");
    private Contract contract;

    @BeforeClass
    public void setup() {
        contract = ContractLoader.load(ROOT.resolve("contracts/fineract_tables.json"));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RecordHeaders headers(String table) {
        RecordHeaders h = new RecordHeaders();
        h.add("fbi.dlt.reason", bytes("WAREHOUSE_REJECTED"));
        h.add("fbi.dlt.message", bytes("test"));
        h.add("fbi.source.topic", bytes("fineract.cdc." + table));
        h.add("fbi.source.partition", bytes("0"));
        h.add("fbi.source.offset", bytes("10"));
        return h;
    }

    private static OwnershipSnapshot ownership(String source, String table, Route.Target target) {
        OwnershipSnapshot.Entry entry = new OwnershipSnapshot.Entry(
                target == Route.Target.PROD ? "KAFKA" : "PYTHON",
                target == Route.Target.SHADOW ? "KAFKA" : null,
                target == Route.Target.HOLD ? "PAUSED" : "STEADY",
                1L);
        return new OwnershipSnapshot(Map.of(OwnershipSnapshot.key(source, table), entry),
                Map.of(source, new OwnershipSnapshot.SourceMeta("tenant-a", 0)),
                System.currentTimeMillis());
    }

    private static Connection mockConnection() {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if ("getAutoCommit".equals(method.getName())) {
                        return true;
                    }
                    return null;
                });
    }

    static class MockWriter implements BatchWriter {
        boolean preflightCalled = false;
        SQLException failureToThrow = null;

        @Override
        public List<WarehouseWriter.Applied> write(Connection c, Collection<RoutedEvent> events,
                Collection<PartitionOffset> offsets, String consumerGroup, Collection<SkipCheck> skips) {
            return List.of();
        }

        @Override
        public List<WarehouseWriter.Applied> preflight(Connection c, Collection<RoutedEvent> events)
                throws SQLException {
            preflightCalled = true;
            if (failureToThrow != null) {
                throw failureToThrow;
            }
            return List.of(new WarehouseWriter.Applied("batch_job_instance", "prod", events.size(), 0));
        }
    }

    @Test
    public void rejectsMissingOrOmittedPayload() throws Exception {
        MockWriter writer = new MockWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        RecordHeaders h = headers("batch_job_instance");
        h.add("fbi.dlt.value_omitted", bytes("true"));
        DeadLetterRecord record = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 1L, h, null, contract);

        WarehousePreflightValidator.ValidationResult result =
                validator.validate(null, ownership("bank-a", "batch_job_instance", Route.Target.PROD), record);

        assertThat(result.passed()).isFalse();
        assertThat(result.reason()).contains("omitted or unavailable");
        assertThat(writer.preflightCalled).isFalse();
    }

    @Test
    public void rejectsInvalidReplayIdentityHeaders() throws Exception {
        MockWriter writer = new MockWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        RecordHeaders h = headers("batch_job_instance");
        h.add("fbi.replay.dlt.topic", bytes("orig.dlt")); // Incomplete! Missing partition and offset

        byte[] payload = bytes("{\"after\":{\"job_instance_id\":1},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord record = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 1L, h, payload, contract);

        WarehousePreflightValidator.ValidationResult result =
                validator.validate(null, ownership("bank-a", "batch_job_instance", Route.Target.PROD), record);

        assertThat(result.passed()).isFalse();
        assertThat(result.reason()).contains("Invalid replay identity headers");
        assertThat(writer.preflightCalled).isFalse();
    }

    @Test
    public void rejectsRouteHold() throws Exception {
        MockWriter writer = new MockWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        RecordHeaders h = headers("batch_job_instance");
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":1},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord record = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 1L, h, payload, contract);

        WarehousePreflightValidator.ValidationResult result =
                validator.validate(null, ownership("bank-a", "batch_job_instance", Route.Target.HOLD), record);

        assertThat(result.passed()).isFalse();
        assertThat(result.reason()).contains("paused (HOLD)");
        assertThat(writer.preflightCalled).isFalse();
    }

    @Test
    public void rejectsRouteSkip() throws Exception {
        MockWriter writer = new MockWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        RecordHeaders h = headers("batch_job_instance");
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":1},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord record = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 1L, h, payload, contract);

        WarehousePreflightValidator.ValidationResult result =
                validator.validate(null, ownership("bank-a", "batch_job_instance", Route.Target.SKIP), record);

        assertThat(result.passed()).isFalse();
        assertThat(result.reason()).contains("SKIP");
        assertThat(writer.preflightCalled).isFalse();
    }

    @Test
    public void rejectsUnknownSource() throws Exception {
        MockWriter writer = new MockWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        RecordHeaders h = headers("batch_job_instance");
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":1},\"source\":{\"name\":\"unregistered-bank\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord record = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 1L, h, payload, contract);

        WarehousePreflightValidator.ValidationResult result =
                validator.validate(null, ownership("bank-a", "batch_job_instance", Route.Target.PROD), record);

        assertThat(result.passed()).isFalse();
        assertThat(result.reason()).contains("Unknown source");
        assertThat(writer.preflightCalled).isFalse();
    }

    @Test
    public void passesValidEventThroughWarehousePreflight() throws Exception {
        MockWriter writer = new MockWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        RecordHeaders h = headers("batch_job_instance");
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":1},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord record = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 1L, h, payload, contract);

        WarehousePreflightValidator.ValidationResult result =
                validator.validate(mockConnection(), ownership("bank-a", "batch_job_instance", Route.Target.PROD), record);

        assertThat(result.passed()).isTrue();
        assertThat(result.reason()).isNull();
        assertThat(writer.preflightCalled).isTrue();
    }

    @Test
    public void capturesWarehouseRejectionDetails() throws Exception {
        MockWriter writer = new MockWriter();
        writer.failureToThrow = new SQLException("numeric field overflow", "22003");
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        RecordHeaders h = headers("batch_job_instance");
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":1},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord record = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 1L, h, payload, contract);

        WarehousePreflightValidator.ValidationResult result =
                validator.validate(mockConnection(), ownership("bank-a", "batch_job_instance", Route.Target.PROD), record);

        assertThat(result.passed()).isFalse();
        assertThat(result.reason()).contains("22003").contains("numeric field overflow");
        assertThat(writer.preflightCalled).isTrue();
    }

    @Test
    public void validateRoutePassesProdAndShadow() {
        MockWriter writer = new MockWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        RecordHeaders h = headers("batch_job_instance");
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":1},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord record = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 1L, h, payload, contract);

        WarehousePreflightValidator.ValidationResult prodResult =
                validator.validateRoute(ownership("bank-a", "batch_job_instance", Route.Target.PROD), record);
        assertThat(prodResult.passed()).isTrue();
        assertThat(prodResult.routedEvent().route().target()).isEqualTo(Route.Target.PROD);

        WarehousePreflightValidator.ValidationResult shadowResult =
                validator.validateRoute(ownership("bank-a", "batch_job_instance", Route.Target.SHADOW), record);
        assertThat(shadowResult.passed()).isTrue();
        assertThat(shadowResult.routedEvent().route().target()).isEqualTo(Route.Target.SHADOW);

        // Preflight writer should not have been called for route validation only
        assertThat(writer.preflightCalled).isFalse();
    }

    @Test
    public void validateRouteRejectsHoldSkipAndUnknownSource() {
        MockWriter writer = new MockWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        RecordHeaders h = headers("batch_job_instance");
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":1},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord record = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 1L, h, payload, contract);

        WarehousePreflightValidator.ValidationResult holdResult =
                validator.validateRoute(ownership("bank-a", "batch_job_instance", Route.Target.HOLD), record);
        assertThat(holdResult.passed()).isFalse();
        assertThat(holdResult.reason()).contains("paused (HOLD)");

        WarehousePreflightValidator.ValidationResult skipResult =
                validator.validateRoute(ownership("bank-a", "batch_job_instance", Route.Target.SKIP), record);
        assertThat(skipResult.passed()).isFalse();
        assertThat(skipResult.reason()).contains("SKIP");

        WarehousePreflightValidator.ValidationResult unknownResult =
                validator.validateRoute(ownership("unregistered", "batch_job_instance", Route.Target.UNKNOWN_SOURCE), record);
        assertThat(unknownResult.passed()).isFalse();
        assertThat(unknownResult.reason()).contains("Unknown source");

        assertThat(writer.preflightCalled).isFalse();
    }

    @Test
    public void validateWarehousePropagatesPreflightRollbackException() {
        MockWriter writer = new MockWriter();
        SQLException cause = new SQLException("disk failure", "08006");
        writer.failureToThrow = new PreflightRollbackException("rollback exploded", cause);
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        RecordHeaders h = headers("batch_job_instance");
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":1},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord record = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 1L, h, payload, contract);

        WarehousePreflightValidator.ValidationResult routeResult =
                validator.validateRoute(ownership("bank-a", "batch_job_instance", Route.Target.PROD), record);
        assertThat(routeResult.passed()).isTrue();

        assertThatThrownBy(() -> validator.validateWarehouse(mockConnection(), routeResult.routedEvent()))
                .isInstanceOf(PreflightRollbackException.class)
                .hasMessageContaining("rollback exploded")
                .hasCause(cause);

        // Also test through validate()
        assertThatThrownBy(() -> validator.validate(mockConnection(), ownership("bank-a", "batch_job_instance", Route.Target.PROD), record))
                .isInstanceOf(PreflightRollbackException.class)
                .hasMessageContaining("rollback exploded");
    }

    @Test
    public void validateWarehousePropagatesSetupFailure() {
        MockWriter writer = new MockWriter();
        SQLException cause = new SQLException("autoCommit setup failed", "08006");
        writer.failureToThrow = new PreflightRollbackException("Failed to initialize connection transaction state for preflight: setup failed", cause);
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);

        RecordHeaders h = headers("batch_job_instance");
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":1},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord record = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 1L, h, payload, contract);

        WarehousePreflightValidator.ValidationResult routeResult =
                validator.validateRoute(ownership("bank-a", "batch_job_instance", Route.Target.PROD), record);
        assertThat(routeResult.passed()).isTrue();

        assertThatThrownBy(() -> validator.validateWarehouse(mockConnection(), routeResult.routedEvent()))
                .isInstanceOf(PreflightRollbackException.class)
                .hasMessageContaining("Failed to initialize connection transaction state")
                .hasCause(cause);

        // Also test through validate()
        assertThatThrownBy(() -> validator.validate(mockConnection(), ownership("bank-a", "batch_job_instance", Route.Target.PROD), record))
                .isInstanceOf(PreflightRollbackException.class)
                .hasMessageContaining("Failed to initialize connection transaction state")
                .hasCause(cause);
    }

    @Test
    public void validateWarehouseRejectsNullConnection() {
        MockWriter writer = new MockWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);
        RecordHeaders h = headers("batch_job_instance");
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":1},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord record = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 1L, h, payload, contract);
        WarehousePreflightValidator.ValidationResult routeResult =
                validator.validateRoute(ownership("bank-a", "batch_job_instance", Route.Target.PROD), record);

        assertThatThrownBy(() -> validator.validateWarehouse(null, routeResult.routedEvent()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Database connection is required");
    }

    @Test
    public void validateWarehouseRejectsNullRoutedEvent() {
        MockWriter writer = new MockWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);
        assertThatThrownBy(() -> validator.validateWarehouse(mockConnection(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("RoutedEvent is required for warehouse preflight");
    }

    @Test
    public void validateRouteRejectsNullArguments() {
        MockWriter writer = new MockWriter();
        WarehousePreflightValidator validator = new WarehousePreflightValidator(contract, writer);
        RecordHeaders h = headers("batch_job_instance");
        byte[] payload = bytes("{\"after\":{\"job_instance_id\":1},\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\",\"ts_ms\":1000},\"op\":\"c\"}");
        DeadLetterRecord record = DeadLetterRecord.parse("fineract.cdc.dlt", 0, 1L, h, payload, contract);
        OwnershipSnapshot os = ownership("bank-a", "batch_job_instance", Route.Target.PROD);

        assertThatThrownBy(() -> validator.validateRoute(null, record))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("OwnershipSnapshot is required for route validation");

        assertThatThrownBy(() -> validator.validateRoute(os, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("DeadLetterRecord is required for route validation");
    }
}
