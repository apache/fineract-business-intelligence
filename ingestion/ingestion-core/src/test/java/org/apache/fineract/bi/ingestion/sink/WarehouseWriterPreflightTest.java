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
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.fineract.bi.ingestion.cdc.ChangeEvent;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.fineract.bi.ingestion.routing.Route;
import org.postgresql.PGConnection;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class WarehouseWriterPreflightTest {

    private Contract contract;
    private WarehouseWriter writer;

    @BeforeClass
    public void setup() {
        contract = ContractLoader.load(Path.of("../../contracts/fineract_tables.json"));
        writer = new WarehouseWriter(contract, "KAFKA");
    }

    private RoutedEvent dummyEvent() {
        String[] values = new String[] {"1", "test-job", null};
        ChangeEvent e = new ChangeEvent("src-1", "batch_job_instance", ChangeEvent.Op.UPSERT, values,
                List.of(), 100L, 1_700_000_000_000_000L);
        Route route = new Route(Route.Target.PROD, 1, "tenant-a", 1, null);
        return new RoutedEvent(e, route);
    }

    private Connection mockConnection(boolean initialAutoCommit, SQLException rollbackException,
            SQLException unwrapException, AtomicBoolean autoCommitState, AtomicInteger rollbackCount) {
        return mockConnection(initialAutoCommit, rollbackException, unwrapException, null, autoCommitState, rollbackCount);
    }

    private Connection mockConnection(boolean initialAutoCommit, SQLException rollbackException,
            SQLException unwrapException, SQLException setAutoCommitException,
            AtomicBoolean autoCommitState, AtomicInteger rollbackCount) {
        return mockConnection(initialAutoCommit, null, null, setAutoCommitException, rollbackException,
                unwrapException, autoCommitState, rollbackCount);
    }

    private Connection mockConnection(
            boolean initialAutoCommit,
            SQLException getAutoCommitException,
            SQLException setAutoCommitFalseException,
            SQLException setAutoCommitTrueException,
            SQLException rollbackException,
            SQLException unwrapException,
            AtomicBoolean autoCommitState,
            AtomicInteger rollbackCount) {
        autoCommitState.set(initialAutoCommit);
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("getAutoCommit".equals(name)) {
                        if (getAutoCommitException != null) {
                            throw getAutoCommitException;
                        }
                        return autoCommitState.get();
                    }
                    if ("setAutoCommit".equals(name)) {
                        boolean targetVal = (Boolean) args[0];
                        if (!targetVal && setAutoCommitFalseException != null) {
                            throw setAutoCommitFalseException;
                        }
                        if (targetVal && setAutoCommitTrueException != null) {
                            throw setAutoCommitTrueException;
                        }
                        autoCommitState.set(targetVal);
                        return null;
                    }
                    if ("rollback".equals(name)) {
                        rollbackCount.incrementAndGet();
                        if (rollbackException != null) {
                            throw rollbackException;
                        }
                        return null;
                    }
                    if ("unwrap".equals(name)) {
                        Class<?> target = (Class<?>) args[0];
                        if (unwrapException != null) {
                            throw unwrapException;
                        }
                        if (target.isAssignableFrom(Connection.class)) {
                            return proxy;
                        }
                        throw new SQLException("Unwrap target not supported: " + target.getName());
                    }
                    if ("isWrapperFor".equals(name)) {
                        Class<?> target = (Class<?>) args[0];
                        return target.isAssignableFrom(Connection.class) || (unwrapException == null && target.isAssignableFrom(PGConnection.class));
                    }
                    if ("close".equals(name)) {
                        return null;
                    }
                    throw new UnsupportedOperationException("Unsupported method: " + name);
                });
    }

    @Test
    public void preflightFailsWhenGetAutoCommitFails() {
        AtomicBoolean autoCommit = new AtomicBoolean();
        AtomicInteger rollbacks = new AtomicInteger();
        SQLException getAutoCommitError = new SQLException("getAutoCommit failed", "08006");
        Connection c = mockConnection(true, getAutoCommitError, null, null, null, null, autoCommit, rollbacks);

        assertThatThrownBy(() -> writer.preflight(c, List.of()))
                .isInstanceOf(PreflightRollbackException.class)
                .hasMessageContaining("Failed to initialize connection transaction state")
                .hasCause(getAutoCommitError);

        assertThat(rollbacks.get()).isEqualTo(0);
    }

    @Test
    public void preflightFailsWhenSetAutoCommitFalseFails() {
        AtomicBoolean autoCommit = new AtomicBoolean();
        AtomicInteger rollbacks = new AtomicInteger();
        SQLException setAutoCommitFalseError = new SQLException("setAutoCommit(false) failed", "08006");
        Connection c = mockConnection(true, null, setAutoCommitFalseError, null, null, null, autoCommit, rollbacks);

        assertThatThrownBy(() -> writer.preflight(c, List.of()))
                .isInstanceOf(PreflightRollbackException.class)
                .hasMessageContaining("Failed to initialize connection transaction state")
                .hasCause(setAutoCommitFalseError);

        assertThat(rollbacks.get()).isEqualTo(0);
    }

    @Test
    public void preflightSucceedsAndRestoresAutoCommitWhenRollbackSucceeds() throws Exception {
        AtomicBoolean autoCommit = new AtomicBoolean();
        AtomicInteger rollbacks = new AtomicInteger();
        Connection c = mockConnection(true, null, null, autoCommit, rollbacks);

        List<WarehouseWriter.Applied> applied = writer.preflight(c, List.of());

        assertThat(applied).isEmpty();
        assertThat(rollbacks.get()).isEqualTo(1);
        assertThat(autoCommit.get()).isTrue();
    }

    @Test
    public void preflightFailsWhenRollbackFailsEvenIfValidationSucceeded() {
        AtomicBoolean autoCommit = new AtomicBoolean();
        AtomicInteger rollbacks = new AtomicInteger();
        SQLException rollbackError = new SQLException("rollback exploded", "08006");
        Connection c = mockConnection(true, rollbackError, null, autoCommit, rollbacks);

        assertThatThrownBy(() -> writer.preflight(c, List.of()))
                .isInstanceOf(PreflightRollbackException.class)
                .hasCause(rollbackError);

        assertThat(rollbacks.get()).isEqualTo(1);
        // Autocommit must NOT have been restored to true when rollback failed
        assertThat(autoCommit.get()).isFalse();
    }

    @Test
    public void preflightPreservesOriginalValidationFailureWhenRollbackSucceeds() {
        AtomicBoolean autoCommit = new AtomicBoolean();
        AtomicInteger rollbacks = new AtomicInteger();
        SQLException unwrapError = new SQLException("database connection broken", "42P01");
        Connection c = mockConnection(true, null, unwrapError, autoCommit, rollbacks);

        assertThatThrownBy(() -> writer.preflight(c, List.of(dummyEvent())))
                .isSameAs(unwrapError);

        assertThat(rollbacks.get()).isEqualTo(1);
        // Autocommit MUST be restored to true immediately after successful rollback
        assertThat(autoCommit.get()).isTrue();
    }

    @Test
    public void preflightFailsWhenAutoCommitRestorationFailsEvenIfRollbackSucceeded() {
        AtomicBoolean autoCommit = new AtomicBoolean();
        AtomicInteger rollbacks = new AtomicInteger();
        SQLException autoCommitError = new SQLException("autoCommit restore failed", "08006");
        Connection c = mockConnection(true, null, null, autoCommitError, autoCommit, rollbacks);

        assertThatThrownBy(() -> writer.preflight(c, List.of()))
                .isInstanceOf(PreflightRollbackException.class)
                .hasCause(autoCommitError);

        assertThat(rollbacks.get()).isEqualTo(1);
    }

    @Test
    public void preflightPreservesValidationFailureWhenAutoCommitRestorationFails() {
        AtomicBoolean autoCommit = new AtomicBoolean();
        AtomicInteger rollbacks = new AtomicInteger();
        SQLException unwrapError = new SQLException("validation failed", "42P01");
        SQLException autoCommitError = new SQLException("autoCommit restore failed", "08006");
        Connection c = mockConnection(true, null, unwrapError, autoCommitError, autoCommit, rollbacks);

        assertThatThrownBy(() -> writer.preflight(c, List.of(dummyEvent())))
                .isInstanceOf(PreflightRollbackException.class)
                .hasCause(autoCommitError)
                .satisfies(e -> {
                    assertThat(e.getSuppressed()).contains(unwrapError);
                });

        assertThat(rollbacks.get()).isEqualTo(1);
    }

    @Test
    public void preflightPreservesValidationFailureAndAttachesSuppressedWhenBothFail() {
        AtomicBoolean autoCommit = new AtomicBoolean();
        AtomicInteger rollbacks = new AtomicInteger();
        SQLException unwrapError = new SQLException("validation failed", "42P01");
        SQLException rollbackError = new SQLException("rollback failed", "08006");
        Connection c = mockConnection(true, rollbackError, unwrapError, autoCommit, rollbacks);

        assertThatThrownBy(() -> writer.preflight(c, List.of(dummyEvent())))
                .isInstanceOf(PreflightRollbackException.class)
                .hasCause(rollbackError)
                .satisfies(e -> {
                    assertThat(e.getSuppressed()).contains(unwrapError);
                });

        assertThat(rollbacks.get()).isEqualTo(1);
        // Autocommit must NOT have been restored to true
        assertThat(autoCommit.get()).isFalse();
    }

    @Test
    public void preflightRejectsConnectionWhenAutoCommitIsAlreadyFalse() {
        AtomicBoolean autoCommit = new AtomicBoolean();
        AtomicInteger rollbacks = new AtomicInteger();
        Connection c = mockConnection(false, null, null, autoCommit, rollbacks);

        assertThatThrownBy(() -> writer.preflight(c, List.of(dummyEvent())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Connection must have autoCommit enabled before entering preflight");

        // Staging was not executed, rollback was not executed, transaction state unchanged
        assertThat(rollbacks.get()).isEqualTo(0);
        assertThat(autoCommit.get()).isFalse();
    }
}
