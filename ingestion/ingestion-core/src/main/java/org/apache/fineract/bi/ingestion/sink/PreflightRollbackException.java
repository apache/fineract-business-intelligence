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

import java.sql.SQLException;

/**
 * Thrown during warehouse preflight validation when transaction setup (reading or configuring
 * initial autoCommit state), transaction rollback, or connection state restoration fails.
 * <p>
 * Because transaction boundaries or initial connection states could not be reliably established
 * or rolled back, the underlying database connection is in an indeterminate or unusable state and
 * must be closed/discarded rather than returned to the pool or reused for subsequent preflight checks.
 */
public final class PreflightRollbackException extends SQLException {

    public static final String SQLSTATE = "08006";

    private final Throwable validationFailure;

    public PreflightRollbackException(String reason, Throwable cause) {
        this(reason, cause, null);
    }

    public PreflightRollbackException(String reason, Throwable cause, Throwable validationFailure) {
        super(reason, SQLSTATE, cause);
        this.validationFailure = validationFailure;
        if (validationFailure != null && validationFailure != cause) {
            addSuppressed(validationFailure);
        }
    }

    public Throwable validationFailure() {
        return validationFailure;
    }
}
