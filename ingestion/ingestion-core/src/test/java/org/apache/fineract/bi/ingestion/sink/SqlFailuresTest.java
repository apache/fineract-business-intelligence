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

import java.sql.SQLException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SqlFailuresTest {

    @DataProvider
    public Object[][] states() {
        return new Object[][] {
            {"08006", SqlFailures.Kind.TRANSIENT},
            {"40001", SqlFailures.Kind.TRANSIENT},
            {"53300", SqlFailures.Kind.TRANSIENT},
            {"57P01", SqlFailures.Kind.TRANSIENT},
            {"55P03", SqlFailures.Kind.TRANSIENT},
            {null, SqlFailures.Kind.TRANSIENT},
            {"22003", SqlFailures.Kind.DATA},
            {"22007", SqlFailures.Kind.DATA},
            {"23505", SqlFailures.Kind.DATA},
            {"23502", SqlFailures.Kind.DATA},
            {"FBI02", SqlFailures.Kind.FATAL},
            {"42703", SqlFailures.Kind.FATAL},
            {"42P01", SqlFailures.Kind.FATAL},
        };
    }

    @Test(dataProvider = "states")
    public void sqlStatesAreClassifiedByHowRetryingCanHelp(String state, SqlFailures.Kind expected) {
        assertThat(SqlFailures.classify(new SQLException("x", state))).isEqualTo(expected);
    }
}
