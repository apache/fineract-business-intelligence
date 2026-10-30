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
package org.apache.fineract.bi.ingestion.cdc;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fineract.bi.ingestion.contract.ColumnSpec;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ValueConverterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @DataProvider
    public Object[][] microsValues() {
        return new Object[][] {
            {5_000_000L, "1970-01-01T00:00:05Z"},
            {86_400_000_000L, "1970-01-02T00:00:00Z"},
            {99_999_999_999_999L, "1973-03-03T09:46:39.999999Z"},
            {1_767_261_600_123_456L, "2026-01-01T10:00:00.123456Z"},
        };
    }

    @Test(dataProvider = "microsValues")
    public void microsecondColumnsAreNeverMistakenForMilliseconds(long value, String expected) throws Exception {
        assertThat(ValueConverter.toPgText(MAPPER.readTree(Long.toString(value)), "timestamptz", ColumnSpec.MICROS))
                .isEqualTo(expected);
    }

    @Test
    public void millisecondColumnsAreReadAsMillisecondsWhenTheContractSaysSo() throws Exception {
        assertThat(ValueConverter.toPgText(MAPPER.readTree("1767261600123"), "timestamptz", ColumnSpec.MILLIS))
                .isEqualTo("2026-01-01T10:00:00.123Z");
    }

    @Test
    public void theUnitDefaultsToMicrosAndRejectsUnknownValues() {
        assertThat(new ColumnSpec("c", "timestamptz", null, false, false).cdcUnit()).isEqualTo("micros");
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> new ColumnSpec("c", "timestamptz", null, false, false, "seconds"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
