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
package org.apache.fineract.bi.ingestion.contract;

import java.util.List;

public record ColumnSpec(String name, String type, List<String> cdc, boolean synthetic, boolean pii, String cdcUnit) {

    public static final String MICROS = "micros";
    public static final String MILLIS = "millis";

    public ColumnSpec {
        cdc = cdc == null || cdc.isEmpty() ? List.of(name) : List.copyOf(cdc);
        cdcUnit = cdcUnit == null || cdcUnit.isBlank() ? MICROS : cdcUnit;
        if (!MICROS.equals(cdcUnit) && !MILLIS.equals(cdcUnit)) {
            throw new IllegalArgumentException("cdc_unit of " + name + " must be micros or millis, got " + cdcUnit);
        }
    }

    public ColumnSpec(String name, String type, List<String> cdc, boolean synthetic, boolean pii) {
        this(name, type, cdc, synthetic, pii, MICROS);
    }

    public List<String> candidates() {
        return cdc;
    }
}
