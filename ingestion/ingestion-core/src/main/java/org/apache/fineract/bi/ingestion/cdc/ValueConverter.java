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

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.time.LocalDate;
import org.apache.fineract.bi.ingestion.contract.ColumnSpec;

public final class ValueConverter {

    private ValueConverter() {
    }

    public static String toPgText(JsonNode node, String type) throws PoisonEventException {
        return toPgText(node, type, ColumnSpec.MICROS);
    }

    public static String toPgText(JsonNode node, String type, String cdcUnit) throws PoisonEventException {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        String base = type.startsWith("numeric") ? "numeric" : type;
        switch (base) {
            case "date":
                if (node.isIntegralNumber()) {
                    return LocalDate.ofEpochDay(node.asLong()).toString();
                }
                return node.asText();
            case "timestamptz":
                if (node.isIntegralNumber()) {
                    long v = node.asLong();
                    Instant instant = ColumnSpec.MILLIS.equals(cdcUnit)
                            ? Instant.ofEpochMilli(v)
                            : Instant.ofEpochSecond(Math.floorDiv(v, 1_000_000L), Math.floorMod(v, 1_000_000L) * 1_000L);
                    return instant.toString();
                }
                return node.asText();
            case "boolean":
                if (node.isBoolean()) {
                    return node.asBoolean() ? "t" : "f";
                }
                if (node.isIntegralNumber()) {
                    return node.asLong() != 0 ? "t" : "f";
                }
                return node.asText();
            case "bigint":
            case "integer":
            case "smallint":
                if (node.isIntegralNumber()) {
                    return node.asText();
                }
                if (node.isTextual()) {
                    String text = node.asText();
                    try {
                        Long.parseLong(text);
                    } catch (NumberFormatException e) {
                        throw new PoisonEventException("TYPE_MISMATCH", "Expected " + type + " but got '" + text + "'", e);
                    }
                    return text;
                }
                throw new PoisonEventException("TYPE_MISMATCH", "Expected " + type + " but got " + node.getNodeType());
            case "numeric":
                if (node.isNumber() || node.isTextual()) {
                    return node.asText();
                }
                throw new PoisonEventException(
                        "TYPE_MISMATCH",
                        "Expected numeric (decimal.handling.mode=string) but got " + node.getNodeType());
            default:
                return node.isTextual() ? node.asText() : node.toString();
        }
    }
}
