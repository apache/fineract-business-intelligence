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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.fineract.bi.ingestion.contract.ColumnSpec;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.TableContract;

public final class DebeziumEnvelopeParser {

    public static final String UNAVAILABLE_PLACEHOLDER = "__debezium_unavailable_value";

    private final Contract contract;
    private final ObjectMapper mapper = new ObjectMapper();

    public DebeziumEnvelopeParser(Contract contract) {
        this.contract = contract;
    }

    public Optional<ChangeEvent> parse(String topic, byte[] value) throws PoisonEventException {
        if (value == null || value.length == 0) {
            return Optional.empty();
        }
        TableContract table = contract.tableForTopic(topic)
                .orElseThrow(() -> new PoisonEventException("UNKNOWN_TOPIC", "No contract table for topic " + topic));
        JsonNode root;
        try {
            root = mapper.readTree(value);
        } catch (IOException e) {
            throw new PoisonEventException("MALFORMED_JSON", "Value is not valid JSON: " + e.getMessage(), e);
        }
        if (root == null || !root.isObject()) {
            throw new PoisonEventException("MALFORMED_ENVELOPE", "Value is not a JSON object");
        }
        if (root.has("schema") && root.has("payload")) {
            root = root.get("payload");
        }
        String op = text(root, "op");
        if (op == null) {
            throw new PoisonEventException("MISSING_OP", "Envelope has no 'op'");
        }
        if ("m".equals(op)) {
            return Optional.empty();
        }
        JsonNode source = root.path("source");
        String sourceId = text(source, "name");
        if (sourceId == null) {
            throw new PoisonEventException("MISSING_SOURCE", "Envelope has no source.name (topic.prefix)");
        }
        String sourceTable = text(source, "table");
        if (sourceTable != null && !sourceTable.equals(table.tableName())) {
            throw new PoisonEventException(
                    "TABLE_MISMATCH", "Topic " + topic + " carries an event for table " + sourceTable);
        }
        Long lsn = source.hasNonNull("lsn") ? source.get("lsn").asLong() : null;
        long tsMicros = source.hasNonNull("ts_us")
                ? source.get("ts_us").asLong()
                : source.path("ts_ms").asLong(System.currentTimeMillis()) * 1_000L;

        ChangeEvent.Op kind;
        JsonNode row;
        switch (op) {
            case "c", "r", "u" -> {
                kind = ChangeEvent.Op.UPSERT;
                row = root.path("after");
            }
            case "d" -> {
                kind = ChangeEvent.Op.DELETE;
                row = root.path("before");
            }
            case "t" -> throw new PoisonEventException(
                    "TRUNCATE_UNSUPPORTED", "TRUNCATE on " + table.tableName() + " requires a controlled resnapshot");
            default -> throw new PoisonEventException("UNKNOWN_OP", "Unknown op '" + op + "'");
        }
        if (!row.isObject()) {
            throw new PoisonEventException("MISSING_ROW", "Op '" + op + "' has no row image");
        }

        List<ColumnSpec> columns = table.columns();
        String[] values = new String[columns.size()];
        List<String> unavailable = new ArrayList<>(0);
        List<String> absent = new ArrayList<>(0);
        for (int i = 0; i < columns.size(); i++) {
            ColumnSpec col = columns.get(i);
            if (col.synthetic()) {
                continue;
            }
            JsonNode node = null;
            for (String candidate : col.candidates()) {
                if (row.has(candidate)) {
                    node = row.get(candidate);
                    break;
                }
            }
            if (node == null && kind == ChangeEvent.Op.UPSERT) {
                unavailable.add(col.name());
                absent.add(col.name());
                continue;
            }
            if (node != null && node.isTextual() && UNAVAILABLE_PLACEHOLDER.equals(node.asText())) {
                unavailable.add(col.name());
                continue;
            }
            values[i] = ValueConverter.toPgText(node, col.type(), col.cdcUnit());
        }
        if (values[table.primaryKeyIndex()] == null) {
            throw new PoisonEventException("MISSING_PRIMARY_KEY", "Row image has no " + table.primaryKey());
        }
        return Optional.of(new ChangeEvent(
                sourceId, table.tableName(), kind, values, unavailable, lsn, tsMicros, absent));
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }
}
