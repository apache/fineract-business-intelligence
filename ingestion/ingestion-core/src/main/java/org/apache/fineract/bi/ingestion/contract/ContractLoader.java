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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class ContractLoader {

    public static final String DEFAULT_PATH = "/opt/fbi/contracts/fineract_tables.json";

    private ContractLoader() {
    }

    public static Contract fromEnvironment() {
        String path = System.getenv().getOrDefault("FBI_CONTRACT_PATH", DEFAULT_PATH);
        return load(Path.of(path));
    }

    public static Contract load(Path path) {
        try (InputStream in = Files.newInputStream(path)) {
            return parse(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read table contract " + path, e);
        }
    }

    public static Contract parse(InputStream in) throws IOException {
        JsonNode root = new ObjectMapper().readTree(in);
        List<TableContract> tables = new ArrayList<>();
        for (JsonNode t : root.path("tables")) {
            List<ColumnSpec> columns = new ArrayList<>();
            for (JsonNode c : t.path("columns")) {
                List<String> cdc = new ArrayList<>();
                c.path("cdc").forEach(n -> cdc.add(n.asText()));
                columns.add(new ColumnSpec(
                        c.path("name").asText(),
                        c.path("type").asText(),
                        cdc,
                        c.path("synthetic").asBoolean(false),
                        c.path("pii").asBoolean(false),
                        c.path("cdc_unit").asText(ColumnSpec.MICROS)));
            }
            JsonNode cursor = t.path("python_cursor");
            tables.add(new TableContract(
                    t.path("table_name").asText(),
                    t.path("raw_table").asText(),
                    t.path("primary_key").asText(),
                    columns,
                    cursor.isTextual() ? cursor.asText() : null,
                    t.path("kafka_partitions").asInt(1)));
        }
        return new Contract(
                root.path("contract_version").asInt(),
                root.path("topic_prefix").asText("fineract.cdc"),
                root.path("source_schema").asText("public"),
                tables);
    }
}
