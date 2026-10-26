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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class ContractVerifier {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ContractVerifier() {
    }

    public static List<String> differences(Connection c, Contract contract) throws SQLException {
        Map<String, JsonNode[]> stored = new HashMap<>();
        try (Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(
                        "SELECT table_name, raw_table, primary_key, columns::text FROM meta.table_contract")) {
            while (rs.next()) {
                try {
                    stored.put(rs.getString(1), new JsonNode[] {
                        MAPPER.getNodeFactory().textNode(rs.getString(2)),
                        MAPPER.getNodeFactory().textNode(rs.getString(3)),
                        MAPPER.readTree(rs.getString(4))});
                } catch (IOException e) {
                    throw new SQLException("meta.table_contract.columns is not valid JSON for " + rs.getString(1), e);
                }
            }
        }
        List<String> diffs = new ArrayList<>();
        for (TableContract t : contract.tables()) {
            JsonNode[] db = stored.remove(t.tableName());
            if (db == null) {
                diffs.add(t.tableName() + ": missing from meta.table_contract");
                continue;
            }
            if (!t.rawTable().equals(db[0].asText())) {
                diffs.add(t.tableName() + ": raw table " + db[0].asText() + " in the warehouse, " + t.rawTable()
                        + " in the sink contract");
            }
            if (!t.primaryKey().equals(db[1].asText())) {
                diffs.add(t.tableName() + ": primary key " + db[1].asText() + " in the warehouse, " + t.primaryKey()
                        + " in the sink contract");
            }
            Map<String, String> warehouse = new LinkedHashMap<>();
            for (JsonNode col : db[2]) {
                warehouse.put(col.path("name").asText(), col.path("type").asText().toLowerCase(Locale.ROOT));
            }
            for (ColumnSpec col : t.columns()) {
                String type = warehouse.remove(col.name());
                if (type == null) {
                    diffs.add(t.tableName() + "." + col.name() + ": missing in the warehouse contract");
                } else if (!type.equals(col.type().toLowerCase(Locale.ROOT))) {
                    diffs.add(t.tableName() + "." + col.name() + ": type " + type + " in the warehouse, " + col.type()
                            + " in the sink contract");
                }
            }
            warehouse.keySet().forEach(name -> diffs.add(t.tableName() + "." + name + ": missing in the sink contract"));
        }
        stored.keySet().forEach(name -> diffs.add(name + ": missing from the sink contract"));
        return diffs;
    }

    public static void verify(Connection c, Contract contract) throws SQLException {
        List<String> diffs = differences(c, contract);
        if (!diffs.isEmpty()) {
            throw new IllegalStateException("The sink contract does not match meta.table_contract (load the same "
                    + "contracts/fineract_tables.json into both before starting the sink): " + String.join("; ", diffs));
        }
    }
}
