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

public record TableContract(
        String tableName,
        String rawTable,
        String primaryKey,
        List<ColumnSpec> columns,
        String pythonCursor,
        int kafkaPartitions) {

    public TableContract {
        columns = List.copyOf(columns);
        if (indexOf(columns, primaryKey) < 0) {
            throw new IllegalArgumentException("Primary key " + primaryKey + " missing from " + tableName);
        }
    }

    public int columnIndex(String name) {
        return indexOf(columns, name);
    }

    public int primaryKeyIndex() {
        return columnIndex(primaryKey);
    }

    public ColumnSpec column(String name) {
        int i = columnIndex(name);
        if (i < 0) {
            throw new IllegalArgumentException(tableName + " has no column " + name);
        }
        return columns.get(i);
    }

    private static int indexOf(List<ColumnSpec> columns, String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }
}
