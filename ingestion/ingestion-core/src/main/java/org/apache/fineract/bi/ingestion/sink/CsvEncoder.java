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

import java.util.List;

public final class CsvEncoder {

    private CsvEncoder() {
    }

    public static void append(StringBuilder sb, String value) {
        if (value == null) {
            return;
        }
        sb.append('"');
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '"') {
                sb.append('"');
            }
            sb.append(ch);
        }
        sb.append('"');
    }

    public static String identifierArray(List<String> names) {
        if (names == null || names.isEmpty()) {
            return null;
        }
        return "{" + String.join(",", names) + "}";
    }
}
