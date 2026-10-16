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
package org.apache.fineract.bi.ingestion.loadgen;

import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class GroundTruth {

    public record Expected(long live, long deleted, long lsnSum, long amountCentsSum) {
    }

    private final int sources;
    private final long baseLsn;
    private final List<String> tables;
    private final int keys;
    private final long[][] maxLsn;
    private final BitSet[] deleted;

    public GroundTruth(int sources, List<String> tables, int keys, long baseLsn) {
        this.sources = sources;
        this.baseLsn = baseLsn;
        this.tables = List.copyOf(tables);
        this.keys = keys;
        this.maxLsn = new long[sources * tables.size()][];
        this.deleted = new BitSet[sources * tables.size()];
        for (int i = 0; i < maxLsn.length; i++) {
            maxLsn[i] = new long[keys];
            deleted[i] = new BitSet(keys);
        }
    }

    private int slot(int source, int table) {
        return source * tables.size() + table;
    }

    public boolean isLive(int source, int table, int key) {
        int s = slot(source, table);
        return maxLsn[s][key] > 0 && !deleted[s].get(key);
    }

    public void record(int source, int table, int key, long lsn, boolean delete) {
        int s = slot(source, table);
        if (lsn > maxLsn[s][key]) {
            maxLsn[s][key] = lsn;
            deleted[s].set(key, delete);
        }
    }

    public Expected expected(int source, int table) {
        int s = slot(source, table);
        boolean amounts = "m_loan_transaction".equals(tables.get(table));
        long live = 0;
        long dead = 0;
        long lsnSum = 0;
        long cents = 0;
        for (int k = 0; k < keys; k++) {
            long lsn = maxLsn[s][k];
            if (lsn == 0) {
                continue;
            }
            if (deleted[s].get(k)) {
                dead++;
            } else {
                live++;
                lsnSum += lsn - baseLsn;
                if (amounts) {
                    cents += SyntheticPayloads.amountCents(lsn);
                }
            }
        }
        return new Expected(live, dead, lsnSum, cents);
    }

    public Map<String, Map<Integer, Expected>> summary() {
        Map<String, Map<Integer, Expected>> out = new LinkedHashMap<>();
        for (int t = 0; t < tables.size(); t++) {
            Map<Integer, Expected> perSource = new LinkedHashMap<>();
            for (int s = 0; s < sources; s++) {
                Expected e = expected(s, t);
                if (e.live() + e.deleted() > 0) {
                    perSource.put(s, e);
                }
            }
            out.put(tables.get(t), perSource);
        }
        return out;
    }
}
