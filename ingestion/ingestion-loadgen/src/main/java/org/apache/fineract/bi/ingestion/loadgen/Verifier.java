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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Verifier {

    private static final Logger LOG = LoggerFactory.getLogger(Verifier.class);

    private Verifier() {
    }

    public static int run(Args args, Contract contract) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode truth = mapper.readTree(new File(args.get("ground-truth", "ground-truth.json")));
        String prefix = truth.path("source_prefix").asText();
        long base = truth.path("base_lsn").asLong();
        long timeoutSec = args.getLong("timeout-sec", 600);
        long pollMs = args.getLong("poll-ms", 2000);
        String url = args.get("db-url", "jdbc:postgresql://localhost:5434/analytics");
        String user = args.get("db-user", "analytics_loader");
        String password = args.get("db-password", "analytics_loader_dev_only");
        long produceStart = truth.path("started_at_epoch_ms").asLong();

        long deadline = System.currentTimeMillis() + timeoutSec * 1000;
        int mismatches = Integer.MAX_VALUE;
        String lastDiff = "";
        try (Connection c = DriverManager.getConnection(url, user, password)) {
            while (System.currentTimeMillis() < deadline) {
                StringBuilder diff = new StringBuilder();
                mismatches = 0;
                for (Iterator<Map.Entry<String, JsonNode>> it = truth.path("tables").fields(); it.hasNext();) {
                    Map.Entry<String, JsonNode> t = it.next();
                    Map<String, long[]> actual = query(c, contract.requireTable(t.getKey()).rawTable(),
                            "m_loan_transaction".equals(t.getKey()), prefix, base);
                    for (JsonNode e : t.getValue()) {
                        String source = e.path("source_id").asText();
                        long[] a = actual.getOrDefault(source, new long[4]);
                        long[] x = {e.path("live").asLong(), e.path("deleted").asLong(),
                            e.path("lsn_sum").asLong(), e.path("amount_cents_sum").asLong()};
                        if (a[0] != x[0] || a[1] != x[1] || a[2] != x[2] || a[3] != x[3]) {
                            mismatches++;
                            if (diff.length() < 600) {
                                diff.append(String.format("%s/%s live %d/%d deleted %d/%d; ", t.getKey(), source,
                                        a[0], x[0], a[1], x[1]));
                            }
                        }
                        actual.remove(source);
                    }
                    if (!actual.isEmpty()) {
                        mismatches += actual.size();
                        diff.append(t.getKey()).append(": unexpected sources ").append(actual.keySet()).append("; ");
                    }
                }
                if (mismatches == 0) {
                    break;
                }
                if (!diff.toString().equals(lastDiff)) {
                    LOG.info("{} (source, table) pairs not yet converged: {}", mismatches, diff);
                    lastDiff = diff.toString();
                }
                Thread.sleep(pollMs);
            }
            long convergedAt = System.currentTimeMillis();
            ObjectNode report = mapper.createObjectNode();
            report.put("passed", mismatches == 0);
            report.put("mismatched_pairs", mismatches == Integer.MAX_VALUE ? -1 : mismatches);
            report.put("generated_events", truth.path("generated_events").asLong());
            report.put("sent_records", truth.path("sent_records").asLong());
            report.put("produce_eps", truth.path("produce_eps").asLong());
            double e2eSeconds = (convergedAt - produceStart) / 1000.0;
            report.put("produce_start_to_converged_seconds", e2eSeconds);
            report.put("effective_end_to_end_eps", Math.round(truth.path("generated_events").asLong() / e2eSeconds));
            latency(c, contract, truth, prefix, base, report);
            String out = args.get("report", "verify-report.json");
            mapper.writerWithDefaultPrettyPrinter().writeValue(new File(out), report);
            LOG.info("Verification {}: {}", mismatches == 0 ? "PASSED" : "FAILED", report);
            return mismatches == 0 ? 0 : 1;
        }
    }

    private static Map<String, long[]> query(Connection c, String rawTable, boolean amounts, String prefix, long base)
            throws Exception {
        String sql = "SELECT source_id, COUNT(*) FILTER (WHERE NOT _is_deleted), COUNT(*) FILTER (WHERE _is_deleted), "
                + "COALESCE(SUM(_source_lsn - ?) FILTER (WHERE NOT _is_deleted), 0), "
                + (amounts ? "COALESCE(SUM(ROUND(amount * 100)) FILTER (WHERE NOT _is_deleted), 0)" : "0")
                + " FROM raw." + rawTable + " WHERE source_id LIKE ? AND _source_lsn > ? GROUP BY source_id";
        Map<String, long[]> out = new HashMap<>();
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, base);
            ps.setString(2, prefix + "-%");
            ps.setLong(3, base);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString(1), new long[] {rs.getLong(2), rs.getLong(3), rs.getLong(4), rs.getLong(5)});
                }
            }
        }
        return out;
    }

    private static void latency(Connection c, Contract contract, JsonNode truth, String prefix, long base,
            ObjectNode report) throws Exception {
        ObjectNode lat = report.putObject("end_to_end_latency_seconds");
        for (Iterator<String> it = truth.path("tables").fieldNames(); it.hasNext();) {
            String table = it.next();
            String sql = "SELECT percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP "
                    + "(ORDER BY EXTRACT(EPOCH FROM (_ingested_at - _source_ts))), MAX(EXTRACT(EPOCH FROM "
                    + "(_ingested_at - _source_ts))) FROM raw." + contract.requireTable(table).rawTable()
                    + " WHERE source_id LIKE ? AND _source_lsn > ?";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, prefix + "-%");
                ps.setLong(2, base);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && rs.getArray(1) != null) {
                        Object[] p = (Object[]) rs.getArray(1).getArray();
                        ObjectNode n = lat.putObject(table);
                        n.put("p50", ((Number) p[0]).doubleValue());
                        n.put("p95", ((Number) p[1]).doubleValue());
                        n.put("p99", ((Number) p[2]).doubleValue());
                        n.put("max", rs.getDouble(2));
                    }
                }
            }
        }
    }
}
