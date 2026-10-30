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
package org.apache.fineract.bi.ingestion.app;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ProgressReporter {

    private static final Logger LOG = LoggerFactory.getLogger(ProgressReporter.class);

    static final class Progress {
        long rows;
        long maxSourceTsMicros;
        long maxLsn = -1;
    }

    static final class Audit {
        long consumed;
        long applied;
        long skipped;
        long deadLettered;
    }

    private final DataSource dataSource;
    private final String writerId;
    private final AtomicReference<Map<String, Progress>> progress = new AtomicReference<>(new ConcurrentHashMap<>());
    private final AtomicReference<Map<String, Audit>> audit = new AtomicReference<>(new ConcurrentHashMap<>());

    public ProgressReporter(DataSource dataSource, String writerId) {
        this.dataSource = dataSource;
        this.writerId = writerId;
    }

    public void applied(String sourceId, String table, String target, long rows, long maxSourceTsMicros, Long maxLsn) {
        Progress p = progress.get().computeIfAbsent(sourceId + '\u0000' + table + '\u0000' + target, k -> new Progress());
        synchronized (p) {
            p.rows += rows;
            p.maxSourceTsMicros = Math.max(p.maxSourceTsMicros, maxSourceTsMicros);
            if (maxLsn != null) {
                p.maxLsn = Math.max(p.maxLsn, maxLsn);
            }
        }
    }

    public void audit(String table, long consumed, long applied, long skipped, long deadLettered) {
        String minute = Instant.now().truncatedTo(ChronoUnit.MINUTES).toString();
        Audit a = audit.get().computeIfAbsent(table + '\u0000' + minute, k -> new Audit());
        synchronized (a) {
            a.consumed += consumed;
            a.applied += applied;
            a.skipped += skipped;
            a.deadLettered += deadLettered;
        }
    }

    public void flush() {
        Map<String, Progress> p = progress.getAndSet(new ConcurrentHashMap<>());
        Map<String, Audit> a = audit.getAndSet(new ConcurrentHashMap<>());
        if (p.isEmpty() && a.isEmpty()) {
            return;
        }
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO meta.ingestion_progress (source_id, table_name, target, writer_id, engine, rows_applied, "
                            + "last_source_ts, last_source_lsn, last_applied_at) VALUES (?, ?, ?, ?, 'KAFKA', ?, ?, ?, NOW()) "
                            + "ON CONFLICT (source_id, table_name, target, writer_id) DO UPDATE SET "
                            + "rows_applied = meta.ingestion_progress.rows_applied + EXCLUDED.rows_applied, "
                            + "last_source_ts = GREATEST(meta.ingestion_progress.last_source_ts, EXCLUDED.last_source_ts), "
                            + "last_source_lsn = GREATEST(meta.ingestion_progress.last_source_lsn, EXCLUDED.last_source_lsn), "
                            + "last_applied_at = NOW()")) {
                for (Map.Entry<String, Progress> e : p.entrySet()) {
                    String[] k = e.getKey().split("\u0000", 3);
                    Progress v = e.getValue();
                    ps.setString(1, k[0]);
                    ps.setString(2, k[1]);
                    ps.setString(3, k[2]);
                    ps.setString(4, writerId);
                    ps.setLong(5, v.rows);
                    ps.setTimestamp(6, Timestamp.from(Instant.EPOCH.plus(v.maxSourceTsMicros, ChronoUnit.MICROS)));
                    if (v.maxLsn >= 0) {
                        ps.setLong(7, v.maxLsn);
                    } else {
                        ps.setNull(7, java.sql.Types.BIGINT);
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO meta.ingestion_audit (table_name, writer_id, minute_bucket, events_consumed, rows_applied, "
                            + "rows_skipped, events_dead_lettered) VALUES (?, ?, ?::timestamptz, ?, ?, ?, ?) "
                            + "ON CONFLICT (table_name, writer_id, minute_bucket) DO UPDATE SET "
                            + "events_consumed = meta.ingestion_audit.events_consumed + EXCLUDED.events_consumed, "
                            + "rows_applied = meta.ingestion_audit.rows_applied + EXCLUDED.rows_applied, "
                            + "rows_skipped = meta.ingestion_audit.rows_skipped + EXCLUDED.rows_skipped, "
                            + "events_dead_lettered = meta.ingestion_audit.events_dead_lettered + EXCLUDED.events_dead_lettered")) {
                for (Map.Entry<String, Audit> e : a.entrySet()) {
                    String[] k = e.getKey().split("\u0000", 2);
                    Audit v = e.getValue();
                    ps.setString(1, k[0]);
                    ps.setString(2, writerId);
                    ps.setString(3, k[1]);
                    ps.setLong(4, v.consumed);
                    ps.setLong(5, v.applied);
                    ps.setLong(6, v.skipped);
                    ps.setLong(7, v.deadLettered);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            c.commit();
        } catch (SQLException e) {
            LOG.warn("Progress flush failed; will retry", e);
            p.forEach((k, v) -> progress.get().merge(k, v, (x, y) -> {
                x.rows += y.rows;
                x.maxSourceTsMicros = Math.max(x.maxSourceTsMicros, y.maxSourceTsMicros);
                x.maxLsn = Math.max(x.maxLsn, y.maxLsn);
                return x;
            }));
            a.forEach((k, v) -> audit.get().merge(k, v, (x, y) -> {
                x.consumed += y.consumed;
                x.applied += y.applied;
                x.skipped += y.skipped;
                x.deadLettered += y.deadLettered;
                return x;
            }));
        }
    }
}
