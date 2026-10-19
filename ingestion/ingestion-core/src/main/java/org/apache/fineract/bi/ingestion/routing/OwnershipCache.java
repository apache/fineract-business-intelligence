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
package org.apache.fineract.bi.ingestion.routing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OwnershipCache implements OwnershipView {

    private static final Logger LOG = LoggerFactory.getLogger(OwnershipCache.class);

    private static final String OWNERSHIP_SQL =
            "SELECT source_id, table_name, engine, shadow_engine, cutover_state, epoch "
                    + "FROM meta.ingestion_ownership WHERE engine = 'KAFKA' OR shadow_engine = 'KAFKA'";
    private static final String SOURCES_SQL =
            "SELECT source_id, tenant_id, capture_generation FROM meta.source_registry";

    private final DataSource dataSource;
    private final long minRefreshIntervalMillis;
    private final AtomicReference<OwnershipSnapshot> current = new AtomicReference<>(OwnershipSnapshot.empty());

    public OwnershipCache(DataSource dataSource, long minRefreshIntervalMillis) {
        this.dataSource = dataSource;
        this.minRefreshIntervalMillis = minRefreshIntervalMillis;
    }

    @Override
    public OwnershipSnapshot snapshot() {
        return current.get();
    }

    @Override
    public synchronized OwnershipSnapshot refresh(boolean force) throws SQLException {
        OwnershipSnapshot existing = current.get();
        long now = System.currentTimeMillis();
        if (!force && now - existing.loadedAtMillis() < minRefreshIntervalMillis) {
            return existing;
        }
        try (Connection c = dataSource.getConnection()) {
            OwnershipSnapshot loaded = load(c, now);
            current.set(loaded);
            return loaded;
        }
    }

    public void refreshQuietly() {
        try {
            refresh(true);
        } catch (SQLException e) {
            LOG.warn("Ownership refresh failed; keeping snapshot from {} ms ago",
                    System.currentTimeMillis() - current.get().loadedAtMillis(), e);
        }
    }

    static OwnershipSnapshot load(Connection c, long now) throws SQLException {
        Map<String, OwnershipSnapshot.Entry> entries = new HashMap<>();
        Map<String, OwnershipSnapshot.SourceMeta> sources = new HashMap<>();
        boolean auto = c.getAutoCommit();
        c.setAutoCommit(true);
        try {
            try (PreparedStatement ps = c.prepareStatement(SOURCES_SQL); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    sources.put(rs.getString(1), new OwnershipSnapshot.SourceMeta(rs.getString(2), rs.getInt(3)));
                }
            }
            try (PreparedStatement ps = c.prepareStatement(OWNERSHIP_SQL); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    entries.put(
                            OwnershipSnapshot.key(rs.getString(1), rs.getString(2)),
                            new OwnershipSnapshot.Entry(rs.getString(3), rs.getString(4), rs.getString(5), rs.getLong(6)));
                }
            }
        } finally {
            c.setAutoCommit(auto);
        }
        return new OwnershipSnapshot(entries, sources, now);
    }
}
