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

import java.util.Map;

public final class OwnershipSnapshot {

    public static final String ENGINE = "KAFKA";

    public record Entry(String engine, String shadowEngine, String state, long epoch) {
    }

    public record SourceMeta(String tenantId, int generation) {
    }

    private final Map<String, Entry> entries;
    private final Map<String, SourceMeta> sources;
    private final long loadedAtMillis;

    public OwnershipSnapshot(Map<String, Entry> entries, Map<String, SourceMeta> sources, long loadedAtMillis) {
        this.entries = Map.copyOf(entries);
        this.sources = Map.copyOf(sources);
        this.loadedAtMillis = loadedAtMillis;
    }

    public static OwnershipSnapshot empty() {
        return new OwnershipSnapshot(Map.of(), Map.of(), 0L);
    }

    public static String key(String sourceId, String table) {
        return sourceId + '\u0000' + table;
    }

    public long loadedAtMillis() {
        return loadedAtMillis;
    }

    public int size() {
        return entries.size();
    }

    public int sourceCount() {
        return sources.size();
    }

    public Route route(String sourceId, String table) {
        SourceMeta meta = sources.get(sourceId);
        if (meta == null) {
            return new Route(Route.Target.UNKNOWN_SOURCE, 0, null, 0, "source not registered");
        }
        Entry e = entries.get(key(sourceId, table));
        if (e == null) {
            return new Route(Route.Target.SKIP, 0, meta.tenantId(), meta.generation(), "not owned by KAFKA");
        }
        if ("PAUSED".equals(e.state())) {
            return new Route(Route.Target.HOLD, e.epoch(), meta.tenantId(), meta.generation(), "ownership paused");
        }
        if (ENGINE.equals(e.engine())) {
            return new Route(Route.Target.PROD, e.epoch(), meta.tenantId(), meta.generation(), null);
        }
        if (ENGINE.equals(e.shadowEngine())) {
            return new Route(Route.Target.SHADOW, e.epoch(), meta.tenantId(), meta.generation(), null);
        }
        return new Route(Route.Target.SKIP, e.epoch(), meta.tenantId(), meta.generation(), "not owned by KAFKA");
    }
}
