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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface BatchWriter {

    record WriteResult(List<WarehouseWriter.Applied> applied, Map<ReplayIdentity, ReplayOutcome> replayOutcomes) {
        public WriteResult {
            applied = applied == null ? List.of() : List.copyOf(applied);
            replayOutcomes = replayOutcomes == null ? Map.of() : Map.copyOf(replayOutcomes);
        }
    }

    record PreflightResult(List<WarehouseWriter.Applied> applied, Map<ReplayIdentity, ReplayOutcome> predictedOutcomes) {
        public PreflightResult {
            applied = applied == null ? List.of() : List.copyOf(applied);
            predictedOutcomes = predictedOutcomes == null ? Map.of() : Map.copyOf(predictedOutcomes);
        }
    }

    WriteResult write(Connection c, Collection<RoutedEvent> events,
            Collection<PartitionOffset> offsets, String consumerGroup, Collection<SkipCheck> skips)
            throws SQLException;

    PreflightResult preflight(Connection c, Collection<RoutedEvent> events)
            throws SQLException;
}
