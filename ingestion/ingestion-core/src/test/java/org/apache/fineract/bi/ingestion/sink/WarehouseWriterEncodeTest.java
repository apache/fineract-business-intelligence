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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.apache.fineract.bi.ingestion.cdc.ChangeEvent;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.fineract.bi.ingestion.routing.Route;
import org.testng.annotations.Test;

public class WarehouseWriterEncodeTest {

    private final Contract contract = ContractLoader.load(Path.of("../../contracts/fineract_tables.json"));

    @Test
    public void encodesStageRowsWithNullsQuotesAndMetadata() {
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");
        String[] values = new String[] {"5", "Say \"hi\"", null};
        ChangeEvent e = new ChangeEvent("s1", "batch_job_instance", ChangeEvent.Op.UPSERT, values,
                List.of("job_key"), 77L, 1_767_261_600_000_000L);
        Route route = new Route(Route.Target.SHADOW, 12, "tenant-a", 2, null);
        String csv = writer.encode(contract.requireTable("batch_job_instance"), List.of(new RoutedEvent(e, route)));
        assertThat(csv).isEqualTo(
                "\"s1\",\"tenant-a\",\"5\",\"Say \"\"hi\"\"\",,2,77,\"2026-01-01T10:00:00Z\",u,\"{job_key}\",12,shadow,\n");
    }

    @Test
    public void deletesWithoutLsnEncodeEmptyPosition() {
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");
        ChangeEvent e = new ChangeEvent("s1", "batch_job_instance", ChangeEvent.Op.DELETE,
                new String[] {"5", null, null}, List.of(), null, 0L);
        Route route = new Route(Route.Target.PROD, 1, "t", 0, null);
        String csv = writer.encode(contract.requireTable("batch_job_instance"), List.of(new RoutedEvent(e, route)));
        assertThat(csv).isEqualTo("\"s1\",\"t\",\"5\",,,0,,\"1970-01-01T00:00:00Z\",d,,1,prod,\n");
    }

    @Test
    public void encodesReplayIdWhenPresent() {
        WarehouseWriter writer = new WarehouseWriter(contract, "KAFKA");
        ChangeEvent e = new ChangeEvent("s1", "batch_job_instance", ChangeEvent.Op.UPSERT,
                new String[] {"5", "Job", null}, List.of(), 10L, 0L);
        Route route = new Route(Route.Target.PROD, 1, "t", 0, null);
        java.util.UUID replayUuid = java.util.UUID.fromString("12345678-1234-1234-1234-123456789abc");
        ReplayIdentity id = new ReplayIdentity(replayUuid, "fbi.dlt.prod", 0, 100L);
        String csv = writer.encode(contract.requireTable("batch_job_instance"), List.of(new RoutedEvent(e, route, id)));
        assertThat(csv).isEqualTo("\"s1\",\"t\",\"5\",\"Job\",,0,10,\"1970-01-01T00:00:00Z\",u,,1,prod,12345678-1234-1234-1234-123456789abc\n");
    }
}
