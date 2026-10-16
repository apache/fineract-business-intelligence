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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.testng.annotations.Test;

public class OwnershipSnapshotTest {

    private final OwnershipSnapshot snapshot = new OwnershipSnapshot(
            Map.of(
                    OwnershipSnapshot.key("s1", "m_loan"), new OwnershipSnapshot.Entry("KAFKA", null, "STEADY", 4),
                    OwnershipSnapshot.key("s1", "m_client"), new OwnershipSnapshot.Entry("PYTHON", "KAFKA", "SHADOWING", 9),
                    OwnershipSnapshot.key("s1", "m_office"), new OwnershipSnapshot.Entry("KAFKA", null, "PAUSED", 2)),
            Map.of("s1", new OwnershipSnapshot.SourceMeta("tenant-1", 3), "s2", new OwnershipSnapshot.SourceMeta("t2", 0)),
            1L);

    @Test
    public void productionOwnerWritesProd() {
        Route r = snapshot.route("s1", "m_loan");
        assertThat(r.target()).isEqualTo(Route.Target.PROD);
        assertThat(r.epoch()).isEqualTo(4);
        assertThat(r.tenantId()).isEqualTo("tenant-1");
        assertThat(r.generation()).isEqualTo(3);
        assertThat(r.stageTarget()).isEqualTo("prod");
    }

    @Test
    public void shadowOwnerWritesShadow() {
        Route r = snapshot.route("s1", "m_client");
        assertThat(r.target()).isEqualTo(Route.Target.SHADOW);
        assertThat(r.stageTarget()).isEqualTo("shadow");
        assertThat(r.epoch()).isEqualTo(9);
    }

    @Test
    public void pausedOwnershipHoldsInsteadOfDropping() {
        assertThat(snapshot.route("s1", "m_office").target()).isEqualTo(Route.Target.HOLD);
    }

    @Test
    public void tablesNotOwnedByKafkaAreSkipped() {
        assertThat(snapshot.route("s1", "m_loan_transaction").target()).isEqualTo(Route.Target.SKIP);
        assertThat(snapshot.route("s2", "m_loan").target()).isEqualTo(Route.Target.SKIP);
    }

    @Test
    public void unregisteredSourcesAreUnknown() {
        assertThat(snapshot.route("intruder", "m_loan").target()).isEqualTo(Route.Target.UNKNOWN_SOURCE);
        assertThat(snapshot.route("intruder", "m_loan").writes()).isFalse();
    }
}
