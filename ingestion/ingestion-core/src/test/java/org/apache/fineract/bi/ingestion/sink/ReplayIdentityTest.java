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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.util.UUID;
import org.testng.annotations.Test;

public class ReplayIdentityTest {

    @Test
    public void sameUuidProducesEqualIdentitiesRegardlessOfCoordinates() {
        UUID id = UUID.randomUUID();
        ReplayIdentity first = new ReplayIdentity(id, "fineract.cdc.dlt", 1, 42);
        ReplayIdentity second = new ReplayIdentity(id, "fineract.cdc.dlt", 2, 99);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    public void differentUuidProducesUnequalIdentitiesEvenWithIdenticalCoordinates() {
        ReplayIdentity first = new ReplayIdentity(UUID.randomUUID(), "fineract.cdc.dlt", 1, 42);
        ReplayIdentity second = new ReplayIdentity(UUID.randomUUID(), "fineract.cdc.dlt", 1, 42);

        assertNotEquals(first, second);
        assertNotEquals(first.hashCode(), second.hashCode());
    }

    @Test
    public void legacyFallbackProducesDeterministicLineageId() {
        ReplayIdentity first = new ReplayIdentity("fineract.cdc.dlt", 1, 42);
        ReplayIdentity second = new ReplayIdentity("fineract.cdc.dlt", 1, 42);
        ReplayIdentity different = new ReplayIdentity("fineract.cdc.dlt", 1, 43);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertNotEquals(first, different);
    }

    @Test
    public void legacyTimestampMetadataFallbackProducesDeterministicId() {
        ReplayIdentity first = new ReplayIdentity(
                "fineract.cdc.dlt", 1, 42, 1000L, "fineract.cdc.m_loan", 0, 50L);
        ReplayIdentity second = new ReplayIdentity(
                "fineract.cdc.dlt", 1, 42, 1000L, "fineract.cdc.m_loan", 0, 50L);
        ReplayIdentity differentTimestamp = new ReplayIdentity(
                "fineract.cdc.dlt", 1, 42, 2000L, "fineract.cdc.m_loan", 0, 50L);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertNotEquals(first, differentTimestamp);
    }

    @Test
    public void legacyFallbackWithDifferentSourceIdsProducesDifferentUuids() {
        ReplayIdentity first = new ReplayIdentity(
                "fineract.cdc.dlt", 1, 42, 1000L, "bank-a", "m_loan", "fineract.cdc.m_loan", 0, 50L);
        ReplayIdentity second = new ReplayIdentity(
                "fineract.cdc.dlt", 1, 42, 1000L, "bank-b", "m_loan", "fineract.cdc.m_loan", 0, 50L);

        assertNotEquals(first, second);
        assertNotEquals(first.replayId(), second.replayId());
    }

    @Test
    public void legacyFallbackWithDifferentTableNamesProducesDifferentUuids() {
        ReplayIdentity first = new ReplayIdentity(
                "fineract.cdc.dlt", 1, 42, 1000L, "bank-a", "m_loan", "fineract.cdc.m_loan", 0, 50L);
        ReplayIdentity second = new ReplayIdentity(
                "fineract.cdc.dlt", 1, 42, 1000L, "bank-a", "m_client", "fineract.cdc.m_loan", 0, 50L);

        assertNotEquals(first, second);
        assertNotEquals(first.replayId(), second.replayId());
    }

    @Test
    public void legacyFallbackRetainsOriginalUuidWhenSourceIdAndTableUnavailable() {
        String topic = "fineract.cdc.dlt";
        int partition = 1;
        long offset = 42L;
        long timestamp = 1000L;
        String origTopic = "fineract.cdc.m_loan";
        int origPart = 0;
        long origOff = 50L;

        String expectedKey = "fbi:dlt:" + topic + ":" + partition + ":" + offset
                + ":" + timestamp
                + ":" + origTopic
                + ":" + origPart
                + ":" + origOff;
        UUID expectedUuid = UUID.nameUUIDFromBytes(expectedKey.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        UUID from7ArgMethod = ReplayIdentity.legacyFallbackId(
                topic, partition, offset, timestamp, origTopic, origPart, origOff);
        assertEquals(from7ArgMethod, expectedUuid);

        UUID from9ArgMethod = ReplayIdentity.legacyFallbackId(
                topic, partition, offset, timestamp, null, null, origTopic, origPart, origOff);
        assertEquals(from9ArgMethod, expectedUuid);

        ReplayIdentity from7ArgConstructor = new ReplayIdentity(
                topic, partition, offset, timestamp, origTopic, origPart, origOff);
        assertEquals(from7ArgConstructor.replayId(), expectedUuid);

        ReplayIdentity from9ArgConstructor = new ReplayIdentity(
                topic, partition, offset, timestamp, null, null, origTopic, origPart, origOff);
        assertEquals(from9ArgConstructor.replayId(), expectedUuid);
    }

    @Test
    public void fallbackEncodingPreventsDelimiterCollisions() {
        UUID id1 = ReplayIdentity.legacyFallbackId(
                "fineract.cdc.dlt", 1, 42, 1000L, "bank:a", "m_loan", "fineract.cdc.m_loan", 0, 50L);
        UUID id2 = ReplayIdentity.legacyFallbackId(
                "fineract.cdc.dlt", 1, 42, 1000L, "bank", "a:m_loan", "fineract.cdc.m_loan", 0, 50L);

        assertNotEquals(id1, id2);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void rejectsNullUuid() {
        new ReplayIdentity(null, "fineract.cdc.dlt", 0, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rejectsBlankDltTopic() {
        new ReplayIdentity(UUID.randomUUID(), " ", 0, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rejectsNegativePartition() {
        new ReplayIdentity(UUID.randomUUID(), "fineract.cdc.dlt", -1, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rejectsNegativeOffset() {
        new ReplayIdentity(UUID.randomUUID(), "fineract.cdc.dlt", 0, -1);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void rejectsNullTopic() {
        new ReplayIdentity(UUID.randomUUID(), null, 0, 0);
    }
}
