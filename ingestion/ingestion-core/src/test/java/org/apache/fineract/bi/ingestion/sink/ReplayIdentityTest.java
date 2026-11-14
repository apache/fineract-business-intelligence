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

import org.testng.annotations.Test;

public class ReplayIdentityTest {

    @Test
    public void sameDltCoordinatesProduceEqualIdentities() {
        ReplayIdentity first = new ReplayIdentity("fineract.cdc.dlt", 1, 42);
        ReplayIdentity second = new ReplayIdentity("fineract.cdc.dlt", 1, 42);

        assertEquals(first, second);
    }

    @Test
    public void differentDltCoordinatesProduceDifferentIdentities() {
        ReplayIdentity first = new ReplayIdentity("fineract.cdc.dlt", 1, 42);
        ReplayIdentity second = new ReplayIdentity("fineract.cdc.dlt", 1, 43);

        assertEquals(first.equals(second), false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rejectsBlankDltTopic() {
        new ReplayIdentity(" ", 0, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rejectsNegativePartition() {
        new ReplayIdentity("fineract.cdc.dlt", -1, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rejectsNegativeOffset() {
        new ReplayIdentity("fineract.cdc.dlt", 0, -1);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void rejectsNullTopic() {
        new ReplayIdentity(null, 0, 0);
    }
}
