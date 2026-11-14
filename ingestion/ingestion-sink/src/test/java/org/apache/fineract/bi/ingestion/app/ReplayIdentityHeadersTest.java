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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.fineract.bi.ingestion.sink.ReplayIdentity;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.testng.annotations.Test;

public class ReplayIdentityHeadersTest {

    @Test
    public void readsNoIdentityWhenHeadersAreAbsent() {
        assertEquals(ReplayIdentityHeaders.read(new RecordHeaders()), Optional.empty());
    }

    @Test
    public void addsAndReadsIdentity() {
        ReplayIdentity identity = new ReplayIdentity("fineract.cdc.dlt", 2, 91);
        RecordHeaders headers = new RecordHeaders();

        ReplayIdentityHeaders.add(headers, identity);

        assertEquals(ReplayIdentityHeaders.read(headers), Optional.of(identity));
    }

    @Test
    public void copyPreservesIdentity() {
        ReplayIdentity identity = new ReplayIdentity("fineract.cdc.dlt", 1, 42);
        RecordHeaders source = new RecordHeaders();
        RecordHeaders target = new RecordHeaders();

        ReplayIdentityHeaders.add(source, identity);
        ReplayIdentityHeaders.copy(source, target);

        assertEquals(ReplayIdentityHeaders.read(target), Optional.of(identity));
    }

    @Test
    public void addReplacesExistingIdentityHeaders() {
        ReplayIdentity first = new ReplayIdentity("fineract.cdc.dlt", 0, 10);
        ReplayIdentity second = new ReplayIdentity("fineract.cdc.dlt", 1, 20);
        RecordHeaders headers = new RecordHeaders();

        ReplayIdentityHeaders.add(headers, first);
        ReplayIdentityHeaders.add(headers, second);

        assertEquals(ReplayIdentityHeaders.read(headers), Optional.of(second));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rejectsIncompleteIdentity() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(ReplayIdentity.TOPIC_HEADER, bytes("fineract.cdc.dlt"));

        ReplayIdentityHeaders.read(headers);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rejectsMissingOffsetInIdentity() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(ReplayIdentity.TOPIC_HEADER, bytes("fineract.cdc.dlt"));
        headers.add(ReplayIdentity.PARTITION_HEADER, bytes("1"));

        ReplayIdentityHeaders.read(headers);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rejectsDuplicateIdentityHeader() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(ReplayIdentity.TOPIC_HEADER, bytes("fineract.cdc.dlt"));
        headers.add(ReplayIdentity.TOPIC_HEADER, bytes("fineract.cdc.dlt"));
        headers.add(ReplayIdentity.PARTITION_HEADER, bytes("0"));
        headers.add(ReplayIdentity.OFFSET_HEADER, bytes("10"));

        ReplayIdentityHeaders.read(headers);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rejectsInvalidPartition() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(ReplayIdentity.TOPIC_HEADER, bytes("fineract.cdc.dlt"));
        headers.add(ReplayIdentity.PARTITION_HEADER, bytes("not-a-number"));
        headers.add(ReplayIdentity.OFFSET_HEADER, bytes("10"));

        ReplayIdentityHeaders.read(headers);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rejectsNegativePartition() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(ReplayIdentity.TOPIC_HEADER, bytes("fineract.cdc.dlt"));
        headers.add(ReplayIdentity.PARTITION_HEADER, bytes("-1"));
        headers.add(ReplayIdentity.OFFSET_HEADER, bytes("10"));

        ReplayIdentityHeaders.read(headers);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rejectsNegativeOffset() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(ReplayIdentity.TOPIC_HEADER, bytes("fineract.cdc.dlt"));
        headers.add(ReplayIdentity.PARTITION_HEADER, bytes("0"));
        headers.add(ReplayIdentity.OFFSET_HEADER, bytes("-1"));

        ReplayIdentityHeaders.read(headers);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rejectsNullHeaderValue() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(ReplayIdentity.TOPIC_HEADER, null);
        headers.add(ReplayIdentity.PARTITION_HEADER, bytes("0"));
        headers.add(ReplayIdentity.OFFSET_HEADER, bytes("10"));

        ReplayIdentityHeaders.read(headers);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rejectsMalformedUtf8InTopicHeader() {
        RecordHeaders headers = new RecordHeaders();
        // Invalid UTF-8 byte sequence: 0xC3 followed by 0x28 is illegal UTF-8
        byte[] invalidUtf8 = new byte[] {(byte) 0xC3, (byte) 0x28};
        headers.add(ReplayIdentity.TOPIC_HEADER, invalidUtf8);
        headers.add(ReplayIdentity.PARTITION_HEADER, bytes("0"));
        headers.add(ReplayIdentity.OFFSET_HEADER, bytes("10"));

        ReplayIdentityHeaders.read(headers);
    }

    @Test
    public void copyPreservesRawMalformedHeadersByteForByteWithoutThrowing() {
        RecordHeaders source = new RecordHeaders();
        byte[] invalidUtf8 = new byte[] {(byte) 0xC3, (byte) 0x28};
        source.add(ReplayIdentity.TOPIC_HEADER, invalidUtf8);
        source.add(ReplayIdentity.PARTITION_HEADER, bytes("not-a-num"));
        source.add(ReplayIdentity.OFFSET_HEADER, bytes("-99"));

        RecordHeaders target = new RecordHeaders();
        ReplayIdentityHeaders.copy(source, target);

        assertNotNull(target.lastHeader(ReplayIdentity.TOPIC_HEADER));
        assertEquals(target.lastHeader(ReplayIdentity.TOPIC_HEADER).value(), invalidUtf8);
        assertEquals(target.lastHeader(ReplayIdentity.PARTITION_HEADER).value(), bytes("not-a-num"));
        assertEquals(target.lastHeader(ReplayIdentity.OFFSET_HEADER).value(), bytes("-99"));
    }

    @Test
    public void copiedIdentityIsIndependentOfSourceHeaders() {
        ReplayIdentity identity = new ReplayIdentity("fineract.cdc.dlt", 0, 5);
        RecordHeaders source = new RecordHeaders();
        RecordHeaders target = new RecordHeaders();

        ReplayIdentityHeaders.add(source, identity);
        ReplayIdentityHeaders.copy(source, target);
        source.remove(ReplayIdentity.TOPIC_HEADER);

        assertTrue(ReplayIdentityHeaders.read(target).isPresent());
        assertEquals(ReplayIdentityHeaders.read(target), Optional.of(identity));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
