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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.fineract.bi.ingestion.sink.ReplayIdentity;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

final class ReplayIdentityHeaders {

    private ReplayIdentityHeaders() {
    }

    static boolean hasReplayHeaders(Headers headers) {
        if (headers == null) {
            return false;
        }
        return headers.lastHeader(ReplayIdentity.TOPIC_HEADER) != null
                || headers.lastHeader(ReplayIdentity.PARTITION_HEADER) != null
                || headers.lastHeader(ReplayIdentity.OFFSET_HEADER) != null;
    }

    static Optional<ReplayIdentity> read(Headers headers) {
        if (headers == null) {
            return Optional.empty();
        }
        Header topic = single(headers, ReplayIdentity.TOPIC_HEADER);
        Header partition = single(headers, ReplayIdentity.PARTITION_HEADER);
        Header offset = single(headers, ReplayIdentity.OFFSET_HEADER);

        int present = (topic == null ? 0 : 1)
                + (partition == null ? 0 : 1)
                + (offset == null ? 0 : 1);

        if (present == 0) {
            return Optional.empty();
        }
        if (present != 3) {
            throw new IllegalArgumentException("Incomplete replay identity headers");
        }

        try {
            String topicValue = text(topic);
            int partitionValue = Integer.parseInt(text(partition));
            long offsetValue = Long.parseLong(text(offset));
            return Optional.of(new ReplayIdentity(topicValue, partitionValue, offsetValue));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric replay identity header", e);
        }
    }

    static void add(Headers headers, ReplayIdentity identity) {
        headers.remove(ReplayIdentity.TOPIC_HEADER);
        headers.remove(ReplayIdentity.PARTITION_HEADER);
        headers.remove(ReplayIdentity.OFFSET_HEADER);

        headers.add(ReplayIdentity.TOPIC_HEADER, bytes(identity.dltTopic()));
        headers.add(ReplayIdentity.PARTITION_HEADER, bytes(Integer.toString(identity.dltPartition())));
        headers.add(ReplayIdentity.OFFSET_HEADER, bytes(Long.toString(identity.dltOffset())));
    }

    static void copy(Headers source, Headers target) {
        if (source == null || target == null) {
            return;
        }
        target.remove(ReplayIdentity.TOPIC_HEADER);
        target.remove(ReplayIdentity.PARTITION_HEADER);
        target.remove(ReplayIdentity.OFFSET_HEADER);

        for (Header h : source.headers(ReplayIdentity.TOPIC_HEADER)) {
            target.add(h.key(), h.value());
        }
        for (Header h : source.headers(ReplayIdentity.PARTITION_HEADER)) {
            target.add(h.key(), h.value());
        }
        for (Header h : source.headers(ReplayIdentity.OFFSET_HEADER)) {
            target.add(h.key(), h.value());
        }
    }

    private static Header single(Headers headers, String name) {
        Header found = null;
        for (Header header : headers.headers(name)) {
            if (found != null) {
                throw new IllegalArgumentException("Duplicate replay identity header: " + name);
            }
            found = header;
        }
        return found;
    }

    private static String text(Header header) {
        if (header.value() == null) {
            throw new IllegalArgumentException("Null replay identity header: " + header.key());
        }
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(header.value()))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Invalid UTF-8 in replay identity header " + header.key(), e);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
