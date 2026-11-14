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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.sink.ReplayIdentity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public record DeadLetterRecord(
        String dltTopic,
        int dltPartition,
        long dltOffset,
        String originalTopic,
        int originalPartition,
        long originalOffset,
        String reason,
        String message,
        String table,
        String sourceId,
        boolean valueOmitted,
        Integer valueBytes,
        byte[] value,
        byte[] key,
        ReplayIdentity replayIdentityHeader,
        boolean hasInvalidReplayIdentity,
        String replayIdentityError) {

    public static final String UNKNOWN_TABLE = "<unknown>";

    private static final String REASON = "fbi.dlt.reason";
    private static final String MESSAGE = "fbi.dlt.message";
    private static final String SOURCE_TOPIC = "fbi.source.topic";
    private static final String SOURCE_PARTITION = "fbi.source.partition";
    private static final String SOURCE_OFFSET = "fbi.source.offset";
    private static final String VALUE_OMITTED = "fbi.dlt.value_omitted";
    private static final String VALUE_BYTES = "fbi.dlt.value_bytes";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public DeadLetterRecord {
        value = value == null ? null : value.clone();
        key = key == null ? null : key.clone();
    }

    public DeadLetterRecord(
            String dltTopic,
            int dltPartition,
            long dltOffset,
            String originalTopic,
            int originalPartition,
            long originalOffset,
            String reason,
            String message,
            String table,
            String sourceId,
            boolean valueOmitted,
            Integer valueBytes,
            byte[] value) {
        this(
                dltTopic,
                dltPartition,
                dltOffset,
                originalTopic,
                originalPartition,
                originalOffset,
                reason,
                message,
                table,
                sourceId,
                valueOmitted,
                valueBytes,
                value,
                null,
                null,
                false,
                null);
    }

    public DeadLetterRecord(
            String dltTopic,
            int dltPartition,
            long dltOffset,
            String originalTopic,
            int originalPartition,
            long originalOffset,
            String reason,
            String message,
            String table,
            String sourceId,
            boolean valueOmitted,
            Integer valueBytes,
            byte[] value,
            byte[] key,
            ReplayIdentity replayIdentityHeader) {
        this(
                dltTopic,
                dltPartition,
                dltOffset,
                originalTopic,
                originalPartition,
                originalOffset,
                reason,
                message,
                table,
                sourceId,
                valueOmitted,
                valueBytes,
                value,
                key,
                replayIdentityHeader,
                false,
                null);
    }

    @Override
    public byte[] value() {
        return value == null ? null : value.clone();
    }

    @Override
    public byte[] key() {
        return key == null ? null : key.clone();
    }

    public boolean payloadAvailable() {
        return !valueOmitted && value != null;
    }

    public boolean hasKnownTable() {
        return !UNKNOWN_TABLE.equals(table);
    }

    public boolean hasReplayHeaders() {
        return replayIdentityHeader != null || hasInvalidReplayIdentity;
    }

    public boolean isReplayEligible() {
        return payloadAvailable() && !hasInvalidReplayIdentity;
    }

    public ReplayIdentity replayIdentity() {
        if (hasInvalidReplayIdentity) {
            throw new IllegalStateException("DLT record has invalid replay identity: " + replayIdentityError);
        }
        return replayIdentityHeader != null
                ? replayIdentityHeader
                : new ReplayIdentity(dltTopic, dltPartition, dltOffset);
    }

    public static DeadLetterRecord parse(ConsumerRecord<byte[], byte[]> record, Contract contract) {
        return parse(
                record.topic(),
                record.partition(),
                record.offset(),
                record.headers(),
                record.key(),
                record.value(),
                contract);
    }

    public static DeadLetterRecord parse(
            String dltTopic,
            int dltPartition,
            long dltOffset,
            Headers headers,
            byte[] value,
            Contract contract) {
        return parse(dltTopic, dltPartition, dltOffset, headers, null, value, contract);
    }

    public static DeadLetterRecord parse(
            String dltTopic,
            int dltPartition,
            long dltOffset,
            Headers headers,
            byte[] key,
            byte[] value,
            Contract contract) {

        if (dltPartition < 0 || dltOffset < 0) {
            throw new IllegalArgumentException("DLT record has an invalid partition or offset");
        }

        String reason = requiredHeader(headers, REASON);
        String originalTopic = requiredHeader(headers, SOURCE_TOPIC);
        int originalPartition = nonNegativeInt(requiredHeader(headers, SOURCE_PARTITION), SOURCE_PARTITION);
        long originalOffset = nonNegativeLong(requiredHeader(headers, SOURCE_OFFSET), SOURCE_OFFSET);

        String message = optionalHeader(headers, MESSAGE);
        if (message == null) {
            message = "";
        }

        String omittedHeader = optionalHeader(headers, VALUE_OMITTED);
        boolean markedOmitted = false;

        if (omittedHeader != null) {
            if ("true".equalsIgnoreCase(omittedHeader)) {
                markedOmitted = true;
            } else if (!"false".equalsIgnoreCase(omittedHeader)) {
                throw new IllegalArgumentException("Invalid " + VALUE_OMITTED + " header");
            }
        }

        if (markedOmitted && value != null) {
            throw new IllegalArgumentException("DLT payload is present but marked as omitted");
        }

        String bytesHeader = optionalHeader(headers, VALUE_BYTES);
        Integer valueBytes = bytesHeader == null ? null : nonNegativeInt(bytesHeader, VALUE_BYTES);

        boolean valueOmitted = markedOmitted || value == null;
        String table = contract.tableForTopic(originalTopic)
                .map(t -> t.tableName())
                .orElse(UNKNOWN_TABLE);

        ReplayIdentity replayId = null;
        boolean invalidReplay = false;
        String replayError = null;

        if (ReplayIdentityHeaders.hasReplayHeaders(headers)) {
            try {
                replayId = ReplayIdentityHeaders.read(headers).orElse(null);
            } catch (Exception e) {
                invalidReplay = true;
                replayError = e.getMessage();
            }
        }

        return new DeadLetterRecord(
                dltTopic,
                dltPartition,
                dltOffset,
                originalTopic,
                originalPartition,
                originalOffset,
                reason,
                message,
                table,
                extractSourceId(value),
                valueOmitted,
                valueBytes,
                value,
                key,
                replayId,
                invalidReplay,
                replayError);
    }

    private static String requiredHeader(Headers headers, String name) {
        String value = optionalHeader(headers, name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("DLT record is missing required header " + name);
        }
        return value;
    }

    private static String optionalHeader(Headers headers, String name) {
        if (headers == null) {
            return null;
        }

        Iterator<Header> matches = headers.headers(name).iterator();
        if (!matches.hasNext()) {
            return null;
        }

        Header header = matches.next();
        if (matches.hasNext()) {
            throw new IllegalArgumentException("DLT record has duplicate header " + name);
        }

        byte[] bytes = header.value();
        if (bytes == null) {
            throw new IllegalArgumentException("DLT record has a null header value for " + name);
        }

        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("DLT record has an invalid UTF-8 header " + name, e);
        }
    }

    private static int nonNegativeInt(String value, String name) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw new IllegalArgumentException(name + " must not be negative");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer in DLT header " + name, e);
        }
    }

    private static long nonNegativeLong(String value, String name) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 0) {
                throw new IllegalArgumentException(name + " must not be negative");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long in DLT header " + name, e);
        }
    }

    private static String extractSourceId(byte[] value) {
        if (value == null) {
            return null;
        }

        try {
            JsonNode root = MAPPER.readTree(value);
            if (root == null || !root.isObject()) {
                return null;
            }
            if (root.has("schema") && root.has("payload")) {
                root = root.get("payload");
            }

            JsonNode sourceId = root.path("source").path("name");
            if (!sourceId.isTextual() || sourceId.asText().isBlank()) {
                return null;
            }
            return sourceId.asText();
        } catch (Exception e) {
            return null;
        }
    }
}
