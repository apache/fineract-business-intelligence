
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.testng.annotations.Test;

public class DeadLetterRecordTest {

    private static final Contract CONTRACT =
            ContractLoader.load(Path.of("../../contracts/fineract_tables.json"));

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static RecordHeaders validHeaders() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("fbi.dlt.reason", bytes("WAREHOUSE_REJECTED"));
        headers.add("fbi.dlt.message", bytes("numeric field overflow"));
        headers.add("fbi.source.topic", bytes("fineract.cdc.batch_job_instance"));
        headers.add("fbi.source.partition", bytes("2"));
        headers.add("fbi.source.offset", bytes("91"));
        return headers;
    }

    private static DeadLetterRecord parse(Headers headers, byte[] value) {
        return DeadLetterRecord.parse(
                "fineract.cdc.dlt", 1, 25L, headers, value, CONTRACT);
    }

    @Test
    public void parsesValidDeadLetterMetadataAndSourceId() {
        byte[] value = bytes(
                "{\"source\":{\"name\":\"bank-a\",\"table\":\"batch_job_instance\"},\"op\":\"c\"}");

        DeadLetterRecord record = parse(validHeaders(), value);

        assertThat(record.dltTopic()).isEqualTo("fineract.cdc.dlt");
        assertThat(record.dltPartition()).isEqualTo(1);
        assertThat(record.dltOffset()).isEqualTo(25L);
        assertThat(record.originalTopic()).isEqualTo("fineract.cdc.batch_job_instance");
        assertThat(record.originalPartition()).isEqualTo(2);
        assertThat(record.originalOffset()).isEqualTo(91L);
        assertThat(record.reason()).isEqualTo("WAREHOUSE_REJECTED");
        assertThat(record.message()).isEqualTo("numeric field overflow");
        assertThat(record.table()).isEqualTo("batch_job_instance");
        assertThat(record.sourceId()).isEqualTo("bank-a");
        assertThat(record.payloadAvailable()).isTrue();
        assertThat(record.hasKnownTable()).isTrue();
    }

    @Test
    public void parsesSchemaWrappedDebeziumEnvelope() {
        byte[] value = bytes(
                "{\"schema\":{},\"payload\":{\"source\":{\"name\":\"bank-a\"}}}");

        DeadLetterRecord record = parse(validHeaders(), value);

        assertThat(record.sourceId()).isEqualTo("bank-a");
    }

    @Test
    public void keepsMalformedPayloadInspectableWithoutInventingSourceId() {
        DeadLetterRecord record = parse(validHeaders(), bytes("{invalid json"));

        assertThat(record.table()).isEqualTo("batch_job_instance");
        assertThat(record.sourceId()).isNull();
        assertThat(record.payloadAvailable()).isTrue();
    }

    @Test
    public void keepsOmittedPayloadInspectableButUnavailableForReplay() {
        RecordHeaders headers = validHeaders();
        headers.add("fbi.dlt.value_omitted", bytes("true"));
        headers.add("fbi.dlt.value_bytes", bytes("500"));

        DeadLetterRecord record = parse(headers, null);

        assertThat(record.valueOmitted()).isTrue();
        assertThat(record.payloadAvailable()).isFalse();
        assertThat(record.valueBytes()).isEqualTo(500);
        assertThat(record.table()).isEqualTo("batch_job_instance");
        assertThat(record.sourceId()).isNull();
    }

    @Test
    public void marksNullPayloadUnavailableEvenWithoutOmissionHeader() {
        DeadLetterRecord record = parse(validHeaders(), null);

        assertThat(record.valueOmitted()).isTrue();
        assertThat(record.payloadAvailable()).isFalse();
    }

    @Test
    public void acceptsUnknownOriginalTopicForInspection() {
        RecordHeaders headers = validHeaders();
        headers.remove("fbi.source.topic");
        headers.add("fbi.source.topic", bytes("fineract.cdc.unknown_table"));

        DeadLetterRecord record = parse(headers, bytes("{invalid"));

        assertThat(record.table()).isEqualTo(DeadLetterRecord.UNKNOWN_TABLE);
        assertThat(record.hasKnownTable()).isFalse();
    }

    @Test
    public void rejectsMissingRequiredHeader() {
        RecordHeaders headers = validHeaders();
        headers.remove("fbi.dlt.reason");

        assertThatThrownBy(() -> parse(headers, bytes("{}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fbi.dlt.reason");
    }

    @Test
    public void rejectsDuplicateRequiredHeader() {
        RecordHeaders headers = validHeaders();
        headers.add("fbi.dlt.reason", bytes("ANOTHER_REASON"));

        assertThatThrownBy(() -> parse(headers, bytes("{}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate header");
    }

    @Test
    public void rejectsNegativeOriginalPartition() {
        RecordHeaders headers = validHeaders();
        headers.remove("fbi.source.partition");
        headers.add("fbi.source.partition", bytes("-1"));

        assertThatThrownBy(() -> parse(headers, bytes("{}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be negative");
    }

    @Test
    public void rejectsInvalidOriginalOffset() {
        RecordHeaders headers = validHeaders();
        headers.remove("fbi.source.offset");
        headers.add("fbi.source.offset", bytes("not-a-number"));

        assertThatThrownBy(() -> parse(headers, bytes("{}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fbi.source.offset");
    }

    @Test
    public void rejectsInvalidOmissionMarker() {
        RecordHeaders headers = validHeaders();
        headers.add("fbi.dlt.value_omitted", bytes("maybe"));

        assertThatThrownBy(() -> parse(headers, bytes("{}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fbi.dlt.value_omitted");
    }

    @Test
    public void rejectsPayloadMarkedAsOmittedWhenPayloadIsPresent() {
        RecordHeaders headers = validHeaders();
        headers.add("fbi.dlt.value_omitted", bytes("true"));

        assertThatThrownBy(() -> parse(headers, bytes("{}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("marked as omitted");
    }

    @Test
    public void rejectsNegativeOmittedPayloadSize() {
        RecordHeaders headers = validHeaders();
        headers.add("fbi.dlt.value_omitted", bytes("true"));
        headers.add("fbi.dlt.value_bytes", bytes("-10"));

        assertThatThrownBy(() -> parse(headers, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be negative");
    }

    @Test
    public void rejectsNegativeDltCoordinates() {
        assertThatThrownBy(() -> DeadLetterRecord.parse(
                "fineract.cdc.dlt", -1, 25L,
                validHeaders(), bytes("{}"), CONTRACT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid partition or offset");
    }

    @Test
    public void protectsPayloadFromExternalMutation() {
        byte[] original = bytes("{\"source\":{\"name\":\"bank-a\"}}");
        DeadLetterRecord record = parse(validHeaders(), original);

        original[0] = 'X';
        byte[] returned = record.value();
        returned[0] = 'Y';

        assertThat(new String(record.value(), StandardCharsets.UTF_8))
                .startsWith("{");
    }

    @Test
    public void preservesKafkaKeyAndProtectsKeyFromMutation() {
        byte[] keyBytes = bytes("job-key-123");
        DeadLetterRecord record = DeadLetterRecord.parse(
                "fineract.cdc.dlt", 1, 25L, validHeaders(), keyBytes, bytes("{}"), CONTRACT);

        assertThat(record.key()).isEqualTo(bytes("job-key-123"));
        keyBytes[0] = 'Z';
        assertThat(record.key()).isEqualTo(bytes("job-key-123"));
        record.key()[0] = 'W';
        assertThat(record.key()).isEqualTo(bytes("job-key-123"));
    }

    @Test
    public void generatesIdentityWhenHeadersAbsent() {
        DeadLetterRecord record = parse(validHeaders(), bytes("{}"));

        assertThat(record.hasReplayHeaders()).isFalse();
        assertThat(record.hasInvalidReplayIdentity()).isFalse();
        assertThat(record.replayIdentity().dltTopic()).isEqualTo("fineract.cdc.dlt");
        assertThat(record.replayIdentity().dltPartition()).isEqualTo(1);
        assertThat(record.replayIdentity().dltOffset()).isEqualTo(25L);
        assertThat(record.isReplayEligible()).isTrue();
    }

    @Test
    public void preservesExistingReplayIdentityHeaders() {
        RecordHeaders headers = validHeaders();
        headers.add("fbi.replay.dlt.topic", bytes("orig.dlt"));
        headers.add("fbi.replay.dlt.partition", bytes("3"));
        headers.add("fbi.replay.dlt.offset", bytes("77"));

        DeadLetterRecord record = parse(headers, bytes("{}"));

        assertThat(record.hasReplayHeaders()).isTrue();
        assertThat(record.hasInvalidReplayIdentity()).isFalse();
        assertThat(record.replayIdentity().dltTopic()).isEqualTo("orig.dlt");
        assertThat(record.replayIdentity().dltPartition()).isEqualTo(3);
        assertThat(record.replayIdentity().dltOffset()).isEqualTo(77L);
        assertThat(record.isReplayEligible()).isTrue();
    }

    @Test
    public void parsesRecordWithMalformedReplayHeadersWithoutFailingInspection() {
        RecordHeaders headers = validHeaders();
        // Incomplete replay header (only topic, missing partition and offset)
        headers.add("fbi.replay.dlt.topic", bytes("orig.dlt"));

        DeadLetterRecord record = parse(headers, bytes("{}"));

        assertThat(record.hasReplayHeaders()).isTrue();
        assertThat(record.hasInvalidReplayIdentity()).isTrue();
        assertThat(record.replayIdentityError()).contains("Incomplete replay identity headers");
        assertThat(record.isReplayEligible()).isFalse();

        assertThatThrownBy(record::replayIdentity)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Incomplete replay identity headers");
    }
}
