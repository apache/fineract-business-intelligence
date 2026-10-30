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
package org.apache.fineract.bi.ingestion.contract;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.testng.annotations.Test;

public class ContractLoaderTest {

    private final Contract contract = ContractLoader.load(Path.of("../../contracts/fineract_tables.json"));

    @Test
    public void loadsEveryTableWithPrimaryKeyFirstClassColumn() {
        assertThat(contract.tables()).hasSizeGreaterThanOrEqualTo(13);
        for (TableContract t : contract.tables()) {
            assertThat(t.primaryKeyIndex()).as(t.tableName()).isGreaterThanOrEqualTo(0);
            assertThat(t.kafkaPartitions()).isPositive();
            assertThat(t.rawTable()).startsWith("raw_");
        }
    }

    @Test
    public void mapsTopicsToTables() {
        assertThat(contract.topicFor("m_loan")).isEqualTo("fineract.cdc.m_loan");
        assertThat(contract.tableForTopic("fineract.cdc.m_loan")).isPresent();
        assertThat(contract.tableForTopic("fineract.cdc.dlt")).isEmpty();
        assertThat(contract.tableForTopic("other.topic")).isEmpty();
    }

    @Test
    public void cdcCandidatesDefaultToColumnName() {
        TableContract job = contract.requireTable("batch_job_execution");
        assertThat(job.column("created_on_utc").candidates()).containsExactly("create_time");
        assertThat(job.column("status").candidates()).containsExactly("status");
        assertThat(contract.requireTable("m_office").column("created_on_utc").synthetic()).isTrue();
    }
}
