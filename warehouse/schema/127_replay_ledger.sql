-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements. See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License. You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Idempotent schema upgrade for CDC dead-letter replay ledger (Issue #127)
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.replay_ledger (
    dlt_topic TEXT NOT NULL,
    dlt_partition INTEGER NOT NULL,
    dlt_offset BIGINT NOT NULL,
    replayed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dlt_topic, dlt_partition, dlt_offset)
);

DO $$
DECLARE
    v_engine_user TEXT := NULLIF(current_setting('fbi.warehouse_engine_user', true), '');
BEGIN
    IF v_engine_user IS NOT NULL THEN
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_engine_user) THEN
            EXECUTE format('GRANT INSERT ON meta.replay_ledger TO %I', v_engine_user);
        ELSE
            RAISE EXCEPTION 'Engine role % specified in fbi.warehouse_engine_user does not exist', v_engine_user;
        END IF;
    ELSIF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analytics_engine') THEN
        GRANT INSERT ON meta.replay_ledger TO analytics_engine;
    END IF;
END
$$;
