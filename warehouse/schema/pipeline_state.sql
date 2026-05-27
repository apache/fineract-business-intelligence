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

CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.pipeline_state (
    run_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    run_mode TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    rows_extracted BIGINT NOT NULL DEFAULT 0,
    rows_loaded BIGINT NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS meta.watermarks (
    tenant_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    cursor_column TEXT NOT NULL,
    last_cursor_value TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, table_name)
);

CREATE TABLE IF NOT EXISTS meta.user_office_mapping (
    username TEXT PRIMARY KEY,
    office_id BIGINT,
    role_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO meta.user_office_mapping (username, office_id, role_name)
VALUES
    ('admin', NULL, 'ADMIN'),
    ('north_manager', 2, 'BRANCH_MANAGER'),
    ('south_manager', 3, 'BRANCH_MANAGER')
ON CONFLICT (username) DO UPDATE
SET office_id = EXCLUDED.office_id,
    role_name = EXCLUDED.role_name,
    updated_at = NOW();
