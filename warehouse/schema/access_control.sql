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

CREATE TABLE IF NOT EXISTS meta.user_office_mapping (
    scope_id BIGSERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    role_name TEXT NOT NULL CHECK (role_name IN ('ADMIN', 'REGION_MANAGER', 'BRANCH_MANAGER', 'ANALYST')),
    region TEXT,
    source_id TEXT,
    office_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT branch_scope_is_qualified CHECK (
        role_name <> 'BRANCH_MANAGER' OR (source_id IS NOT NULL AND office_id IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_user_office_mapping_scope
    ON meta.user_office_mapping (username, COALESCE(region, ''), COALESCE(source_id, ''), COALESCE(office_id, -1));

CREATE OR REPLACE VIEW meta.v_user_access_scope AS
SELECT DISTINCT
    m.username,
    m.role_name,
    r.source_id,
    m.office_id
FROM meta.user_office_mapping m
JOIN meta.source_registry r
  ON (m.source_id IS NULL OR m.source_id = r.source_id)
 AND (m.region IS NULL OR m.region = r.region)
WHERE m.role_name <> 'ADMIN';

INSERT INTO meta.source_registry (source_id, tenant_id, display_name, lifecycle_state)
VALUES ('default', 'default', 'Default Fineract tenant', 'ACTIVE')
ON CONFLICT (source_id) DO NOTHING;

INSERT INTO meta.user_office_mapping (username, role_name, source_id, office_id)
VALUES
    ('admin', 'ADMIN', NULL, NULL),
    ('north_manager', 'BRANCH_MANAGER', 'default', 101),
    ('south_manager', 'BRANCH_MANAGER', 'default', 102)
ON CONFLICT DO NOTHING;
