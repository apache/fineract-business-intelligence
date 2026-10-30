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

ALTER TABLE meta.user_office_mapping DROP CONSTRAINT IF EXISTS branch_scope_is_qualified;
ALTER TABLE meta.user_office_mapping DROP CONSTRAINT IF EXISTS scope_is_qualified;
ALTER TABLE meta.user_office_mapping ADD CONSTRAINT scope_is_qualified CHECK (
    role_name = 'ADMIN'
    OR (role_name = 'BRANCH_MANAGER' AND source_id IS NOT NULL AND office_id IS NOT NULL)
    OR (role_name = 'REGION_MANAGER' AND region IS NOT NULL)
    OR (role_name = 'ANALYST' AND (source_id IS NOT NULL OR region IS NOT NULL))
) NOT VALID;

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
WHERE m.role_name <> 'ADMIN'
  AND (m.source_id IS NOT NULL OR m.region IS NOT NULL);

CREATE OR REPLACE FUNCTION meta.is_admin_user(p_username TEXT)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = meta, pg_temp
AS $$
    SELECT COALESCE(p_username, '') <> ''
       AND EXISTS (SELECT 1 FROM meta.user_office_mapping m WHERE m.username = p_username AND m.role_name = 'ADMIN')
$$;

CREATE OR REPLACE FUNCTION meta.user_scope(p_username TEXT)
RETURNS TABLE (source_id TEXT, office_id BIGINT)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = meta, pg_temp
AS $$
    SELECT s.source_id, s.office_id
    FROM meta.v_user_access_scope s
    WHERE COALESCE(p_username, '') <> '' AND s.username = p_username
$$;

REVOKE ALL ON FUNCTION meta.is_admin_user(TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION meta.user_scope(TEXT) FROM PUBLIC;

INSERT INTO meta.source_registry (source_id, tenant_id, display_name, lifecycle_state)
VALUES ('default', 'default', 'Default Fineract tenant', 'ACTIVE')
ON CONFLICT (source_id) DO NOTHING;

INSERT INTO meta.user_office_mapping (username, role_name, source_id, office_id)
VALUES
    ('admin', 'ADMIN', NULL, NULL),
    ('north_manager', 'BRANCH_MANAGER', 'default', 101),
    ('south_manager', 'BRANCH_MANAGER', 'default', 102)
ON CONFLICT DO NOTHING;
