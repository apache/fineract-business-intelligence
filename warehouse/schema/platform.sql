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

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS raw_shadow;
CREATE SCHEMA IF NOT EXISTS raw_views;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.platform_settings (
    setting_key TEXT PRIMARY KEY,
    setting_value TEXT NOT NULL,
    migration_sensitive BOOLEAN NOT NULL DEFAULT FALSE,
    description TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO meta.platform_settings (setting_key, setting_value, migration_sensitive, description)
VALUES
    ('raw_hash_partitions', '8', TRUE,
     'Hash partitions per raw table, keyed by source_id. Changing requires a re-partition migration.'),
    ('raw_fillfactor', '85', FALSE,
     'Fillfactor for new raw partitions; below 100 keeps HOT updates for frequently updated rows.'),
    ('require_dual_control', 'true', FALSE,
     'When true a cutover must be approved by a different actor than the requester.'),
    ('dual_control_mode', 'parameter', FALSE,
     'parameter: approver is a CLI/DAG parameter (development). database: the approver must be a different '
     'database login that recorded the approval through meta.approve_cutover().'),
    ('reconciliation_max_age_minutes', '120', FALSE,
     'A cutover needs a PASSED shadow_vs_prod reconciliation for the same source and table newer than this.'),
    ('change_log_retention_days', '14', FALSE,
     'Consumed entity_change_log partitions older than this are dropped by maintenance.'),
    ('cursor_stale_days', '7', FALSE,
     'A dbt change cursor that has not moved for this long no longer pins change-log retention.'),
    ('progress_retention_days', '7', FALSE,
     'ingestion_progress and ingestion_audit rows older than this are purged by maintenance.'),
    ('ownership_audit_retention_days', '730', FALSE,
     'ingestion_ownership_audit rows older than this are purged by meta.purge_ownership_audit().')
ON CONFLICT (setting_key) DO NOTHING;

CREATE OR REPLACE FUNCTION meta.setting(p_key TEXT, p_default TEXT DEFAULT NULL)
RETURNS TEXT
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE((SELECT setting_value FROM meta.platform_settings WHERE setting_key = p_key), p_default)
$$;

CREATE TABLE IF NOT EXISTS meta.table_contract (
    table_name TEXT PRIMARY KEY,
    raw_table TEXT NOT NULL UNIQUE,
    primary_key TEXT NOT NULL,
    columns JSONB NOT NULL,
    indexes JSONB NOT NULL DEFAULT '[]'::jsonb,
    change_entity TEXT CHECK (change_entity IN ('loan', 'client', 'global')),
    change_key TEXT,
    change_date TEXT,
    contract_version INTEGER NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE meta.table_contract ADD COLUMN IF NOT EXISTS change_history_columns JSONB;

CREATE OR REPLACE FUNCTION meta.assert_identifier(p_value TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF p_value IS NULL OR p_value !~ '^[a-z_][a-z0-9_]{0,62}$' THEN
        RAISE EXCEPTION 'Invalid identifier in table contract: %', p_value USING ERRCODE = '22023';
    END IF;
    RETURN p_value;
END;
$$;

CREATE OR REPLACE FUNCTION meta.load_table_contract(p_contract JSONB)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    t JSONB;
    col JSONB;
    n INTEGER := 0;
BEGIN
    FOR t IN SELECT value FROM jsonb_array_elements(p_contract -> 'tables') LOOP
        PERFORM meta.assert_identifier(t ->> 'table_name');
        PERFORM meta.assert_identifier(t ->> 'raw_table');
        PERFORM meta.assert_identifier(t ->> 'primary_key');
        FOR col IN SELECT value FROM jsonb_array_elements(t -> 'columns') LOOP
            PERFORM meta.assert_identifier(col ->> 'name');
            IF (col ->> 'type') !~ '^(bigint|integer|smallint|text|date|timestamptz|boolean|numeric\(\d{1,2},\d{1,2}\))$' THEN
                RAISE EXCEPTION 'Unsupported column type % for %.%',
                    col ->> 'type', t ->> 'table_name', col ->> 'name' USING ERRCODE = '22023';
            END IF;
        END LOOP;
        IF t -> 'change_tracking' -> 'history_columns' IS NOT NULL THEN
            FOR col IN SELECT value FROM jsonb_array_elements(t -> 'change_tracking' -> 'history_columns') LOOP
                PERFORM meta.assert_identifier(col #>> '{}');
                IF NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements(t -> 'columns') c WHERE c ->> 'name' = col #>> '{}'
                ) THEN
                    RAISE EXCEPTION 'History column % is not a column of %', col #>> '{}', t ->> 'table_name';
                END IF;
            END LOOP;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM jsonb_array_elements(t -> 'columns') c WHERE c ->> 'name' = t ->> 'primary_key'
        ) THEN
            RAISE EXCEPTION 'Primary key % missing from columns of %', t ->> 'primary_key', t ->> 'table_name';
        END IF;

        INSERT INTO meta.table_contract (
            table_name, raw_table, primary_key, columns, indexes,
            change_entity, change_key, change_date, change_history_columns, contract_version
        )
        VALUES (
            t ->> 'table_name',
            t ->> 'raw_table',
            t ->> 'primary_key',
            t -> 'columns',
            COALESCE(t -> 'indexes', '[]'::jsonb),
            t -> 'change_tracking' ->> 'entity',
            meta.assert_identifier(COALESCE(t -> 'change_tracking' ->> 'key', 'id')),
            CASE WHEN t -> 'change_tracking' ->> 'date' IS NULL THEN NULL
                 ELSE meta.assert_identifier(t -> 'change_tracking' ->> 'date') END,
            t -> 'change_tracking' -> 'history_columns',
            (p_contract ->> 'contract_version')::INTEGER
        )
        ON CONFLICT (table_name) DO UPDATE
        SET raw_table = EXCLUDED.raw_table,
            primary_key = EXCLUDED.primary_key,
            columns = EXCLUDED.columns,
            indexes = EXCLUDED.indexes,
            change_entity = EXCLUDED.change_entity,
            change_key = EXCLUDED.change_key,
            change_date = EXCLUDED.change_date,
            change_history_columns = EXCLUDED.change_history_columns,
            contract_version = EXCLUDED.contract_version,
            loaded_at = NOW();
        n := n + 1;
    END LOOP;
    RETURN n;
END;
$$;

CREATE OR REPLACE FUNCTION meta.provision_raw_tables()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    c RECORD;
    s TEXT;
    col JSONB;
    idx JSONB;
    ddl TEXT;
    idx_cols TEXT;
    idx_name TEXT;
    parts INTEGER := meta.setting('raw_hash_partitions', '8')::INTEGER;
    ff INTEGER := meta.setting('raw_fillfactor', '85')::INTEGER;
    i INTEGER;
BEGIN
    FOR c IN SELECT * FROM meta.table_contract ORDER BY table_name LOOP
        FOREACH s IN ARRAY ARRAY['raw', 'raw_shadow'] LOOP
            IF to_regclass(format('%I.%I', s, c.raw_table)) IS NULL THEN
                ddl := format('CREATE TABLE %I.%I (source_id TEXT NOT NULL, tenant_id TEXT NOT NULL', s, c.raw_table);
                FOR col IN SELECT value FROM jsonb_array_elements(c.columns) LOOP
                    ddl := ddl || format(', %I %s', col ->> 'name', col ->> 'type');
                END LOOP;
                ddl := ddl
                    || ', _source_generation INTEGER NOT NULL DEFAULT 0'
                    || ', _source_lsn BIGINT'
                    || ', _source_ts TIMESTAMPTZ'
                    || ', _ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()'
                    || ', _ingest_engine TEXT NOT NULL'
                    || ', _is_deleted BOOLEAN NOT NULL DEFAULT FALSE'
                    || format(', PRIMARY KEY (source_id, tenant_id, %I)) PARTITION BY HASH (source_id)', c.primary_key);
                EXECUTE ddl;
                FOR i IN 0 .. parts - 1 LOOP
                    EXECUTE format(
                        'CREATE TABLE %I.%I PARTITION OF %I.%I FOR VALUES WITH (MODULUS %s, REMAINDER %s) WITH (fillfactor = %s)',
                        s, c.raw_table || '_p' || lpad(i::TEXT, 3, '0'), s, c.raw_table, parts, i, ff
                    );
                END LOOP;
            ELSE
                FOR col IN SELECT value FROM jsonb_array_elements(c.columns) LOOP
                    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS %I %s',
                                   s, c.raw_table, col ->> 'name', col ->> 'type');
                END LOOP;
            END IF;

            EXECUTE format('CREATE STATISTICS IF NOT EXISTS %I.%I (dependencies, ndistinct) ON source_id, tenant_id, %I FROM %I.%I',
                           s, left('st_' || c.raw_table || '_identity', 63), c.primary_key, s, c.raw_table);

            FOR idx IN SELECT value FROM jsonb_array_elements(c.indexes) LOOP
                SELECT string_agg(format('%I', meta.assert_identifier(v)), ', '),
                       string_agg(v, '_')
                  INTO idx_cols, idx_name
                  FROM jsonb_array_elements_text(idx) AS v;
                idx_name := left('ix_' || c.raw_table || '_' || idx_name, 63);
                EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I (source_id, tenant_id, %s)',
                               idx_name, s, c.raw_table, idx_cols);
            END LOOP;
        END LOOP;
    END LOOP;
END;
$$;

CREATE TABLE IF NOT EXISTS meta.source_registry (
    source_id TEXT PRIMARY KEY CHECK (source_id ~ '^[a-z0-9][a-z0-9_-]{0,62}$'),
    tenant_id TEXT NOT NULL,
    display_name TEXT,
    region TEXT NOT NULL DEFAULT 'default',
    tier TEXT NOT NULL DEFAULT 'standard',
    db_host TEXT,
    db_port INTEGER NOT NULL DEFAULT 5432,
    db_name TEXT,
    db_schema TEXT NOT NULL DEFAULT 'public',
    compat_schema TEXT NOT NULL DEFAULT 'bi_connector_source',
    db_user TEXT,
    secret_ref TEXT,
    connect_cluster TEXT NOT NULL DEFAULT 'default',
    lifecycle_state TEXT NOT NULL DEFAULT 'REQUESTED' CHECK (lifecycle_state IN (
        'REQUESTED', 'VALIDATING', 'PROVISIONING', 'ACTIVE',
        'PAUSED', 'DEGRADED', 'RETIRING', 'RETIRED'
    )),
    capture_generation INTEGER NOT NULL DEFAULT 0,
    cob_job_name TEXT NOT NULL DEFAULT 'LOAN_COB',
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT secret_ref_is_reference CHECK (secret_ref IS NULL OR secret_ref ~ '^(env|file):')
);

ALTER TABLE meta.source_registry DROP CONSTRAINT IF EXISTS secret_ref_is_reference;
ALTER TABLE meta.source_registry ADD CONSTRAINT secret_ref_is_reference
    CHECK (secret_ref IS NULL OR secret_ref ~ '^(env|file):') NOT VALID;

CREATE INDEX IF NOT EXISTS ix_source_registry_region ON meta.source_registry (region, lifecycle_state);

CREATE TABLE IF NOT EXISTS meta.source_generation_floor (
    source_id TEXT PRIMARY KEY,
    next_generation INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS meta.ingestion_ownership (
    source_id TEXT NOT NULL REFERENCES meta.source_registry (source_id) ON DELETE CASCADE,
    table_name TEXT NOT NULL REFERENCES meta.table_contract (table_name),
    engine TEXT NOT NULL CHECK (engine IN ('PYTHON', 'KAFKA', 'NONE')),
    shadow_engine TEXT CHECK (shadow_engine IN ('PYTHON', 'KAFKA')),
    cutover_state TEXT NOT NULL DEFAULT 'STEADY' CHECK (cutover_state IN (
        'STEADY', 'SHADOWING', 'RECONCILING', 'CUTOVER_READY', 'PAUSED'
    )),
    epoch BIGINT NOT NULL DEFAULT 1,
    last_reconciliation_id BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by TEXT NOT NULL DEFAULT 'system',
    change_reason TEXT,
    PRIMARY KEY (source_id, table_name),
    CONSTRAINT shadow_differs_from_engine CHECK (shadow_engine IS NULL OR shadow_engine <> engine),
    CONSTRAINT shadow_state_consistent CHECK (
        (shadow_engine IS NULL AND cutover_state IN ('STEADY', 'PAUSED'))
        OR (shadow_engine IS NOT NULL AND cutover_state IN ('SHADOWING', 'RECONCILING', 'CUTOVER_READY', 'PAUSED'))
    )
);

ALTER TABLE meta.ingestion_ownership ADD COLUMN IF NOT EXISTS shadow_requested_by TEXT;
ALTER TABLE meta.ingestion_ownership ADD COLUMN IF NOT EXISTS shadow_requested_db_user TEXT;

CREATE INDEX IF NOT EXISTS ix_ingestion_ownership_engine ON meta.ingestion_ownership (engine, shadow_engine);

CREATE TABLE IF NOT EXISTS meta.ingestion_ownership_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    source_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    action TEXT NOT NULL,
    previous_state JSONB,
    new_state JSONB,
    actor TEXT NOT NULL,
    approved_by TEXT,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE meta.ingestion_ownership_audit ADD COLUMN IF NOT EXISTS db_user TEXT NOT NULL DEFAULT session_user;

CREATE INDEX IF NOT EXISTS ix_ownership_audit_source ON meta.ingestion_ownership_audit (source_id, created_at);

CREATE OR REPLACE FUNCTION meta.protect_ownership_audit()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF COALESCE(current_setting('fbi.audit_maintenance', TRUE), '') = 'on' THEN
        RETURN CASE TG_OP WHEN 'DELETE' THEN OLD ELSE NEW END;
    END IF;
    RAISE EXCEPTION 'meta.ingestion_ownership_audit is append-only (% rejected)', TG_OP USING ERRCODE = 'FBI03';
END;
$$;

DROP TRIGGER IF EXISTS trg_ownership_audit_append_only ON meta.ingestion_ownership_audit;
CREATE TRIGGER trg_ownership_audit_append_only
    BEFORE UPDATE OR DELETE ON meta.ingestion_ownership_audit
    FOR EACH ROW EXECUTE FUNCTION meta.protect_ownership_audit();

CREATE TABLE IF NOT EXISTS meta.cutover_approval (
    approval_id BIGSERIAL PRIMARY KEY,
    source_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    epoch BIGINT NOT NULL,
    approved_db_user TEXT NOT NULL DEFAULT session_user,
    approved_label TEXT,
    approved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_id, table_name, epoch, approved_db_user)
);

CREATE OR REPLACE FUNCTION meta.approve_cutover(p_source_id TEXT, p_table_name TEXT, p_label TEXT DEFAULT NULL)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    o meta.ingestion_ownership%ROWTYPE;
    v_id BIGINT;
BEGIN
    SELECT * INTO o FROM meta.ingestion_ownership WHERE source_id = p_source_id AND table_name = p_table_name;
    IF NOT FOUND OR o.cutover_state <> 'CUTOVER_READY' THEN
        RAISE EXCEPTION '%/% is not CUTOVER_READY', p_source_id, p_table_name USING ERRCODE = 'FBI03';
    END IF;
    IF o.shadow_requested_db_user IS NOT DISTINCT FROM session_user THEN
        RAISE EXCEPTION 'The database login that requested the shadow (%) cannot approve its own cutover', session_user
            USING ERRCODE = 'FBI03';
    END IF;
    INSERT INTO meta.cutover_approval (source_id, table_name, epoch, approved_label)
    VALUES (p_source_id, p_table_name, o.epoch, p_label)
    ON CONFLICT (source_id, table_name, epoch, approved_db_user) DO UPDATE SET approved_at = NOW()
    RETURNING approval_id INTO v_id;
    RETURN v_id;
END;
$$;

CREATE OR REPLACE FUNCTION meta.guard_ownership_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_max_age INTEGER := meta.setting('reconciliation_max_age_minutes', '120')::INTEGER;
    v_mode TEXT := meta.setting('dual_control_mode', 'parameter');
    v_ok BOOLEAN;
BEGIN
    IF NEW.engine IS DISTINCT FROM OLD.engine THEN
        IF OLD.engine = 'NONE' OR NEW.engine = 'NONE' THEN
            NULL;
        ELSIF OLD.shadow_engine IS NOT NULL AND NEW.engine = OLD.shadow_engine
              AND NEW.shadow_engine IS NULL AND OLD.cutover_state = 'CUTOVER_READY' THEN
            SELECT EXISTS (
                SELECT 1 FROM meta.reconciliation_run r
                WHERE r.reconciliation_id = OLD.last_reconciliation_id
                  AND r.source_id = OLD.source_id
                  AND r.table_name = OLD.table_name
                  AND r.comparison = 'shadow_vs_prod'
                  AND r.status = 'PASSED'
                  AND r.finished_at > NOW() - make_interval(mins => v_max_age)
            ) INTO v_ok;
            IF NOT v_ok THEN
                RAISE EXCEPTION 'Cutover of %/% needs a PASSED shadow_vs_prod reconciliation of this table newer than % minutes',
                    OLD.source_id, OLD.table_name, v_max_age USING ERRCODE = 'FBI03';
            END IF;
            IF v_mode = 'database' THEN
                SELECT EXISTS (
                    SELECT 1 FROM meta.cutover_approval a
                    WHERE a.source_id = OLD.source_id AND a.table_name = OLD.table_name AND a.epoch = OLD.epoch
                      AND a.approved_db_user <> COALESCE(OLD.shadow_requested_db_user, '')
                      AND a.approved_db_user <> session_user
                ) INTO v_ok;
                IF NOT v_ok THEN
                    RAISE EXCEPTION 'Cutover of %/% needs an approval recorded by a different database login (meta.approve_cutover)',
                        OLD.source_id, OLD.table_name USING ERRCODE = 'FBI03';
                END IF;
            END IF;
        ELSIF COALESCE(current_setting('fbi.force_approved_by', TRUE), '') <> '' THEN
            NULL;
        ELSE
            RAISE EXCEPTION 'Illegal engine change % -> % for %/%', OLD.engine, NEW.engine, OLD.source_id, OLD.table_name
                USING ERRCODE = 'FBI03';
        END IF;
    END IF;
    IF (NEW.engine IS DISTINCT FROM OLD.engine OR NEW.shadow_engine IS DISTINCT FROM OLD.shadow_engine)
       AND NEW.epoch <= OLD.epoch THEN
        RAISE EXCEPTION 'Ownership of %/% changed without a higher epoch', OLD.source_id, OLD.table_name
            USING ERRCODE = 'FBI03';
    END IF;
    IF NEW.shadow_engine IS NOT NULL AND OLD.shadow_engine IS NULL THEN
        NEW.shadow_requested_db_user := session_user;
    ELSIF NEW.shadow_engine IS NULL THEN
        NEW.shadow_requested_db_user := NULL;
        NEW.shadow_requested_by := NULL;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_ownership_guard ON meta.ingestion_ownership;
CREATE TRIGGER trg_ownership_guard
    BEFORE UPDATE ON meta.ingestion_ownership
    FOR EACH ROW EXECUTE FUNCTION meta.guard_ownership_update();

CREATE OR REPLACE FUNCTION meta.ownership_lock_key(p_source_id TEXT, p_table_name TEXT)
RETURNS BIGINT
LANGUAGE sql
STABLE
AS $$
    SELECT hashtextextended(p_table_name || chr(31) || p_source_id, 7)
$$;

CREATE OR REPLACE FUNCTION meta.lock_ownership_exclusive(p_source_id TEXT, p_table_name TEXT)
RETURNS VOID
LANGUAGE sql
AS $$
    SELECT pg_advisory_xact_lock(meta.ownership_lock_key(p_source_id, p_table_name))
$$;

CREATE TABLE IF NOT EXISTS meta.consumer_offsets (
    consumer_group TEXT NOT NULL,
    topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    next_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_group, topic, kafka_partition)
);

CREATE TABLE IF NOT EXISTS meta.extract_watermarks (
    source_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    target TEXT NOT NULL CHECK (target IN ('prod', 'shadow')),
    cursor_value TIMESTAMPTZ,
    last_pk BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_id, table_name, target)
);

CREATE TABLE IF NOT EXISTS meta.ingestion_progress (
    source_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    target TEXT NOT NULL,
    writer_id TEXT NOT NULL,
    engine TEXT NOT NULL,
    rows_applied BIGINT NOT NULL DEFAULT 0,
    last_source_ts TIMESTAMPTZ,
    last_source_lsn BIGINT,
    last_applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_id, table_name, target, writer_id)
);

CREATE TABLE IF NOT EXISTS meta.ingestion_audit (
    table_name TEXT NOT NULL,
    writer_id TEXT NOT NULL,
    minute_bucket TIMESTAMPTZ NOT NULL,
    events_consumed BIGINT NOT NULL DEFAULT 0,
    rows_applied BIGINT NOT NULL DEFAULT 0,
    rows_skipped BIGINT NOT NULL DEFAULT 0,
    events_dead_lettered BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (table_name, writer_id, minute_bucket)
);

DO $$
BEGIN
    IF to_regclass('meta.entity_change_log') IS NOT NULL
       AND (SELECT relkind FROM pg_class WHERE oid = to_regclass('meta.entity_change_log')) = 'r' THEN
        ALTER TABLE meta.entity_change_log RENAME TO entity_change_log_legacy;
        ALTER TABLE meta.entity_change_log_legacy RENAME CONSTRAINT entity_change_log_pkey TO entity_change_log_legacy_pkey;
        ALTER SEQUENCE meta.entity_change_log_change_id_seq OWNED BY NONE;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS meta.entity_change_log (
    change_id BIGINT NOT NULL DEFAULT 0,
    source_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    entity TEXT NOT NULL CHECK (entity IN ('loan', 'client', 'global', 'source')),
    entity_id BIGINT NOT NULL,
    min_affected_date DATE NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    txid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    PRIMARY KEY (changed_at, change_id)
) PARTITION BY RANGE (changed_at);

CREATE SEQUENCE IF NOT EXISTS meta.entity_change_log_change_id_seq;
ALTER TABLE meta.entity_change_log ALTER COLUMN change_id SET DEFAULT nextval('meta.entity_change_log_change_id_seq');

CREATE TABLE IF NOT EXISTS meta.entity_change_log_default PARTITION OF meta.entity_change_log DEFAULT;

CREATE INDEX IF NOT EXISTS ix_entity_change_log_txid ON meta.entity_change_log (txid);
CREATE INDEX IF NOT EXISTS ix_entity_change_log_source ON meta.entity_change_log (source_id, entity, entity_id);

CREATE OR REPLACE FUNCTION meta.ensure_change_log_partitions(p_days_back INTEGER DEFAULT 1, p_days_ahead INTEGER DEFAULT 3)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = meta, pg_temp
AS $$
DECLARE
    d DATE;
    v_name TEXT;
    n INTEGER := 0;
BEGIN
    FOR d IN SELECT generate_series(CURRENT_DATE - p_days_back, CURRENT_DATE + p_days_ahead, INTERVAL '1 day')::DATE LOOP
        v_name := 'entity_change_log_' || to_char(d, 'YYYYMMDD');
        IF to_regclass('meta.' || v_name) IS NULL THEN
            IF EXISTS (
                SELECT 1 FROM meta.entity_change_log_default
                WHERE changed_at >= d::TIMESTAMPTZ AND changed_at < (d + 1)::TIMESTAMPTZ
            ) THEN
                EXECUTE format('CREATE TABLE meta.%I (LIKE meta.entity_change_log INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', v_name);
                EXECUTE format(
                    'WITH moved AS (DELETE FROM meta.entity_change_log_default WHERE changed_at >= %L AND changed_at < %L '
                    'RETURNING *) INSERT INTO meta.%I SELECT * FROM moved',
                    d::TIMESTAMPTZ, (d + 1)::TIMESTAMPTZ, v_name);
                EXECUTE format(
                    'ALTER TABLE meta.entity_change_log ATTACH PARTITION meta.%I FOR VALUES FROM (%L) TO (%L)',
                    v_name, d::TIMESTAMPTZ, (d + 1)::TIMESTAMPTZ);
            ELSE
                EXECUTE format(
                    'CREATE TABLE meta.%I PARTITION OF meta.entity_change_log FOR VALUES FROM (%L) TO (%L)',
                    v_name, d::TIMESTAMPTZ, (d + 1)::TIMESTAMPTZ);
            END IF;
            n := n + 1;
        END IF;
    END LOOP;
    RETURN n;
END;
$$;

DO $$
BEGIN
    IF to_regclass('meta.entity_change_log_legacy') IS NOT NULL THEN
        INSERT INTO meta.entity_change_log (change_id, source_id, tenant_id, entity, entity_id, min_affected_date, changed_at, txid)
        SELECT change_id, source_id, tenant_id, entity, entity_id, min_affected_date, changed_at, '0'::xid8
        FROM meta.entity_change_log_legacy;
        PERFORM setval('meta.entity_change_log_change_id_seq',
                       GREATEST((SELECT COALESCE(MAX(change_id), 0) FROM meta.entity_change_log_legacy), 1));
        DROP TABLE meta.entity_change_log_legacy;
    END IF;
END
$$;

SELECT meta.ensure_change_log_partitions();

CREATE TABLE IF NOT EXISTS meta.dbt_change_cursor (
    model_name TEXT PRIMARY KEY,
    last_change_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE meta.dbt_change_cursor ADD COLUMN IF NOT EXISTS last_txid xid8 NOT NULL DEFAULT '0'::xid8;
ALTER TABLE meta.dbt_change_cursor ADD COLUMN IF NOT EXISTS last_run_id BIGINT NOT NULL DEFAULT 0;

CREATE SEQUENCE IF NOT EXISTS meta.dbt_run_seq;

CREATE TABLE IF NOT EXISTS meta.dbt_mart_cursor (
    model_name TEXT PRIMARY KEY,
    last_run_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS meta.dbt_mart_dirty (
    run_id BIGINT NOT NULL,
    source_id TEXT NOT NULL,
    office_id BIGINT,
    product_id BIGINT,
    from_date DATE NOT NULL,
    changed_from DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_dbt_mart_dirty_run ON meta.dbt_mart_dirty (run_id);

CREATE OR REPLACE FUNCTION meta.change_log_horizon()
RETURNS xid8
LANGUAGE sql
VOLATILE
AS $$
    SELECT pg_snapshot_xmin(pg_current_snapshot())
$$;

CREATE OR REPLACE FUNCTION meta.purge_change_log(p_retention_days INTEGER DEFAULT NULL)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = meta, pg_temp
AS $$
DECLARE
    v_days INTEGER := COALESCE(p_retention_days, meta.setting('change_log_retention_days', '14')::INTEGER);
    v_stale INTEGER := meta.setting('cursor_stale_days', '7')::INTEGER;
    v_floor xid8;
    r RECORD;
    v_bound DATE;
    v_has_pending BOOLEAN;
    n INTEGER := 0;
BEGIN
    SELECT MIN(last_txid) INTO v_floor
    FROM meta.dbt_change_cursor
    WHERE updated_at > NOW() - make_interval(days => v_stale);
    IF v_floor IS NULL THEN
        RETURN 0;
    END IF;
    PERFORM meta.ensure_change_log_partitions();
    FOR r IN
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'meta.entity_change_log'::regclass
          AND c.relname ~ '^entity_change_log_[0-9]{8}$'
    LOOP
        v_bound := to_date(right(r.relname, 8), 'YYYYMMDD');
        IF v_bound + 1 > (NOW() - make_interval(days => v_days))::DATE THEN
            CONTINUE;
        END IF;
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM meta.%I WHERE txid >= $1)', r.relname) INTO v_has_pending USING v_floor;
        IF NOT v_has_pending THEN
            EXECUTE format('DROP TABLE meta.%I', r.relname);
            n := n + 1;
        END IF;
    END LOOP;
    DELETE FROM meta.entity_change_log_default
    WHERE changed_at < NOW() - make_interval(days => v_days) AND txid < v_floor;
    RETURN n;
END;
$$;

CREATE OR REPLACE FUNCTION meta.purge_progress(p_days INTEGER DEFAULT NULL)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_days INTEGER := COALESCE(p_days, meta.setting('progress_retention_days', '7')::INTEGER);
    v_a BIGINT;
    v_b BIGINT;
BEGIN
    DELETE FROM meta.ingestion_progress WHERE last_applied_at < NOW() - make_interval(days => v_days);
    GET DIAGNOSTICS v_a = ROW_COUNT;
    DELETE FROM meta.ingestion_audit WHERE minute_bucket < NOW() - make_interval(days => v_days);
    GET DIAGNOSTICS v_b = ROW_COUNT;
    RETURN v_a + v_b;
END;
$$;

CREATE OR REPLACE FUNCTION meta.purge_ownership_audit(p_days INTEGER DEFAULT NULL)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = meta, pg_temp
AS $$
DECLARE
    v_days INTEGER := COALESCE(p_days, meta.setting('ownership_audit_retention_days', '730')::INTEGER);
    v_n BIGINT;
BEGIN
    PERFORM set_config('fbi.audit_maintenance', 'on', TRUE);
    DELETE FROM meta.ingestion_ownership_audit WHERE created_at < NOW() - make_interval(days => v_days);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    PERFORM set_config('fbi.audit_maintenance', 'off', TRUE);
    RETURN v_n;
END;
$$;

CREATE TABLE IF NOT EXISTS meta.reconciliation_run (
    reconciliation_id BIGSERIAL PRIMARY KEY,
    source_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    comparison TEXT NOT NULL CHECK (comparison IN ('shadow_vs_prod', 'source_vs_warehouse')),
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'PASSED', 'FAILED', 'ERROR')),
    compared_rows BIGINT,
    mismatched_rows BIGINT,
    missing_left BIGINT,
    missing_right BIGINT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_reconciliation_source ON meta.reconciliation_run (source_id, table_name, started_at DESC);

CREATE TABLE IF NOT EXISTS meta.connector_state (
    source_id TEXT PRIMARY KEY,
    connector_name TEXT NOT NULL,
    connect_cluster TEXT NOT NULL,
    desired_state TEXT NOT NULL,
    observed_state TEXT,
    task_states JSONB NOT NULL DEFAULT '[]'::jsonb,
    last_error TEXT,
    config_hash TEXT,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE meta.connector_state ADD COLUMN IF NOT EXISTS slot_checked_at TIMESTAMPTZ;
ALTER TABLE meta.connector_state ADD COLUMN IF NOT EXISTS slot_check_key TEXT;

CREATE TABLE IF NOT EXISTS meta.pipeline_state (
    run_id UUID PRIMARY KEY,
    source_id TEXT NOT NULL DEFAULT 'default',
    tenant_id TEXT NOT NULL,
    run_mode TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    rows_extracted BIGINT NOT NULL DEFAULT 0,
    rows_loaded BIGINT NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE OR REPLACE FUNCTION meta.create_stage_table(p_table TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    c meta.table_contract%ROWTYPE;
    v_stage TEXT;
    ddl TEXT;
    col JSONB;
BEGIN
    SELECT * INTO c FROM meta.table_contract WHERE table_name = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown contract table %', p_table USING ERRCODE = 'FBI02';
    END IF;
    v_stage := 'stage_' || c.raw_table;
    IF to_regclass('pg_temp.' || quote_ident(v_stage)) IS NULL THEN
        ddl := format('CREATE TEMP TABLE %I (source_id TEXT NOT NULL, tenant_id TEXT NOT NULL', v_stage);
        FOR col IN SELECT value FROM jsonb_array_elements(c.columns) LOOP
            ddl := ddl || format(', %I %s', col ->> 'name', col ->> 'type');
        END LOOP;
        ddl := ddl
            || ', _source_generation INTEGER NOT NULL DEFAULT 0'
            || ', _source_lsn BIGINT'
            || ', _source_ts TIMESTAMPTZ'
            || ', _op CHAR(1) NOT NULL DEFAULT ''u'''
            || ', _unavailable TEXT[]'
            || ', _owner_epoch BIGINT NOT NULL'
            || ', _target TEXT NOT NULL'
            || ', _stage_seq BIGINT GENERATED ALWAYS AS IDENTITY'
            || ') ON COMMIT DELETE ROWS';
        EXECUTE ddl;
    END IF;
    RETURN v_stage;
END;
$$;

CREATE OR REPLACE FUNCTION meta.stage_copy_columns(p_table TEXT)
RETURNS TEXT[]
LANGUAGE sql
STABLE
AS $$
    SELECT ARRAY['source_id', 'tenant_id']
        || ARRAY(SELECT c ->> 'name' FROM meta.table_contract t, jsonb_array_elements(t.columns) WITH ORDINALITY AS x(c, ord)
                 WHERE t.table_name = p_table ORDER BY ord)
        || ARRAY['_source_generation', '_source_lsn', '_source_ts', '_op', '_unavailable', '_owner_epoch', '_target']
$$;

CREATE OR REPLACE FUNCTION meta.apply_staged_batch(p_table TEXT, p_engine TEXT)
RETURNS TABLE (target TEXT, rows_applied BIGINT, rows_deleted BIGINT)
LANGUAGE plpgsql
AS $$
DECLARE
    c meta.table_contract%ROWTYPE;
    v_stage TEXT;
    v_key BIGINT;
    v_bad RECORD;
    v_target TEXT;
    v_schema TEXT;
    v_cols TEXT;
    v_sel_cols TEXT;
    v_set TEXT;
    v_set_unavailable TEXT;
    v_guard TEXT;
    v_dedup TEXT;
    v_log TEXT;
    v_applied BIGINT;
    v_deleted BIGINT;
    v_has_unavailable BOOLEAN;
    v_pk TEXT;
    v_rc BIGINT;
    v_deleted2 BIGINT;
    v_merge_cols TEXT;
    v_unavail_expr TEXT;
    v_hist TEXT;
    v_changed TEXT;
    v_old TEXT := '';
    v_old_sql TEXT;
    v_use_log BOOLEAN;
BEGIN
    IF p_engine NOT IN ('PYTHON', 'KAFKA') THEN
        RAISE EXCEPTION 'Unknown engine %', p_engine USING ERRCODE = '22023';
    END IF;
    SELECT * INTO c FROM meta.table_contract WHERE table_name = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown contract table %', p_table USING ERRCODE = 'FBI02';
    END IF;
    v_stage := 'stage_' || c.raw_table;
    v_pk := quote_ident(c.primary_key);

    FOR v_key IN EXECUTE format(
        'SELECT DISTINCT meta.ownership_lock_key(source_id, $1) AS k FROM pg_temp.%I ORDER BY k', v_stage
    ) USING p_table LOOP
        PERFORM pg_advisory_xact_lock_shared(v_key);
    END LOOP;

    EXECUTE format($q$
        SELECT s.source_id, s._target, s._owner_epoch, o.engine, o.shadow_engine, o.epoch, o.cutover_state
        FROM (SELECT DISTINCT source_id, _target, _owner_epoch FROM pg_temp.%I) s
        LEFT JOIN meta.ingestion_ownership o
               ON o.source_id = s.source_id AND o.table_name = $1
        WHERE o.source_id IS NULL
           OR o.epoch <> s._owner_epoch
           OR o.cutover_state = 'PAUSED'
           OR s._target NOT IN ('prod', 'shadow')
           OR (s._target = 'prod' AND o.engine <> $2)
           OR (s._target = 'shadow' AND o.shadow_engine IS DISTINCT FROM $2)
        LIMIT 1
    $q$, v_stage) INTO v_bad USING p_table, p_engine;
    GET DIAGNOSTICS v_rc = ROW_COUNT;

    IF v_rc > 0 THEN
        RAISE EXCEPTION 'Writer % fenced for source %, table %, target %: expected epoch %, owner engine %, shadow %, epoch %, state %',
            p_engine, v_bad.source_id, p_table, v_bad._target, v_bad._owner_epoch,
            v_bad.engine, v_bad.shadow_engine, v_bad.epoch, v_bad.cutover_state
            USING ERRCODE = 'FBI01';
    END IF;

    SELECT string_agg(format('%I', col ->> 'name'), ', ' ORDER BY ord),
           string_agg(format('d.%I', col ->> 'name'), ', ' ORDER BY ord),
           string_agg(
               format('%1$I = CASE WHEN EXCLUDED._is_deleted THEN t.%1$I ELSE EXCLUDED.%1$I END', col ->> 'name'),
               ', ' ORDER BY ord
           ) FILTER (WHERE col ->> 'name' <> c.primary_key),
           string_agg(
               format('%1$I = CASE WHEN d._op = ''d'' OR %2$L = ANY(d._unavailable) THEN t.%1$I ELSE d.%1$I END',
                      col ->> 'name', col ->> 'name'),
               ', ' ORDER BY ord
           ) FILTER (WHERE col ->> 'name' <> c.primary_key)
      INTO v_cols, v_sel_cols, v_set, v_set_unavailable
      FROM jsonb_array_elements(c.columns) WITH ORDINALITY AS x(col, ord);

    v_guard := CASE p_engine
        WHEN 'KAFKA' THEN
            '(%1$s._source_generation, COALESCE(%1$s._source_lsn, -1)) >= (t._source_generation, COALESCE(t._source_lsn, -1))'
        ELSE 'TRUE'
    END;

    EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_temp.%I WHERE cardinality(_unavailable) > 0)', v_stage)
        INTO v_has_unavailable;

    IF v_has_unavailable THEN
        SELECT string_agg(
                   format('(array_agg(%1$I ORDER BY _source_generation DESC, _source_lsn DESC NULLS LAST, _stage_seq DESC) '
                          || 'FILTER (WHERE %1$L <> ALL(COALESCE(_unavailable, ARRAY[]::TEXT[]))))[1] AS %1$I',
                          col ->> 'name'),
                   ', ' ORDER BY ord) FILTER (WHERE col ->> 'name' <> c.primary_key),
               string_agg(
                   format('CASE WHEN NOT bool_or(%1$L <> ALL(COALESCE(_unavailable, ARRAY[]::TEXT[]))) THEN %1$L END',
                          col ->> 'name'),
                   ', ' ORDER BY ord) FILTER (WHERE col ->> 'name' <> c.primary_key)
          INTO v_merge_cols, v_unavail_expr
          FROM jsonb_array_elements(c.columns) WITH ORDINALITY AS x(col, ord);

        v_dedup := format(
            'SELECT source_id, tenant_id, %1$s, %2$s, '
            || '(array_agg(_source_generation ORDER BY _source_generation DESC, _source_lsn DESC NULLS LAST, _stage_seq DESC))[1] AS _source_generation, '
            || '(array_agg(_source_lsn ORDER BY _source_generation DESC, _source_lsn DESC NULLS LAST, _stage_seq DESC))[1] AS _source_lsn, '
            || '(array_agg(_source_ts ORDER BY _source_generation DESC, _source_lsn DESC NULLS LAST, _stage_seq DESC))[1] AS _source_ts, '
            || '(array_agg(_op ORDER BY _source_generation DESC, _source_lsn DESC NULLS LAST, _stage_seq DESC))[1] AS _op, '
            || 'array_remove(ARRAY[%3$s]::TEXT[], NULL) AS _unavailable '
            || 'FROM pg_temp.%4$I WHERE _target = $2 GROUP BY source_id, tenant_id, %1$s',
            v_pk, v_merge_cols, v_unavail_expr, v_stage
        );
    ELSE
        v_dedup := format(
            'SELECT DISTINCT ON (source_id, tenant_id, %1$s) * FROM pg_temp.%2$I WHERE _target = $2 '
            || 'ORDER BY source_id, tenant_id, %1$s, _source_generation DESC, _source_lsn DESC NULLS LAST, _stage_seq DESC',
            v_pk, v_stage
        );
    END IF;

    IF c.change_entity IN ('loan', 'client') THEN
        IF c.change_history_columns IS NULL OR jsonb_array_length(c.change_history_columns) = 0 THEN
            v_hist := 'TRUE';
        ELSE
            SELECT '(' || string_agg(format('a.%I', h), ', ') || ') IS DISTINCT FROM ('
                       || string_agg(format('o.%I', h), ', ') || ')'
              INTO v_hist
              FROM jsonb_array_elements_text(c.change_history_columns) AS h;
        END IF;
        v_changed := format('(o.%1$I IS NULL OR a._is_deleted IS DISTINCT FROM o._is_deleted OR %2$s)', c.primary_key, v_hist);
        v_old := format(
            ', old AS (SELECT o0.* FROM %%1$I.%%2$I o0 JOIN dedup d ON o0.source_id = d.source_id '
            || 'AND o0.tenant_id = d.tenant_id AND o0.%1$s = d.%1$s)', v_pk);
        IF c.change_entity = 'loan' THEN
            v_log := format(
                'INSERT INTO meta.entity_change_log (source_id, tenant_id, entity, entity_id, min_affected_date) '
                || 'SELECT a.source_id, a.tenant_id, ''loan'', a.%1$I, MIN(CASE WHEN %2$s THEN '
                || 'LEAST(COALESCE(%3$s, DATE ''1900-01-01''), COALESCE(%4$s, DATE ''9999-12-31''), CURRENT_DATE) '
                || 'ELSE CURRENT_DATE END) '
                || 'FROM applied a LEFT JOIN old o ON o.source_id = a.source_id AND o.tenant_id = a.tenant_id '
                || 'AND o.%5$I = a.%5$I WHERE a.%1$I IS NOT NULL GROUP BY a.source_id, a.tenant_id, a.%1$I RETURNING 1',
                c.change_key, v_changed,
                CASE WHEN c.change_date IS NULL THEN 'NULL::date' ELSE format('a.%I', c.change_date) END,
                CASE WHEN c.change_date IS NULL THEN 'NULL::date' ELSE format('o.%I', c.change_date) END,
                c.primary_key);
        ELSE
            v_log := format(
                'INSERT INTO meta.entity_change_log (source_id, tenant_id, entity, entity_id, min_affected_date) '
                || 'SELECT a.source_id, a.tenant_id, ''client'', a.%1$I, DATE ''1900-01-01'' '
                || 'FROM applied a LEFT JOIN old o ON o.source_id = a.source_id AND o.tenant_id = a.tenant_id '
                || 'AND o.%2$I = a.%2$I WHERE a.%1$I IS NOT NULL AND %3$s RETURNING 1',
                c.change_key, c.primary_key, v_changed);
        END IF;
    ELSIF c.change_entity = 'global' THEN
        v_old := '';
        v_log := 'INSERT INTO meta.entity_change_log (source_id, tenant_id, entity, entity_id, min_affected_date) '
            || 'SELECT source_id, tenant_id, ''global'', 0, DATE ''1900-01-01'' FROM applied '
            || 'GROUP BY source_id, tenant_id RETURNING 1';
    ELSE
        v_old := '';
        v_log := NULL;
    END IF;

    FOREACH v_target IN ARRAY ARRAY['prod', 'shadow'] LOOP
        v_schema := CASE v_target WHEN 'prod' THEN 'raw' ELSE 'raw_shadow' END;
        v_use_log := v_log IS NOT NULL AND v_target = 'prod';
        v_old_sql := CASE WHEN v_use_log AND v_old <> '' THEN format(v_old, v_schema, c.raw_table) ELSE '' END;

        EXECUTE format($q$
            WITH dedup AS (%1$s)%10$s,
            applied AS (
                INSERT INTO %2$I.%3$I AS t (
                    source_id, tenant_id, %4$s,
                    _source_generation, _source_lsn, _source_ts, _ingested_at, _ingest_engine, _is_deleted
                )
                SELECT d.source_id, d.tenant_id, %5$s,
                       d._source_generation, d._source_lsn, d._source_ts, NOW(), $1, d._op = 'd'
                FROM dedup d
                WHERE COALESCE(cardinality(d._unavailable), 0) = 0
                ON CONFLICT (source_id, tenant_id, %6$s) DO UPDATE
                SET %7$s,
                    _source_generation = EXCLUDED._source_generation,
                    _source_lsn = EXCLUDED._source_lsn,
                    _source_ts = EXCLUDED._source_ts,
                    _ingested_at = EXCLUDED._ingested_at,
                    _ingest_engine = EXCLUDED._ingest_engine,
                    _is_deleted = EXCLUDED._is_deleted
                WHERE %8$s
                RETURNING t.*
            )%9$s
            SELECT COUNT(*), COUNT(*) FILTER (WHERE _is_deleted) FROM applied
        $q$,
            v_dedup, v_schema, c.raw_table, v_cols, v_sel_cols, v_pk, v_set,
            format(v_guard, 'EXCLUDED'),
            CASE WHEN v_use_log THEN ', logged AS (' || v_log || ')' ELSE '' END,
            v_old_sql
        ) INTO v_applied, v_deleted USING p_engine, v_target;

        IF v_has_unavailable THEN
            EXECUTE format($q$
                WITH dedup AS (%1$s)%10$s,
                ins AS (
                    INSERT INTO %2$I.%3$I AS t (
                        source_id, tenant_id, %4$s,
                        _source_generation, _source_lsn, _source_ts, _ingested_at, _ingest_engine, _is_deleted
                    )
                    SELECT d.source_id, d.tenant_id, %5$s,
                           d._source_generation, d._source_lsn, d._source_ts, NOW(), $1, d._op = 'd'
                    FROM dedup d
                    WHERE cardinality(d._unavailable) > 0
                    ON CONFLICT (source_id, tenant_id, %6$s) DO NOTHING
                    RETURNING t.*
                ),
                upd AS (
                    UPDATE %2$I.%3$I AS t
                    SET %7$s,
                        _source_generation = d._source_generation,
                        _source_lsn = d._source_lsn,
                        _source_ts = d._source_ts,
                        _ingested_at = NOW(),
                        _ingest_engine = $1,
                        _is_deleted = d._op = 'd'
                    FROM dedup d
                    WHERE cardinality(d._unavailable) > 0
                      AND t.source_id = d.source_id AND t.tenant_id = d.tenant_id AND t.%6$s = d.%6$s
                      AND %8$s
                    RETURNING t.*
                ),
                applied AS (SELECT * FROM ins UNION ALL SELECT * FROM upd)%9$s
                SELECT COUNT(*), COUNT(*) FILTER (WHERE _is_deleted) FROM applied
            $q$,
                v_dedup, v_schema, c.raw_table, v_cols, v_sel_cols, v_pk, v_set_unavailable,
                format(v_guard, 'd'),
                CASE WHEN v_use_log THEN ', logged AS (' || v_log || ')' ELSE '' END,
                v_old_sql
            ) INTO v_rc, v_deleted2 USING p_engine, v_target;
            v_applied := v_applied + v_rc;
            v_deleted := v_deleted + v_deleted2;
        END IF;

        IF v_applied > 0 THEN
            target := v_target;
            rows_applied := v_applied;
            rows_deleted := v_deleted;
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION meta.promote_shadow(p_source_id TEXT, p_table TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    c meta.table_contract%ROWTYPE;
    v_cols TEXT;
    v_rows BIGINT;
BEGIN
    SELECT * INTO c FROM meta.table_contract WHERE table_name = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown contract table %', p_table USING ERRCODE = 'FBI02';
    END IF;
    SELECT 'source_id, tenant_id, '
           || string_agg(format('%I', col ->> 'name'), ', ' ORDER BY ord)
           || ', _source_generation, _source_lsn, _source_ts, _ingested_at, _ingest_engine, _is_deleted'
      INTO v_cols
      FROM jsonb_array_elements(c.columns) WITH ORDINALITY AS x(col, ord);

    EXECUTE format('DELETE FROM raw.%I WHERE source_id = $1', c.raw_table) USING p_source_id;
    EXECUTE format('INSERT INTO raw.%1$I (%2$s) SELECT %2$s FROM raw_shadow.%1$I WHERE source_id = $1',
                   c.raw_table, v_cols) USING p_source_id;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    EXECUTE format('DELETE FROM raw_shadow.%I WHERE source_id = $1', c.raw_table) USING p_source_id;

    INSERT INTO meta.entity_change_log (source_id, tenant_id, entity, entity_id, min_affected_date)
    SELECT source_id, tenant_id, 'source', 0, DATE '1900-01-01'
    FROM meta.source_registry WHERE source_id = p_source_id;
    RETURN v_rows;
END;
$$;

CREATE OR REPLACE FUNCTION meta.clear_shadow(p_source_id TEXT, p_table TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_raw TEXT;
    v_rows BIGINT;
BEGIN
    SELECT raw_table INTO v_raw FROM meta.table_contract WHERE table_name = p_table;
    EXECUTE format('DELETE FROM raw_shadow.%I WHERE source_id = $1', v_raw) USING p_source_id;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    DELETE FROM meta.extract_watermarks WHERE source_id = p_source_id AND table_name = p_table AND target = 'shadow';
    RETURN v_rows;
END;
$$;

CREATE OR REPLACE FUNCTION meta.reconcile_shadow(p_source_id TEXT, p_table TEXT, p_settle INTERVAL)
RETURNS TABLE (compared_rows BIGINT, mismatched_rows BIGINT, missing_in_prod BIGINT,
               missing_in_shadow BIGINT, sample_keys JSONB)
LANGUAGE plpgsql
AS $$
DECLARE
    c meta.table_contract%ROWTYPE;
    v_row_expr_p TEXT;
    v_row_expr_s TEXT;
BEGIN
    SELECT * INTO c FROM meta.table_contract WHERE table_name = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown contract table %', p_table USING ERRCODE = 'FBI02';
    END IF;
    SELECT string_agg(format('p.%I', col ->> 'name'), ', ' ORDER BY ord),
           string_agg(format('s.%I', col ->> 'name'), ', ' ORDER BY ord)
      INTO v_row_expr_p, v_row_expr_s
      FROM jsonb_array_elements(c.columns) WITH ORDINALITY AS x(col, ord)
     WHERE NOT COALESCE((col ->> 'synthetic')::BOOLEAN, FALSE);

    RETURN QUERY EXECUTE format($q$
        WITH p AS (SELECT * FROM raw.%1$I WHERE source_id = $1 AND NOT _is_deleted),
             s AS (SELECT * FROM raw_shadow.%1$I WHERE source_id = $1 AND NOT _is_deleted),
             j AS (
                SELECT COALESCE(p.%2$I, s.%2$I) AS k,
                       p.%2$I IS NULL AS missing_p,
                       s.%2$I IS NULL AS missing_s,
                       md5(ROW(%3$s)::TEXT) IS DISTINCT FROM md5(ROW(%4$s)::TEXT) AS differs
                FROM p FULL OUTER JOIN s ON p.tenant_id = s.tenant_id AND p.%2$I = s.%2$I
                WHERE GREATEST(p._ingested_at, s._ingested_at, p._source_ts, s._source_ts) < NOW() - $2
             )
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE differs AND NOT missing_p AND NOT missing_s),
               COUNT(*) FILTER (WHERE missing_p),
               COUNT(*) FILTER (WHERE missing_s),
               COALESCE((SELECT jsonb_agg(k) FROM (SELECT k FROM j WHERE differs LIMIT 20) sample), '[]'::jsonb)
        FROM j
    $q$, c.raw_table, c.primary_key, v_row_expr_p, v_row_expr_s) USING p_source_id, p_settle;
END;
$$;

CREATE OR REPLACE VIEW meta.v_table_freshness AS
SELECT
    p.source_id,
    p.table_name,
    p.target,
    MAX(p.engine) AS engine,
    SUM(p.rows_applied) AS rows_applied,
    MAX(p.last_source_ts) AS last_source_ts,
    MAX(p.last_applied_at) AS last_applied_at,
    EXTRACT(EPOCH FROM (NOW() - MAX(p.last_applied_at)))::BIGINT AS seconds_since_last_apply
FROM meta.ingestion_progress p
GROUP BY p.source_id, p.table_name, p.target;

CREATE OR REPLACE VIEW meta.v_ownership_overview AS
SELECT
    r.region,
    o.engine,
    o.shadow_engine,
    o.cutover_state,
    COUNT(DISTINCT o.source_id) AS sources,
    COUNT(*) AS source_tables
FROM meta.ingestion_ownership o
JOIN meta.source_registry r USING (source_id)
GROUP BY r.region, o.engine, o.shadow_engine, o.cutover_state;
