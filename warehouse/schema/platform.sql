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
    ('ownership_lock_buckets', '64', TRUE,
     'Advisory-lock buckets per table used to fence writers against ownership transitions.'),
    ('require_dual_control', 'true', FALSE,
     'When true a cutover must be approved by a different actor than the requester.'),
    ('change_log_retention_days', '14', FALSE,
     'Consumed entity_change_log rows older than this are purged by maintenance.')
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
        IF NOT EXISTS (
            SELECT 1 FROM jsonb_array_elements(t -> 'columns') c WHERE c ->> 'name' = t ->> 'primary_key'
        ) THEN
            RAISE EXCEPTION 'Primary key % missing from columns of %', t ->> 'primary_key', t ->> 'table_name';
        END IF;

        INSERT INTO meta.table_contract (
            table_name, raw_table, primary_key, columns, indexes,
            change_entity, change_key, change_date, contract_version
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
    CONSTRAINT secret_ref_is_reference CHECK (secret_ref IS NULL OR secret_ref ~ '^(env|file|vault):')
);

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

CREATE INDEX IF NOT EXISTS ix_ownership_audit_source ON meta.ingestion_ownership_audit (source_id, created_at);

CREATE OR REPLACE FUNCTION meta.ownership_lock_key(p_source_id TEXT, p_table_name TEXT)
RETURNS BIGINT
LANGUAGE sql
STABLE
AS $$
    SELECT hashtextextended(
        p_table_name || ':' || (
            abs(hashtextextended(p_source_id, 0)) % meta.setting('ownership_lock_buckets', '64')::BIGINT
        )::TEXT,
        7
    )
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

CREATE TABLE IF NOT EXISTS meta.entity_change_log (
    change_id BIGSERIAL PRIMARY KEY,
    source_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    entity TEXT NOT NULL CHECK (entity IN ('loan', 'client', 'global', 'source')),
    entity_id BIGINT NOT NULL,
    min_affected_date DATE NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS meta.dbt_change_cursor (
    model_name TEXT PRIMARY KEY,
    last_change_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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

    v_dedup := format(
        'SELECT DISTINCT ON (source_id, tenant_id, %1$s) * FROM pg_temp.%2$I WHERE _target = $2 '
        || 'ORDER BY source_id, tenant_id, %1$s, _source_generation DESC, _source_lsn DESC NULLS LAST, _stage_seq DESC',
        v_pk, v_stage
    );

    EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_temp.%I WHERE cardinality(_unavailable) > 0)', v_stage)
        INTO v_has_unavailable;

    v_log := CASE
        WHEN c.change_entity IN ('loan', 'client') THEN format(
            'INSERT INTO meta.entity_change_log (source_id, tenant_id, entity, entity_id, min_affected_date) '
            || 'SELECT source_id, tenant_id, %L, %I, MIN(LEAST(COALESCE(%s, DATE ''1900-01-01''), CURRENT_DATE)) '
            || 'FROM applied WHERE %I IS NOT NULL GROUP BY source_id, tenant_id, %I RETURNING 1',
            c.change_entity, c.change_key,
            CASE WHEN c.change_date IS NULL THEN 'NULL::date' ELSE quote_ident(c.change_date) END,
            c.change_key, c.change_key)
        WHEN c.change_entity = 'global' THEN
            'INSERT INTO meta.entity_change_log (source_id, tenant_id, entity, entity_id, min_affected_date) '
            || 'SELECT source_id, tenant_id, ''global'', 0, DATE ''1900-01-01'' FROM applied '
            || 'GROUP BY source_id, tenant_id RETURNING 1'
        ELSE NULL
    END;

    FOREACH v_target IN ARRAY ARRAY['prod', 'shadow'] LOOP
        v_schema := CASE v_target WHEN 'prod' THEN 'raw' ELSE 'raw_shadow' END;

        EXECUTE format($q$
            WITH dedup AS (%1$s),
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
            CASE WHEN v_log IS NOT NULL AND v_target = 'prod' THEN ', logged AS (' || v_log || ')' ELSE '' END
        ) INTO v_applied, v_deleted USING p_engine, v_target;

        IF v_has_unavailable THEN
            EXECUTE format($q$
                WITH dedup AS (%1$s)
                INSERT INTO %2$I.%3$I AS t (
                    source_id, tenant_id, %4$s,
                    _source_generation, _source_lsn, _source_ts, _ingested_at, _ingest_engine, _is_deleted
                )
                SELECT d.source_id, d.tenant_id, %5$s,
                       d._source_generation, d._source_lsn, d._source_ts, NOW(), $1, d._op = 'd'
                FROM dedup d
                WHERE cardinality(d._unavailable) > 0
                ON CONFLICT (source_id, tenant_id, %6$s) DO NOTHING
            $q$, v_dedup, v_schema, c.raw_table, v_cols, v_sel_cols, v_pk) USING p_engine, v_target;

            EXECUTE format($q$
                WITH dedup AS (%1$s)
                UPDATE %2$I.%3$I AS t
                SET %4$s,
                    _source_generation = d._source_generation,
                    _source_lsn = d._source_lsn,
                    _source_ts = d._source_ts,
                    _ingested_at = NOW(),
                    _ingest_engine = $1,
                    _is_deleted = d._op = 'd'
                FROM dedup d
                WHERE cardinality(d._unavailable) > 0
                  AND t.source_id = d.source_id AND t.tenant_id = d.tenant_id AND t.%5$s = d.%5$s
                  AND %6$s
            $q$, v_dedup, v_schema, c.raw_table, v_set_unavailable, v_pk, format(v_guard, 'd'))
            USING p_engine, v_target;
            GET DIAGNOSTICS v_rc = ROW_COUNT;
            v_applied := v_applied + v_rc;
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
