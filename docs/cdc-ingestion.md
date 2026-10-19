<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Kafka CDC Ingestion, Control Plane and Feature Flag

This guide explains the scalable ingestion layer: what it is, how the pieces fit together, and the commands to run, check and operate every part of it. It covers the Python and CDC engines, the feature flag that switches between them, the control plane, Airflow, scaling, health checks and troubleshooting.

Commands are written for **PowerShell** (Windows) and also work in bash if you replace the `$env:NAME = "x"` lines with `export NAME=x`. All `docker compose` commands run from the repository root.

| # | Section |
|---|---|
| 1 | [Overview](#1-overview) |
| 2 | [Architecture](#2-architecture) |
| 3 | [Containers and profiles](#3-containers-and-profiles) |
| 4 | [Quick start](#4-quick-start) |
| 5 | [The Python engine](#5-the-python-engine) |
| 6 | [The CDC engine](#6-the-cdc-engine) |
| 7 | [The feature flag](#7-the-feature-flag) |
| 8 | [Control plane and Airflow](#8-control-plane-and-airflow) |
| 9 | [dbt and Superset](#9-dbt-and-superset) |
| 10 | [Configuration and scaling](#10-configuration-and-scaling) |
| 11 | [Health, freshness and correctness](#11-health-freshness-and-correctness) |
| 12 | [Load testing](#12-load-testing) |
| 13 | [Removing a source](#13-removing-a-source) |
| 14 | [Troubleshooting](#14-troubleshooting) |
| 15 | [Limitations](#15-limitations) |

---

## 1. Overview

A bank can run many Fineract databases (one per tenant, region or branch group). The BI platform copies their data into one reporting warehouse, without touching the source databases beyond read access, then transforms it with dbt and shows it in Superset.

There are two ways to do the copying. A **feature flag**, stored as data in the warehouse, decides which one owns each source and table.

| | Python engine | Kafka CDC engine |
|---|---|---|
| Idea | A scheduled job asks "what changed since last time?" | The database's own change log is streamed as it happens |
| Freshness | Minutes to hours (your schedule) | About a second |
| Deletes | Only found by a separate comparison job | Captured automatically |
| Source load | Queries each source on every run | Reads the log; no queries on the tables |
| Needs | PostgreSQL only | Kafka, Kafka Connect (Debezium), the Java sink |
| Best for | Small and medium deployments | Large banks, many sources, sub-minute freshness |

Both engines write through one function, `meta.apply_staged_batch()`, so the rows in the warehouse are identical whichever engine wrote them.

Python-only deployments do not need Kafka. Nothing in the CDC stack starts unless you ask for the `cdc` profile.

---

## 2. Architecture

```
  FINERACT POSTGRESQL DATABASES (one per bank / region / tenant), read-only for us
  +---------+   +---------+   +---------+          +---------+
  |  DB 1   |   |  DB 2   |   |  DB 3   |   ...    |  DB N   |
  +----+----+   +----+----+   +----+----+          +----+----+
       |             |             |                    |
       |  PATH A: scheduled        |   PATH B: real time (change log / WAL)
       |  "what changed?"          |
       v                           v
  +-------------------+     +---------------------------------+
  | Python extractor  |     | Debezium on Kafka Connect       |
  | keyset pagination |     | one connector per source DB     |
  +---------+---------+     +----------------+----------------+
            |                                |
            |                                v
            |                  +----------------------------+     bad events
            |                  | Kafka: one topic per table |--> dead-letter topic
            |                  | shared by every source     |    (kept, never lost)
            |                  +-------------+--------------+
            |                                |
            |                                v
            |                  +----------------------------+
            |                  | Java sink                  |
            |                  | micro-batches; offsets are |
            |                  | saved in the same DB       |
            |                  | transaction as the data    |
            |                  +-------------+--------------+
            |                                |
            v                                v
  +------------------------------------------------------------------+
  |  meta.apply_staged_batch()   (the ONE write path)                |
  |  - rejects writers with a stale epoch (fencing)                  |
  |  - drops duplicates, ignores older versions (capture gen + LSN)  |
  |  - deletes become markers, not removed rows                      |
  |  - records which loans changed (entity_change_log)               |
  +----------------------------+-------------------------------------+
                               |
                               v
  WAREHOUSE (PostgreSQL 18)
    raw.*          current data, one row per (source, table, id)
    raw_shadow.*   trial copy, used only while switching engines
    meta.*         registry, FEATURE FLAG (ownership), audit, offsets,
                   change log, reconciliation results
                               |
                               v
  dbt:  staging -> intermediate (only changed loans) -> facts -> marts
                               |
                               v
  Superset dashboards  (row-level security by source + office)

  +------------------------------------------------------------------+
  |  CONTROL PLANE                                                   |
  |  Airflow + the `controlplane` package + meta.* tables            |
  |  registers sources, creates/repairs Debezium connectors, runs    |
  |  the engine-switch workflow, schedules Python ingestion and dbt, |
  |  checks freshness. It does NOT carry data: if Airflow is down,   |
  |  CDC keeps flowing.                                              |
  +------------------------------------------------------------------+
```

### 2.1 How one change travels (CDC)

1. A row changes in a Fineract database. PostgreSQL writes it to its write-ahead log (WAL).
2. Debezium reads the WAL through a **replication slot** (a bookmark PostgreSQL keeps so the log is not discarded before it is read) and a **publication** (the list of tables to stream).
3. Debezium publishes one JSON message per change to the topic for that table, for example `fineract.cdc.m_loan_transaction`. The message has the before and after values, the source id, the log position (LSN) and the operation (`c` create, `u` update, `d` delete, `r` initial snapshot).
4. The Java sink reads batches, looks up who owns the source and table, and writes the batch into the warehouse in **one transaction together with its Kafka offsets**. A crash can neither lose a batch nor apply it twice.
5. The write function applies the rows: it rejects a stale writer, ignores duplicates and older versions, keeps deletes as markers, and records which loans changed.
6. dbt reads the change log and recomputes only those loans.

### 2.2 Design decisions

1. **One write path for both engines.** Identical rows regardless of engine.
2. **Ownership as data, with a fencing epoch.** The database itself rejects a writer that is out of date.
3. **One Kafka topic per table, shared by all sources.** A thousand sources do not need thirteen thousand topics. Messages are keyed by row id so one row stays in order.
4. **Offsets stored in the warehouse, in the same transaction as the data.** Kafka may redeliver; the result is still exact.
5. **Soft deletes plus an ordering guard** on `(capture_generation, LSN)`, so a late update cannot resurrect a deleted row.
6. **Source-qualified keys.** Fineract is database-per-tenant, so ids repeat. Every key is `(source_id, tenant_id, id)`.
7. **Change-log-driven incremental dbt.** Only changed loans are recomputed.
8. **Airflow orchestrates and never carries data.**
9. **Secrets by reference.** The registry stores `env:NAME`, never a password.
10. **Fail loudly.** Debezium refuses to resume from a missing log position; bad events go to a dead-letter topic.

### 2.3 Where things live

| Component | Path | Role |
|---|---|---|
| Table contract | `contracts/fineract_tables.json` | The single list of tables and columns every component reads |
| Warehouse logic | `warehouse/schema/platform.sql` | Write function, ownership flag, registry, audit, offsets, change log |
| Python engine | `extractor/` | Scheduled extraction |
| Control plane | `controlplane/` | Registry, flag state machine, connector reconciler, cutover gate, source reconciliation, `retire-source`, CLI |
| Kafka to warehouse | `ingestion/ingestion-core`, `ingestion/ingestion-sink` | Parser, routing, writer, sink application |
| Load generator | `ingestion/ingestion-loadgen`, `loadtest/` | Synthetic traffic and exactness checks |
| Workflows | `airflow/dags/` | Scheduled and manual operations |
| Transformations | `dbt/` | Models, metric macros, tests |
| Dashboards | `superset/`, `docker/superset/` | Datasets, charts, row-level security |
| Scale references | `compose.kafka-cluster.yaml`, `deploy/k8s/` | Multi-broker and Kubernetes examples (not tested) |

---

## 3. Containers and profiles

An image is a packaged program and a container is a running copy. `docker compose --profile X ...` turns on an optional group.

| Container | Profile | What it is | Used for |
|---|---|---|---|
| `fineract-db` | (core) | PostgreSQL | Demo source databases that stand in for a bank's Fineract databases |
| `warehouse` | (core) | PostgreSQL 18 | The reporting database: raw data plus the control-plane tables |
| `dbt` | (core) | dbt | Builds dashboard-ready tables (idle container, you `exec` into it) |
| `superset` | (core) | Apache Superset | Dashboards |
| `extractor` | `python` | Python image | Runs the Python engine and the control-plane CLI inside Docker |
| `kafka` | `cdc` | Apache Kafka (KRaft, no ZooKeeper) | The message queue |
| `kafka-init` | `cdc` | Java image | Creates the topics, then exits (normal) |
| `connect` | `cdc` | Kafka Connect with Debezium | Reads databases' change logs into Kafka |
| `sink` | `cdc` | Java image | Kafka to warehouse writer, plus a `/metrics` and `/health` endpoint |
| `airflow-*` | `airflow` | Apache Airflow | API server, scheduler and DAG processor |
| `loadgen` | `loadtest` | Java image | Synthetic load generator and verifier |

---

## 4. Quick start

```powershell
cd <project folder>
function wh  { docker exec fineract-bi-warehouse psql -U analytics_admin -d analytics @args }
function src { docker exec fineract-bi-fineract-db psql -U root -d fineract_default @args }
```

`wh` runs SQL on the warehouse and `src` runs SQL on the demo source database. They need no password because the connection is inside the container. Use them like `wh -c "SELECT 1"`.

**Python only**

```powershell
docker compose up -d warehouse fineract-db dbt superset
```

**Secrets and keys in `.env`**

Copy `.env.example` to `.env`. Besides the database passwords it now needs `AIRFLOW_FERNET_KEY`, `AIRFLOW_JWT_SECRET`, `AIRFLOW_ADMIN_PASSWORD`, `FBI_PII_HASH_KEY`, `FBI_CONNECT_USER`, `FBI_CONNECT_PASSWORD`, `WAREHOUSE_ENGINE_USER` and `WAREHOUSE_ENGINE_PASSWORD`. Compose refuses to start without them, so there are no built-in default secrets. The sample file carries development-only values; replace them outside a laptop. `FBI_DEFAULT_SSLMODE=disable` in the sample is also development-only: the code default for source connections is `verify-full`. The Kafka Connect REST port (8083), Kafka's external listener (29092) and Airflow (8080) are published on `127.0.0.1` only. The Kafka Connect REST API also requires HTTP Basic authentication with `FBI_CONNECT_USER` and `FBI_CONNECT_PASSWORD`, and the control plane sends the same credentials. In PowerShell, build the header once per window: `$ch = @{ Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("fbi_connect:<connect password>")) }`, then add `-Headers $ch` to every `Invoke-RestMethod` call against port 8083. To run the control-plane commands from the host, also set `$env:FBI_CONNECT_USER="fbi_connect"; $env:FBI_CONNECT_PASSWORD="<connect password>"`.

**Upgrading an existing environment.** Role creation runs only when the warehouse volume is first created. On an existing warehouse, re-apply `warehouse/schema/platform.sql` as the admin role, create the engine role with the grants in `docker/postgres-warehouse/initdb/002_create_warehouse_roles.sh`, run `UPDATE meta.source_registry SET secret_ref = 'env:FBI_SRC_DEMO_PASSWORD' WHERE secret_ref = 'env:FINERACT_CDC_PASSWORD'` (and rename the variable wherever you set it), recreate `connect` and `sink`, and run one `dbt build --full-refresh`.

**Add the CDC stack**

```powershell
$env:FBI_SRC_DEMO_PASSWORD = "<dev password>"     # must be set before 'connect' starts
$env:KAFKA_HEAP = "384m"; $env:CONNECT_HEAP = "384m"
docker compose up -d warehouse fineract-db
docker compose --profile cdc up -d kafka kafka-init connect sink
docker compose --profile cdc ps -a --format "table {{.Service}}\t{{.State}}\t{{.Status}}"
docker compose --profile cdc logs --tail 15 sink
```

Expect `kafka`, `connect` and `sink` as `running (healthy)` (connect takes about 30 seconds) and `kafka-init` as `Exited (0)`. The sink log shows `Loaded ownership: N KAFKA routes` and `Sink started: 2 workers`. Early `Connection ... could not be established` warnings only mean the sink started before Kafka was ready; it retries.

`KAFKA_HEAP` sets both the starting and the maximum Java memory (`-Xms` equals `-Xmx`), and it must fit inside the container's memory limit. 384 MB is for a small laptop; servers use 4 to 6 GB.

**Add Airflow**

```powershell
docker compose --profile airflow up -d airflow-apiserver airflow-scheduler airflow-dag-processor
```

UI: http://localhost:8080 (user `admin`, password from `.env`).

**Stop everything (keeps data)**

```powershell
docker compose --profile cdc --profile airflow --profile python --profile loadtest stop
```

`docker compose --profile cdc down -v` also deletes the Kafka and warehouse volumes. That cannot be undone.

---

## 5. The Python engine

The Python engine reads each source through read-only views and copies changed rows on a schedule. It reads in pages of `EXTRACT_PAGE_SIZE` rows using a keyset watermark `(cursor, primary key)`, so memory stays flat however big a table is, and it re-reads a 10-minute lookback window (`EXTRACT_LOOKBACK_SECONDS=600`) for commits that become visible late. A COB (end-of-day batch) gate stops it from copying half-processed books. Incremental mode cannot see deleted rows, so a separate `reconcile` run compares keys with bucketed checksums. The page query is `WHERE (cursor, pk) > (...) ORDER BY cursor, pk` on the raw column, so it can only avoid scanning the whole table when the source has an index on `(last_modified_on_utc, id)` for each large table (the compatibility views are plain projections, so the index is used through them). Ask the source DBA to create it before the first backfill of a big database. Rows whose cursor column is NULL are read in a separate first pass by primary key. Each (source, table, target) is processed under an advisory lock, so overlapping runs of the ingestion, delete-reconcile and feature-flag follow-up jobs queue instead of racing, and delete candidates are re-checked against the source immediately before they are soft-deleted.

**Point the extractor at the demo source and the warehouse**

```powershell
$env:WAREHOUSE_DB_HOST="localhost"; $env:WAREHOUSE_DB_PORT="5434"
$env:WAREHOUSE_DB_USER="analytics_loader"; $env:WAREHOUSE_DB_PASSWORD="<dev password>"
$env:SOURCE_DB_HOST="localhost"; $env:SOURCE_DB_PORT="5433"; $env:SOURCE_DB_NAME="fineract_default"
$env:SOURCE_DB_USER="fineract_reader"; $env:FBI_SRC_DEFAULT_PASSWORD="<dev password>"
$env:SOURCE_DB_SCHEMA="bi_connector_source"
$env:COB_LOOKBACK_HOURS="99999"; $env:LOG_LEVEL="WARNING"
```

**First full copy, then changes only**

```powershell
$r = (python -m extractor.cli backfill | Out-String) | ConvertFrom-Json
$r | Format-Table source_id, status, rows_read, rows_applied, rows_deleted
$r.tables | Format-Table table, target, rows_read, rows_applied, status

src -q -t -A -c "UPDATE m_loan SET last_modified_on_utc = clock_timestamp() WHERE id = (SELECT min(id) FROM m_loan) RETURNING id, last_modified_on_utc"
$i = (python -m extractor.cli incremental | Out-String) | ConvertFrom-Json
$i.tables | Where-Object { $_.table -eq "m_loan" } | Format-Table table, rows_read, rows_applied
python -m extractor.cli reconcile
```

Sources owned by Kafka show `skipped`; the Python engine only touches tables it owns (section 7).

**Check what landed**

```powershell
wh -c "SELECT count(*) AS raw_rows FROM raw.raw_m_loan WHERE source_id='default'"
wh -c "SELECT table_name, target, cursor_value, last_pk FROM meta.extract_watermarks WHERE source_id='default' ORDER BY 1 LIMIT 6"
wh -x -c "SELECT source_id, id, loan_id, amount, _source_lsn, _is_deleted, _ingest_engine, _ingested_at FROM raw.raw_m_loan_transaction WHERE source_id='default' AND NOT _is_deleted ORDER BY id LIMIT 1"
```

Every raw row carries these extra columns:

| Column | Meaning |
|---|---|
| `source_id` | Which database the row came from |
| `_ingest_engine` | `PYTHON` or `KAFKA` |
| `_source_lsn` | Log position (empty for Python rows) |
| `_source_ts` | When the change committed at the source (CDC rows) |
| `_is_deleted` | Deletes are kept as markers, not removed |
| `_ingested_at` | When the warehouse wrote it (used to measure freshness) |

---

## 6. The CDC engine

### 6.1 Concepts

| Piece | Plain-English role |
|---|---|
| **WAL** | The write-ahead log every PostgreSQL keeps for crash recovery |
| **`wal_level=logical`** | A setting that lets other programs read the WAL as row-level changes |
| **Publication** | The list of tables to stream. A DBA creates it |
| **Replication slot** | A bookmark that stops PostgreSQL discarding log pages before Debezium reads them. If Debezium disappears for good, pages pile up on the source disk, so unused slots are dropped (the reconciler does this) |
| **Debezium** | The reader. Produces one message per change |
| **Kafka Connect** | Runs Debezium, one connector per source database. REST API on port 8083 |
| **Kafka topic** | One per table (`fineract.cdc.<table>`), shared by every source, kept 72 hours. Housekeeping topics: `fineract.cdc.dlt` (dead letters), `fineract.cdc.signals`, `fineract.cdc.heartbeats` |
| **Partitions** | Parallel lanes in a topic. Messages are keyed by row id so one row stays in order |
| **Java sink** | Reads batches and writes them with their offsets in one transaction. Bad messages go to the dead-letter topic |

Only the columns listed in the contract leave the source (`column.include.list`), and columns flagged `pii` in the contract (today `date_of_birth`) are left out unless you set `FBI_EXCLUDE_PII=false`. The Python engine applies the same switch, so both engines write the same raw rows. Connector passwords are never stored: the registry holds a reference such as `env:FBI_SRC_DEMO_PASSWORD`, and Connect resolves it from its own environment.

### 6.2 Look at what exists

```powershell
docker compose --profile cdc exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose --profile cdc exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic fineract.cdc.m_loan
(Invoke-RestMethod -Headers $ch "http://localhost:8083/connectors?expand=status").PSObject.Properties | ForEach-Object { "{0}: {1} tasks={2}" -f $_.Name, $_.Value.status.connector.state, (($_.Value.status.tasks | ForEach-Object { $_.state }) -join ",") }
```

You should see the 13 `fineract.cdc.<table>` topics plus the housekeeping topics. `describe` shows `PartitionCount: 6` and `ReplicationFactor: 1` on the laptop profile (production: 3 copies).

### 6.3 Add a CDC source

**Step 1. Prepare a source database** (here a new demo database; on a real source your DBA does the equivalent):

```powershell
$tl = ((Get-Content contracts\fineract_tables.json -Raw | ConvertFrom-Json).tables.table_name | ForEach-Object { "public.$_" }) -join ", "
docker exec fineract-bi-fineract-db psql -U root -d postgres -c "CREATE ROLE fbi_cdc LOGIN REPLICATION PASSWORD '<dev password>'"
docker exec fineract-bi-fineract-db psql -U root -d postgres -c "CREATE DATABASE fineract_src_004"
docker cp warehouse\seed\schema_fineract_source.sql fineract-bi-fineract-db:/tmp/schema.sql
docker exec fineract-bi-fineract-db psql -U root -d fineract_src_004 -q -f /tmp/schema.sql
docker exec fineract-bi-fineract-db psql -U root -d fineract_src_004 -c "GRANT CONNECT ON DATABASE fineract_src_004 TO fbi_cdc; GRANT USAGE ON SCHEMA public TO fbi_cdc; GRANT SELECT ON ALL TABLES IN SCHEMA public TO fbi_cdc; CREATE PUBLICATION fbi_cdc FOR TABLE $tl"
```

`fbi_cdc` is a limited login that may read the log and `SELECT`, nothing else. The source needs `wal_level=logical` (the demo database already has it).

**Step 2. Register it in the control plane**

```powershell
$env:WAREHOUSE_DB_HOST="localhost"; $env:WAREHOUSE_DB_PORT="5434"; $env:WAREHOUSE_DB_USER="analytics_loader"; $env:WAREHOUSE_DB_PASSWORD="<dev password>"; $env:LOG_LEVEL="WARNING"
python -m controlplane.cli --actor you register-source --source-id cdc-src-004 --tenant-id t-004 --region west --host fineract-bi-fineract-db --db fineract_src_004 --user fbi_cdc --secret-ref env:FBI_SRC_DEMO_PASSWORD --engine KAFKA
```

This writes one registry row and 13 ownership rows ("KAFKA owns every table").

`--secret-ref` must be an allowed reference. An `env:` name has to match the allowlist in `FBI_SECRET_ENV_ALLOWLIST` (by default only names starting with `FBI_SRC_`, such as `FBI_SRC_DEMO_PASSWORD`), and a `file:` path has to sit below `FBI_SECRET_DIR`. Anything else is rejected, because a source's `secret_ref` is resolved on the workers and sent to the source's `db_host`. Kafka Connect applies the same limits (`CONNECT_ENV_ALLOWLIST`, `CONNECT_FILE_ALLOWED_PATHS`). Source connections verify TLS by default; for the local demo database set `$env:FBI_DEFAULT_SSLMODE = "disable"` in the window where you run the control-plane commands.

**Step 3. Create the connector.** Wait about 8 seconds first so the sink learns about the new source (it refreshes every 5 seconds); otherwise the first messages are dead-lettered as `UNKNOWN_SOURCE`.

```powershell
Start-Sleep 8
$env:FBI_CONNECT_CLUSTERS='{"default":"http://localhost:8083"}'; $env:FBI_DEBEZIUM_PUBLICATION_MODE="disabled"; $env:FBI_RECONCILER_DROP_STALE_SLOTS="false"
$rep = (python -m controlplane.cli reconcile-connectors | Out-String) | ConvertFrom-Json
$rep.actions | Format-Table kind, name
```

*Reconcile* means: compare what should exist (the registry) with what exists (Kafka Connect) and fix the difference. Expect `create fbi-src-cdc-src-004-g0`. The `g0` is the **capture generation**: it increases each time a source is re-created, so a new connector never reuses a deleted connector's saved log position.

**Step 4. Confirm it works. Look at the task, not only the connector.**

```powershell
Start-Sleep 10
Invoke-RestMethod -Headers $ch http://localhost:8083/connectors/fbi-src-cdc-src-004-g0/status | ConvertTo-Json -Depth 4
docker exec fineract-bi-fineract-db psql -U root -d postgres -c "SELECT slot_name, active FROM pg_replication_slots WHERE slot_name LIKE '%004%'"
```

Healthy means connector `RUNNING`, **task `RUNNING`**, and the slot `active = t`. A `FAILED` task under a `RUNNING` connector means no data flows; read its `trace`.

### 6.4 Send traffic and follow each hop

```powershell
$rootpw = (Select-String -Path .env -Pattern '^SOURCE_BOOTSTRAP_PASSWORD=').Line.Split('=')[1]
docker compose --profile loadtest run --rm loadgen source-workload --sources=1 --jdbc-template=jdbc:postgresql://fineract-bi-fineract-db:5432/fineract_src_004 --db-user=root --db-password=$rootpw --seed-loans=200 --tps=100 --duration-sec=30
```

This runs real transactions (new loans, repayments, reversals, deletes) at 100 per second for 30 seconds. Then check each hop:

```powershell
# Hop 1: messages in Kafka (topic:partition:offset)
docker compose --profile cdc exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic fineract.cdc.m_loan_transaction 2>$null
# Hop 2: sink backlog (lag 0 means caught up)
docker compose --profile cdc exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group fbi-warehouse-sink 2>$null | Select-String "m_loan_transaction"
# Hop 3: what the sink has written
docker compose --profile cdc exec sink bash -c "exec 3<>/dev/tcp/127.0.0.1/9404; printf 'GET /metrics HTTP/1.0\r\n\r\n' >&3; cat <&3" 2>$null | Select-String "^fbi_sink_(batches_total|db_errors_total|rows_written_total.*m_loan_transaction)"
# Hop 4: the warehouse
wh -c "SELECT count(*) FILTER (WHERE NOT _is_deleted) AS live, count(*) FILTER (WHERE _is_deleted) AS tombstones FROM raw.raw_m_loan_transaction WHERE source_id='cdc-src-004'"
```

Offsets grow, lag returns to 0, `db_errors_total` stays 0, and the warehouse has live rows and tombstones.

**One raw Debezium message**

```powershell
$m = docker compose --profile cdc exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fineract.cdc.m_loan --partition 0 --offset earliest --max-messages 1 --timeout-ms 8000 2>$null
$j = ($m | Select-Object -First 1) | ConvertFrom-Json
"op=$($j.op)  bank=$($j.source.name)  table=$($j.source.table)  log position=$($j.source.lsn)"
$j.after | Select-Object id, account_no, principal_amount | Format-List
```

`op=r` is the initial snapshot; `c`, `u` and `d` are live changes.

**A delete, end to end**

```powershell
$id = (docker exec fineract-bi-fineract-db psql -U root -d fineract_src_004 -t -A -c "SELECT max(id) FROM m_loan_transaction").Trim()
docker exec fineract-bi-fineract-db psql -U root -d fineract_src_004 -c "DELETE FROM m_loan_transaction WHERE id = $id"
Start-Sleep 4
wh -c "SELECT id, _is_deleted, _ingest_engine, _source_lsn FROM raw.raw_m_loan_transaction WHERE source_id='cdc-src-004' AND id = $id"
```

The row is not removed in the warehouse. It is marked `_is_deleted = t` with its log position, and the ordering guard stops a late update from bringing it back. The Python engine cannot see this.

### 6.5 Prove it is exactly right

```powershell
$r = (docker compose --profile python run --rm -T --no-deps -e FBI_SRC_DEMO_PASSWORD=<dev password> extractor python -m controlplane.cli reconcile-source --source-id cdc-src-004 --bucket-size 1000 | Out-String) | ConvertFrom-Json
$r.PSObject.Properties | ForEach-Object { "{0,-34} {1} rows={2}" -f $_.Name, $_.Value.status, $_.Value.compared_rows }
```

This compares the source and the warehouse with a checksum per block of rows, so it scales to huge tables. It runs in a container because the registered address (`fineract-bi-fineract-db`) is only visible inside Docker. Expect `PASSED` on all 13 tables. If tables with rows say `FAILED` while the warehouse is empty, data never arrived: go back to step 4.

---

## 7. The feature flag

### 7.1 The idea

"Who is in charge of each source" is data. For every (source, table), one row in `meta.ingestion_ownership` says `engine = PYTHON` or `KAFKA`. That table is the feature flag. New sources get their first engine from `INGESTION_ENGINE_DEFAULT` (`PYTHON`, `KAFKA` or `NONE`).

Switching follows a fixed procedure:

| Step | What happens | Why |
|---|---|---|
| **start shadow** | The new engine fills a trial copy (`raw_shadow`) while the old engine stays in charge of the real data (`raw`) | Dashboards keep reading real data, so there is no risk |
| **cutover gate** | The system compares the trial copy and the real data table by table, row by row | Proof that the new engine produces the same data |
| **cutover** | A **second person** approves; the trial copy becomes real data and ownership flips | Nobody can approve their own change |
| **rollback** | The same steps with the engines swapped | Undoing is a normal operation |

Every ownership change bumps an **epoch**. Writers must present the current epoch and the **database rejects old ones** (SQLSTATE `FBI01`). The sink log line `Batch fenced ... expected epoch 11 ... epoch 12` is this protection working. Every step is written to `meta.ingestion_ownership_audit` with who asked, who approved, when and why.

The rules are enforced in the database as well as in the CLI. The sink and the Python extractor connect as a separate engine role that has no write access to the ownership, audit, settings or registry tables, so a compromised engine cannot flip its own ownership:

- A trigger on `meta.ingestion_ownership` rejects an engine change that did not follow a legal transition (SQLSTATE `FBI03`). A cutover needs a `PASSED` `shadow_vs_prod` reconciliation of the same source and table that finished within `reconciliation_max_age_minutes` (default 120).
- The approver must differ from the actor and from whoever started the shadow (`shadow_requested_by`).
- `meta.ingestion_ownership_audit` is append-only, and it records the database login (`db_user`) of every change. The loader role used by the engines cannot update or delete it, and cannot change `meta.platform_settings`.
- `dual_control_mode` is `parameter` by default: the approver is a name passed on the command line or in the DAG, which is only as trustworthy as whoever runs it. Set it to `database` (as the admin role) when the requester and approver are different people with different database logins. A cutover then needs `meta.approve_cutover()` to have been called by a login other than the one that started the shadow. From PowerShell: run `cpc approve-cutover --source-id default --tables all` under the approver's own database login, then run the cutover.
- Through Airflow the actor is the authenticated triggering user. Set `FBI_REQUIRE_AUTHENTICATED_ACTOR=true` to refuse runs that only carry a `requested_by` parameter.


### 7.2 Run the tools inside Docker

Debezium (in Docker) and the control-plane tools must refer to the source database by the same address. Inside Docker that is `fineract-bi-fineract-db`, so these helpers run the tools in a container:

```powershell
$tl = ((Get-Content contracts\fineract_tables.json -Raw | ConvertFrom-Json).tables.table_name | ForEach-Object { "public.$_" }) -join ", "
src -c "GRANT CONNECT ON DATABASE fineract_default TO fbi_cdc; GRANT USAGE ON SCHEMA public, bi_connector_source TO fbi_cdc; GRANT SELECT ON ALL TABLES IN SCHEMA public, bi_connector_source TO fbi_cdc"
src -c "CREATE PUBLICATION fbi_cdc FOR TABLE $tl"      # "already exists" is fine

$E = @("-e","SOURCE_DB_HOST=fineract-bi-fineract-db","-e","SOURCE_DB_PORT=5432","-e","SOURCE_DB_NAME=fineract_default",
       "-e","SOURCE_DB_USER=fbi_cdc","-e","FBI_SRC_DEFAULT_PASSWORD=<dev password>","-e","SOURCE_DB_SECRET_REF=env:FBI_SRC_DEMO_PASSWORD",
       "-e","FBI_SRC_DEMO_PASSWORD=<dev password>","-e","SOURCE_DB_SCHEMA=bi_connector_source","-e","COB_LOOKBACK_HOURS=999999",
       "-e","FBI_DEBEZIUM_PUBLICATION_MODE=disabled","-e","LOG_LEVEL=WARNING")
function ext { docker compose --profile python run --rm -T --no-deps @E extractor python -m extractor.cli @args }
function cpc { docker compose --profile python run --rm -T --no-deps @E extractor python -m controlplane.cli @args }
$q = "SELECT engine, coalesce(shadow_engine,'-') AS shadow, cutover_state, count(*) AS tables FROM meta.ingestion_ownership WHERE source_id='default' GROUP BY 1,2,3"
```

`ext` runs the Python extractor and `cpc` runs the control-plane tool. `$q` is a saved query you re-run after every step. The `fbi_cdc` role comes from section 6.3.

### 7.3 Move a source from Python to Kafka, and back

**1. Python owns the source and loads it**

```powershell
(ext backfill --source-id default | Out-String) | ConvertFrom-Json | Format-Table source_id, status, rows_applied
wh -c $q          # PYTHON / - / STEADY / 13
```

**2. Start the shadow with Kafka**

```powershell
cpc --actor alice ownership-apply --source-id default --tables all --action start_shadow --engine KAFKA --reason "migrate to CDC" | Out-Null
wh -c $q          # PYTHON / KAFKA / SHADOWING
```

**3. Create the Debezium connector, then wait for the trial copy to catch up**

```powershell
$rep = (cpc --actor alice reconcile-connectors | Out-String) | ConvertFrom-Json
$rep.actions | Format-Table kind, name
for ($i = 0; $i -lt 40; $i++) { $c = wh -t -A -c "SELECT (SELECT count(*) FROM raw_shadow.raw_m_loan_transaction WHERE source_id='default' AND NOT _is_deleted) || ' shadow vs ' || (SELECT count(*) FROM raw.raw_m_loan_transaction WHERE source_id='default' AND NOT _is_deleted) || ' production'"; $c; if ($c -match '^(\d+) shadow vs \1 production' -and $Matches[1] -ne '0') { break }; Start-Sleep 3 }
```

Check the connector **task** is `RUNNING` (section 6.3, step 4). The count goes from `0 shadow vs N production` to equal numbers: that is Debezium's initial snapshot filling `raw_shadow`.

**4. The cutover gate**

```powershell
$g = (cpc --actor alice cutover-gate --source-id default --settle-seconds 0 | Out-String) | ConvertFrom-Json
$g.PSObject.Properties | ForEach-Object { "{0,-34} passed={1} rows={2} discrepancies={3}" -f $_.Name, $_.Value.passed, $_.Value.compared_rows, $_.Value.discrepancies }
wh -c $q          # PYTHON / KAFKA / CUTOVER_READY
```

Every table must show `passed=True ... discrepancies=0`. The system refuses a cutover without this. `--settle-seconds 0` skips the "ignore rows changed in the last N seconds" allowance; production would use something like 900.

**5. Dual control: approving your own change must fail**

```powershell
(cpc --actor alice ownership-apply --source-id default --tables all --action cutover --approved-by alice --reason x 2>&1 | Out-String) -split "`n" | Select-String "TransitionError:" | Select-Object -First 1
```

**6. Cutover, approved by a second person**

```powershell
cpc --actor alice ownership-apply --source-id default --tables all --action cutover --approved-by bob --reason "go live" | Out-Null
wh -c $q          # KAFKA / - / STEADY
```

**7. Prove CDC now feeds production**

```powershell
src -q -t -A -c "UPDATE m_loan SET last_modified_on_utc = clock_timestamp() WHERE id = (SELECT min(id) FROM m_loan) RETURNING last_modified_on_utc"
Start-Sleep 5
wh -c "SELECT id, last_modified_on_utc, _ingest_engine FROM raw.raw_m_loan WHERE source_id='default' ORDER BY last_modified_on_utc DESC LIMIT 1"
```

The warehouse has the new timestamp and `_ingest_engine = KAFKA`, and you did not run the Python extractor.

**8. Rollback is the same steps with the engines swapped**

```powershell
cpc --actor carol ownership-apply --source-id default --tables all --action start_shadow --engine PYTHON --reason "rollback drill" | Out-Null
(ext backfill --source-id default | Out-String) | ConvertFrom-Json | Out-Null
$g = (cpc --actor carol cutover-gate --source-id default --settle-seconds 0 | Out-String) | ConvertFrom-Json
"tables passed: {0} of {1}" -f @($g.PSObject.Properties | Where-Object { $_.Value.passed }).Count, @($g.PSObject.Properties).Count
cpc --actor carol ownership-apply --source-id default --tables all --action cutover --approved-by dave --reason "rollback done" | Out-Null
$rep = (cpc --actor carol reconcile-connectors | Out-String) | ConvertFrom-Json; $rep.actions | Format-Table kind, name
wh -c $q          # PYTHON / - / STEADY, and the connector is deleted
```

### 7.4 The audit trail

```powershell
wh -c "SELECT to_char(created_at,'HH24:MI:SS') AS at, action, actor, approved_by, reason FROM meta.ingestion_ownership_audit WHERE source_id='default' AND table_name='m_loan' ORDER BY audit_id DESC LIMIT 8"
```

### 7.5 Other actions

| `--action` | Meaning |
|---|---|
| `assign` | Give a brand-new source its first engine |
| `start_shadow` / `abort_shadow` | Begin or cancel the trial copy |
| `begin_reconciliation` / `record_reconciliation` | Used by the cutover gate |
| `cutover` | Flip ownership (needs `--approved-by`, a different person) |
| `pause` / `resume` | Stop writing temporarily (writers are fenced while paused) |
| `force_assign` | Emergency override (needs an approver) |

Use `--tables m_loan,m_client` to switch only some tables instead of `all`.

---

## 8. Control plane and Airflow

The **control plane** is the part that decides and manages. It does not move data.

- the `controlplane/` package: source registry, flag rules, connector reconciler, cutover gate, source reconciliation, `retire-source`, and the `python -m controlplane.cli` command
- the `meta.*` tables: `source_registry`, `ingestion_ownership`, `ingestion_ownership_audit`, `connector_state`, `source_generation_floor`, offsets, change log

**Airflow** is the scheduler that runs these routines on a timer or on demand. It never carries data: if Airflow is down, CDC keeps flowing and only the scheduled and orchestrated jobs pause.

| DAG | When | What it does |
|---|---|---|
| `fbi_connector_reconciler` | every 2 min | Makes Debezium match the registry: create, update, pause, delete connectors, restart failed ones, drop stale slots |
| `fbi_python_ingestion` | hourly | Runs the Python extractor for Python-owned tables, then signals "new raw data" |
| `fbi_dbt_build` | on that signal, or every 15 min | `dbt build`, one run at a time |
| `fbi_freshness_monitor` | every 5 min | Fails (so it can alert) if an owned source has not updated within the limit |
| `fbi_ingestion_feature_flag` | manual | The engine-switch workflow |
| `fbi_source_onboarding` | manual | Register, validate and activate a new source |
| `fbi_source_reconciliation` | daily | Checksum sources against the warehouse |
| `fbi_python_delete_reconcile`, `fbi_maintenance`, `fbi_dbt_full_refresh_presentations` | daily / weekly | Detect deletes, purge old bookkeeping, refresh marts |

**Start it** (about 1.5 GB of RAM; these settings keep it small and point its tools at the demo database):

```powershell
$env:SOURCE_DB_HOST="fineract-bi-fineract-db"; $env:SOURCE_DB_HOST_PORT="5432"; $env:SOURCE_REPLICA_USER="fbi_cdc"; $env:SOURCE_REPLICA_PASSWORD="<dev password>"
$env:SOURCE_DB_SECRET_REF="env:FBI_SRC_DEMO_PASSWORD"; $env:FBI_SRC_DEMO_PASSWORD="<dev password>"; $env:FBI_DEBEZIUM_PUBLICATION_MODE="disabled"
$env:AIRFLOW_PARALLELISM="2"; $env:AIRFLOW_MAX_ACTIVE_TASKS_PER_DAG="2"; $env:FBI_POOL_SOURCE_EXTRACT="1"
docker compose --profile airflow up -d airflow-apiserver airflow-scheduler airflow-dag-processor
```

**Drive it from PowerShell**

```powershell
$tok = (Invoke-RestMethod -Method Post -Uri http://localhost:8080/auth/token -ContentType "application/json" -Body '{"username":"admin","password":"<airflow admin password>"}').access_token
$h = @{ Authorization = "Bearer $tok" }
(Invoke-RestMethod -Uri "http://localhost:8080/api/v2/dags?limit=50" -Headers $h).dags | Format-Table dag_id, is_paused
$run = Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/v2/dags/fbi_python_ingestion/dagRuns -Headers $h -ContentType "application/json" -Body '{"logical_date": null, "conf": {}}'
(Invoke-RestMethod -Uri "http://localhost:8080/api/v2/dags/fbi_python_ingestion/dagRuns/$($run.dag_run_id)" -Headers $h).state
```

Run the last line a few times: `queued`, `running`, `success`. When `fbi_python_ingestion` succeeds it automatically triggers `fbi_dbt_build`.

---

## 9. dbt and Superset

**dbt** turns the raw tables into dashboard-ready ones: `staging` (clean names, soft-deleted rows dropped, `source_id` on every join) → `intermediate` (which loans need recomputing, each loan's daily status) → `analytics` dimensions, `fact_loan_snapshot` (one row per loan per day) and the three marts. Three things changed for this feature:

- **Only changed loans and partitions are recomputed.** The write path records every change in `meta.entity_change_log`. Each dbt run reads the new entries and recomputes just those loans from their earliest affected date. The fact build also records which (source, office, product) partitions it touched, including the partition a loan or client moved away from, in `meta.dbt_mart_dirty`. Each mart then recomputes only those partitions from the earliest affected date. Every consumer has its own cursor: `fact_loan_snapshot` in `meta.dbt_change_cursor` and each mart in `meta.dbt_mart_cursor`. A cursor advances from that model's own successful build, never because of a test failure, and a mart never reads past the point the fact has reached. Running `dbt build -s +mart_delinquency_par` therefore leaves the other marts' cursors where they were, and they catch up on the next full build.
- **Metrics are defined once** (`dbt/macros/metrics.sql`, catalog in `dbt/seeds/metric_definitions.csv`) and reused by every mart. A test proves the marts agree.
- **Removed sources are detected.** `assert_no_orphan_sources_in_marts` fails if derived rows exist for a source that is no longer registered (fix it with `retire-source`, section 13).

```powershell
docker compose up -d warehouse fineract-db dbt
docker compose exec -T dbt dbt deps
docker compose exec -T dbt dbt build 2>&1 | Select-String "OK|PASS|FAIL|ERROR|Done\."
```

`dbt build` loads the seed, creates every model and runs every test. Expect `ERROR=0`. To check that the incremental shortcut equals a full rebuild:

```powershell
$fp = "SELECT count(*) AS rows, sum(principal_outstanding) AS principal, sum(total_outstanding) AS outstanding, count(*) FILTER (WHERE is_par_30) AS par30_rows FROM analytics.fact_loan_snapshot"
wh -c $fp
docker compose exec -T dbt dbt build --full-refresh 2>&1 | Select-String "Done\."
wh -c $fp
```

The two fingerprints must be identical.

**Superset** reads the marts (it stores no data). Because office ids repeat across banks (every bank has an office 101), row-level security uses the pair `(source_id, office_id)`. The rules live in `meta.user_office_mapping` and are expanded by `meta.v_user_access_scope`. Each dashboard reads a "secure" dataset that includes the logged-in username and keeps only allowed pairs. An unknown or empty username returns no rows. The dashboard database user can read `analytics` and `meta`, never `raw` or `raw_shadow`.

```powershell
docker compose up -d warehouse superset
wh -c "SELECT username, role_name, region, source_id, office_id FROM meta.user_office_mapping ORDER BY 1"
wh -c "SELECT * FROM meta.v_user_access_scope ORDER BY 1, 3"
```

Open http://localhost:8088 and log in as `admin`, `north_manager` (office 101 only) and `south_manager` (office 102 only) to see the difference. Passwords are in `.env`.

---

## 10. Configuration and scaling

Everything is configured through environment variables (see `.env.example` for the full list). The main dials:

| Dial | Controls | Setting | Rule of thumb |
|---|---|---|---|
| Partitions | Parallel lanes per Kafka topic | `TOPIC_PARTITION_SCALE`, `TOPIC_MAX_PARTITIONS` | Laptop 6; production up to 48 on the busiest table |
| Sink threads | Reader threads per sink container | `SINK_THREADS` | Total threads at most the number of partitions |
| Sink replicas | More sink containers | `SINK_REPLICAS` | replicas x threads at most partitions |
| Batch size | Events per database write | `SINK_MAX_BATCH_EVENTS`, `SINK_MAX_BATCH_WAIT_MS` | Bigger is more efficient and slightly slower |
| Connect workers | Containers running Debezium | `CONNECT_REPLICAS` | One connector reads one database's log; scale by number of databases |
| Heap | Java working memory | `KAFKA_HEAP`, `CONNECT_HEAP`, and the matching `*_MEM_LIMIT` | Laptop 384 MB; servers 4 to 6 GB; must fit the container limit |
| Kafka safety | Brokers and copies | `compose.kafka-cluster.yaml` | 3 brokers, 3 copies, 2 must acknowledge |
| Warehouse | Where rows land | `WAREHOUSE_MEM_LIMIT`, `WAREHOUSE_SHARED_BUFFERS`, `WAREHOUSE_MAX_CONNECTIONS` | Usually the ceiling: give it fast disk and CPU |
| Python engine | Page size, parallelism | `EXTRACT_PAGE_SIZE`, `EXTRACT_PARALLELISM`, `EXTRACT_LOOKBACK_SECONDS` | |
| Airflow | Concurrency | `AIRFLOW_PARALLELISM`, `AIRFLOW_MAX_ACTIVE_TASKS_PER_DAG`, `FBI_POOL_SOURCE_EXTRACT` | |
| Superset | Dashboard load | worker and cache settings, `SUPERSET_WAREHOUSE_URI` | Point at a read replica for more users |
| Sink health | When the sink reports unhealthy | `SINK_HEALTH_STUCK_MS` (default 300000) | `/health` fails when batches have kept failing for longer than this |
| Offset gap | A stored offset older than the log start | `SINK_ALLOW_OFFSET_RESET` (default false) | The sink stops with an error instead of silently skipping events whose topic retention expired |
| Dead-letter size | Largest value copied into the dead-letter topic | `SINK_DLT_MAX_BYTES` | Larger events are recorded with metadata only (`fbi.dlt.value_omitted`) |
| Slot cleanup | How often a source's replication slots are inspected | `FBI_SLOT_CLEANUP_INTERVAL_MINUTES` (default 1440), `FBI_SOURCE_CONNECT_TIMEOUT_SECONDS` (default 10) | A source is inspected only when its capture generation, owner or lifecycle state changed since the last check, when it is retired, or once per interval as a safety net. Each connection has a timeout and the task an `execution_timeout` |
| Change log | Retention of `meta.entity_change_log` | `change_log_retention_days`, `cursor_stale_days`, `progress_retention_days`, `ownership_audit_retention_days` in `meta.platform_settings` | The change log is partitioned by day and whole partitions are dropped by `fbi_maintenance` |

The slowest stage sets the speed, and it is usually the warehouse. For regional topologies, Kafka on Kubernetes (Strimzi) and lag-based autoscaling (KEDA), see the reference manifests in `deploy/k8s/`. They are examples and have not been tested.

---

## 11. Health, freshness and correctness

"Healthy" means four things.

| Question | How to check |
|---|---|
| Is it running? | `docker compose --profile cdc ps`, connector **task** state, replication slot `active` |
| Is it keeping up? | Consumer lag |
| Is it fresh? | Seconds since the last write; source-commit to warehouse delay |
| Is it right? | Dead letters, completeness audit, checksum comparison |

**Running**

```powershell
docker compose --profile cdc ps --format "table {{.Service}}\t{{.State}}\t{{.Status}}"
(Invoke-RestMethod -Headers $ch "http://localhost:8083/connectors?expand=status").PSObject.Properties | ForEach-Object { "{0}: {1} tasks={2}" -f $_.Name, $_.Value.status.connector.state, (($_.Value.status.tasks | ForEach-Object { $_.state }) -join ",") }
docker exec fineract-bi-fineract-db psql -U root -d postgres -c "SELECT slot_name, active FROM pg_replication_slots"
wh -c "SELECT source_id, connector_name, observed_state, task_states FROM meta.connector_state"
```

Every CDC source needs a slot with `active = t`. An inactive slot makes the source keep log pages and eventually fill its disk.

**Keeping up (lag per topic)**

```powershell
docker compose --profile cdc exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group fbi-warehouse-sink 2>$null |
  Select-Object -Skip 2 | ForEach-Object { $f = ($_ -split '\s+'); if ($f.Count -ge 6 -and $f[5] -match '^\d+$') { [pscustomobject]@{Topic=$f[1]; Lag=[int]$f[5]} } } |
  Group-Object Topic | ForEach-Object { [pscustomobject]@{Topic=$_.Name; Partitions=$_.Count; TotalLag=($_.Group | Measure-Object Lag -Sum).Sum} } | Format-Table -AutoSize
```

Near 0 when idle is healthy. During a burst it rises and then returns to 0. If it only grows, the warehouse is the bottleneck.

**Fresh**

```powershell
wh -c "SELECT source_id, engine, SUM(rows_applied) AS rows, MAX(seconds_since_last_apply) AS secs_since_last_write FROM meta.v_table_freshness WHERE target='prod' GROUP BY 1,2 ORDER BY 4 DESC"
wh -c "SELECT source_id, count(*) AS rows, round(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM (_ingested_at - _source_ts)))::numeric, 2) AS p50_sec, round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM (_ingested_at - _source_ts)))::numeric, 2) AS p95_sec FROM raw.raw_m_loan_transaction WHERE _ingest_engine='KAFKA' AND _ingested_at > now() - interval '30 minutes' GROUP BY 1"
```

The first query shows seconds since each source last got a write (idle sources legitimately show large values). The second is the real delay from source commit to warehouse write for CDC rows. The target is p95 under 60 seconds. It is empty if there was no CDC traffic in the last 30 minutes.

**Right**

```powershell
$dlt = docker compose --profile cdc exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic fineract.cdc.dlt 2>$null
"dead-lettered events: " + (($dlt | ForEach-Object { [int]($_ -split ':')[2] }) | Measure-Object -Sum).Sum
docker compose --profile cdc exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fineract.cdc.dlt --from-beginning --timeout-ms 8000 --property print.headers=true 2>$null
wh -c "SELECT table_name, SUM(events_consumed) AS consumed, SUM(rows_applied) AS applied, SUM(rows_skipped) AS skipped, SUM(events_dead_lettered) AS dead FROM meta.ingestion_audit WHERE minute_bucket > now() - interval '60 minutes' GROUP BY 1 ORDER BY 2 DESC"
docker compose --profile cdc exec sink bash -c "exec 3<>/dev/tcp/127.0.0.1/9404; printf 'GET /metrics HTTP/1.0\r\n\r\n' >&3; cat <&3" 2>$null | Select-String "^fbi_sink_(batches_total|db_errors_total|fenced_retries_total|dead_lettered_total)"
```

- **Dead letters** should be 0. The `fbi.dlt.reason` header gives the reason: `UNKNOWN_SOURCE` (register the source first), `MALFORMED_JSON` or `TYPE_MISMATCH` (a data problem).
- **Completeness audit:** consumed = applied + skipped + dead-lettered. Nothing vanishes silently.
- Sink counters: `fenced_retries_total` rises briefly during a switch (normal). `db_errors_total` rising means the warehouse is unreachable.
- The checksum comparison (section 6.5) is the strongest proof.

The sink serves `/metrics` (plain text) and `/health` on port 9404 inside its container. No metrics server or alerting is bundled; health is read with the commands above and the SQL views.

**Daily checklist:** containers healthy, connector tasks `RUNNING`, slots active, lag near 0, seconds-since-last-write small for active sources, dead letters 0, last `dbt build` ended with `ERROR=0`, daily `reconcile-source` passed.

---

## 12. Load testing

A fake-event generator (`loadgen`) produces Debezium-style messages for many fake banks at a chosen rate, with duplicates, out-of-order versions and deletes on purpose, and remembers the final correct state. The verifier waits until the warehouse equals it exactly (row counts, deleted rows, a sum of log positions, a sum of amounts) and reports delay percentiles. A test passes only if no row was lost, duplicated or wrong. It needs `warehouse`, `kafka` and `sink` (not `connect`).

```powershell
$env:WAREHOUSE_DB_HOST="localhost"; $env:WAREHOUSE_DB_PORT="5434"; $env:WAREHOUSE_DB_USER="analytics_loader"; $env:WAREHOUSE_DB_PASSWORD="<dev password>"; $env:LOG_LEVEL="WARNING"
python -m controlplane.cli --actor loadtest register-synthetic --count 100 --prefix loadsrc --engine KAFKA
Start-Sleep 6
docker compose --profile loadtest run --rm loadgen produce --events=10000 --sources=100 --keys=5000 --rate=2000 --source-prefix=loadsrc --ground-truth=/results/stage1-truth.json
docker compose --profile loadtest run --rm loadgen verify --ground-truth=/results/stage1-truth.json --report=/results/stage1-report.json --timeout-sec=300
$rep = Get-Content loadtest\results\stage1-report.json | ConvertFrom-Json
"passed=$($rep.passed) mismatched=$($rep.mismatched_pairs) effective_eps=$($rep.effective_end_to_end_eps)"
```

`register-synthetic` creates 100 fake banks owned by Kafka with no real database or connector (the generator plays Debezium's part). `--rate=0` means as fast as possible. Goal: `passed=True mismatched=0`. The other stages (1,000,000 events at 8,000/s, a burst to 10,000/s, a backlog drain and a chaos run) are in `loadtest/README.md`.

What I measured on one laptop (12 vCPUs, 8 GB for Docker, a single Kafka broker, synthetic events of about 0.7 KB). These are headroom numbers, not production capacity:

| Test | Result |
|---|---|
| 10,000 events with duplicates, reordering and deletes | exact; p95 about 0.9 to 2.7 s |
| 1,000,000 events at 8,000/s | exact; p50 0.5 s, p99 1.5 s |
| Burst 3,500, then 10,000, then 3,500 per second | exact; no backlog |
| Kafka intake | about 144,000 to 210,000 events/s |
| Sink and warehouse drain | about 28,000 to 33,000 events/s |
| Sink killed, and warehouse down for 30 s, under load | exact; nothing lost |
| Real Debezium on 3 to 4 databases at 100 to 150 tx/s | warehouse identical to the sources |

Per day, a rate of events per second times 86,400 gives events per day: 1,200/s is about 100M, 8,000/s about 690M, 30,000/s about 2.6B.

**Clean up before running dbt.** Fake banks left in the warehouse make dbt build loan history for them. If dbt and Airflow have not run since they were added, you can delete them directly; otherwise retire each one (section 13).

```powershell
$tables = (Get-Content contracts\fineract_tables.json -Raw | ConvertFrom-Json).tables.raw_table
foreach ($t in $tables) { wh -q -c "DELETE FROM raw.$t WHERE source_id LIKE 'loadsrc-%'"; wh -q -c "DELETE FROM raw_shadow.$t WHERE source_id LIKE 'loadsrc-%'" }
wh -q -c "DELETE FROM meta.entity_change_log WHERE source_id LIKE 'loadsrc-%'"
wh -q -c "DELETE FROM meta.ingestion_progress WHERE source_id LIKE 'loadsrc-%'"
wh -c "DELETE FROM meta.source_registry WHERE source_id LIKE 'loadsrc-%'"
```

---

## 13. Removing a source

Never just delete rows by hand. Derived dashboard rows would remain and break the dbt tests. Use `retire-source`, which removes the source from every layer in one transaction, blocks writers, and records a new capture generation so a re-created source gets a fresh connector name.

```powershell
python -m controlplane.cli retire-source --source-id cdc-src-004 --dry-run
python -m controlplane.cli --actor alice retire-source --source-id cdc-src-004 --approved-by bob --forget
docker exec fineract-bi-fineract-db psql -U root -d postgres -c "DROP DATABASE fineract_src_004"
```

`--dry-run` only counts what would be purged. The real run needs `--approved-by` (a different person, while dual control is on) and refuses an unregistered id unless you pass `--allow-unregistered`. `retire-source` removes the data from `raw`, `raw_shadow` and the dbt fact and mart tables, clears the bookkeeping, deletes the source's Debezium connectors and drops its replication slots on the source. `--forget` deletes the registry row only once the slot is confirmed gone. If the source database cannot be reached, or the slot is still active, the row stays as a `RETIRED` tombstone labelled `forget_after_cleanup`, and the connector reconciler finishes the cleanup and removes the row itself. An abandoned replication slot makes the source database keep WAL until its disk fills, so do not drop the database before the slot is gone.

---

## 14. Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `docker ... error during connect ... dockerDesktopLinuxEngine` | Docker Desktop is not running. Start it and wait. |
| Connector `RUNNING` but task `FAILED`: "position no longer available on the server" | Kafka Connect kept a deleted connector's saved log position and a re-created database does not have it. Use a new source id, or add a generation floor row for the id. `retire-source` prevents this. |
| CDC warehouse count stays 0 | Check the connector **task**, that the slot is `active`, that you registered the source and waited about 8 s before `reconcile-connectors`, and the dead-letter topic. |
| Dead letters `UNKNOWN_SOURCE` | Events arrived before the sink knew the source. Register first, wait, then create the connector. There is no replay tool yet. |
| `reconcile-source` `FAILED` on tables with rows | The warehouse is missing data (see above), or the source changed in the last few seconds. |
| dbt tests fail for a removed source (`assert_no_orphan_sources_in_marts`) | Orphaned derived rows. Run `retire-source --source-id <id>`, then `dbt build`. |
| `dbt build` takes very long | Fake `loadsrc-*` banks are still in the warehouse. Clean up (section 12). |
| Sink log `Batch fenced ... expected epoch` | Normal during a cutover (the protection working). Persistent means ownership changed; check the audit table. |
| Sink log `Connection to node -1 ... could not be established` at start | The sink started before Kafka was ready. It retries. |
| Out of memory | Lower `KAFKA_HEAP`/`CONNECT_HEAP`, stop profiles you are not using, or raise Docker Desktop's memory. |
| `ext`/`cpc` say a source is `skipped` | The source is owned by Kafka. The Python engine only touches tables it owns. |
| Airflow triggered dbt unexpectedly | By design: a successful Python ingestion signals "new raw data". |
| Existing install after an upgrade | The warehouse is PostgreSQL 18 on a new volume. Run a Python backfill, then `dbt build`. Re-apply the schema if needed (it is idempotent): `docker exec fineract-bi-warehouse psql -U analytics_admin -d analytics -q -f /warehouse-schema/platform.sql`. |

---

## 15. Limitations

- Measured on a laptop with synthetic events. No 24-hour soak test and no server-class hardware run.
- Multi-broker Kafka (RF=3) is configured but not fault-tested, and source-database failover is not drilled.
- The Kubernetes manifests in `deploy/k8s/` and `compose.kafka-cluster.yaml` are references that have not been tested.
- Secret references support allowlisted `env:` names and `file:` paths below `FBI_SECRET_DIR`. A Vault-style resolver is a follow-up.
- A mart that falls behind keeps its entries in `meta.dbt_mart_dirty` until it catches up. A mart that is dropped from the project must have its row removed from `meta.dbt_mart_cursor`, or the log is never pruned. After upgrading from the single-cursor design, run one `dbt build --full-refresh`.
- The sink and the Python extractor run as the **engine** role (`WAREHOUSE_ENGINE_USER`). It can write `raw` and `raw_shadow`, offsets, watermarks, progress and the change log, and read the control tables, but cannot change ownership, the audit trail, platform settings, the registry or reconciliation records. dbt, Airflow and the control-plane CLI use the **loader** role. The Compose `extractor` service also carries loader credentials because it doubles as the host for `cpc`; give production extraction workers the engine credentials only.
- The sink's delivery paths are tested twice: with a mock Kafka consumer (rebalance, outage, holds, offset gaps) and with Testcontainers against a real Kafka broker and a real PostgreSQL (`SinkKafkaPostgresTest`: exactly-once application, restart from warehouse offsets, a second sink joining the group, a poison event, a paused table and a stale epoch). The container tests are skipped when Docker is not available, and CI fails if they were skipped.
- A source that was already loaded with `date_of_birth` keeps those values in `raw` until it is re-backfilled, because the CDC engine leaves an excluded column alone instead of overwriting it.
- No metrics server or alerting is bundled. Alert delivery to people is a follow-up.
- No replay tool for dead-lettered events yet.
- CDC supports PostgreSQL sources only. MySQL and MariaDB Fineract sources are out of scope (the Python engine works with any PostgreSQL).
