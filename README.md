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

# Apache Fineract Business Intelligence

The analytics pipeline for [Apache Fineract](https://fineract.apache.org/): the open-source core banking platform for financial inclusion.

This project reads data from a Fineract PostgreSQL database, transforms it through a layered dbt pipeline, and serves interactive dashboards in Apache Superset. It is designed to be **downstream and separate** from Fineract: the only connection is a read-only credential to the Fineract database. Everything else — the analytics warehouse, transformations, and dashboards — runs independently.

---

## Dashboards

| Dashboard | What it shows |
|---|---|
| **Portfolio Health** | Gross Loan Portfolio, active borrowers vs loans, PAR ratio, NPA ratio, disbursement and collection trends, portfolio composition by branch and product |
| **Delinquency & PAR** | Portfolio At Risk by DPD bucket (PAR 30/60/90/NPA), delinquency trend over time, bucket migration |

---

## Key assumption: Fineract database

This project connects to a **real Apache Fineract PostgreSQL database**. It does not manage or start the Fineract application — you run Fineract separately and point this project at its database.

For local development, clone and run Fineract locally (see [Setup](#setup) below). In production, set the `SOURCE_*` environment variables to point at your existing Fineract PostgreSQL instance.

The extractor connects via a **read-only** credential (`SOURCE_REPLICA_USER`) created by `bootstrap_source.sh`. It never writes to Fineract.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Apache Fineract PostgreSQL  (your Fineract database)    │
│  fineract_default → bi_connector_source (read-only views)│
└───────────────────────────┬─────────────────────────────┘
                            │ read-only (fineract_reader)
                            ▼
┌─────────────────────────────────────────────────────────┐
│  Extractor  (Python)                                     │
│  Incremental watermark-based CDC                        │
│  → raw.raw_m_* tables in Analytics Warehouse            │
└───────────────────────────┬─────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│  Analytics Warehouse  (PostgreSQL)                       │
│  raw → staging (views) → facts (incremental) → marts    │
│  Transformation engine: dbt                             │
└───────────────────────────┬─────────────────────────────┘
                            │ read-only (analytics_reader)
                            ▼
┌─────────────────────────────────────────────────────────┐
│  Apache Superset  :8088                                  │
│  Row-level security — branch managers see their office   │
│  Admin sees all offices                                  │
└─────────────────────────────────────────────────────────┘
```

**Pipeline loop** (runs automatically inside the extractor container):

```
backfill once on startup
  └─► loop every PIPELINE_INTERVAL_SECONDS (default: 1 hour):
        1. extractor incremental  — pull changed rows from Fineract DB
        2. dbt build              — rebuild marts (only if step 1 succeeded)
        3. superset refresh       — sync chart metadata (only if step 2 succeeded)
```

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Desktop | 24+ | Must be running |
| Docker Compose | v2 (plugin) | `docker compose version` |
| Git | any | |
| Git Bash | any | For running `.sh` scripts on Windows |

No local Python, Java, or database tools required.

---

## Setup

### Step 1 — Start the Fineract database

Clone the Fineract repository and start its PostgreSQL container:

```powershell
git clone -b develop https://github.com/apache/fineract.git
cd fineract
$env:PWD = (Get-Location).Path.Replace('\', '/')
docker compose -f docker-compose-postgresql.yml up -d db
```

Wait ~10 seconds for the container to become healthy.

### Step 2 — Pull and tag the Fineract image

Fineract publishes a pre-built image to Docker Hub — no local build needed:

```powershell
docker pull apache/fineract:latest
docker tag apache/fineract:latest fineract:latest
```

### Step 3 — Start Fineract (runs Flyway migrations)

```powershell
cd "C:\Users\<you>\Desktop\fineract"
$env:PWD = (Get-Location).Path.Replace('\', '/')
docker compose -f docker-compose-postgresql.yml up -d fineract
```

Watch the logs until Flyway finishes creating all `m_*` tables (2–5 minutes):

```powershell
cd "C:\Users\<you>\Desktop\fineract"
$env:PWD = (Get-Location).Path.Replace('\', '/')
docker compose -f docker-compose-postgresql.yml logs -f fineract
```

Wait for this line then press `Ctrl+C`:

```
Started FineractApplication in X.XXX seconds
```

### Step 4 — Verify tables exist

```powershell
cd "C:\Users\<you>\Desktop\fineract"
$env:PWD = (Get-Location).Path.Replace('\', '/')
docker compose -f docker-compose-postgresql.yml exec db psql -U root -d fineract_default -c '\dt m_*'
```

You should see 100+ tables like `m_loan`, `m_client`, `m_office`, etc.

### Step 5 — Seed demo data

Load demo data into the Fineract DB. This gives you:
- **25 clients**, **71 loans** across 3 offices and 4 products
- Staggered vintages from 1 month to 36 months ago — enough history for trend charts
- PAR-30, PAR-60, PAR-90, and NPA loans — every delinquency bucket populated

Run from PowerShell (from the `fineract-business-intelligence` directory):

```powershell
Get-Content "C:\Users\<you>\Desktop\fineract-business-intelligence\warehouse\seed\seed_fineract_source.sql" | docker exec -i fineract-db-1 psql -U root -d fineract_default
```

> Skip this step if pointing at a real Fineract instance that already has data.

### Step 6 — Clone and configure this project

```powershell
cd "C:\Users\<you>\Desktop"
git clone https://github.com/apache/fineract-business-intelligence.git
cd fineract-business-intelligence
Copy-Item .env.example .env
```

The defaults in `.env` work for local development without any edits.

### Step 7 — Bootstrap the source database

One-time step. Run from Git Bash inside the `fineract-business-intelligence` directory:

```bash
bash scripts/bootstrap_source.sh
```

This creates the `bi_connector_source` schema with compatibility views on the Fineract DB and grants read-only access to `fineract_reader`. Safe to re-run — all operations are idempotent.

Expected output:
```
[bootstrap-source] Connection OK
[bootstrap-source] Compatibility views created in schema 'bi_connector_source'
[bootstrap-source] Creating replica user if not exists...
[bootstrap-source] Read access granted to 'fineract_reader'
[bootstrap-source] === Source bootstrap complete. You can now run the pipeline. ===
```

### Step 8 — Start the BI stack

```powershell
cd "C:\Users\<you>\Desktop\fineract-business-intelligence"
docker compose up -d warehouse superset dbt extractor
```

This starts 4 services:

| Service | Role | Port |
|---|---|---|
| `warehouse` | Analytics PostgreSQL warehouse | 5434 |
| `extractor` | ETL pipeline — runs automatically on schedule | — |
| `dbt` | Transformation container | — |
| `superset` | Dashboard UI | **8088** |

The extractor waits 30 seconds for Superset to initialise, runs a full backfill, then loops every hour. Watch it:

```powershell
docker compose logs -f extractor
```

Wait for:
```
Done. PASS=81 WARN=0 ERROR=0
[pipeline] Pipeline run complete in Xs
```

### Step 9 — Open Superset

```
http://localhost:8088
```

| Role | Username | Password (default) | Sees |
|---|---|---|---|
| Admin | `admin` | `admin_dev_only` | All offices |
| North Branch Manager | `north_manager` | `north_manager_dev_only` | North Branch only |
| South Branch Manager | `south_manager` | `south_manager_dev_only` | South Branch only |

Navigate to **Dashboards** → **Portfolio Health** and **Delinquency & PAR**.

---

## Keeping Dashboards Fresh

### Automatic (default)

The extractor runs the full pipeline every hour automatically. No action needed.

### After changing data in Fineract

Force an immediate update instead of waiting for the next hour:

```powershell
docker compose restart extractor
```

### Force full pipeline run manually

```powershell
docker compose logs --tail=30 extractor   # check current status first
```

Then from Git Bash:
```bash
docker exec fineract-bi-extractor bash -c "bash /app/scripts/run_pipeline.sh backfill"
```

### Check all container logs

```powershell
docker compose logs --tail=30 warehouse superset dbt extractor
```

What to look for:

| Container | Healthy sign |
|---|---|
| `warehouse` | `database system is ready to accept connections` |
| `superset` | `Portfolio Health dashboard created` |
| `dbt` | (idle — no output expected) |
| `extractor` | `PASS=81 WARN=0 ERROR=0` + `Pipeline run complete` |

---

## After a Machine Reboot

Fineract DB and the BI stack are separate — restart both:

```powershell
# 1. Restart Fineract DB
cd "C:\Users\<you>\Desktop\fineract"
$env:PWD = (Get-Location).Path.Replace('\', '/')
docker compose -f docker-compose-postgresql.yml up -d db fineract

# 2. Restart BI stack
cd "C:\Users\<you>\Desktop\fineract-business-intelligence"
docker compose up -d warehouse superset dbt extractor
```

No need to re-run bootstrap or re-seed — data is persisted in Docker volumes.

---

## Production Deployment

### Connecting to a remote Fineract database

Set these in `.env`:

```bash
SOURCE_DB_HOST=<your-fineract-db-host>
SOURCE_DB_HOST_PORT=5432
SOURCE_DB_NAME=fineract_default
SOURCE_BOOTSTRAP_USER=<admin-user>
SOURCE_BOOTSTRAP_PASSWORD=<secret>
SOURCE_REPLICA_USER=fineract_reader
SOURCE_REPLICA_PASSWORD=<secret>
SOURCE_DB_SCHEMA=bi_connector_source
```

Run bootstrap once against the remote database (from Git Bash):

```bash
bash scripts/bootstrap_source.sh
```

Then start the BI stack — it connects to the remote Fineract DB directly via `SOURCE_DB_HOST`.

### Recommended production settings

```bash
# Generate with: python -c "import secrets; print(secrets.token_hex(32))"
SUPERSET_SECRET_KEY=<64-char-hex>

# Run pipeline daily after COB completes
PIPELINE_INTERVAL_SECONDS=86400

# All passwords via your secrets manager
WAREHOUSE_ADMIN_PASSWORD=<secret>
WAREHOUSE_LOADER_PASSWORD=<secret>
WAREHOUSE_READER_PASSWORD=<secret>
SUPERSET_ADMIN_PASSWORD=<secret>
SUPERSET_NORTH_MANAGER_PASSWORD=<secret>
SUPERSET_SOUTH_MANAGER_PASSWORD=<secret>
```

---

## Data Pipeline Details

### Layer architecture

```
fineract_default.public.*          Fineract source tables
        │
        │  bi_connector_source.*   Compatibility views (bootstrap_source.sh)
        │                          Normalises schema differences across Fineract versions
        │
        ▼
raw.raw_m_*                        Raw layer — exact copy of source rows
                                   + tenant_id + source_loaded_at
        │
        ▼
staging.stg_m_*                    Staging views — rename columns, cast types,
                                   drop PII (date_of_birth → age_band),
                                   add pseudonymous client_hash
        │
        ▼
analytics.fact_loan_snapshot       Daily grain: one row per (loan, date)
analytics.fact_delinquency_event   One row per delinquency tag lifecycle event
        │
        ▼
analytics.mart_portfolio_health    Grain: office × product × currency × date
analytics.mart_delinquency_par     Grain: office × product × bucket × date
```

### PII handling

- `date_of_birth` is dropped in `stg_m_client` and replaced with `age_band` (6 cohorts)
- `client_id` is replaced downstream by `client_hash` = MD5(tenant_id || '::' || id)
- All presentation marts are aggregated at office × product level — no individual client rows reach Superset
- Row-level security in Superset restricts branch managers to their own office data

### Watermark-based incremental extraction

The extractor tracks a per-table `last_modified_on_utc` cursor in `meta.watermarks`. Each incremental run fetches only rows changed since the last successful extraction. A 10-minute lookback window (`EXTRACT_LOOKBACK_SECONDS=600`) handles clock skew and late-arriving updates.

---

## Project Structure

```
fineract-business-intelligence/
├── compose.yaml                    Docker Compose — 4-service BI stack
├── .env.example                    Environment template (copy to .env)
│
├── scripts/
│   ├── common.sh                   Shared helpers (docker checks, env loading)
│   ├── bootstrap_source.sh         One-time: create views + grants on Fineract DB
│   └── run_pipeline.sh             Full pipeline: extractor → dbt → superset
│
├── extractor/                      Python ETL service
│   ├── cli.py                      Entry point: backfill | incremental
│   ├── extractor.py                Extraction logic (11 tables, watermark-based)
│   ├── config.py                   Config from environment variables
│   └── watermark_manager.py        Per-table cursor tracking
│
├── dbt/                            dbt transformation project (fineract_bi)
│   ├── models/
│   │   ├── staging/                stg_* views (clean + rename)
│   │   └── marts/
│   │       ├── dimensions/         dim_office, dim_client, dim_product, …
│   │       ├── facts/              fact_loan_snapshot, fact_delinquency_event
│   │       └── presentations/      mart_portfolio_health, mart_delinquency_par
│   └── macros/
│       └── safe_divide.sql         NULL-safe division macro
│
├── warehouse/
│   ├── schema/                     DDL for raw, staging, mart, meta schemas
│   └── seed/
│       └── seed_fineract_source.sql  Demo data (25 clients, 71 loans)
│
└── docker/
    ├── postgres-warehouse/         Warehouse init scripts (roles, permissions)
    ├── extractor/                  Extractor Dockerfile (includes Docker CLI)
    ├── dbt/                        dbt Dockerfile (dbt-postgres pinned <2.0)
    └── superset/
        ├── Dockerfile
        ├── init_superset.sh        First-run: DB migrate, admin user, dashboards
        ├── refresh_superset_assets.sh
        ├── bootstrap_superset_assets.py
        └── superset_config.py
```

---

## Contributing

### Running dbt manually

```bash
# Enter the dbt container
docker compose exec dbt bash

# Run all models
dbt build

# Run a specific model
dbt run --select mart_portfolio_health

# Run tests only
dbt test

# Full rebuild (drops and recreates incremental tables)
dbt build --full-refresh
```

### Adding a new dbt model

1. Add SQL in the appropriate `dbt/models/` subdirectory
2. Add schema tests in the corresponding `_*.yml` file
3. Add Apache license header to the SQL file
4. Run `dbt build --select <your_model>` to verify

### Code standards

- All source files must carry the Apache License 2.0 header
- SQL: snake_case identifiers, explicit column lists (no `SELECT *` in models)
- Python: type annotations, dataclasses for config, no hardcoded credentials
- Shell: `set -euo pipefail`, source `common.sh` for shared helpers

---

## Troubleshooting

### Extractor fails: `role "fineract_reader" does not exist`

Bootstrap has not been run, or the Fineract DB was recreated. Re-run from Git Bash:

```bash
bash scripts/bootstrap_source.sh
```

Then restart the extractor:

```powershell
docker compose restart extractor
```

### Extractor cannot reach the Fineract DB

The extractor connects to the Fineract DB container by name (`fineract-db-1`) on the shared Docker network. If your Fineract DB container has a different name, update `SOURCE_DB_HOST` in `.env`:

```bash
SOURCE_DB_HOST=<your-fineract-db-container-name-or-host>
```

### Dashboards show "No data"

The pipeline has not completed yet. Check:

```powershell
docker compose logs --tail=30 extractor
```

Look for `Pipeline run complete`. If it shows `ERROR`, check the specific error and consult the relevant section below.

Force an immediate run:

```bash
docker exec fineract-bi-extractor bash -c "bash /app/scripts/run_pipeline.sh backfill"
```

### dbt fails: `adapter is not yet supported by dbt Fusion`

dbt 2.0 dropped PostgreSQL support. The dbt image pin (`dbt-postgres<2.0.0a1`) should prevent this, but if it happens rebuild the image:

```powershell
docker compose build dbt
docker compose up -d --force-recreate dbt
```

### dbt test fails: `unique_fact_delinquency_event_delinquency_event_key`

Duplicate rows in the incremental table from a previous run. Fix with a full-refresh of that model:

```bash
docker exec fineract-bi-dbt bash -c "cd /app/dbt && dbt build --full-refresh --select fact_delinquency_event"
```

### Shell scripts fail with `\r': command not found`

Windows Git added CRLF line endings. Fix with:

```bash
sed -i 's/\r//' scripts/bootstrap_source.sh scripts/common.sh scripts/run_pipeline.sh
```

### Warehouse container exits with code 126

An init script has CRLF line endings. Fix and restart:

```bash
sed -i 's/\r//' docker/postgres-warehouse/initdb/002_create_warehouse_roles.sh
```

```powershell
docker compose down -v
docker compose up -d warehouse superset dbt extractor
```

### Full clean slate

```powershell
# Stop everything and remove volumes
docker compose down -v

# Restart from Step 8
docker compose up -d warehouse superset dbt extractor
```

> `docker compose down -v` deletes all warehouse data and Superset metadata. The Fineract DB (managed separately) is unaffected. Re-run bootstrap (Step 7) before starting the BI stack again.

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).

This project is part of the [Apache Fineract](https://fineract.apache.org/) ecosystem, started as part of Google Summer of Code 2026.
