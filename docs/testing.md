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

# Testing & Quality Guide

This guide covers every check that must pass before merging a change: unit tests, dbt model parsing, integration tests, linters, and license compliance verification.

All commands target the repository root unless stated otherwise. Run them in a standard Unix shell (bash/zsh on Linux and macOS; Git Bash on Windows).

---

## Quick Reference

| Check | Command | Needs stack? |
|---|---|---|
| Python unit tests — extractor | `pytest tests/unit/extractor -v` | No |
| Python unit tests — superset | `pytest tests/unit/superset -v` | No |
| dbt model parse | `cd dbt && dbt deps && dbt parse --profiles-dir . --target local` | No |
| dbt schema/data tests | `docker compose exec dbt dbt test` | Yes |
| Integration tests (RLS) | `pytest tests/integration -v` | Yes |
| Python linter | `ruff check .` | No |
| SQL linter | `sqlfluff lint dbt/models dbt/tests --exclude-rules structure.column_order,structure.join_condition_order,references.qualification` | No |
| Shell linter | see full command below | No |
| Dependency license audit | `python scripts/check_licenses.py` | No |
| Apache header audit | `java -jar apache-rat-0.17.jar --input-exclude-file .rat-excludes -- .` (see §6 for the download step) | No |

---

## Environment Setup

Install Python dependencies into a virtual environment once before running any test or lint command locally:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install \
  -r extractor/requirements.txt \
  -r dbt/requirements.txt \
  -r requirements-dev.txt
```

Activate the environment at the start of each session:

```bash
source .venv/bin/activate
```

> On Windows (Git Bash), `source .venv/bin/activate` works identically. On Windows PowerShell, use `.\.venv\Scripts\Activate.ps1` instead, then run the same commands.

---

## 1. Python Unit Tests

Unit tests run without a database or Docker.

```bash
pytest tests/unit/extractor -v
pytest tests/unit/superset -v
```

### What they cover

**`tests/unit/extractor/`** — watermark tracking, incremental state calculation, source query construction, upsert statement generation, replica lag checks, config parsing.

**`tests/unit/superset/`** — dashboard asset specification builders, RLS Jinja2 template rendering, chart layout construction, metric definitions.

---

## 2. dbt Model Parsing (no database required)

Validate that all SQL models compile and all `ref()` dependencies resolve:

```bash
cd dbt
dbt deps
dbt parse --profiles-dir . --target local
```

`dbt deps` downloads the `dbt-utils` package declared in `packages.yml` — an internet connection is required for this step. Once downloaded, the package is cached in `dbt/dbt_packages/`.

`dbt parse` fails if any model contains a syntax error, unknown macro, or dangling `ref()`. No database connection is made.

---

## 3. dbt Schema & Data Tests (requires running stack)

Ensure `docker compose up -d warehouse dbt` is running, then:

```bash
# Run all schema tests (unique, not_null, relationships, accepted_values)
docker compose exec dbt dbt test

# Run tests for a single model
docker compose exec dbt dbt test --select mart_portfolio_health

# Rebuild all incremental models from scratch, then test
docker compose exec dbt dbt build --full-refresh
```

---

## 4. Integration Tests — Row-Level Security

These tests verify that the Superset RLS policy in `meta.user_office_mapping` isolates data correctly between users. They require a fully running stack with seeded demo data.

### Prerequisites

1. Fineract DB seeded with `warehouse/seed/seed_fineract_source.sql`.
2. BI stack running: `docker compose up -d warehouse superset dbt extractor`.
3. At least one successful pipeline run (check `docker compose logs -f extractor`).

### Run

```bash
pytest tests/integration -v
```

### What is verified

- `north_manager` queries return rows only for `office_id = 101` (North Branch).
- `south_manager` queries return rows only for `office_id = 102` (South Branch).
- `admin` queries return rows across all offices.
- No user can bypass the `meta.user_office_mapping` lookup.

---

## 5. Code Quality — Linters

### Python (Ruff)

```bash
# Check for violations
ruff check .

# Auto-fix safe violations
ruff check --fix .
```

Configuration is in `pyproject.toml` (`line-length = 120`, rules: E, F, W, I, UP, B, SIM).

### SQL (SQLFluff)

```bash
sqlfluff lint dbt/models dbt/tests \
  --exclude-rules structure.column_order,structure.join_condition_order,references.qualification
```

Auto-fix:

```bash
sqlfluff fix dbt/models dbt/tests \
  --exclude-rules structure.column_order,structure.join_condition_order,references.qualification
```

Configuration is in `.sqlfluff` at the repository root.

### Shell (ShellCheck)

Run exactly the set of files checked in CI (with `--severity=warning`):

```bash
shellcheck --severity=warning \
  docker/fineract-postgresql/initdb/001_init_fineract_databases.sh \
  docker/postgres-warehouse/initdb/002_create_warehouse_roles.sh \
  docker/superset/init_superset.sh \
  docker/superset/refresh_superset_assets.sh \
  scripts/bootstrap_fineract_source.sh \
  scripts/bootstrap_source.sh \
  scripts/common.sh \
  scripts/run_extractor_backfill.sh \
  scripts/run_extractor_incremental.sh \
  scripts/run_pipeline.sh \
  scripts/stop_fineract_backend.sh \
  scripts/verify_extraction.sh
```

Install ShellCheck if needed:
- Linux (Debian/Ubuntu): `sudo apt-get install shellcheck`
- Linux (RHEL/Fedora): `sudo dnf install ShellCheck`
- macOS: `brew install shellcheck`
- Windows: ShellCheck is not available as a standalone native binary. Run it inside WSL (`sudo apt-get install shellcheck` in WSL), or use the [ShellCheck GitHub Action](https://github.com/marketplace/actions/shellcheck) in CI. `ci.yml` runs on `ubuntu-24.04` and `apache-rat.yml` runs on `ubuntu-latest`; both GitHub-hosted runner images ship `shellcheck` pre-installed.

---

## 6. License Compliance

### Python dependency license audit

Checks that every direct Python dependency carries a recognized license category compatible with Apache redistribution policy:

```bash
python scripts/check_licenses.py
```

The script checks installed direct dependencies and exits `0` as long as no `RESTRICTED` (GPL/Copyleft) licenses are found. It prints `[WARN] UNKNOWN` for packages whose metadata does not self-report a recognized license string (e.g., `pytest 9.1.1`). Verify those manually at [https://www.apache.org/legal/resolved.html](https://www.apache.org/legal/resolved.html).

This check runs in a **separate** `license-check.yml` workflow, not in `ci.yml`.

### Apache source header audit

Verifies that every tracked source file contains the Apache License 2.0 header. This check runs in a **separate** `apache-rat.yml` workflow, not in `ci.yml`. Download the RAT jar once (or reuse an existing copy):

```bash
curl -sL https://repo1.maven.org/maven2/org/apache/rat/apache-rat/0.17/apache-rat-0.17.jar -o apache-rat-0.17.jar
```

Then run it from the repository root:

```bash
java -jar apache-rat-0.17.jar --input-exclude-file .rat-excludes -- .
```

The `-E` flag is deprecated; use `--input-exclude-file` as shown above (matching the CI workflow).

The exclusion list is in `.rat-excludes`. Files listed there (auto-generated files, binary assets, third-party dashboards) are intentionally exempt.

---

## 7. Running the Full Pre-Commit Suite

Run all checks that do not require a running stack:

```bash
source .venv/bin/activate

ruff check .

pytest tests/unit/extractor -v
pytest tests/unit/superset -v

cd dbt && dbt deps && dbt parse --profiles-dir . --target local && cd ..

sqlfluff lint dbt/models dbt/tests \
  --exclude-rules structure.column_order,structure.join_condition_order,references.qualification

shellcheck --severity=warning \
  docker/fineract-postgresql/initdb/001_init_fineract_databases.sh \
  docker/postgres-warehouse/initdb/002_create_warehouse_roles.sh \
  docker/superset/init_superset.sh \
  docker/superset/refresh_superset_assets.sh \
  scripts/bootstrap_fineract_source.sh \
  scripts/bootstrap_source.sh \
  scripts/common.sh \
  scripts/run_extractor_backfill.sh \
  scripts/run_extractor_incremental.sh \
  scripts/run_pipeline.sh \
  scripts/stop_fineract_backend.sh \
  scripts/verify_extraction.sh

python scripts/check_licenses.py
```

> The `shellcheck` file list above must match both the "Shell (ShellCheck)" subsection in §5 and the `Lint - shellcheck` step in `.github/workflows/ci.yml`. If you add or remove a shell script, update all three.

`ruff`, `pytest`, `dbt parse`, and `sqlfluff` run in the main `ci.yml` workflow on every push. `shellcheck` also runs in `ci.yml`. `python scripts/check_licenses.py` runs in the separate `license-check.yml` workflow. Apache RAT runs in the separate `apache-rat.yml` workflow.

---

## 8. Verifying an Extraction End-to-End

After the stack is running and at least one pipeline run has completed, verify extraction correctness:

```bash
bash scripts/verify_extraction.sh
```

This script checks service health, confirms all five warehouse schemas exist, prints row counts for every raw table, and verifies at least one successful pipeline run is recorded in `meta.pipeline_state`.
