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

# Security Policy

> [!IMPORTANT]
> This project is currently **not release-ready**.

This is a project of the Apache Software Foundation and follows the ASF vulnerability
handling process.

---

# Fineract Business Intelligence : Threat Model

This document is for human security researchers, AI-assisted security researchers, and
project maintainers handling vulnerability reports. Read it alongside the
[Apache Fineract Threat Model](https://github.com/apache/fineract/blob/develop/SECURITY.md)
which governs the source banking application whose data this pipeline consumes.

---

## §1 Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report vulnerabilities privately using the ASF vulnerability reporting process:

- **ASF Security team:** security@apache.org
- **Project mailing list (private):** private@apache.org (Apache committers only)
- **GitHub private reporting:** Use the
  [Security Advisories](https://github.com/apache/fineract-business-intelligence/security/advisories)
  tab → "Report a vulnerability"

Include in your report:

- A description of the vulnerability and affected component
- Steps to reproduce or a proof-of-concept
- The potential impact and attack scenario
- Any suggested remediation

You will receive an acknowledgement within 72 hours. The ASF Security team coordinates
disclosure timing with maintainers. Public disclosure happens only after a fix is available.

---

## §2 Scope and intended use

Source code repository: <https://github.com/apache/fineract-business-intelligence>

This document covers the **fineract-business-intelligence data pipeline and BI layer**:

| Component | Description |
|---|---|
| **Extractor** (`extractor/`) | Python service that reads from the Fineract PostgreSQL source DB via a read-only replica role and writes raw data to the analytics warehouse |
| **dbt models** (`dbt/`) | SQL transformation layer that builds marts and dimensions in the analytics warehouse |
| **Apache Superset** (`docker/superset/`) | Dashboard and visualisation layer connected to the analytics warehouse via a read-only reader role |
| **Docker Compose stack** (`compose.yaml`) | Local and CI orchestration of all services |
| **GitHub Actions workflows** (`.github/workflows/`) | CI pipeline including build, compilation, smoke tests, license, and security checks |
| **Seed / schema scripts** (`warehouse/seed/`) | SQL scripts that initialise the Fineract source schema for CI and development; not run in production against live data |

### Deployment contexts

| Context | Description |
|---|---|
| **Docker Compose (local dev)** | `docker compose up` on a developer machine. All credentials from `.env` (copied from `.env.example`). Not suitable for production. |
| **Docker Compose (CI)** | Same stack, driven by GitHub Actions. Credentials are hardcoded development values; no production data is present. |
| **Production (operator-managed)** | Operator deploys services independently (e.g., Kubernetes, managed PostgreSQL). Credentials must be rotated; TLS must be configured. The `compose.yaml` is a reference, not a production config. |

### Primary intended use cases

- **Analytics for microfinance operations:** Branch managers and analysts view loan
  portfolio health, delinquency trends, PAR (Portfolio at Risk) metrics, and batch job
  status through Superset dashboards.
- **Read-only extraction:** The pipeline never writes to the Fineract source database.
  All mutations are on the analytics warehouse only.
- **Incremental extraction:** The extractor uses watermarks to extract only records
  modified since the last run, bounded by a COB (Close of Business) completion gate.

### Caller roles

| Role | Trust level | Description |
|---|---|---|
| Operator / deployer | Full host / container access | Configures credentials, TLS, network isolation, secret management |
| Superset analyst (read-only) | Valid Superset session, no DB access | Branch manager, MFI analyst — sees dashboards only |
| Superset admin | Full Superset admin rights | Can add database connections, modify dashboards, access all data |
| CI runner | GitHub-hosted `ubuntu-24.04` | Ephemeral; no production data; dev-only credentials |

---

## §3 Out of scope

### Components not modelled here

- **Apache Fineract backend** (Spring Boot, JDBC, Kafka, COB batch, core banking logic)
  — covered by the [Fineract security team](https://github.com/apache/fineract/blob/develop/SECURITY.md).
- **Fineract source database internals** — the extractor connects read-only; what happens
  inside the source DB is out of scope.
- **Superset upstream vulnerabilities** — bugs in the Apache Superset project should be
  reported to the [Superset security team](https://superset.apache.org/docs/security/).
- **Customer-facing portals** — Superset is back-office only; bank customers are not users.

### Threats the project does not attempt to defend against

- **Fineract source DB compromise:** A compromised upstream DB will cause corrupted data
  to be extracted and propagated. Defence is Fineract's responsibility.
- **Network-level DDoS against services:** Volumetric DDoS mitigation is the operator's
  responsibility (CDN, cloud WAF, rate limiting at the load balancer).
- **Physical access to the host machine:** OS-level access is assumed to be already
  compromised.
- **Supply-chain attack on Python/dbt/Superset packages:** Dependency scanning
  (Dependabot, `pip audit`) is a downstream responsibility; this model assumes packages
  are not themselves compromised.

---

## §4 Trust boundaries and data flow

### Trust boundaries

| Boundary | Description |
|---|---|
| **Fineract source database** | PostgreSQL instance owned by the Fineract application. Extractor connects as `fineract_reader` — a `SELECT`-only replica role. No write access is granted at any point. |
| **Analytics warehouse** | Separate PostgreSQL instance. Extractor writes to `raw` schema via `analytics_loader` role. dbt runs transformations via the same loader role. Superset reads via `analytics_reader` (`SELECT` on `analytics` schema only). |
| **Superset metadata database** | Stores dashboard definitions, user sessions, and dataset connection strings. Admin credentials must be rotated before any non-local deployment. |
| **Docker internal network** | Services communicate over a Docker bridge network (`fineract-business-intelligence_default`). Not TLS-encrypted by default. |
| **External Docker network** (`fineract_default`) | Declared external in `compose.yaml`; created by the operator before stack startup. Bridges the extractor to the Fineract source DB in the local dev setup. |
| **CI runner** | GitHub-hosted `ubuntu-24.04` ephemeral runner. Contains development-only credentials; no production data. Isolated per run. |
| **Docker socket** | The extractor service mounts `/var/run/docker.sock` in `compose.yaml`. This grants the extractor container the ability to manage Docker on the host. See §9 for implications. |

### Data flow

```
[Apache Fineract DB — fineract_reader role, SELECT only]
        |
        | PostgreSQL / pg8000 (TLS optional)
        v
[Extractor — Python service]
  - COB gate: verifies batch_job_execution before extracting
  - Watermark manager: incremental extraction by last_modified_on_utc
  - Replica lag check: aborts if replica is too far behind
        |
        | PostgreSQL / pg8000
        v
[Analytics Warehouse — raw schema]
        |
        | dbt SQL transformations
        v
[Analytics Warehouse — analytics schema (marts, dims, presentations)]
        |
        | PostgreSQL / psycopg2 (read-only reader role)
        v
[Apache Superset — dashboard layer]
        |
        | HTTPS (browser, TLS in production)
        v
[End user — branch manager, MFI analyst]
```

### Trust transitions

1. **Extractor → Fineract source DB:** Read-only replica user (`fineract_reader`). The
   `bi_connector_source` schema provides views that add synthetic `created_on_utc` /
   `last_modified_on_utc` columns for tables that lack them natively (`m_office`,
   `m_currency`, `m_product_loan`). The extractor queries these views, not the underlying
   tables directly.
2. **Extractor → Analytics warehouse:** `analytics_loader` role. Writes are scoped to
   the `raw` schema. The loader role has no access to `analytics` schema where the
   marts live.
3. **dbt → Analytics warehouse:** Runs as `analytics_loader`. Materialises models in
   `analytics` schema. Reads from `raw`; writes to `analytics`.
4. **Superset → Analytics warehouse:** `analytics_reader` role. `SELECT` on `analytics`
   schema only. Cannot read `raw`, `meta`, or other schemas.
5. **CI runner → fineract-db container:** Uses development-only bootstrap credentials
   (`root` / `skdcnwauicn2ucnaecasdsajdnizucawencascdca`). These are not production
   credentials and appear in plain text in `.env.example` and `ci.yml` deliberately
   — they are seeded into an ephemeral container and discarded after each run.

---

## §5 Assumptions about the environment

### Host / operating system

- Docker Engine and Docker Compose are installed and up to date.
- The host is not shared with untrusted workloads (no multi-tenant container platform
  without network isolation).
- The Docker socket is not exposed to the public network.

### Network

- In local dev: all services are on a private Docker bridge network, not reachable from
  outside the host by default.
- In production: the warehouse and Superset must be placed behind a firewall; neither
  should be reachable from the public internet without TLS termination and access control.
- All browser-to-Superset communication must be over TLS in production. The default
  `compose.yaml` exposes Superset on HTTP port 8088 only.

### Concurrency and state

- The watermark manager (`meta.extract_watermarks`) serialises extraction windows per tenant and
  source table. Concurrent extractor runs for the same tenant would cause undefined
  watermark state. The pipeline is designed for one run at a time per tenant.

### Clock

- The extractor uses `datetime.now(timezone.utc)` for pipeline state timestamps.
  Clock skew between the extractor host and the Fineract DB host can affect the
  `extract_lookback_seconds` window. A skew larger than `REPLICA_LAG_THRESHOLD_SECONDS`
  (default: 300 s) will cause the replica lag check to abort the run.

---

## §6 Assumptions about inputs

### Input sources

| Source | Trust | Notes |
|---|---|---|
| Fineract source DB rows (via `bi_connector_source` views) | Partially trusted — data integrity is Fineract's responsibility | Extractor uses parameterised queries (`%s` placeholders via pg8000); no raw string interpolation of source data into SQL |
| `.env` file / container environment variables | Operator-trusted | Whoever controls the host controls these values. Must not be committed to source control. |
| `dbt/profiles.yml` env var defaults | CI-trusted for `PGUSER`/`PGPASSWORD` defaults (`ci`); operator must override in production | The fallback defaults exist only to allow `dbt parse` in CI without a live DB connection |
| Superset dashboard configs (`bootstrap_superset_assets.py`) | Operator-trusted | Loaded at Superset initialisation; a tampered bootstrap script could register malicious database connections |
| CI workflow inputs (branch names, PR authors) | Untrusted | Workflow uses `contents: read` only; no `${{ github.event.* }}` variables are interpolated into `run:` steps |
| Seed SQL (`warehouse/seed/schema_fineract_source.sql`) | Trusted (static, version-controlled) | Runs only in CI / local dev against a throw-away container; not applied to production |

### Size and rate

- **Extraction batch size:** Controlled by `extract_batch_size` in `AppConfig`; batches are
  committed incrementally. Very large tables will produce many small commits — this is
  by design.
- **Pipeline interval:** Default `PIPELINE_INTERVAL_SECONDS=3600`. No rate limiting
  beyond this interval is enforced by the pipeline itself.
- **Superset query limits:** Not configured by this project. Operators must configure
  Superset's `ROW_LIMIT` and async query timeouts to prevent runaway queries from
  loading the warehouse.

---

## §7 Adversary model

### Who is in scope

| Adversary | Capability | What they are trying to do |
|---|---|---|
| **Passive network observer** | Can observe unencrypted traffic on the Docker network or between services | Capture database credentials or query results in transit if TLS is absent |
| **Unauthenticated external user** | Can reach Superset HTTP port if exposed | Access dashboards without credentials; exploit Superset login for credential stuffing |
| **Authenticated Superset analyst** | Valid Superset session, read-only role | Exfiltrate financial data beyond their permitted dashboards; pivot to warehouse via SQL Lab if enabled |
| **Authenticated Superset admin** | Full Superset admin rights | Register additional database connections (escalation); export all datasets; access connection strings stored in metadata DB |
| **Malicious CI PR author** | Can open a PR to the repository | Inject `run:` steps that exfiltrate CI secrets; exploit `pull_request_target` if misconfigured; introduce backdoored dependencies |
| **Compromised extractor process** | Code execution inside the extractor container | Read `SOURCE_REPLICA_PASSWORD` and `WAREHOUSE_LOADER_PASSWORD` from environment; connect to warehouse as loader and corrupt raw data; use Docker socket (if mounted) to escalate to host |

### Who is explicitly out of scope

- **Fineract DB admin:** Full access to the source DB is assumed to be already
  authorised; this model cannot defend against it.
- **Host OS / hypervisor attacker:** Physical or OS-level access is assumed compromised.
- **Browser zero-day exploiter:** Superset UI browser attacks via unpatched engine
  vulnerabilities are not modelled.
- **Supply-chain attacker (PyPI / npm):** Compromise of a dependency package is out
  of scope; addressed by dependency scanning tooling.

---

## §8 Security properties the project provides

- **Read-only source access** — the extractor connects to the Fineract DB as a
  `SELECT`-only replica user (`fineract_reader`). No INSERT, UPDATE, or DELETE is
  possible on the source database from this pipeline.
- **Role separation in the warehouse** — three distinct roles: `analytics_admin` (DDL),
  `analytics_loader` (write to `raw`), `analytics_reader` (SELECT on `analytics` only).
  Superset uses `analytics_reader` and cannot reach raw or meta schemas.
- **Parameterised queries throughout the extractor** — all source queries use `%s`
  placeholders via pg8000; column and table names are quoted with `_quote_identifier()`
  which double-quotes and escapes the identifier. No raw string interpolation of
  source data into SQL.
- **COB gate** — the extractor refuses to run if no `COMPLETED` batch job execution is
  found within the configured lookback window, preventing extraction of inconsistent
  mid-batch state.
- **Replica lag check** — aborts extraction if the source replica is too far behind the
  primary, preventing stale data from entering the warehouse silently.
- **Watermark-based incremental extraction** — extraction windows are bounded by
  `last_modified_on_utc` cursors stored in `meta.extract_watermarks`, preventing unbounded
  full-table scans after initial backfill.
- **Pinned GitHub Actions** — all workflow action references are pinned to full commit
  SHAs to prevent supply-chain attacks via mutable version tags.
- **`persist-credentials: false`** on all checkout steps to limit credential exposure
  on CI runners.
- **Least-privilege CI permissions** — workflow-level `permissions: contents: read`.
- **Concurrency controls** — CI workflow uses `cancel-in-progress: true` to prevent
  duplicate runs accumulating on the same branch.
- **Zizmor audit** — all Actions workflow files are audited by Zizmor on every change
  for unpinned refs, script injection, and excessive permissions.

---

## §9 Security properties the project does not provide (by default)

- **TLS between services** — Docker Compose uses an internal bridge network without
  TLS. Container-to-container credentials and query results are transmitted in plaintext.
  Production deployments must add TLS or mTLS between all services.
- **Docker socket isolation** — the extractor service mounts `/var/run/docker.sock`
  in `compose.yaml`. This gives the extractor container full Docker control over the
  host (equivalent to root). In production, remove this mount unless the pipeline
  management scripts genuinely require it; if required, use Docker's authorisation
  plugin or rootless Docker instead.
- **Superset MFA** — multi-factor authentication for Superset is not configured. Enforce
  at the Superset layer in production (`AUTH_TYPE`, LDAP, or SAML integration).
- **Superset SQL Lab restriction** — SQL Lab is not disabled by default. An analyst with
  SQL Lab access can run arbitrary SELECT queries against the warehouse reader connection.
  Restrict SQL Lab access to trusted roles in production Superset configuration.
- **Warehouse encryption at rest** — not configured in the Docker Compose setup.
  Production PostgreSQL must use filesystem-level or Transparent Data Encryption.
- **Audit log for pipeline runs** — no tamper-resistant audit trail of extractor runs
  or dbt executions beyond container logs and the `meta.pipeline_state` table (which is
  mutable by the loader role).
- **Network egress restrictions** — the extractor has no outbound firewall rules. In
  production, restrict egress to the warehouse host and source DB host only.
- **Secret manager integration** — credentials are passed via environment variables.
  Production deployments should use a secret manager (HashiCorp Vault, AWS Secrets
  Manager, Kubernetes Secrets) rather than plain `.env` files.
- **CI credential rotation** — the development PostgreSQL password
  (`skdcnwauicn2ucnaecasdsajdnizucawencascdca`) appears in plain text in `.env.example`
  and `ci.yml`. This is intentional: it is a development-only seed credential for an
  ephemeral CI container containing no real data. It must not be used in any non-CI
  deployment. See §10 for the production credential requirement.

### Known attack classes this project cannot defend against

- **Warehouse loader role abuse:** If `WAREHOUSE_LOADER_PASSWORD` is leaked, an attacker
  can corrupt or delete raw extraction data. The `analytics` schema is protected (reader
  role only), but raw data integrity is lost.
- **Superset metadata DB access via admin credentials:** If `WAREHOUSE_ADMIN_PASSWORD`
  is leaked, an attacker can read all Superset dashboard configs, database connection
  strings, and user session data stored in the metadata DB.
- **Docker socket escalation:** A compromised extractor process with access to
  `/var/run/docker.sock` can spawn privileged containers and escape to the host.
  This is a known Docker architecture limitation; see §9 mitigation above.
- **Fineract source DB poisoning:** If the upstream Fineract DB is compromised before
  extraction, malicious data will propagate through the pipeline to dashboards. The
  pipeline has no integrity-verification layer on source data.

---

## §10 Downstream responsibilities (production operators)

1. **Replace all `.env.example` credentials** before any non-local deployment. Every
   password in `.env.example` is a placeholder — none may be used in production.
2. **Remove the Docker socket mount** from the extractor service in `compose.yaml`
   unless the pipeline management scripts genuinely require Docker access. If required,
   use the minimum necessary Docker API scope.
3. **Terminate TLS** in front of Superset. The default `compose.yaml` exposes Superset
   on HTTP port 8088 only.
4. **Set `SUPERSET_SECRET_KEY`** to a cryptographically random value. The `.env.example`
   placeholder (`change_me_before_production_use_a_random_32_byte_hex`) must be replaced.
   A compromised secret key allows session forgery for all Superset users.

   ```bash
   python -c "import secrets; print(secrets.token_hex(32))"
   ```

5. **Restrict network access** — neither the warehouse nor the Fineract source DB should
   be reachable from the public internet. Place them on private subnets.
6. **Disable or restrict Superset SQL Lab** for analyst roles. SQL Lab gives direct
   query access to the warehouse reader connection.
7. **Enable Superset MFA** for admin accounts.
8. **Use a secret manager** for all credentials. Do not use plain `.env` files in
   production.
9. **Rotate credentials** on a regular schedule and after any personnel change.
10. **Monitor pipeline state** via the `meta.pipeline_state` table and container logs, or
    integrate with a log aggregation platform for failure alerting.
11. **Run `pip audit`** against the extractor's dependencies before production deployment
    to detect known CVEs in Python packages.

---

## §11 Known misuse patterns

1. **Using `.env.example` credentials in production.** The bootstrap password
   (`skdcnwauicn2ucnaecasdsajdnizucawencascdca`) is public, appears in the repository,
   and is known to anyone who has read this file. Any deployment using it is exploitable
   by any reader of this repository.

2. **Exposing Superset on HTTP without TLS.** Port 8088 in `compose.yaml` is HTTP only.
   All session cookies, credentials, and dashboard data are transmitted in plaintext to
   any network observer.

3. **Leaving the Docker socket mounted in production.** The `compose.yaml` mount of
   `/var/run/docker.sock` into the extractor container is a development convenience for
   running `docker compose` commands from within the pipeline. In a production deployment,
   this grants the extractor (and any code it executes) full Docker host control.

4. **Deploying `compose.yaml` directly to production without changes.** The file is
   optimised for local development: no TLS, hardcoded dev credential defaults, volume
   paths suited to local developer machines, and the Docker socket mount. It is not a
   production-ready deployment manifest.

5. **Enabling SQL Lab for all Superset users.** Analysts who should only see pre-built
   dashboards gain the ability to run arbitrary queries against the warehouse if SQL Lab
   is not restricted by role.

6. **Running multiple extractor instances for the same tenant simultaneously.** The
   watermark manager uses per-table, per-tenant rows in `meta.extract_watermarks` but does not
   hold a distributed lock. Concurrent runs will produce undefined watermark state and
   may load duplicate or missing rows.

---

## §12 Conditions that would change this model

The following changes should trigger a revision of this threat model:

1. **Write-back to Fineract source DB** — if any component is granted INSERT/UPDATE
   privileges on the Fineract source DB, the read-only source trust boundary is broken
   and the entire model must be reconsidered.
2. **Addition of a public-facing API layer** — e.g., a REST API exposing warehouse data
   directly. This introduces a new trust boundary and attack surface not covered here.
3. **Multi-tenant SaaS deployment** — if a single warehouse instance serves multiple
   organisations, tenant isolation in the `raw` and `analytics` schemas must be formally
   modelled (currently assumed single-tenant per warehouse instance).
4. **New extraction targets** — adding extraction of PII beyond what is currently
   extracted (client names, dates of birth, account numbers) requires a PII inventory
   review and potentially a GDPR/data-protection impact assessment.
5. **Streaming / CDC extraction** — replacing batch watermark extraction with Change
   Data Capture (Debezium, logical replication) introduces replication slot management,
   new failure modes, and potential for replay attacks.
6. **Superset embedded analytics** — if Superset is embedded in an external web
   application via the Embedded SDK, the browser trust boundary changes and CSRF/iframe
   protections must be modelled.
7. **Removal of the COB gate** — the `_ensure_cob_completed` check is a data-integrity
   gate, not just a scheduling convenience. Removing it without an equivalent guarantee
   creates a window where partially-committed Fineract batches are extracted.
8. **New CVE affecting pg8000, psycopg2, dbt-core, or Apache Superset** that cannot be
   cleanly routed to one of the §13 dispositions — this indicates a `MODEL-GAP` and
   requires model revision.

---

## §13 Triage dispositions

Use these dispositions when evaluating a security report before escalating to the ASF
Security team.

| Disposition | Meaning | Licensed by |
|---|---|---|
| `VALID` | Violates a property the project claims, via an in-scope adversary and in-scope input | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property is violated, but the finding makes a §11 misuse easier or exposes a defence-in-depth gap. Reported privately; fixed at maintainer discretion; typically no CVE. | §11 |
| `OUT-OF-MODEL: upstream-layer` | Finding targets the Fineract backend, Superset upstream, or PostgreSQL engine — not this pipeline. Route to the relevant upstream project's security team. | §3 |
| `OUT-OF-MODEL: trusted-input` | Requires attacker control of a parameter the model marks as operator-trusted (`.env`, container env vars, `compose.yaml`, Superset admin account). | §6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires an attacker capability the model excludes (physical host access, Docker socket access not granted in production, supply-chain compromise of pip packages). | §7 |
| `OUT-OF-MODEL: unsupported-deployment` | Only manifests under a deployment the project does not support (e.g., using `compose.yaml` in production without changes, using dev credentials in production). | §4, §11 |
| `BY-DESIGN: property-disclaimed` | Concerns a property the project explicitly does not provide (TLS between services, Docker socket isolation, SQL Lab restriction, audit log, storage encryption at rest). | §9 |
| `KNOWN-NON-FINDING` | Matches a known misuse pattern documented in §11 or a CI-only credential known to be public. | §11 |
| `MODEL-GAP` | Cannot be cleanly routed to any of the above. Triggers §12 revision and requires model update before triage can conclude. | §12 |

---

## §14 CI-enforced security controls

The following automated security gates run on every pull request and push via GitHub Actions.
All workflows pin action references to full commit SHAs and use `persist-credentials: false`.

### GitHub Actions CI — build, compilation, smoke tests

**Workflow:** [`.github/workflows/ci.yml`](.github/workflows/ci.yml)  
**Trigger:** push on any branch; pull_request on `main` and `develop`

Two-stage pipeline:

- **Stage 1 — compilation checks:** Verifies `dbt parse` succeeds (all model references
  resolve, env var defaults are present), that the Extractor Python package installs
  cleanly, and that the Apache Superset bootstrap assets script is syntactically valid.
- **Stage 2 — smoke tests:** Brings up the full Docker Compose stack, seeds the Fineract
  schema, creates `bi_connector_source` views, runs the extractor in backfill mode, runs
  `dbt run`, and asserts 12 data-integrity invariants against the warehouse.

Security-relevant properties of the CI workflow:

- `permissions: contents: read` only — no write, deploy, or secret access.
- `cancel-in-progress: true` — prevents credential or resource accumulation from
  orphaned runs.
- External Docker network (`fineract_default`) created before stack startup; not
  inherited from a previous run.
- Stale volumes cleaned up with `docker compose down -v` before each run to prevent
  data leakage between runs.
- No `pull_request_target` trigger — untrusted PR code cannot access repository secrets.

### GitHub Actions Security Analysis — Zizmor

**Workflow:** [`.github/workflows/zizmor.yml`](.github/workflows/zizmor.yml)  
**Trigger:** push/PR on any branch when `.github/workflows/**` files change

[Zizmor](https://github.com/zizmorcore/zizmor) audits all GitHub Actions workflow YAML
files for security misconfigurations, including:

- Unpinned action references (all actions must be pinned to a full commit SHA)
- Script injection via `github.event.*` context variables in `run:` steps
- Excessive workflow permissions
- Dangerous `pull_request_target` patterns

All action references in this repository are pinned to SHA hashes. Tag-based references
(`@v3`, `@v4`) are not permitted and will fail the Zizmor check.

Results are uploaded to GitHub's Security tab as SARIF for review.

### CodeQL Static Analysis

**Workflow:** [`.github/workflows/codeql.yml`](.github/workflows/codeql.yml)  
**Trigger:** push/PR to `main`; weekly scheduled scan  
**Languages:** `python`, `actions`

GitHub CodeQL performs deep static analysis on the Python extractor code and on the
GitHub Actions workflow files. It catches:

- SQL injection patterns in Python DB code
- Command injection in shell `run:` steps
- Insecure use of `subprocess`, `eval`, `exec`
- Workflow-level injection via untrusted inputs

Results appear in GitHub's Security tab.

### Apache RAT — License Header Enforcement

**Workflow:** [`.github/workflows/apache-rat.yml`](.github/workflows/apache-rat.yml)

Scans every file for an Apache License 2.0 header. Fails if any project-owned file is
missing a header. Prevents unlicensed third-party code from entering the repository.

### ASF Allowlist Check

**Workflow:** [`.github/workflows/asf-allowlist-check.yml`](.github/workflows/asf-allowlist-check.yml)

Verifies that all dependencies are on the ASF-approved licence allowlist. Blocks
GPL, LGPL, AGPL, and other ASF-incompatible licences.

### License Check

**Workflow:** [`.github/workflows/license-check.yml`](.github/workflows/license-check.yml)

Per-file Apache header enforcement across source files using a custom script.

---

## §15 Credential and secret hygiene

| Secret | Where configured | CI value | Production requirement |
|---|---|---|---|
| `SOURCE_BOOTSTRAP_PASSWORD` | `.env` / CI `.env.example` | Public placeholder | Rotate; never commit |
| `SOURCE_REPLICA_PASSWORD` | `.env` / CI `.env.example` | Public placeholder | Rotate; use a dedicated read-only role |
| `WAREHOUSE_ADMIN_PASSWORD` | `.env` / CI `.env.example` | Public placeholder | Rotate; restrict to DDL operations only |
| `WAREHOUSE_LOADER_PASSWORD` | `.env` / CI `.env.example` | Public placeholder | Rotate |
| `WAREHOUSE_READER_PASSWORD` | `.env` / CI `.env.example` | Public placeholder | Rotate; `SELECT`-only grants enforced |
| `SUPERSET_SECRET_KEY` | `.env` / CI `.env.example` | Public placeholder | **Must** be a random 32-byte hex value |
| `SUPERSET_ADMIN_PASSWORD` | `.env` / CI `.env.example` | Public placeholder | Rotate; enforce strong password policy |
| `PGPASSWORD` (CI fineract-db) | `ci.yml` plain text | `skdcnwauicn2ucnaecasdsajdnizucawencascdca` | Not applicable — CI-only, ephemeral container, no real data |

The `PGPASSWORD` in `ci.yml` is intentionally plain text. It is the PostgreSQL bootstrap
password for an ephemeral container seeded with synthetic test data. It is not a secret;
it is the same value as `SOURCE_BOOTSTRAP_PASSWORD` in `.env.example` which is already
public in the repository. Any reader of this repository already has this value.

Generate a secure `SUPERSET_SECRET_KEY` for production:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

---

## §16 Contributor responsibilities

All contributors — human or AI-assisted — must satisfy the following before a PR can merge.

### Every PR

| Responsibility | How to satisfy it |
|---|---|
| **Apache License 2.0 header on every new file** | Copy the header block from any existing source file of the same type. Apache RAT will fail without it. |
| **No hard-coded credentials or secrets** | Use environment variables with `.env` / container env. The only exception is the CI development seed password documented in §15. |
| **No new `pull_request_target` triggers** | This trigger grants secrets access to untrusted PR code. Zizmor will flag it; do not add it. |
| **All new Action references pinned to SHA** | Tag-based refs (`@v4`) are not permitted. Zizmor enforces this. |
| **Parameterised queries for any new DB access** | Do not use f-strings or string concatenation to build SQL with user-supplied or source-data values. Use `%s` placeholders. |
| **ASF-compatible dependency licences** | Before adding a Python package, verify its licence is in the ASF Category A list (Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC, PSF). |
| **No Docker socket mount in new services** | Do not add `/var/run/docker.sock` mounts to new services. If genuinely required, document the justification in the PR and in §9. |

### Security-sensitive areas — extra review required

PRs that touch the files below require explicit maintainer sign-off on the security
implications before merge:

| File / area | Why |
|---|---|
| `extractor/extractor.py` | Source query construction, watermark logic, upsert SQL — SQL injection surface |
| `extractor/config.py` | Credential loading from environment; any change could leak secrets or weaken role separation |
| `scripts/bootstrap_source.sh` | Creates `bi_connector_source` views and grants; a bug here could grant excessive access to the source DB |
| `docker/fineract-postgresql/initdb/` | Database initialisation scripts; sets up roles and grants on the source DB |
| `docker/postgres-warehouse/initdb/` | Warehouse role setup; defines the loader/reader permission boundaries |
| `compose.yaml` | Service network config, credential env vars, Docker socket mount, volume mounts |
| `dbt/profiles.yml` | Database connection credentials; env var defaults must not weaken production security |
| `.github/workflows/` | Any workflow change is also audited by Zizmor and CodeQL (`actions` language) |

### Threat model maintenance

- **When you add a feature that changes the attack surface** (see §12 for the trigger
  list), update this document in the same PR.
- **When CI flags a finding you believe is a false positive**, document the rationale in
  the PR description. Do not suppress linter rules or audit findings silently.
- **When a new CVE in pg8000, dbt-core, or Apache Superset is published**, open a
  tracking issue within 48 hours and reference the relevant §13 disposition.

---

## §17 Licence compliance

This is an Apache Software Foundation project. Every file and every dependency must be
compatible with the [ASF Licensing Policy](https://www.apache.org/legal/resolved.html).

### Source file headers

Every project-owned source file must carry the full Apache License 2.0 header. Apache RAT
and the license-check workflow enforce this on every PR. Files legitimately excluded from
RAT (generated assets, lockfiles, binary images) must be listed in `.rat-excludes` with a
documented justification.

To add a header to a new file, copy the block from any existing file of the same type.

### Python / dbt dependency licence allowlist

Before adding any Python package to `extractor/requirements.txt` or any dbt package to
`dbt/packages.yml`, verify its licence is in the ASF Category A list:

| SPDX identifier | ASF category |
|---|---|
| `Apache-2.0` | Category A — free to use |
| `MIT` | Category A — free to use |
| `BSD-2-Clause` | Category A — free to use |
| `BSD-3-Clause` | Category A — free to use |
| `ISC` | Category A — free to use |
| `PSF-2.0` | Category A — free to use |

The following are **blocked**:

| Blocked family | Examples | Reason |
|---|---|---|
| Copyleft (strong) | GPL-2.0, GPL-3.0, AGPL-3.0 | Incompatible with ALv2 |
| Copyleft (weak) | LGPL-2.0, LGPL-2.1, LGPL-3.0 | Incompatible when statically linked |
| Non-commercial | CC-BY-NC-* | Restricts commercial use |
| Proprietary / EULA | various | Cannot be distributed under ALv2 |

> [!CAUTION]
> Do not add a package with a blocked licence to any allowlist override. Find an
> ASF-compatible alternative, or raise a formal ASF legal exception request on the
> mailing list.

### ASF branch protection (`asf.yaml`)

[`.asf.yaml`](.asf.yaml) enforces the following on the `main` branch:

| Rule | Setting |
|---|---|
| Signed commits required | `required_signatures: true` |
| Force push prohibited | `restrict_force_push: true` |
| Branch deletion prohibited | `restrict_deletion: true` |
| Minimum approving reviews | 1 |
| Conversations must be resolved | `required_conversation_resolution: true` |

---

## §18 Open questions for maintainers

Do not add open questions here. Raise sensitive questions via the ASF security reporting
process (§1). Raise non-sensitive questions on the mailing list, in GitHub Discussions,
or in a PR comment.

## §19 Kafka CDC, control plane and feature flag: attack surface

This section covers the optional `cdc` and `airflow` profiles. The Python-only deployment does not start any of it.

### What an attacker could reach, and how it is limited

| Surface | Risk | Default mitigation | What the operator must still do |
|---|---|---|---|
| Kafka Connect REST (`:8083`) | Anyone who can reach it and holds the REST credentials can create connectors, delete the managed `fbi-src-*` connectors or point a connector at a host they control | HTTP Basic authentication on every REST call (`BasicAuthSecurityRestExtension`, credentials `FBI_CONNECT_USER` / `FBI_CONNECT_PASSWORD`, required by compose); published on `127.0.0.1` only; config providers are restricted to an allowlist (`config.providers.env.param.allowlist.pattern`, `config.providers.file.param.allowed.paths`); `connector.client.config.override.policy=None`; secrets are referenced as `${env:NAME}`, never stored | Rotate the REST credentials outside a laptop and never publish the port beyond the host. In Kubernetes keep the NetworkPolicy in `deploy/k8s/connect-and-sink.yaml` (Strimzi does not add REST authentication, so the policy is the control there) or put the REST API behind a mesh |
| Kafka (`:29092`) | PLAINTEXT and unauthenticated in the compose file | Published on `127.0.0.1` only | Use TLS and SASL (`SINK_KAFKA_*`, `SINK_PRODUCER_*`) outside a laptop |
| Airflow UI and API (`:8080`) | Can trigger ownership changes, onboarding and Python ingestion | Published on `127.0.0.1`; the Fernet key, JWT secret and admin password must be set (`${VAR:?}`, no defaults) | Replace the sample values, use a real auth manager, and set `FBI_REQUIRE_AUTHENTICATED_ACTOR=true` so the actor is the logged-in user, not a DAG parameter |
| `secret_ref` of a registered source | A registrant could point `secret_ref` at any worker secret and `db_host` at their own server | Only `env:` names matching `FBI_SECRET_ENV_ALLOWLIST` (default only `FBI_SRC_*`; the demo uses `FBI_SRC_DEMO_PASSWORD`) and `file:` paths under `FBI_SECRET_DIR` are accepted; registry, connector builder and resolver share one validator; source connections default to `FBI_DEFAULT_SSLMODE=verify-full` | Restrict who can register sources; never put a worker's own credentials in the allowlist |
| Raw date of birth | Personal data flows through Kafka, the dead-letter topic and `raw` | `FBI_EXCLUDE_PII=true` (default) keeps contract columns flagged `pii` out of the Debezium column list and the Python engine's reads; `client_hash` is an HMAC keyed by `FBI_PII_HASH_KEY`, kept outside the warehouse | Treat `FBI_PII_HASH_KEY` like a credential and rotate it with a full refresh |

### Control-plane integrity

- Ownership changes are guarded by a database trigger: an engine change needs a legal transition, and a cutover needs a PASSED, recent `shadow_vs_prod` reconciliation of the same table.
- `meta.ingestion_ownership_audit` is append-only (trigger plus revoked `UPDATE`/`DELETE` for the loader role) and records `session_user`.
- With `dual_control_mode = 'database'` a cutover also needs an approval recorded by a different database login through `meta.approve_cutover()`. In the default `parameter` mode approval is a CLI or DAG parameter and is only as trustworthy as the caller; use the database mode wherever the requester and approver are different people.
- The dashboard reader cannot read the `meta` tables. Row-level security goes through `meta.is_admin_user()` and `meta.user_scope()`.
- The Kafka sink and the Python extractor connect as the **engine** role (`WAREHOUSE_ENGINE_USER`). It writes `raw`, `raw_shadow`, offsets, watermarks, progress and the change log, and reads the control tables, but has no write access to `meta.ingestion_ownership`, the audit trail, `meta.platform_settings`, the source registry or the reconciliation and approval records. The loader role (dbt, Airflow, control-plane CLI) can change ownership rows (it must, to run the control plane) but not platform settings or the audit trail. A compromised engine can therefore corrupt rows of the sources it writes, but cannot give itself ownership, approve its own cutover or erase the audit trail.
