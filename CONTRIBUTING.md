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

# Contributing to Apache Fineract Business Intelligence

Thank you for your interest in contributing! All contributions : code, dbt models, dashboard improvements, documentation, and bug reports are welcome.

This project is part of the [Apache Fineract](https://fineract.apache.org/) ecosystem and is governed by the [Apache Software Foundation (ASF)](https://www.apache.org/).

---

## Code of Conduct

All participants are expected to follow the [ASF Code of Conduct](https://www.apache.org/foundation/policies/conduct.html). See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for reporting procedures.

---

## Community

| Channel | Address |
|---|---|
| Developer mailing list | [dev@fineract.apache.org](mailto:dev@fineract.apache.org) — subscribe at `dev-subscribe@fineract.apache.org` |
| Issue tracker | [GitHub Issues](https://github.com/apache/fineract-business-intelligence/issues) |
| Chat | [#apache-fineract-home on Matrix](https://app.element.io/#/room/#apache-fineract-home:matrix.org) |

For significant features or architectural changes, open an issue or start a mailing-list thread before writing code.

---

## Setting Up for Development

Follow the setup steps in [README.md](README.md) to get a running local stack. After that, see the **Environment Setup** section of [docs/testing.md](docs/testing.md) to install the Python tooling needed to run tests and linters, and the rest of that guide for the full pre-commit checklist.

---

## Contribution Workflow

1. **Fork** `apache/fineract-business-intelligence`.
2. **Create a branch** from `main`:
   ```bash
   git checkout -b feature/short-description
   ```
3. **Make your changes** following the coding standards below.
4. **Run the pre-commit suite** : all checks in [docs/testing.md](docs/testing.md) must pass.
5. **Commit** with a clear message. Reference any related GitHub issue:
   ```
   feat(extractor): add replica lag guard before incremental run (#42)
   ```
6. **Open a pull request** targeting `main`. Fill in the PR template.
7. **Respond to review** feedback promptly.

### Branch naming

| Type | Pattern | Example |
|---|---|---|
| Feature | `feature/<short-name>` or `feat/<short-name>` | `feature/add-repayment-mart` |
| Bug fix | `fix/<short-name>` | `fix/watermark-off-by-one` |
| Documentation | `docs/<short-name>` | `docs/update-architecture` |
| Chore/refactor | `chore/<short-name>` or `refactor/<short-name>` | `chore/pin-dbt-version` |
| Tests | `test/<short-name>` | `test/add-extractor-unit-tests` |
| CI/CD | `ci/<short-name>` | `ci/build-check-and-smoke-tests` |
| Infrastructure | `infra/<short-name>` | `infra/enable-github-issues` |

---

## Code Standards

### License headers

Every new source file (Python, SQL, Shell, YAML, Markdown) must carry the Apache License 2.0 header. Copy it from any existing file in the repository.

### Python

- Style enforced by `ruff` (`pyproject.toml`): line length 120, rules E, F, W, I, UP, B, SIM.
- Use explicit type annotations on public functions and methods.
- Use `dataclasses` for configuration objects; avoid plain dictionaries for structured data.
- Use the standard library `logging` module; do not use `print()` in library code.
- No hardcoded credentials or host names.

### SQL (dbt models)

- `snake_case` for model names, column names, and table aliases.
- Explicit column lists in all staging models — no `SELECT *`.
- Every model must have a description in its `_*.yml` schema file.
- Column descriptions required for all columns in presentation mart models.

### Shell scripts

- Shebang: `#!/usr/bin/env bash`
- Always start with `set -euo pipefail`.
- Source `scripts/common.sh` for shared helpers (`log`, `require_command`, `load_environment`).
- LF line endings only. If your editor or Git adds CRLF, convert before committing:
  ```bash
  sed -i 's/\r//' scripts/your_script.sh
  ```

> **Exception**: scripts under `docker/*/initdb/` run inside `docker-entrypoint-initdb.d` on Alpine-based Postgres images, where `/bin/sh` is BusyBox and does not support `pipefail`. Those scripts use `#!/bin/sh` with `set -eu` instead — do not "fix" them to bash.

---

## Adding a New dbt Model

1. Place the SQL file in the appropriate subdirectory of `dbt/models/`:
   - `staging/` — normalisation and PII removal views
   - `marts/dimensions/` — dimension tables
   - `marts/facts/` — incremental fact tables
   - `marts/presentations/` — pre-aggregated mart tables for Superset
2. Add schema tests (`unique`, `not_null`, relationship checks) in the corresponding `_*.yml` file.
3. Add the Apache License 2.0 header as a SQL comment block at the top of the file.
4. Verify the model compiles and tests pass:
   ```bash
   docker compose exec dbt dbt build --select <your_model>
   ```

---

## Questions?

Open a [GitHub Issue](https://github.com/apache/fineract-business-intelligence/issues) or post to [dev@fineract.apache.org](mailto:dev@fineract.apache.org).
