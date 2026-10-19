#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"
# shellcheck disable=SC1091
set -a; source .env; set +a

COUNT="${1:-3}"
CONTAINER="${SOURCE_DB_CONTAINER:-fineract-bi-fineract-db}"
CDC_USER="${FINERACT_CDC_USER:-fbi_cdc}"
CDC_PASSWORD="${FBI_SRC_DEMO_PASSWORD:-fbi_cdc_dev_only}"
PUBLICATION="${FBI_DEBEZIUM_PUBLICATION_NAME:-fbi_cdc}"
TABLES="$(python -c "import json; print(', '.join('public.' + t['table_name'] for t in json.load(open('contracts/fineract_tables.json'))['tables']))")"

psql_admin() {
    docker exec -i -e PGPASSWORD="${SOURCE_BOOTSTRAP_PASSWORD}" "${CONTAINER}" \
        psql -v ON_ERROR_STOP=1 -q -U "${SOURCE_BOOTSTRAP_USER}" "$@"
}

psql_admin -d postgres -c "DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${CDC_USER}') THEN
    CREATE ROLE ${CDC_USER} LOGIN REPLICATION PASSWORD '${CDC_PASSWORD}';
  END IF;
END \$\$;"

for i in $(seq 1 "${COUNT}"); do
    db="$(printf 'fineract_src_%03d' "${i}")"
    if ! psql_admin -d postgres -At -c "SELECT 1 FROM pg_database WHERE datname = '${db}'" | grep -q 1; then
        psql_admin -d postgres -c "CREATE DATABASE ${db}"
        psql_admin -d "${db}" < warehouse/seed/schema_fineract_source.sql >/dev/null
    fi
    psql_admin -d "${db}" <<SQL
GRANT CONNECT ON DATABASE ${db} TO ${CDC_USER};
GRANT USAGE ON SCHEMA public TO ${CDC_USER};
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${CDC_USER};
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '${PUBLICATION}') THEN
    CREATE PUBLICATION ${PUBLICATION} FOR TABLE ${TABLES};
  END IF;
END \$\$;
SQL
    echo "[bootstrap-cdc] ${db} ready (publication ${PUBLICATION}, role ${CDC_USER})"
done
