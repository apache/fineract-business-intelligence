#!/bin/bash
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

set -e
export PGPASSWORD="${POSTGRES_PASSWORD}"

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<-EOSQL
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${FINERACT_APP_USER}') THEN
        EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', '${FINERACT_APP_USER}', '${FINERACT_APP_PASSWORD}');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${SOURCE_REPLICA_USER}') THEN
        EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', '${SOURCE_REPLICA_USER}', '${SOURCE_REPLICA_PASSWORD}');
    END IF;
END
\$\$;

CREATE DATABASE ${FINERACT_TENANTS_DB_NAME};
CREATE DATABASE ${FINERACT_DEFAULT_DB_NAME};

GRANT ALL PRIVILEGES ON DATABASE ${FINERACT_TENANTS_DB_NAME} TO ${FINERACT_APP_USER};
GRANT ALL PRIVILEGES ON DATABASE ${FINERACT_DEFAULT_DB_NAME} TO ${FINERACT_APP_USER};
GRANT CONNECT ON DATABASE ${FINERACT_DEFAULT_DB_NAME} TO ${SOURCE_REPLICA_USER};
ALTER ROLE ${SOURCE_REPLICA_USER} SET default_transaction_read_only = on;

\c ${FINERACT_TENANTS_DB_NAME}
GRANT ALL ON SCHEMA public TO ${FINERACT_APP_USER};

\c ${FINERACT_DEFAULT_DB_NAME}
GRANT ALL ON SCHEMA public TO ${FINERACT_APP_USER};
GRANT USAGE ON SCHEMA public TO ${SOURCE_REPLICA_USER};
ALTER DEFAULT PRIVILEGES FOR ROLE ${FINERACT_APP_USER} IN SCHEMA public
    GRANT SELECT ON TABLES TO ${SOURCE_REPLICA_USER};
ALTER DEFAULT PRIVILEGES FOR ROLE ${FINERACT_APP_USER} IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO ${SOURCE_REPLICA_USER};
EOSQL
