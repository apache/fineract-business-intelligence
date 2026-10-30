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

echo "[superset] Running database migrations..."
superset db upgrade

echo "[superset] Creating admin user..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME}" \
  --email "${SUPERSET_ADMIN_EMAIL}" \
  --password "${SUPERSET_ADMIN_PASSWORD}" || true

echo "[superset] Initialising roles and permissions..."
superset init

echo "[superset] Bootstrapping branch manager users..."
python /workspace/docker/superset/bootstrap_superset_security.py

echo "[superset] Bootstrapping dashboard assets..."
python /workspace/docker/superset/bootstrap_superset_assets.py || {
    echo "[superset][WARN] Dashboard asset bootstrap failed — Superset will start without pre-built dashboards."
    echo "[superset][INFO] Re-run once data is available:"
    echo "                  docker compose exec superset bash /workspace/docker/superset/refresh_superset_assets.sh"
}

echo "[superset] Starting web server on :8088..."
if [ "${SUPERSET_DEV_SERVER:-false}" = "true" ]; then
    exec superset run -h 0.0.0.0 -p 8088 --with-threads
fi
exec gunicorn     --bind 0.0.0.0:8088     --workers "${SUPERSET_WEBSERVER_WORKERS:-2}"     --worker-class gthread     --threads "${SUPERSET_WEBSERVER_THREADS:-4}"     --timeout "${SUPERSET_WEBSERVER_TIMEOUT:-120}"     --limit-request-line 0     --limit-request-field_size 0     "superset.app:create_app()"
