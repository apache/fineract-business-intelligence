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

set -uo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

FINERACT_PID_FILE="${RUNTIME_DIR}/fineract-devrun.pid"

echo "=== Stopping Fineract Backend ==="

if [[ -f "${FINERACT_PID_FILE}" ]]; then
  existing_pid="$(cat "${FINERACT_PID_FILE}")"
  echo "Found PID file: ${FINERACT_PID_FILE} (PID: ${existing_pid})"

  if kill -0 "${existing_pid}" >/dev/null 2>&1; then
    echo "Killing Fineract process ${existing_pid}..."
    kill -TERM "${existing_pid}" >/dev/null 2>&1 || true
    sleep 2
    if kill -0 "${existing_pid}" >/dev/null 2>&1; then
      echo "Process still running, forcing kill..."
      kill -KILL "${existing_pid}" >/dev/null 2>&1 || true
    fi
    echo "✅ Fineract backend stopped"
  else
    echo "Process ${existing_pid} not running"
  fi

  rm -f "${FINERACT_PID_FILE}"
else
  echo "No PID file found at ${FINERACT_PID_FILE}"
  echo "Trying to find and kill gradle/java processes..."

  if command -v powershell.exe >/dev/null 2>&1; then
    powershell.exe -NoProfile -Command "
      Get-Process -Name java -ErrorAction SilentlyContinue | Where-Object {
        \$_.CommandLine -like '*fineract-provider*'
      } | ForEach-Object {
        Write-Host \"Killing java PID: \$(\$_.Id)\";
        Stop-Process -Id \$_.Id -Force
      }
    "
  fi
fi

echo "=== Fineract Backend Stopped ==="
