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

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/compose.yaml"
ENV_FILE="${ROOT_DIR}/.env"
RUNTIME_DIR="${ROOT_DIR}/.runtime"
DEFAULT_FINERACT_REPO_PATH="${ROOT_DIR}/../fineract"

require_command() {
  local command_name="$1"
  command -v "${command_name}" >/dev/null 2>&1 || {
    echo "${command_name} is required but not installed or not on PATH." >&2
    exit 1
  }
}

ensure_docker_prerequisites() {
  require_command docker
  docker compose version >/dev/null 2>&1 || {
    echo "docker compose v2 is required." >&2
    exit 1
  }
}

load_environment() {
  if [[ ! -f "${ENV_FILE}" ]]; then
    echo "Missing ${ENV_FILE}. Copy .env.example to .env and set credentials first." >&2
    exit 1
  fi

  set -a
  source "${ENV_FILE}"
  set +a

  FINERACT_REPO_PATH="${FINERACT_REPO_PATH:-${DEFAULT_FINERACT_REPO_PATH}}"
  case "${FINERACT_REPO_PATH}" in
    [A-Za-z]:*|/*)
      ;;
    *)
      FINERACT_REPO_PATH="${ROOT_DIR}/${FINERACT_REPO_PATH}"
      ;;
  esac
  SOURCE_DB_HOST_PORT="${SOURCE_DB_HOST_PORT:-5433}"
  FINERACT_HEALTH_URL="${FINERACT_HEALTH_URL:-https://localhost:8443/fineract-provider/actuator/health}"
  FINERACT_BASE_URL="${FINERACT_BASE_URL:-https://localhost:8443}"
  FINERACT_TEST_USERNAME="${FINERACT_TEST_USERNAME:-mifos}"
  FINERACT_TEST_PASSWORD="${FINERACT_TEST_PASSWORD:-password}"
  FINERACT_TENANT_ID="${FINERACT_TENANT_ID:-${TENANT_ID:-default}}"
  FINERACT_CUCUMBER_INITIALIZATION_ENABLED="${FINERACT_CUCUMBER_INITIALIZATION_ENABLED:-true}"
  mkdir -p "${RUNTIME_DIR}"
}

ensure_fineract_repo() {
  if [[ ! -d "${FINERACT_REPO_PATH}" ]]; then
    echo "Fineract repository not found at ${FINERACT_REPO_PATH}." >&2
    exit 1
  fi
  if [[ ! -f "${FINERACT_REPO_PATH}/gradlew" ]]; then
    echo "Expected gradlew at ${FINERACT_REPO_PATH}/gradlew." >&2
    exit 1
  fi
}

wait_for_compose_service() {
  local service="$1"
  local max_attempts="$2"
  shift 2

  local attempt=1
  until docker compose -f "${COMPOSE_FILE}" exec -T "${service}" "$@" >/dev/null 2>&1; do
    if [[ "${attempt}" -ge "${max_attempts}" ]]; then
      echo "Timed out waiting for service '${service}'." >&2
      exit 1
    fi
    attempt=$((attempt + 1))
    sleep 2
  done
}

wait_for_https_health() {
  local url="$1"
  local max_attempts="$2"
  local attempt=1

  until check_https_health "${url}"; do
    if [[ "${attempt}" -ge "${max_attempts}" ]]; then
      echo "Timed out waiting for health endpoint ${url}." >&2
      exit 1
    fi
    attempt=$((attempt + 1))
    sleep 5
  done
}

check_https_health() {
  local url="$1"

  if command -v curl.exe >/dev/null 2>&1; then
    curl.exe -k -s -f "${url}" >/dev/null 2>&1
    return $?
  fi

  curl --insecure --silent --fail "${url}" >/dev/null 2>&1
}

ensure_java_available() {
  local candidates=(
    "/c/Program Files/Java/jdk-21"
    "/mnt/c/Program Files/Java/jdk-21"
    "/c/Program Files/Eclipse Adoptium/jdk-21"
    "/mnt/c/Program Files/Eclipse Adoptium/jdk-21"
  )
  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -x "${candidate}/bin/java" || -x "${candidate}/bin/java.exe" ]]; then
      export JAVA_HOME="${candidate}"
      export PATH="${JAVA_HOME}/bin:${PATH}"
      return
    fi
  done

  if [[ -n "${JAVA_HOME:-}" && ( -x "${JAVA_HOME}/bin/java" || -x "${JAVA_HOME}/bin/java.exe" ) ]]; then
    export PATH="${JAVA_HOME}/bin:${PATH}"
    return
  fi

  if command -v java >/dev/null 2>&1; then
    return
  fi

  echo "java is required but not installed or on PATH." >&2
  exit 1
}

to_windows_path() {
  local unix_path="$1"
  if command -v wslpath >/dev/null 2>&1; then
    wslpath -w "${unix_path}"
    return
  fi
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -w "${unix_path}"
    return
  fi
  printf '%s' "${unix_path}"
}

escape_for_powershell() {
  printf "%s" "$1" | sed "s/'/''/g"
}
