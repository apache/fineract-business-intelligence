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


from __future__ import annotations

import os
import socket
from dataclasses import dataclass

from controlplane.db import PgConfig, warehouse_config_from_env


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Required environment variable '{name}' is not set.")
    return value


def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class ExtractSettings:

    page_size: int = 5000
    lookback_seconds: int = 600
    replica_lag_threshold_seconds: int = 300
    cob_gate_enabled: bool = True
    cob_lookback_hours: int = 48
    reconcile_bucket_size: int = 100_000
    parallelism: int = 4
    default_engine: str = "PYTHON"
    writer_id: str = "python"

    @classmethod
    def from_env(cls) -> ExtractSettings:
        engine = os.getenv("INGESTION_ENGINE_DEFAULT", "PYTHON").strip().upper()
        if engine not in ("PYTHON", "KAFKA", "NONE"):
            raise ValueError(f"INGESTION_ENGINE_DEFAULT must be PYTHON, KAFKA or NONE, got {engine}")
        return cls(
            page_size=int(os.getenv("EXTRACT_PAGE_SIZE", os.getenv("EXTRACT_BATCH_SIZE", "5000"))),
            lookback_seconds=int(os.getenv("EXTRACT_LOOKBACK_SECONDS", "600")),
            replica_lag_threshold_seconds=int(os.getenv("REPLICA_LAG_THRESHOLD_SECONDS", "300")),
            cob_gate_enabled=_bool("COB_GATE_ENABLED", True),
            cob_lookback_hours=int(os.getenv("COB_LOOKBACK_HOURS", "48")),
            reconcile_bucket_size=int(os.getenv("RECONCILE_BUCKET_SIZE", "100000")),
            parallelism=int(os.getenv("EXTRACT_PARALLELISM", "4")),
            default_engine=engine,
            writer_id=os.getenv("EXTRACT_WRITER_ID", f"python-{socket.gethostname()}"),
        )


@dataclass(frozen=True)
class EnvSourceBootstrap:

    source_id: str
    tenant_id: str
    host: str
    port: int
    dbname: str
    user: str
    compat_schema: str
    ssl_mode: str
    cob_job_name: str
    secret_ref: str = "env:SOURCE_DB_PASSWORD"

    @classmethod
    def from_env(cls) -> EnvSourceBootstrap | None:
        if not os.getenv("SOURCE_DB_USER"):
            return None
        return cls(
            source_id=os.getenv("SOURCE_ID", "default"),
            tenant_id=os.getenv("TENANT_ID", "default"),
            host=os.getenv("SOURCE_DB_HOST", "localhost"),
            port=int(os.getenv("SOURCE_DB_PORT", "5433")),
            dbname=os.getenv("SOURCE_DB_NAME", "fineract_default"),
            user=os.getenv("SOURCE_DB_USER", ""),
            compat_schema=os.getenv("SOURCE_DB_SCHEMA", "bi_connector_source"),
            ssl_mode=os.getenv("SOURCE_DB_SSL_MODE", "disable"),
            cob_job_name=os.getenv("COB_JOB_NAME", "LOAN_COB"),
            secret_ref=os.getenv("SOURCE_DB_SECRET_REF", "env:SOURCE_DB_PASSWORD"),
        )


def warehouse_config() -> PgConfig:
    return warehouse_config_from_env(application_name="fbi-python-engine")
