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
from dataclasses import dataclass


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Required environment variable '{name}' is not set.")
    return value


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    schema: str | None = None

    @property
    def connect_kwargs(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "database": self.dbname,
            "user": self.user,
            "password": self.password,
        }


@dataclass(frozen=True)
class AppConfig:
    source: DatabaseConfig
    warehouse: DatabaseConfig
    tenant_id: str
    replica_lag_threshold_seconds: int
    cob_lookback_hours: int
    extract_batch_size: int
    extract_lookback_seconds: int

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            source=DatabaseConfig(
                host=os.getenv("SOURCE_DB_HOST", "localhost"),
                port=int(os.getenv("SOURCE_DB_PORT", "5433")),
                dbname=os.getenv("SOURCE_DB_NAME", "fineract_default"),
                user=require_env("SOURCE_DB_USER"),
                password=require_env("SOURCE_DB_PASSWORD"),
                schema=os.getenv("SOURCE_DB_SCHEMA", "bi_connector_source"),
            ),
            warehouse=DatabaseConfig(
                host=os.getenv("WAREHOUSE_DB_HOST", "localhost"),
                port=int(os.getenv("WAREHOUSE_DB_PORT", "5434")),
                dbname=os.getenv("WAREHOUSE_DB_NAME", "analytics"),
                user=require_env("WAREHOUSE_DB_USER"),
                password=require_env("WAREHOUSE_DB_PASSWORD"),
                schema=os.getenv("WAREHOUSE_DB_SCHEMA", "raw"),
            ),
            tenant_id=os.getenv("TENANT_ID", "default"),
            replica_lag_threshold_seconds=int(os.getenv("REPLICA_LAG_THRESHOLD_SECONDS", "300")),
            cob_lookback_hours=int(os.getenv("COB_LOOKBACK_HOURS", "48")),
            extract_batch_size=int(os.getenv("EXTRACT_BATCH_SIZE", "1000")),
            extract_lookback_seconds=int(os.getenv("EXTRACT_LOOKBACK_SECONDS", "600")),
        )
