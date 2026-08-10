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

import dataclasses

import pytest

from extractor.config import AppConfig, DatabaseConfig, require_env

REQUIRED = {
    "SOURCE_DB_USER": "reader",
    "SOURCE_DB_PASSWORD": "source-secret",
    "WAREHOUSE_DB_USER": "analytics_admin",
    "WAREHOUSE_DB_PASSWORD": "warehouse-secret",
}

OPTIONAL = [
    "SOURCE_DB_HOST",
    "SOURCE_DB_PORT",
    "SOURCE_DB_NAME",
    "SOURCE_DB_SCHEMA",
    "WAREHOUSE_DB_HOST",
    "WAREHOUSE_DB_PORT",
    "WAREHOUSE_DB_NAME",
    "WAREHOUSE_DB_SCHEMA",
    "TENANT_ID",
    "REPLICA_LAG_THRESHOLD_SECONDS",
    "COB_LOOKBACK_HOURS",
    "EXTRACT_BATCH_SIZE",
    "EXTRACT_LOOKBACK_SECONDS",
]


@pytest.fixture
def clean_env(monkeypatch):
    for name in list(REQUIRED) + OPTIONAL:
        monkeypatch.delenv(name, raising=False)
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)
    return monkeypatch


def test_require_env_returns_the_value_when_set(monkeypatch):
    monkeypatch.setenv("SOME_VAR", "value")
    assert require_env("SOME_VAR") == "value"


def test_require_env_raises_when_missing(monkeypatch):
    monkeypatch.delenv("SOME_VAR", raising=False)
    with pytest.raises(ValueError, match="'SOME_VAR' is not set"):
        require_env("SOME_VAR")


def test_require_env_treats_empty_string_as_missing(monkeypatch):
    monkeypatch.setenv("SOME_VAR", "")
    with pytest.raises(ValueError, match="'SOME_VAR' is not set"):
        require_env("SOME_VAR")


@pytest.mark.parametrize("missing", sorted(REQUIRED))
def test_every_credential_is_mandatory(clean_env, missing):
    clean_env.delenv(missing)
    with pytest.raises(ValueError, match=missing):
        AppConfig.from_env()


def test_defaults_are_applied_for_optional_settings(clean_env):
    config = AppConfig.from_env()

    assert config.tenant_id == "default"
    assert config.source.schema == "bi_connector_source"
    assert config.warehouse.schema == "raw"
    assert config.replica_lag_threshold_seconds == 300
    assert config.cob_lookback_hours == 48
    assert config.extract_batch_size == 1000
    assert config.extract_lookback_seconds == 600


def test_numeric_settings_are_cast_from_strings(clean_env):
    clean_env.setenv("EXTRACT_BATCH_SIZE", "5000")
    clean_env.setenv("COB_LOOKBACK_HOURS", "12")
    clean_env.setenv("SOURCE_DB_PORT", "6543")

    config = AppConfig.from_env()

    assert config.extract_batch_size == 5000
    assert config.cob_lookback_hours == 12
    assert config.source.port == 6543
    assert isinstance(config.source.port, int)


def test_non_numeric_setting_fails_fast(clean_env):
    clean_env.setenv("EXTRACT_BATCH_SIZE", "not-a-number")
    with pytest.raises(ValueError):
        AppConfig.from_env()


def test_connect_kwargs_maps_dbname_onto_the_driver_key():
    kwargs = DatabaseConfig(
        host="h", port=5432, dbname="analytics", user="u", password="p"
    ).connect_kwargs

    assert kwargs == {
        "host": "h",
        "port": 5432,
        "database": "analytics",
        "user": "u",
        "password": "p",
    }


def test_connect_kwargs_does_not_leak_the_schema_to_the_driver():
    kwargs = DatabaseConfig(
        host="h", port=5432, dbname="d", user="u", password="p", schema="raw"
    ).connect_kwargs

    assert "schema" not in kwargs


def test_config_objects_are_immutable(app_config):
    with pytest.raises(dataclasses.FrozenInstanceError):
        app_config.tenant_id = "other"
