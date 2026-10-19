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

import pytest

from controlplane.connectors import (
    ActionKind,
    CdcSettings,
    DesiredConnector,
    ObservedConnector,
    build_connector_config,
    plan_actions,
)
from controlplane.reconciler import stale_slots
from controlplane.registry import RegistryError, SourceRecord, connector_name, source_id_from_connector
from controlplane.secrets import SecretError, connect_placeholder, resolve


def settings(**kw) -> CdcSettings:
    return CdcSettings(connect_clusters={"default": "http://connect:8083"}, **kw)


def test_connector_config_is_secret_free_minimal_and_routed(source_record, contract, monkeypatch):
    monkeypatch.delenv("FBI_DEFAULT_SSLMODE", raising=False)
    cfg = build_connector_config(source_record, contract, settings())
    assert cfg["database.password"] == "${env:FBI_SRC_TEST_PASSWORD}"
    assert cfg["database.sslmode"] == "verify-full"
    assert not any(k.startswith("producer.override.") for k in cfg), "per-connector client overrides are disabled"
    assert "secret" not in str(cfg).lower().replace("secret_ref", "")
    assert cfg["topic.prefix"] == "branch-001"
    assert cfg["slot.name"] == "fbi_branch_001_g3"
    assert cfg["decimal.handling.mode"] == "string"
    assert cfg["transforms.route.replacement"] == "fineract.cdc.$1"
    assert cfg["transforms"] == "route,heartbeat"
    assert cfg["transforms.heartbeat.replacement"] == "fineract.cdc.heartbeats"
    assert "public.m_loan_transaction" in cfg["table.include.list"].split(",")
    cols = cfg["column.include.list"]
    assert r"public\.m_client\.date_of_birth" not in cols, "pii columns stay out of the capture by default"
    assert r"public\.m_client\.office_id" in cols
    assert r"public\.m_office\.created_on_utc" not in cols
    assert r"public\.batch_job_execution\.create_time" in cols
    assert cfg["signal.enabled.channels"] == "kafka"


def test_pii_columns_are_captured_only_when_explicitly_allowed(source_record, contract):
    cfg = build_connector_config(source_record, contract, settings(exclude_pii=False))
    assert r"public\.m_client\.date_of_birth" in cfg["column.include.list"]


def test_sslmode_comes_from_label_then_environment_default(source_record, contract, monkeypatch):
    labelled = SourceRecord(**{**source_record.__dict__, "labels": {"sslmode": "require"}})
    assert build_connector_config(labelled, contract, settings())["database.sslmode"] == "require"
    monkeypatch.setenv("FBI_DEFAULT_SSLMODE", "disable")
    assert build_connector_config(source_record, contract, settings())["database.sslmode"] == "disable"


@pytest.mark.parametrize("label", ["heartbeat_table", "signal_table"])
@pytest.mark.parametrize("bad", ["x; DROP TABLE y", "a b", "x'--", "1abc", "a.b.c"])
def test_label_identifiers_are_validated_before_reaching_source_sql(source_record, contract, label, bad):
    rec = SourceRecord(**{**source_record.__dict__, "labels": {label: bad}})
    with pytest.raises(ValueError, match=label):
        build_connector_config(rec, contract, settings())


def test_connector_config_requires_connection_details(contract):
    with pytest.raises(ValueError, match="db_host"):
        build_connector_config(SourceRecord("s1", "t1", secret_ref="env:X"), contract, settings())


def test_overrides_win_and_signal_table_enables_source_channel(source_record, contract):
    rec = SourceRecord(**{**source_record.__dict__, "labels": {"signal_table": "public.dbz_signal"}})
    cfg = build_connector_config(rec, contract, settings(overrides={"max.batch.size": "8192"}))
    assert cfg["max.batch.size"] == "8192"
    assert cfg["signal.enabled.channels"] == "source,kafka"
    assert cfg["signal.data.collection"] == "public.dbz_signal"


def _desired(i, paused=False, config=None):
    return DesiredConnector(f"s{i}", connector_name(f"s{i}", 0), "default", config or {"a": "1"}, paused)


def _observed(name, state="RUNNING", tasks=("RUNNING",), config=None):
    return ObservedConnector(name, config or {"a": "1"}, state, tasks)


def test_plan_creates_updates_restarts_and_deletes():
    desired = [_desired(1), _desired(2, config={"a": "2"}), _desired(3), _desired(4, paused=True)]
    observed = {"default": {
        connector_name("s2", 0): _observed(connector_name("s2", 0)),
        connector_name("s3", 0): _observed(connector_name("s3", 0), tasks=("FAILED",)),
        connector_name("s4", 0): _observed(connector_name("s4", 0)),
        connector_name("gone", 0): _observed(connector_name("gone", 0)),
        "someone-elses-connector": _observed("someone-elses-connector"),
    }}
    actions, deferred = plan_actions(desired, observed, settings())
    kinds = {(a.kind, a.name) for a in actions}
    assert (ActionKind.CREATE, connector_name("s1", 0)) in kinds
    assert (ActionKind.UPDATE, connector_name("s2", 0)) in kinds
    assert (ActionKind.RESTART_FAILED, connector_name("s3", 0)) in kinds
    assert (ActionKind.PAUSE, connector_name("s4", 0)) in kinds
    assert (ActionKind.DELETE, connector_name("gone", 0)) in kinds
    assert not any(a.name == "someone-elses-connector" for a in actions), "unmanaged connectors are untouched"
    assert deferred == []


def test_settings_that_are_no_longer_desired_count_as_drift():
    name = connector_name("s1", 0)
    observed = {"default": {name: _observed(name, config={"a": "1", "producer.override.acks": "all", "name": name})}}
    actions, _ = plan_actions([_desired(1)], observed, settings())
    assert [(a.kind, a.name) for a in actions] == [(ActionKind.UPDATE, name)]
    clean = {"default": {name: _observed(name, config={"a": "1", "name": name})}}
    assert plan_actions([_desired(1)], clean, settings())[0] == []


def test_blast_radius_limits_defer_work():
    desired = [_desired(i) for i in range(10)]
    observed = {"default": {connector_name(f"old{i}", 0): _observed(connector_name(f"old{i}", 0)) for i in range(5)}}
    actions, deferred = plan_actions(desired, observed, settings(max_creates_per_run=3, max_deletes_per_run=2))
    assert sum(a.kind is ActionKind.CREATE for a in actions) == 3
    assert sum(a.kind is ActionKind.DELETE for a in actions) == 2
    assert len(deferred) == 7 + 3


def test_connectors_of_sources_with_unbuildable_config_are_never_deleted():
    unknown = connector_name("broken", 3)
    gone = connector_name("gone", 0)
    observed = {"default": {unknown: _observed(unknown), gone: _observed(gone)}}
    actions, _ = plan_actions([], observed, settings(), protected=frozenset({unknown}))
    assert [(a.kind, a.name) for a in actions] == [(ActionKind.DELETE, connector_name("gone", 0))]


def test_orphan_deletion_can_be_disabled():
    observed = {"default": {connector_name("gone", 0): _observed(connector_name("gone", 0))}}
    actions, _ = plan_actions([], observed, settings(delete_orphans=False))
    assert actions == []


@pytest.mark.parametrize("name,expected", [
    ("fbi-src-branch-goa-g12", "branch-goa"),
    ("fbi-src-s1-g0", "s1"),
    ("fbi-src-s1", None),
    ("other-connector", None),
])
def test_connector_name_round_trip(name, expected):
    assert source_id_from_connector(name) == expected


def test_stale_slot_detection_keeps_current_and_active_slots():
    slots = [("fbi_s1_g0", False), ("fbi_s1_g1", False), ("fbi_s1_g2", True), ("fbi_s1_g3", False),
             ("fbi_s10_g0", False), ("other_slot", False)]
    assert stale_slots(slots, "s1", keep="fbi_s1_g3") == ["fbi_s1_g0", "fbi_s1_g1"]


@pytest.mark.parametrize("bad", ["Branch-1", "-x", "a" * 64, "has space", "dot.ted"])
def test_source_ids_must_be_topic_and_slot_safe(bad):
    with pytest.raises(RegistryError):
        SourceRecord(bad, "t").validate()


def test_secret_refs_never_hold_credentials():
    with pytest.raises(RegistryError, match="rejected"):
        SourceRecord("s1", "t", secret_ref="hunter2").validate()


def test_registry_and_resolver_accept_the_same_schemes():
    with pytest.raises(RegistryError, match="rejected"):
        SourceRecord("s1", "t", secret_ref="vault:secret/data/s1#password").validate()
    with pytest.raises(SecretError):
        resolve("vault:secret/data/s1#password")
    with pytest.raises(SecretError):
        connect_placeholder("vault:secret/data/s1#password")
    SourceRecord("s1", "t", secret_ref="env:FBI_SRC_S1_PASSWORD").validate()


def test_secret_resolution(monkeypatch, tmp_path):
    monkeypatch.setenv("FBI_SRC_TEST_PW", "s3cret")
    assert resolve("env:FBI_SRC_TEST_PW") == "s3cret"
    f = tmp_path / "creds.properties"
    f.write_text("# comment\npassword = abc=def\n", encoding="utf-8")
    monkeypatch.setenv("FBI_SECRET_DIR", str(tmp_path))
    if f.as_posix().startswith("/"):
        assert resolve(f"file:{f.as_posix()}:password") == "abc=def"
    assert connect_placeholder("env:FBI_SRC_TEST_PW") == "${env:FBI_SRC_TEST_PW}"
    with pytest.raises(SecretError):
        resolve("env:FBI_SRC_DOES_NOT_EXIST_123")
    with pytest.raises(SecretError):
        resolve("plaintext")


@pytest.mark.parametrize(
    "name", ["WAREHOUSE_DB_PASSWORD", "AIRFLOW__API_AUTH__JWT_SECRET", "PATH", "AWS_SECRET_ACCESS_KEY"]
)
def test_secret_refs_cannot_name_arbitrary_worker_secrets(monkeypatch, name):
    monkeypatch.setenv(name, "value")
    with pytest.raises(SecretError, match="not allowed"):
        resolve(f"env:{name}")
    with pytest.raises(SecretError, match="not allowed"):
        connect_placeholder(f"env:{name}")
    with pytest.raises(RegistryError, match="rejected"):
        SourceRecord("s1", "t", secret_ref=f"env:{name}").validate()


def test_secret_file_references_are_confined_to_the_secret_directory(monkeypatch, tmp_path):
    inside = tmp_path / "secrets"
    inside.mkdir()
    monkeypatch.setenv("FBI_SECRET_DIR", str(inside))
    outside = tmp_path / "other.properties"
    outside.write_text("password = x\n", encoding="utf-8")
    ref = f"file:{outside.as_posix()}:password"
    if outside.as_posix().startswith("/"):
        with pytest.raises(SecretError, match="outside"):
            resolve(ref)
        monkeypatch.delenv("FBI_SECRET_DIR")
        with pytest.raises(SecretError, match="FBI_SECRET_DIR"):
            resolve(ref)


def test_env_allowlist_is_configurable(monkeypatch):
    monkeypatch.setenv("FBI_SECRET_ENV_ALLOWLIST", "CUSTOM_[A-Z]+")
    monkeypatch.setenv("CUSTOM_PW", "p")
    assert resolve("env:CUSTOM_PW") == "p"
    with pytest.raises(SecretError):
        resolve("env:FBI_SRC_TEST_PW")


@pytest.mark.parametrize("state,path", [
    ("RETIRED", []),
    ("REQUESTED", ["RETIRED"]),
    ("VALIDATING", ["RETIRED"]),
    ("RETIRING", ["RETIRED"]),
    ("PROVISIONING", ["RETIRING", "RETIRED"]),
    ("ACTIVE", ["RETIRING", "RETIRED"]),
    ("PAUSED", ["RETIRING", "RETIRED"]),
    ("DEGRADED", ["RETIRING", "RETIRED"]),
])
def test_retirement_walks_only_allowed_lifecycle_transitions(state, path):
    from controlplane.registry import LIFECYCLE_TRANSITIONS
    from controlplane.retire import lifecycle_path

    assert lifecycle_path(state) == path
    current = state
    for nxt in path:
        assert nxt in LIFECYCLE_TRANSITIONS[current], f"{current} -> {nxt} not allowed"
        current = nxt


def test_every_derived_table_is_a_dbt_model_with_source_id():
    import re
    from pathlib import Path

    from controlplane.retire import DERIVED_TABLES

    root = Path(__file__).resolve().parents[3] / "dbt" / "models"
    for name in DERIVED_TABLES:
        sql = next(root.rglob(f"{name}.sql")).read_text(encoding="utf-8")
        assert re.search(r"\bsource_id\b", sql), f"{name} has no source_id column to purge by"
