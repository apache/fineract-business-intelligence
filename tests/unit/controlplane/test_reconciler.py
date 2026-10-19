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

from controlplane import reconciler
from controlplane.connectors import CdcSettings, ConnectClient, ObservedConnector
from controlplane.reconciler import (
    DesiredState,
    observe_cluster,
    reconcile,
    retire_artifacts,
    slot_cleanup_decision,
)
from controlplane.registry import SourceRecord
from tests.conftest import FakeConnection, FakeCursor


@pytest.fixture(autouse=True)
def source_password(monkeypatch):
    monkeypatch.setenv("FBI_SRC_TEST_PASSWORD", "pw")
    monkeypatch.setenv("FBI_DEFAULT_SSLMODE", "disable")


def settings(**kw) -> CdcSettings:
    return CdcSettings(connect_clusters={"default": "http://connect:8083"}, **kw)


def source(source_id="bank-a", generation=0, state="ACTIVE", secret_ref="env:FBI_SRC_TEST_PASSWORD", **kw):
    return SourceRecord(
        source_id,
        "t",
        db_host="db.example",
        db_name="fineract",
        db_user="fbi_cdc",
        secret_ref=secret_ref,
        lifecycle_state=state,
        capture_generation=generation,
        **kw,
    )


class FakeConnect:
    def __init__(self, connectors: dict[str, ObservedConnector]):
        self.connectors = dict(connectors)
        self.deleted: list[str] = []
        self.executed: list = []
        self.described: list[str] = []

    def list_names(self):
        return sorted(self.connectors)

    def describe(self, name):
        self.described.append(name)
        return self.connectors[name]

    def execute(self, action):
        self.executed.append((action.kind.value, action.name))
        if action.kind.value == "delete":
            self.deleted.append(action.name)
            self.connectors.pop(action.name, None)

    def delete(self, name):
        self.deleted.append(name)
        self.connectors.pop(name, None)


class FakeSourceCursor:
    def __init__(self, slots, dropped):
        self._slots, self._dropped, self._last, self._params = slots, dropped, "", None

    def execute(self, sql, params=None):
        self._last, self._params = sql, params
        if "pg_drop_replication_slot" in sql:
            self._dropped.append(params[0])
            self._slots[:] = [s for s in self._slots if s[0] != params[0]]

    def fetchall(self):
        if "FROM pg_replication_slots" in self._last and "LIKE" in self._last:
            prefix = self._params[0].rstrip("%")
            return [(name,) for name, _ in self._slots if name.startswith(prefix)]
        return [(name, active) for name, active in self._slots]


class FakeSource:
    def __init__(self, slots):
        self.slots = list(slots)
        self.dropped: list[str] = []
        self.closed = False

    def cursor(self):
        return FakeSourceCursor(self.slots, self.dropped)

    def commit(self):
        pass

    def close(self):
        self.closed = True


def observed(name, state="RUNNING"):
    return ObservedConnector(name, {}, state, ("RUNNING",))


@pytest.fixture
def fake_registry(monkeypatch):
    holder = {"sources": [], "kafka": set()}

    class Registry:
        def __init__(self, conn):
            pass

        def list(self, **kwargs):
            return holder["sources"]

    monkeypatch.setattr(reconciler, "SourceRegistry", Registry)
    monkeypatch.setattr(reconciler, "kafka_owned_sources", lambda conn: holder["kafka"])
    return holder


def test_a_config_error_never_deletes_the_connector_or_drops_its_slot(fake_registry, contract):
    bad = source("bank-a", generation=3, secret_ref="vault:secret/data/a#password")
    fake_registry["sources"] = [bad]
    fake_registry["kafka"] = {"bank-a"}
    connect = FakeConnect({"fbi-src-bank-a-g3": observed("fbi-src-bank-a-g3")})
    src = FakeSource([("fbi_bank_a_g3", False), ("fbi_bank_a_g2", False)])
    conn = FakeConnection(FakeCursor(fetchall_results=[[("bank-a",)]]))

    report = reconcile(
        conn, contract, settings(), clients={"default": connect}, connect_source=lambda cfg: src
    )

    assert report.errors and report.errors[0]["source_id"] == "bank-a"
    assert connect.deleted == [] and connect.executed == [], "the running connector keeps its capture position"
    assert src.dropped == [], "no slot of a source in an unknown state may be dropped"


def test_a_source_that_left_kafka_ownership_loses_only_inactive_slots(fake_registry, contract, monkeypatch):
    monkeypatch.setenv("FBI_SECRET_ENV_ALLOWLIST", "FBI_SRC_[A-Z_]+")
    monkeypatch.setenv("FBI_SRC_TEST_PASSWORD", "pw")
    fake_registry["sources"] = [source("bank-a", generation=2)]
    fake_registry["kafka"] = set()
    monkeypatch.setattr(reconciler, "_slot_check_due", lambda conn, sid, key, minutes: True)
    connect = FakeConnect({})
    src = FakeSource([("fbi_bank_a_g0", False), ("fbi_bank_a_g1", True), ("fbi_bank_a_g2", False)])
    conn = FakeConnection(FakeCursor(fetchall_results=[[("bank-a",)]]))

    report = reconcile(conn, contract, settings(), clients={"default": connect}, connect_source=lambda cfg: src)

    assert sorted(src.dropped) == ["fbi_bank_a_g0", "fbi_bank_a_g2"]
    assert {a["name"] for a in report.actions if a["kind"] == "drop_slot"} == set(src.dropped)


@pytest.mark.parametrize(
    "state,kafka_owned,desired,unknown,expected",
    [
        ("ACTIVE", True, True, False, (True, "fbi_bank_a_g1")),
        ("ACTIVE", True, False, True, (False, None)),
        ("ACTIVE", False, False, False, (True, None)),
        ("RETIRED", False, False, False, (True, None)),
        ("RETIRING", True, False, False, (True, None)),
        ("PROVISIONING", True, False, False, (False, None)),
        ("REQUESTED", True, False, False, (False, None)),
    ],
)
def test_slot_cleanup_only_acts_on_definitive_states(state, kafka_owned, desired, unknown, expected):
    s = source("bank-a", generation=1, state=state)
    desired_slots = {"bank-a": "fbi_bank_a_g1"} if desired else {}
    kafka = {"bank-a"} if kafka_owned else set()
    decision = slot_cleanup_decision(s, kafka, desired_slots, {"bank-a"} if unknown else set())
    assert decision == expected


def test_the_slot_check_is_rate_limited_per_source(fake_registry, contract, monkeypatch):
    fake_registry["sources"] = [source("bank-a", generation=2)]
    fake_registry["kafka"] = set()
    src = FakeSource([("fbi_bank_a_g0", False)])
    monkeypatch.setattr(reconciler, "_slot_check_due", lambda conn, sid, key, minutes: False)
    state = DesiredState([], {"bank-a"}, [], set())
    actions = reconciler.cleanup_stale_slots(
        FakeConnection(), state, {}, None, None, reconciler.ReconcileReport(), lambda cfg: src
    )
    assert actions == [] and src.dropped == []


def test_the_slot_check_key_changes_with_generation_owner_and_lifecycle():
    s = source("bank-a", generation=1)
    base = reconciler.slot_check_key(s, {"bank-a"})
    assert base != reconciler.slot_check_key(source("bank-a", generation=2), {"bank-a"})
    assert base != reconciler.slot_check_key(s, set()), "leaving KAFKA ownership triggers one inspection"
    assert base != reconciler.slot_check_key(source("bank-a", generation=1, state="RETIRING"), {"bank-a"})


@pytest.mark.parametrize(
    "row,expected",
    [(None, False), ((False,), False), ((True,), True)],
)
def test_a_slot_check_is_due_only_when_the_key_changed_or_the_interval_passed(row, expected):
    conn = FakeConnection(FakeCursor(fetchone_results=[row]))
    assert reconciler._slot_check_due(conn, "bank-a", "1:kafka:ACTIVE", 1440) is expected


def test_the_connect_client_sends_basic_credentials(monkeypatch):
    seen = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return b"[]"

    def fake_urlopen(req, timeout):
        seen["auth"] = req.get_header("Authorization")
        return Response()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    ConnectClient("http://connect:8083", user="fbi", password="pw").list_names()
    assert seen["auth"] == "Basic ZmJpOnB3"
    ConnectClient("http://connect:8083").list_names()
    assert seen["auth"] is None


def test_only_this_shards_connectors_are_described():
    connect = FakeConnect(
        {
            "fbi-src-mine-g0": observed("fbi-src-mine-g0"),
            "fbi-src-theirs-g0": observed("fbi-src-theirs-g0"),
            "fbi-src-orphan-g0": observed("fbi-src-orphan-g0"),
            "unrelated": observed("unrelated"),
        }
    )
    result = observe_cluster(connect, {"mine"}, {"mine", "theirs"}, shard_index=1)
    assert sorted(result) == ["fbi-src-mine-g0"]
    assert connect.described == ["fbi-src-mine-g0"]
    first = observe_cluster(FakeConnect({"fbi-src-orphan-g0": observed("fbi-src-orphan-g0")}), set(), set(), 0)
    assert list(first) == ["fbi-src-orphan-g0"], "shard 0 owns unregistered orphans"


def test_retire_artifacts_removes_every_generation_and_confirms_the_slot_is_gone():
    connect = FakeConnect(
        {
            "fbi-src-bank-a-g0": observed("fbi-src-bank-a-g0"),
            "fbi-src-bank-a-g1": observed("fbi-src-bank-a-g1"),
            "fbi-src-bank-b-g0": observed("fbi-src-bank-b-g0"),
        }
    )
    src = FakeSource([("fbi_bank_a_g0", False), ("fbi_bank_a_g1", False), ("fbi_bank_b_g0", False)])
    result = retire_artifacts(
        None, source("bank-a", generation=1), settings(), {"default": connect}, lambda cfg: src, wait_seconds=0
    )
    assert sorted(result["connectors_deleted"]) == ["fbi-src-bank-a-g0", "fbi-src-bank-a-g1"]
    assert "fbi-src-bank-b-g0" in connect.connectors
    assert sorted(result["slots_dropped"]) == ["fbi_bank_a_g0", "fbi_bank_a_g1"]
    assert result["slots_remaining"] == [] and result["errors"] == []
    assert src.slots == [("fbi_bank_b_g0", False)]


def test_retire_artifacts_reports_a_slot_that_is_still_active():
    src = FakeSource([("fbi_bank_a_g0", True)])
    result = retire_artifacts(
        None, source("bank-a"), settings(), {"default": FakeConnect({})}, lambda cfg: src, wait_seconds=0
    )
    assert result["slots_remaining"] == ["fbi_bank_a_g0"]


def test_retire_artifacts_records_unreachable_infrastructure_instead_of_raising():
    class Down(FakeConnect):
        def list_names(self):
            raise RuntimeError("connect is down")

    def unreachable(cfg):
        raise OSError("no route to source")

    result = retire_artifacts(None, source("bank-a"), settings(), {"default": Down({})}, unreachable, wait_seconds=0)
    assert any("connect is down" in e for e in result["errors"])
    assert any("no route" in e for e in result["errors"])
