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

import json
from unittest.mock import MagicMock, patch
from urllib.error import URLError

import pytest

from controlplane.alerts import (
    AlertConfig,
    compute_backoff_seconds,
    detect_dead_letter_spikes,
    detect_failed_connectors,
    detect_freshness_breaches,
    detect_stuck_tombstones,
    evaluate_alerts,
    flush_outbox,
    purge_alert_history,
)
from tests.conftest import FakeConnection, FakeCursor


def test_alert_config_defaults_and_from_env(monkeypatch):
    cfg = AlertConfig.from_env()
    assert cfg.webhook_url == ""
    assert cfg.webhook_headers == {}
    assert cfg.freshness_slo_seconds == 900
    assert cfg.dead_letter_threshold == 10
    assert cfg.dead_letter_window_minutes == 15
    assert cfg.tombstone_max_hours == 24
    assert cfg.retention_days == 90

    monkeypatch.setenv("FBI_ALERT_WEBHOOK_URL", "https://example.com/webhook")
    monkeypatch.setenv("FBI_ALERT_DEAD_LETTER_THRESHOLD", "5")
    monkeypatch.setenv("FBI_ALERT_WEBHOOK_HEADERS", '{"X-Auth": "secret"}')
    cfg_custom = AlertConfig.from_env()
    assert cfg_custom.webhook_url == "https://example.com/webhook"
    assert cfg_custom.dead_letter_threshold == 5
    assert cfg_custom.webhook_headers == {"X-Auth": "secret"}


def test_alert_config_invalid_json_headers(monkeypatch):
    monkeypatch.setenv("FBI_ALERT_WEBHOOK_HEADERS", "invalid-json{")
    with pytest.raises(ValueError, match="Invalid JSON"):
        AlertConfig.from_env()


def test_alert_config_non_dict_headers(monkeypatch):
    monkeypatch.setenv("FBI_ALERT_WEBHOOK_HEADERS", '["not", "a", "dict"]')
    with pytest.raises(ValueError, match="must be a dictionary"):
        AlertConfig.from_env()


def test_alert_config_invalid_header_types(monkeypatch):
    monkeypatch.setenv("FBI_ALERT_WEBHOOK_HEADERS", '{"X-Auth": 123}')
    with pytest.raises(ValueError, match="must contain only string keys and string values"):
        AlertConfig.from_env()


def test_alert_config_non_string_header_key():
    with pytest.raises(ValueError, match="must contain only string keys and string values"):
        AlertConfig(webhook_headers={123: "secret"})


def test_compute_backoff_seconds():
    assert compute_backoff_seconds(0) == 60
    assert compute_backoff_seconds(1) == 120
    assert compute_backoff_seconds(2) == 240
    assert compute_backoff_seconds(3) == 480
    assert compute_backoff_seconds(4) == 960
    assert compute_backoff_seconds(5) == 1920
    assert compute_backoff_seconds(6) == 3600
    assert compute_backoff_seconds(10) == 3600


def test_detect_freshness_breaches_with_passed_breaches():
    breaches = [
        {
            "source_id": "branch-01",
            "table_name": "m_loan",
            "engine": "KAFKA",
            "age_seconds": 1500,
            "last_applied_at": "2026-11-23 00:00:00+00",
        }
    ]
    alerts = detect_freshness_breaches(breaches=breaches, slo_seconds=900)
    assert len(alerts) == 1
    assert alerts[0].alert_key == "freshness:branch-01:m_loan"
    assert alerts[0].alert_type == "FRESHNESS"
    assert alerts[0].severity == "CRITICAL"
    assert alerts[0].details["source_id"] == "branch-01"
    assert alerts[0].details["table_name"] == "m_loan"
    assert alerts[0].details["age_seconds"] == 1500
    assert alerts[0].details["slo_seconds"] == 900


def test_detect_freshness_breaches_from_database():
    cur = FakeCursor(fetchall_results=[[("branch-02", "m_client", "PYTHON", "2026-11-23 01:00:00+00", 2000)]])
    conn = FakeConnection(cur)
    alerts = detect_freshness_breaches(conn=conn, slo_seconds=900)
    assert len(alerts) == 1
    assert alerts[0].alert_key == "freshness:branch-02:m_client"
    assert alerts[0].details["source_id"] == "branch-02"


def test_detect_freshness_breaches_unbounded_and_no_false_resolution():
    stale_rows = [(f"branch-{i:04d}", f"table_{i}", "PYTHON", "2026-11-23 00:00:00+00", 2000) for i in range(550)]
    cur = FakeCursor(fetchall_results=[list(stale_rows)])
    conn = FakeConnection(cur)
    alerts = detect_freshness_breaches(conn=conn, slo_seconds=900)
    assert len(alerts) == 550
    executed_sql = cur.executed[0][0]
    assert "LIMIT" not in executed_sql

    existing_rows = [
        (f"freshness:branch-{i:04d}:table_{i}", "FRESHNESS", "FIRING", "CRITICAL", "{}")
        for i in range(550)
    ]
    cur2 = FakeCursor(fetchall_results=[list(stale_rows), [], [], [], existing_rows])
    conn2 = FakeConnection(cur2)
    config = AlertConfig()
    summary = evaluate_alerts(conn2, config)
    assert summary["active_count"] == 550
    assert summary["resolved"] == 0
    assert summary["suppressed_firing"] == 550


def test_detect_dead_letter_spikes():
    cur = FakeCursor(fetchall_results=[[("m_loan", 25)]])
    conn = FakeConnection(cur)
    alerts = detect_dead_letter_spikes(conn, threshold=10, window_minutes=15)
    assert len(alerts) == 1
    assert alerts[0].alert_key == "dead_letter:m_loan"
    assert alerts[0].alert_type == "DEAD_LETTER"
    assert alerts[0].severity == "CRITICAL"
    assert alerts[0].details["table_name"] == "m_loan"
    assert alerts[0].details["dead_letter_count"] == 25
    assert alerts[0].details["threshold"] == 10
    assert alerts[0].details["window_minutes"] == 15
    assert cur.last_params == (15, 10)


def test_detect_failed_connectors_metadata_only():
    cur = FakeCursor(
        fetchall_results=[
            [
                (
                    "branch-01",
                    "fbi-cdc-branch-01",
                    "default",
                    "RUNNING",
                    "FAILED",
                    json.dumps(["FAILED"]),
                    "2026-11-23 01:00:00+00",
                )
            ]
        ]
    )
    conn = FakeConnection(cur)
    alerts = detect_failed_connectors(conn)
    assert len(alerts) == 1
    assert alerts[0].alert_key == "connector:branch-01"
    assert alerts[0].alert_type == "CONNECTOR_FAILED"
    assert alerts[0].severity == "CRITICAL"
    details = alerts[0].details
    assert details["source_id"] == "branch-01"
    assert details["connector_name"] == "fbi-cdc-branch-01"
    assert details["cluster"] == "default"
    assert details["observed_state"] == "FAILED"
    assert details["task_states"] == ["FAILED"]
    assert "last_error" not in details
    assert "trace" not in details


def test_detect_stuck_tombstones():
    cur = FakeCursor(fetchall_results=[[("src-dead", "t-001", "2026-11-21 00:00:00+00", 172800)]])
    conn = FakeConnection(cur)
    alerts = detect_stuck_tombstones(conn, max_hours=24)
    assert len(alerts) == 1
    assert alerts[0].alert_key == "tombstone:src-dead"
    assert alerts[0].alert_type == "TOMBSTONE_STUCK"
    assert alerts[0].severity == "WARNING"
    assert alerts[0].details["source_id"] == "src-dead"
    assert alerts[0].details["stuck_hours"] == 48.0
    assert alerts[0].details["threshold_hours"] == 24


def test_detector_failure_aborts_transaction():
    class BrokenCursor(FakeCursor):
        def execute(self, sql: str, params=None, stream=None):
            if "meta.connector_state" in sql:
                raise RuntimeError("database connection lost")
            super().execute(sql, params, stream)

    cur = BrokenCursor(
        fetchall_results=[
            [("m_loan", 25)],
        ]
    )
    conn = FakeConnection(cur)
    config = AlertConfig()
    with pytest.raises(RuntimeError, match="database connection lost"):
        evaluate_alerts(conn, config, freshness_breaches=[])

    assert conn.rolled_back >= 1
    assert conn.committed == 0


def test_evaluate_alerts_first_firing_creates_outbox_event():
    cur = FakeCursor(
        fetchall_results=[
            [("m_loan", 15)],
            [],
            [],
            [],
        ]
    )
    conn = FakeConnection(cur)
    config = AlertConfig()
    summary = evaluate_alerts(conn, config, freshness_breaches=[])

    assert summary["active_count"] == 1
    assert summary["new_firing"] == 1
    assert summary["suppressed_firing"] == 0
    assert summary["resolved"] == 0
    assert conn.committed == 1

    executed_sqls = [sql for sql, _ in cur.executed]
    assert any("INSERT INTO meta.alert_state" in sql for sql in executed_sqls)
    assert any("INSERT INTO meta.alert_outbox" in sql for sql in executed_sqls)

    outbox_calls = [params for sql, params in cur.executed if "INSERT INTO meta.alert_outbox" in sql]
    assert len(outbox_calls) == 1
    event_id, alert_key, alert_type, severity, payload_json = outbox_calls[0]
    assert alert_key == "dead_letter:m_loan"
    assert alert_type == "DEAD_LETTER"
    assert severity == "CRITICAL"
    payload = json.loads(payload_json)
    assert payload["event_id"] == event_id
    assert payload["event_type"] == "ALERT_FIRING"
    assert payload["alert_key"] == "dead_letter:m_loan"


def test_evaluate_alerts_repeated_firing_suppresses_outbox_event():
    cur = FakeCursor(
        fetchall_results=[
            [("m_loan", 20)],
            [],
            [],
            [("dead_letter:m_loan", "DEAD_LETTER", "FIRING", "CRITICAL", json.dumps({"table_name": "m_loan"}))],
        ]
    )
    conn = FakeConnection(cur)
    config = AlertConfig()
    summary = evaluate_alerts(conn, config, freshness_breaches=[])

    assert summary["active_count"] == 1
    assert summary["new_firing"] == 0
    assert summary["suppressed_firing"] == 1
    assert summary["resolved"] == 0
    assert conn.committed == 1

    executed_sqls = [sql for sql, _ in cur.executed]
    assert any("UPDATE meta.alert_state" in sql and "last_fired_at = NOW()" in sql for sql in executed_sqls)
    assert not any("INSERT INTO meta.alert_outbox" in sql for sql in executed_sqls)


def test_evaluate_alerts_continuous_firing_preserves_first_fired_at():
    cur = FakeCursor(
        fetchall_results=[
            [("m_loan", 20)],
            [],
            [],
            [("dead_letter:m_loan", "DEAD_LETTER", "FIRING", "CRITICAL", json.dumps({"table_name": "m_loan"}))],
        ]
    )
    conn = FakeConnection(cur)
    config = AlertConfig()
    evaluate_alerts(conn, config, freshness_breaches=[])

    update_sqls = [sql for sql, _ in cur.executed if "UPDATE meta.alert_state" in sql]
    assert len(update_sqls) == 1
    assert "last_fired_at = NOW()" in update_sqls[0]
    assert "first_fired_at" not in update_sqls[0]


def test_evaluate_alerts_resolved_to_firing_resets_first_fired_at():
    cur = FakeCursor(
        fetchall_results=[
            [("m_loan", 20)],
            [],
            [],
            [("dead_letter:m_loan", "DEAD_LETTER", "RESOLVED", "CRITICAL", json.dumps({"table_name": "m_loan"}))],
        ]
    )
    conn = FakeConnection(cur)
    config = AlertConfig()
    summary = evaluate_alerts(conn, config, freshness_breaches=[])

    assert summary["new_firing"] == 1
    conflict_sqls = [sql for sql, _ in cur.executed if "ON CONFLICT (alert_key) DO UPDATE" in sql]
    assert len(conflict_sqls) == 1
    assert "first_fired_at = NOW()" in conflict_sqls[0]
    assert "resolved_at = NULL" in conflict_sqls[0]
    assert any("INSERT INTO meta.alert_outbox" in sql for sql, _ in cur.executed)


def test_evaluate_alerts_recovery_creates_outbox_event():
    cur = FakeCursor(
        fetchall_results=[
            [],
            [],
            [],
            [("freshness:branch-01:m_loan", "FRESHNESS", "FIRING", "CRITICAL", "{}")],
        ]
    )
    conn = FakeConnection(cur)
    config = AlertConfig()
    summary = evaluate_alerts(conn, config, freshness_breaches=[])

    assert summary["active_count"] == 0
    assert summary["new_firing"] == 0
    assert summary["suppressed_firing"] == 0
    assert summary["resolved"] == 1
    assert conn.committed == 1

    outbox_calls = [params for sql, params in cur.executed if "INSERT INTO meta.alert_outbox" in sql]
    assert len(outbox_calls) == 1
    event_id, alert_key, alert_type, severity, payload_json = outbox_calls[0]
    assert alert_key == "freshness:branch-01:m_loan"
    assert alert_type == "FRESHNESS"
    assert severity == "CRITICAL"
    payload = json.loads(payload_json)
    assert payload["event_id"] == event_id
    assert payload["event_type"] == "ALERT_RESOLVED"


def test_flush_outbox_opt_in_disabled():
    cur = FakeCursor()
    conn = FakeConnection(cur)
    config = AlertConfig(webhook_url="")
    res = flush_outbox(conn, config)
    assert res == {"dispatched": 0, "failed": 0, "status": "disabled"}
    assert len(cur.executed) == 0


def test_flush_outbox_idle_commits_transaction():
    cur = FakeCursor(fetchall_results=[[]])
    conn = FakeConnection(cur)
    config = AlertConfig(webhook_url="https://hooks.example.com/alerts")
    res = flush_outbox(conn, config)
    assert res == {"dispatched": 0, "failed": 0, "status": "idle"}
    assert conn.committed == 1
    assert any("FOR UPDATE SKIP LOCKED" in sql for sql, _ in cur.executed)


def test_flush_outbox_successful_delivery():
    event_id = "550e8400-e29b-41d4-a716-446655440000"
    payload = {"event_id": event_id, "event_type": "ALERT_FIRING", "alert_key": "connector:b1"}
    cur = FakeCursor(
        fetchall_results=[
            [(event_id, json.dumps(payload), 0)],
        ]
    )
    conn = FakeConnection(cur)
    config = AlertConfig(webhook_url="https://hooks.example.com/alerts")

    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp):
        res = flush_outbox(conn, config)

    assert res == {"dispatched": 1, "failed": 0, "status": "processed"}
    executed_sqls = [sql for sql, _ in cur.executed]
    assert any("FOR UPDATE SKIP LOCKED" in sql for sql in executed_sqls)
    assert any("UPDATE meta.alert_outbox" in sql and "status = 'SENT'" in sql for sql in executed_sqls)
    assert conn.committed == 1


def test_flush_outbox_holds_row_locks_until_commit():
    event_id = "550e8400-e29b-41d4-a716-446655440000"
    payload = {"event_id": event_id, "event_type": "ALERT_FIRING", "alert_key": "connector:b1"}
    cur = FakeCursor(
        fetchall_results=[
            [(event_id, json.dumps(payload), 0)],
        ]
    )
    conn = FakeConnection(cur)
    config = AlertConfig(webhook_url="https://hooks.example.com/alerts")

    committed_during_delivery = None

    def fake_urlopen(*args, **kwargs):
        nonlocal committed_during_delivery
        committed_during_delivery = conn.committed
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__.return_value = mock_resp
        return mock_resp

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        flush_outbox(conn, config)

    assert committed_during_delivery == 0
    assert conn.committed == 1
    assert any("FOR UPDATE SKIP LOCKED" in sql for sql, _ in cur.executed)


def test_flush_outbox_failed_delivery_exponential_backoff():
    event_id = "660e8400-e29b-41d4-a716-446655440000"
    payload = {"event_id": event_id, "event_type": "ALERT_FIRING", "alert_key": "connector:b2"}
    cur = FakeCursor(
        fetchall_results=[
            [(event_id, json.dumps(payload), 0)],
        ]
    )
    conn = FakeConnection(cur)
    config = AlertConfig(webhook_url="https://hooks.example.com/alerts")

    with patch("urllib.request.urlopen", side_effect=URLError("Connection refused")):
        res = flush_outbox(conn, config)

    assert res == {"dispatched": 0, "failed": 1, "status": "processed"}
    update_calls = [
        params
        for sql, params in cur.executed
        if "UPDATE meta.alert_outbox" in sql and "next_attempt_at = NOW() + make_interval" in sql
    ]
    assert len(update_calls) == 1
    backoff_secs, err_str, eid = update_calls[0]
    assert backoff_secs == 60
    assert err_str == "URLError: URLError"
    assert eid == event_id
    assert conn.committed == 1


def test_flush_outbox_rollback_on_database_error():
    event_id = "770e8400-e29b-41d4-a716-446655440000"
    payload = {"event_id": event_id, "event_type": "ALERT_FIRING", "alert_key": "connector:b3"}

    class FailingCursor(FakeCursor):
        def execute(self, sql: str, params=None, stream=None):
            if "UPDATE meta.alert_outbox" in sql:
                raise RuntimeError("disk failure")
            super().execute(sql, params, stream)

    cur = FailingCursor(
        fetchall_results=[
            [(event_id, json.dumps(payload), 0)],
        ]
    )
    conn = FakeConnection(cur)
    config = AlertConfig(webhook_url="https://hooks.example.com/alerts")

    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp), pytest.raises(RuntimeError, match="disk failure"):
        flush_outbox(conn, config)

    assert conn.rolled_back >= 1
    assert conn.committed == 0


def test_purge_alert_history():
    cur = FakeCursor(fetchone_results=[('{"purged_outbox_rows": 5, "purged_state_rows": 2}',)])
    conn = FakeConnection(cur)
    res = purge_alert_history(conn, retention_days=60)
    assert res == {"purged_outbox_rows": 5, "purged_state_rows": 2}
    assert cur.last_sql == "SELECT meta.purge_alert_history(%s)"
    assert cur.last_params == (60,)
    assert conn.committed == 1
