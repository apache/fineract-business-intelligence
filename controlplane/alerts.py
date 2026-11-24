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
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


@dataclass(frozen=True)
class AlertConfig:
    webhook_url: str = ""
    webhook_timeout_seconds: float = 10.0
    webhook_headers: dict[str, str] | None = None
    environment: str = "production"
    freshness_slo_seconds: int = 900
    dead_letter_threshold: int = 10
    dead_letter_window_minutes: int = 15
    tombstone_max_hours: int = 24
    retention_days: int = 90

    def __post_init__(self) -> None:
        if self.webhook_headers is not None:
            if not isinstance(self.webhook_headers, dict):
                raise ValueError("webhook_headers must be a dictionary")
            if not all(isinstance(k, str) and isinstance(v, str) for k, v in self.webhook_headers.items()):
                raise ValueError("webhook_headers must contain only string keys and string values")

    @classmethod
    def from_env(cls) -> AlertConfig:
        headers_raw = os.getenv("FBI_ALERT_WEBHOOK_HEADERS")
        if headers_raw is not None and headers_raw.strip() != "":
            try:
                headers = json.loads(headers_raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in FBI_ALERT_WEBHOOK_HEADERS: {exc}") from exc
            if not isinstance(headers, dict):
                raise ValueError("FBI_ALERT_WEBHOOK_HEADERS must be a dictionary")
            if not all(isinstance(k, str) and isinstance(v, str) for k, v in headers.items()):
                raise ValueError("FBI_ALERT_WEBHOOK_HEADERS must contain only string keys and string values")
        else:
            headers = {}
        return cls(
            webhook_url=os.getenv("FBI_ALERT_WEBHOOK_URL", ""),
            webhook_timeout_seconds=float(os.getenv("FBI_ALERT_WEBHOOK_TIMEOUT_SECONDS", "10")),
            webhook_headers=headers,
            environment=os.getenv("FBI_ALERT_ENVIRONMENT", "production"),
            freshness_slo_seconds=int(os.getenv("FBI_FRESHNESS_SLO_SECONDS", "900")),
            dead_letter_threshold=int(os.getenv("FBI_ALERT_DEAD_LETTER_THRESHOLD", "10")),
            dead_letter_window_minutes=int(os.getenv("FBI_ALERT_DEAD_LETTER_WINDOW_MINUTES", "15")),
            tombstone_max_hours=int(os.getenv("FBI_ALERT_TOMBSTONE_MAX_HOURS", "24")),
            retention_days=int(os.getenv("FBI_ALERT_RETENTION_DAYS", "90")),
        )


@dataclass(frozen=True)
class DetectedAlert:
    alert_key: str
    alert_type: str
    severity: str
    details: dict[str, Any]


def detect_freshness_breaches(
    conn: Any = None,
    breaches: list[dict[str, Any]] | None = None,
    slo_seconds: int = 900,
) -> list[DetectedAlert]:
    if breaches is None:
        if conn is None:
            raise ValueError("Either conn or breaches must be provided")
        cur = conn.cursor()
        cur.execute(
            """
            SELECT o.source_id, o.table_name, o.engine, f.last_applied_at,
                   EXTRACT(EPOCH FROM (NOW() - f.last_applied_at))::bigint AS age_seconds
            FROM meta.ingestion_ownership o
            JOIN meta.source_registry r USING (source_id)
            LEFT JOIN meta.v_table_freshness f
              ON f.source_id = o.source_id AND f.table_name = o.table_name AND f.target = 'prod'
            WHERE r.lifecycle_state = 'ACTIVE'
              AND o.engine IN ('PYTHON', 'KAFKA')
              AND o.cutover_state <> 'PAUSED'
              AND COALESCE((r.labels ->> 'synthetic')::boolean, false) = false
              AND (f.last_applied_at IS NULL OR f.last_applied_at < NOW() - make_interval(secs => %s))
            ORDER BY age_seconds DESC NULLS FIRST
            """,
            (slo_seconds,),
        )
        rows = cur.fetchall()
        breaches = [
            {
                "source_id": r[0],
                "table_name": r[1],
                "engine": r[2],
                "last_applied_at": r[3],
                "age_seconds": r[4],
            }
            for r in rows
        ]

    alerts: list[DetectedAlert] = []
    for b in breaches:
        alerts.append(
            DetectedAlert(
                alert_key=f"freshness:{b['source_id']}:{b['table_name']}",
                alert_type="FRESHNESS",
                severity="CRITICAL",
                details={
                    "source_id": b["source_id"],
                    "table_name": b["table_name"],
                    "engine": b.get("engine"),
                    "age_seconds": b.get("age_seconds"),
                    "slo_seconds": slo_seconds,
                    "last_applied_at": str(b.get("last_applied_at")) if b.get("last_applied_at") else None,
                },
            )
        )
    return alerts


def detect_dead_letter_spikes(conn: Any, threshold: int, window_minutes: int) -> list[DetectedAlert]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT table_name, SUM(events_dead_lettered) AS dead_letter_count
        FROM meta.ingestion_audit
        WHERE minute_bucket >= NOW() - make_interval(mins => %s)
        GROUP BY table_name
        HAVING SUM(events_dead_lettered) > %s
        ORDER BY dead_letter_count DESC
        """,
        (window_minutes, threshold),
    )
    rows = cur.fetchall()
    return [
        DetectedAlert(
            alert_key=f"dead_letter:{r[0]}",
            alert_type="DEAD_LETTER",
            severity="CRITICAL",
            details={
                "table_name": r[0],
                "dead_letter_count": int(r[1]),
                "window_minutes": window_minutes,
                "threshold": threshold,
            },
        )
        for r in rows
    ]


def detect_failed_connectors(conn: Any) -> list[DetectedAlert]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT source_id, connector_name, connect_cluster, desired_state, observed_state,
               task_states, observed_at
        FROM meta.connector_state
        WHERE desired_state = 'RUNNING'
          AND (
            observed_state = 'FAILED'
            OR task_states @> '["FAILED"]'::jsonb
          )
        ORDER BY observed_at DESC
        """
    )
    rows = cur.fetchall()
    alerts: list[DetectedAlert] = []
    for r in rows:
        tasks = r[5] if isinstance(r[5], list) else (json.loads(r[5]) if isinstance(r[5], str) else [])
        alerts.append(
            DetectedAlert(
                alert_key=f"connector:{r[0]}",
                alert_type="CONNECTOR_FAILED",
                severity="CRITICAL",
                details={
                    "source_id": r[0],
                    "connector_name": r[1],
                    "cluster": r[2],
                    "desired_state": r[3],
                    "observed_state": r[4],
                    "task_states": tasks,
                },
            )
        )
    return alerts


def detect_stuck_tombstones(conn: Any, max_hours: int) -> list[DetectedAlert]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT source_id, tenant_id, updated_at,
               EXTRACT(EPOCH FROM (NOW() - updated_at))::bigint AS age_seconds
        FROM meta.source_registry
        WHERE lifecycle_state = 'RETIRED'
          AND COALESCE((labels ->> 'forget_after_cleanup')::boolean, false) = true
          AND updated_at < NOW() - make_interval(hours => %s)
        ORDER BY updated_at ASC
        """,
        (max_hours,),
    )
    rows = cur.fetchall()
    alerts: list[DetectedAlert] = []
    for r in rows:
        stuck_hours = round(r[3] / 3600, 1) if r[3] is not None else None
        alerts.append(
            DetectedAlert(
                alert_key=f"tombstone:{r[0]}",
                alert_type="TOMBSTONE_STUCK",
                severity="WARNING",
                details={
                    "source_id": r[0],
                    "tenant_id": r[1],
                    "stuck_hours": stuck_hours,
                    "threshold_hours": max_hours,
                    "updated_at": str(r[2]) if r[2] else None,
                },
            )
        )
    return alerts


def evaluate_alerts(
    conn: Any,
    config: AlertConfig,
    freshness_breaches: list[dict[str, Any]] | None = None,
) -> dict[str, int]:
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('meta.alert_state'))")

        active_alerts: dict[str, DetectedAlert] = {}
        breaches = detect_freshness_breaches(
            conn=conn, breaches=freshness_breaches, slo_seconds=config.freshness_slo_seconds
        )
        for a in breaches:
            active_alerts[a.alert_key] = a
        for a in detect_dead_letter_spikes(conn, config.dead_letter_threshold, config.dead_letter_window_minutes):
            active_alerts[a.alert_key] = a
        for a in detect_failed_connectors(conn):
            active_alerts[a.alert_key] = a
        for a in detect_stuck_tombstones(conn, config.tombstone_max_hours):
            active_alerts[a.alert_key] = a

        cur.execute("SELECT alert_key, alert_type, status, severity, details FROM meta.alert_state")
        existing_rows = cur.fetchall()
        existing_states: dict[str, dict[str, Any]] = {
            r[0]: {
                "alert_type": r[1],
                "status": r[2],
                "severity": r[3],
                "details": r[4] if isinstance(r[4], dict) else (json.loads(r[4]) if r[4] else {}),
            }
            for r in existing_rows
        }

        now_iso = datetime.now(UTC).isoformat()
        new_firing = 0
        suppressed_firing = 0
        resolved = 0

        for key, alert in active_alerts.items():
            existing = existing_states.get(key)
            if not existing or existing["status"] == "RESOLVED":
                cur.execute(
                    """
                    INSERT INTO meta.alert_state (
                        alert_key, alert_type, status, severity, first_fired_at, last_fired_at,
                        resolved_at, details, updated_at
                    )
                    VALUES (%s, %s, 'FIRING', %s, NOW(), NOW(), NULL, %s, NOW())
                    ON CONFLICT (alert_key) DO UPDATE SET
                        status = 'FIRING',
                        severity = EXCLUDED.severity,
                        first_fired_at = NOW(),
                        last_fired_at = NOW(),
                        resolved_at = NULL,
                        details = EXCLUDED.details,
                        updated_at = NOW()
                    """,
                    (key, alert.alert_type, alert.severity, json.dumps(alert.details)),
                )
                event_id = str(uuid4())
                payload: dict[str, Any] = {
                    "event_id": event_id,
                    "event_type": "ALERT_FIRING",
                    "alert_key": key,
                    "alert_type": alert.alert_type,
                    "severity": alert.severity,
                    "environment": config.environment,
                    "timestamp": now_iso,
                    "details": alert.details,
                }
                cur.execute(
                    """
                    INSERT INTO meta.alert_outbox (
                        event_id, alert_key, alert_type, event_type, severity, payload, status, next_attempt_at
                    )
                    VALUES (%s, %s, %s, 'ALERT_FIRING', %s, %s, 'PENDING', NOW())
                    """,
                    (event_id, key, alert.alert_type, alert.severity, json.dumps(payload)),
                )
                new_firing += 1
            else:
                cur.execute(
                    """
                    UPDATE meta.alert_state
                    SET last_fired_at = NOW(), details = %s, updated_at = NOW()
                    WHERE alert_key = %s
                    """,
                    (json.dumps(alert.details), key),
                )
                suppressed_firing += 1

        for key, state in existing_states.items():
            if state["status"] == "FIRING" and key not in active_alerts:
                cur.execute(
                    """
                    UPDATE meta.alert_state
                    SET status = 'RESOLVED', resolved_at = NOW(), updated_at = NOW()
                    WHERE alert_key = %s
                    """,
                    (key,),
                )
                event_id = str(uuid4())
                payload = {
                    "event_id": event_id,
                    "event_type": "ALERT_RESOLVED",
                    "alert_key": key,
                    "alert_type": state["alert_type"],
                    "severity": state["severity"],
                    "environment": config.environment,
                    "timestamp": now_iso,
                    "details": state.get("details") or {},
                }
                cur.execute(
                    """
                    INSERT INTO meta.alert_outbox (
                        event_id, alert_key, alert_type, event_type, severity, payload, status, next_attempt_at
                    )
                    VALUES (%s, %s, %s, 'ALERT_RESOLVED', %s, %s, 'PENDING', NOW())
                    """,
                    (event_id, key, state["alert_type"], state["severity"], json.dumps(payload)),
                )
                resolved += 1

        conn.commit()
        return {
            "active_count": len(active_alerts),
            "new_firing": new_firing,
            "suppressed_firing": suppressed_firing,
            "resolved": resolved,
        }
    except Exception:
        conn.rollback()
        raise


def compute_backoff_seconds(attempts: int) -> int:
    return min(60 * (2 ** min(attempts, 6)), 3600)


def _sanitize_delivery_error(exc: Exception) -> str:
    if isinstance(exc, urllib.error.HTTPError):
        return f"HTTPError: {exc.code}"
    if isinstance(exc, urllib.error.URLError):
        reason = getattr(exc, "reason", None)
        reason_name = type(reason).__name__ if isinstance(reason, Exception) else type(exc).__name__
        return f"URLError: {reason_name}"
    if isinstance(exc, TimeoutError):
        return "TimeoutError"
    return type(exc).__name__


def flush_outbox(conn: Any, config: AlertConfig, limit: int = 50) -> dict[str, Any]:
    if not config.webhook_url:
        return {"dispatched": 0, "failed": 0, "status": "disabled"}

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT event_id, payload, attempts
            FROM meta.alert_outbox
            WHERE status = 'PENDING'
              AND next_attempt_at <= NOW()
            ORDER BY created_at ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """,
            (limit,),
        )
        rows = cur.fetchall()
        if not rows:
            conn.commit()
            return {"dispatched": 0, "failed": 0, "status": "idle"}

        dispatched = 0
        failed = 0

        for event_id, payload_raw, attempts in rows:
            payload = payload_raw if isinstance(payload_raw, dict) else json.loads(payload_raw)
            headers: dict[str, str] = {
                "Content-Type": "application/json",
                "User-Agent": "Apache-Fineract-BI-Alerts/1.0",
            }
            if config.webhook_headers:
                headers.update(config.webhook_headers)

            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(config.webhook_url, data=data, headers=headers, method="POST")

            delivery_error: str | None = None
            try:
                with urllib.request.urlopen(req, timeout=config.webhook_timeout_seconds) as resp:
                    status_code = getattr(resp, "status", 200)
                    if not (200 <= status_code < 300):
                        delivery_error = f"HTTPError: {status_code}"
            except Exception as exc:
                delivery_error = _sanitize_delivery_error(exc)

            if delivery_error is None:
                cur.execute(
                    """
                    UPDATE meta.alert_outbox
                    SET status = 'SENT', sent_at = NOW(), attempts = attempts + 1,
                        last_attempt_at = NOW(), last_error = NULL
                    WHERE event_id = %s
                    """,
                    (event_id,),
                )
                dispatched += 1
            else:
                backoff = compute_backoff_seconds(attempts)
                cur.execute(
                    """
                    UPDATE meta.alert_outbox
                    SET attempts = attempts + 1,
                        last_attempt_at = NOW(),
                        next_attempt_at = NOW() + make_interval(secs => %s),
                        last_error = %s
                    WHERE event_id = %s
                    """,
                    (backoff, delivery_error, event_id),
                )
                failed += 1

        conn.commit()
        return {"dispatched": dispatched, "failed": failed, "status": "processed"}
    except Exception:
        conn.rollback()
        raise


def purge_alert_history(conn: Any, retention_days: int | None = None) -> dict[str, Any]:
    cur = conn.cursor()
    cur.execute("SELECT meta.purge_alert_history(%s)", (retention_days,))
    row = cur.fetchone()
    conn.commit()
    res = row[0] if row else {}
    return res if isinstance(res, dict) else json.loads(res)
