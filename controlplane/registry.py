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
import re
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field

from controlplane.db import rows_as_dicts
from controlplane.secrets import SecretError, check_reference

SOURCE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")

LIFECYCLE_TRANSITIONS: dict[str, frozenset[str]] = {
    "REQUESTED": frozenset({"VALIDATING", "RETIRED"}),
    "VALIDATING": frozenset({"PROVISIONING", "REQUESTED", "RETIRED"}),
    "PROVISIONING": frozenset({"ACTIVE", "DEGRADED", "RETIRING"}),
    "ACTIVE": frozenset({"PAUSED", "DEGRADED", "RETIRING"}),
    "PAUSED": frozenset({"ACTIVE", "RETIRING"}),
    "DEGRADED": frozenset({"ACTIVE", "PAUSED", "RETIRING"}),
    "RETIRING": frozenset({"RETIRED"}),
    "RETIRED": frozenset(),
}

RUNNING_STATES = frozenset({"PROVISIONING", "ACTIVE", "DEGRADED"})


class RegistryError(ValueError):
    pass


@dataclass(frozen=True)
class SourceRecord:
    source_id: str
    tenant_id: str
    display_name: str | None = None
    region: str = "default"
    tier: str = "standard"
    db_host: str | None = None
    db_port: int = 5432
    db_name: str | None = None
    db_schema: str = "public"
    compat_schema: str = "bi_connector_source"
    db_user: str | None = None
    secret_ref: str | None = None
    connect_cluster: str = "default"
    lifecycle_state: str = "REQUESTED"
    capture_generation: int = 0
    cob_job_name: str = "LOAN_COB"
    labels: dict = field(default_factory=dict)

    def validate(self) -> None:
        if not SOURCE_ID_PATTERN.match(self.source_id):
            raise RegistryError(
                f"source_id {self.source_id!r} must match {SOURCE_ID_PATTERN.pattern} "
                "(it becomes a Kafka topic prefix and a replication slot name)"
            )
        if self.lifecycle_state not in LIFECYCLE_TRANSITIONS:
            raise RegistryError(f"Unknown lifecycle state {self.lifecycle_state}")
        if self.secret_ref:
            try:
                check_reference(self.secret_ref)
            except SecretError as exc:
                raise RegistryError(f"secret_ref rejected: {exc}") from exc

    @property
    def slot_name(self) -> str:
        return f"fbi_{self.source_id.replace('-', '_')}_g{self.capture_generation}"

    @property
    def connector_name(self) -> str:
        return connector_name(self.source_id, self.capture_generation)


def connector_name(source_id: str, generation: int) -> str:
    return f"fbi-src-{source_id}-g{generation}"


def source_id_from_connector(name: str) -> str | None:
    if not name.startswith("fbi-src-") or "-g" not in name:
        return None
    base, _, gen = name[len("fbi-src-") :].rpartition("-g")
    return base if base and gen.isdigit() else None


_COLUMNS = tuple(SourceRecord.__dataclass_fields__)


def _record(row: dict) -> SourceRecord:
    values = {k: row[k] for k in _COLUMNS if k in row}
    if isinstance(values.get("labels"), str):
        values["labels"] = json.loads(values["labels"])
    return SourceRecord(**values)


class SourceRegistry:
    def __init__(self, conn) -> None:
        self._conn = conn

    def upsert(self, records: Iterable[SourceRecord]) -> int:
        rows = []
        for r in records:
            r.validate()
            d = asdict(r)
            d["labels"] = json.dumps(d["labels"])
            values: list = []
            for c in _COLUMNS:
                values.append(d[c])
                if c == "capture_generation":
                    values.append(r.source_id)
            rows.append(tuple(values))
        if not rows:
            return 0
        cols = ", ".join(_COLUMNS)
        floor = (
            "GREATEST(%s, COALESCE((SELECT next_generation FROM meta.source_generation_floor f "
            "WHERE f.source_id = %s), 0))"
        )
        placeholders = ", ".join(floor if c == "capture_generation" else "%s" for c in _COLUMNS)
        updates = ", ".join(
            f"{c} = EXCLUDED.{c}" for c in _COLUMNS if c not in ("source_id", "lifecycle_state", "capture_generation")
        )
        cursor = self._conn.cursor()
        cursor.executemany(
            f"INSERT INTO meta.source_registry ({cols}) VALUES ({placeholders}) "
            f"ON CONFLICT (source_id) DO UPDATE SET {updates}, updated_at = NOW()",
            rows,
        )
        return len(rows)

    def get(self, source_id: str) -> SourceRecord:
        cursor = self._conn.cursor()
        cursor.execute(f"SELECT {', '.join(_COLUMNS)} FROM meta.source_registry WHERE source_id = %s", (source_id,))
        rows = rows_as_dicts(cursor)
        if not rows:
            raise RegistryError(f"Unknown source {source_id}")
        return _record(rows[0])

    def list(
        self,
        states: Iterable[str] | None = None,
        region: str | None = None,
        shard_index: int | None = None,
        shard_count: int | None = None,
    ) -> list[SourceRecord]:
        clauses, params = [], []
        if states:
            clauses.append("lifecycle_state = ANY(%s)")
            params.append(list(states))
        if region:
            clauses.append("region = %s")
            params.append(region)
        if shard_count:
            clauses.append("abs(hashtext(source_id)) %% %s = %s")
            params.extend([shard_count, shard_index or 0])
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        cursor = self._conn.cursor()
        cursor.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM meta.source_registry {where} ORDER BY source_id", tuple(params)
        )
        return [_record(r) for r in rows_as_dicts(cursor)]

    def set_lifecycle(self, source_id: str, new_state: str) -> None:
        current = self.get(source_id).lifecycle_state
        if new_state == current:
            return
        if new_state not in LIFECYCLE_TRANSITIONS.get(current, frozenset()):
            raise RegistryError(f"{source_id}: lifecycle {current} -> {new_state} is not allowed")
        cursor = self._conn.cursor()
        cursor.execute(
            "UPDATE meta.source_registry SET lifecycle_state = %s, updated_at = NOW() "
            "WHERE source_id = %s AND lifecycle_state = %s",
            (new_state, source_id, current),
        )
        if cursor.rowcount != 1:
            raise RegistryError(f"{source_id}: concurrent lifecycle change detected")

    def bump_capture_generation(self, source_id: str) -> int:
        cursor = self._conn.cursor()
        cursor.execute(
            "UPDATE meta.source_registry SET capture_generation = capture_generation + 1, updated_at = NOW() "
            "WHERE source_id = %s RETURNING capture_generation",
            (source_id,),
        )
        row = cursor.fetchone()
        if not row:
            raise RegistryError(f"Unknown source {source_id}")
        return int(row[0])
