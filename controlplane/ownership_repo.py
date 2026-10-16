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


from __future__ import annotations

import json
from collections.abc import Iterable

from controlplane.db import rows_as_dicts
from controlplane.ownership import (
    Action,
    CutoverState,
    Engine,
    Ownership,
    SideEffect,
    TransitionRequest,
    TransitionResult,
    transition,
)

_SELECT = (
    "SELECT source_id, table_name, engine, shadow_engine, cutover_state, epoch, last_reconciliation_id "
    "FROM meta.ingestion_ownership"
)


def _from_row(row: dict) -> Ownership:
    return Ownership(
        source_id=row["source_id"],
        table_name=row["table_name"],
        engine=Engine(row["engine"]),
        shadow_engine=Engine(row["shadow_engine"]) if row["shadow_engine"] else None,
        cutover_state=CutoverState(row["cutover_state"]),
        epoch=int(row["epoch"]),
        last_reconciliation_id=row["last_reconciliation_id"],
    )


class OwnershipRepository:
    def __init__(self, conn) -> None:
        self._conn = conn

    def list(self, source_id: str | None = None, engine: Engine | None = None) -> list[Ownership]:
        clauses, params = [], []
        if source_id:
            clauses.append("source_id = %s")
            params.append(source_id)
        if engine:
            clauses.append("(engine = %s OR shadow_engine = %s)")
            params.extend([engine.value, engine.value])
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        cursor = self._conn.cursor()
        cursor.execute(_SELECT + where + " ORDER BY source_id, table_name", tuple(params))
        return [_from_row(r) for r in rows_as_dicts(cursor)]

    def get(self, source_id: str, table_name: str) -> Ownership:
        cursor = self._conn.cursor()
        cursor.execute(_SELECT + " WHERE source_id = %s AND table_name = %s", (source_id, table_name))
        rows = rows_as_dicts(cursor)
        if not rows:
            raise KeyError(f"No ownership row for {source_id}/{table_name}")
        return _from_row(rows[0])

    def ensure_rows(self, source_id: str, tables: Iterable[str]) -> int:
        cursor = self._conn.cursor()
        created = 0
        for table in tables:
            cursor.execute(
                "INSERT INTO meta.ingestion_ownership (source_id, table_name, engine, updated_by, change_reason) "
                "VALUES (%s, %s, 'NONE', 'controlplane', 'registered') "
                "ON CONFLICT (source_id, table_name) DO NOTHING",
                (source_id, table),
            )
            created += cursor.rowcount
        return created

    def dual_control_required(self) -> bool:
        cursor = self._conn.cursor()
        cursor.execute("SELECT meta.setting('require_dual_control', 'true')")
        return str(cursor.fetchone()[0]).lower() == "true"

    def apply(self, source_id: str, tables: Iterable[str], request: TransitionRequest) -> list[TransitionResult]:
        dual = self.dual_control_required()
        req = TransitionRequest(**{**request.__dict__, "require_dual_control": dual})
        results: list[TransitionResult] = []
        cursor = self._conn.cursor()
        try:
            for table in sorted(set(tables)):
                cursor.execute("SELECT meta.lock_ownership_exclusive(%s, %s)", (source_id, table))
                cursor.execute(_SELECT + " WHERE source_id = %s AND table_name = %s FOR UPDATE", (source_id, table))
                rows = rows_as_dicts(cursor)
                if not rows:
                    raise KeyError(f"No ownership row for {source_id}/{table}")
                result = transition(_from_row(rows[0]), req)
                cur = result.current
                cursor.execute(
                    "UPDATE meta.ingestion_ownership SET engine = %s, shadow_engine = %s, cutover_state = %s, "
                    "epoch = %s, last_reconciliation_id = %s, updated_at = NOW(), updated_by = %s, change_reason = %s "
                    "WHERE source_id = %s AND table_name = %s AND epoch = %s",
                    (
                        cur.engine.value,
                        cur.shadow_engine.value if cur.shadow_engine else None,
                        cur.cutover_state.value,
                        cur.epoch,
                        cur.last_reconciliation_id,
                        req.actor,
                        req.reason,
                        source_id,
                        table,
                        result.previous.epoch,
                    ),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError(f"Optimistic concurrency failure on {source_id}/{table}")
                for effect in result.side_effects:
                    if effect is SideEffect.PROMOTE_SHADOW:
                        cursor.execute("SELECT meta.promote_shadow(%s, %s)", (source_id, table))
                    elif effect is SideEffect.CLEAR_SHADOW:
                        cursor.execute("SELECT meta.clear_shadow(%s, %s)", (source_id, table))
                cursor.execute(
                    "INSERT INTO meta.ingestion_ownership_audit "
                    "(source_id, table_name, action, previous_state, new_state, actor, approved_by, reason) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        source_id,
                        table,
                        req.action.value,
                        json.dumps(result.previous.as_dict()),
                        json.dumps(cur.as_dict()),
                        req.actor,
                        req.approved_by,
                        req.reason,
                    ),
                )
                results.append(result)
            if req.action is Action.START_SHADOW and req.engine is Engine.KAFKA and results:
                cursor.execute(
                    "UPDATE meta.source_registry SET capture_generation = capture_generation + 1, updated_at = NOW() "
                    "WHERE source_id = %s",
                    (source_id,),
                )
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise
        return results
