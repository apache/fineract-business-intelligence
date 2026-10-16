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
import logging

from controlplane.contract import load_contract
from controlplane.db import quote_ident
from controlplane.registry import RegistryError, SourceRegistry

log = logging.getLogger(__name__)

DERIVED_TABLES = (
    "fact_loan_snapshot",
    "fact_delinquency_event",
    "mart_delinquency_par",
    "mart_portfolio_health",
    "mart_repayment_behavior",
)
_BOOKKEEPING = (
    "meta.entity_change_log",
    "meta.extract_watermarks",
    "meta.ingestion_progress",
    "meta.connector_state",
)


def lifecycle_path(state: str) -> list[str]:
    if state == "RETIRED":
        return []
    if state in ("REQUESTED", "VALIDATING", "RETIRING"):
        return ["RETIRED"]
    return ["RETIRING", "RETIRED"]


def retire_source(conn, source_id: str, forget: bool = False) -> dict:
    contract = load_contract()
    registry = SourceRegistry(conn)
    summary: dict = {"source_id": source_id, "raw_rows": 0, "derived_rows": {}, "lifecycle": "not registered"}
    cursor = conn.cursor()
    try:
        try:
            existing = registry.get(source_id)
            state = existing.lifecycle_state
            generation = existing.capture_generation
        except RegistryError:
            state, generation = None, None
        if generation is not None:
            cursor.execute(
                "INSERT INTO meta.source_generation_floor (source_id, next_generation) VALUES (%s, %s) "
                "ON CONFLICT (source_id) DO UPDATE SET next_generation = "
                "GREATEST(meta.source_generation_floor.next_generation, EXCLUDED.next_generation), updated_at = NOW()",
                (source_id, generation + 1),
            )
            summary["next_capture_generation"] = generation + 1
        if state is not None:
            for nxt in lifecycle_path(state):
                registry.set_lifecycle(source_id, nxt)
            summary["lifecycle"] = "RETIRED"

        for table in sorted(contract.table_names):
            cursor.execute("SELECT meta.lock_ownership_exclusive(%s, %s)", (source_id, table))
        cursor.execute(
            "UPDATE meta.ingestion_ownership SET engine = 'NONE', shadow_engine = NULL, cutover_state = 'STEADY', "
            "epoch = epoch + 1, updated_at = NOW(), updated_by = 'retire', change_reason = 'source retired' "
            "WHERE source_id = %s",
            (source_id,),
        )

        for table in contract.tables:
            for schema in ("raw", "raw_shadow"):
                relation = f"{schema}.{quote_ident(table.raw_table)}"
                cursor.execute(f"DELETE FROM {relation} WHERE source_id = %s", (source_id,))
                if schema == "raw":
                    summary["raw_rows"] += cursor.rowcount

        for name in DERIVED_TABLES:
            cursor.execute("SELECT to_regclass(%s)", (f"analytics.{name}",))
            if cursor.fetchone()[0] is None:
                continue
            cursor.execute(f"DELETE FROM analytics.{quote_ident(name)} WHERE source_id = %s", (source_id,))
            summary["derived_rows"][name] = cursor.rowcount

        for relation in _BOOKKEEPING:
            cursor.execute(f"DELETE FROM {relation} WHERE source_id = %s", (source_id,))
        if forget:
            cursor.execute("DELETE FROM meta.source_registry WHERE source_id = %s", (source_id,))
            summary["lifecycle"] = "removed from registry"
        cursor.execute(
            "INSERT INTO meta.ingestion_ownership_audit (source_id, table_name, action, previous_state, new_state, "
            "actor, reason) VALUES (%s, '*', 'retire', NULL, %s, 'retire', 'source retired and purged')",
            (source_id, json.dumps(summary)),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    log.info("retired %s: %s", source_id, summary)
    return summary
