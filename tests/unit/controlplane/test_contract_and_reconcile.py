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

import re
from pathlib import Path

import pytest

from controlplane.reconcile import ReconciliationResult, source_row_expression, warehouse_row_expression

ROOT = Path(__file__).resolve().parents[3]
IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
TYPES = re.compile(r"^(bigint|integer|smallint|text|date|timestamptz|boolean|numeric\(\d{1,2},\d{1,2}\))$")


def test_contract_is_valid_for_the_warehouse_provisioner(contract):
    assert contract.version >= 2
    names = set()
    for t in contract.tables:
        assert IDENT.match(t.table_name) and IDENT.match(t.raw_table) and IDENT.match(t.primary_key)
        assert t.table_name not in names
        names.add(t.table_name)
        assert t.primary_key in t.column_names
        assert len(set(t.column_names)) == len(t.column_names)
        for c in t.columns:
            assert IDENT.match(c.name), c.name
            assert TYPES.match(c.type), (t.table_name, c.name, c.type)
        if t.python_cursor:
            assert t.python_cursor in t.column_names
        assert 1 <= t.kafka_partitions <= 1024
        for idx in t.indexes:
            assert set(idx) <= set(t.column_names)


def test_every_contract_table_has_a_python_compat_view(contract):
    script = (ROOT / "scripts" / "bootstrap_source.sh").read_text(encoding="utf-8")
    for t in contract.tables:
        assert f"${{SOURCE_DB_SCHEMA}}.{t.table_name} AS" in script, f"missing compat view for {t.table_name}"


def test_every_contract_table_is_a_dbt_source(contract):
    sources = (ROOT / "dbt" / "models" / "sources.yml").read_text(encoding="utf-8")
    for t in contract.tables:
        assert f"- name: {t.raw_table}" in sources, t.raw_table


def test_pii_columns_are_flagged(contract):
    assert contract.table("m_client").column("date_of_birth").pii


def test_row_expressions_cast_both_sides_to_contract_types(contract):
    t = contract.table("batch_job_execution")
    src = source_row_expression(t, {"job_execution_id", "status", "create_time", "last_updated"})
    assert 'CAST("create_time" AS timestamptz)' in src
    assert "CAST(NULL AS bigint)" in src, "missing source columns compare as NULL"
    wh = warehouse_row_expression(t)
    assert 'CAST("created_on_utc" AS timestamptz)' in wh
    office = contract.table("m_office")
    assert "created_on_utc" not in warehouse_row_expression(office), "synthetic columns are excluded"


@pytest.mark.parametrize("mismatch,left,right,limit,passed", [
    (0, 0, 0, 0, True), (1, 0, 0, 0, False), (0, 2, 1, 3, True), (0, 2, 2, 3, False)])
def test_reconciliation_pass_threshold(mismatch, left, right, limit, passed):
    r = ReconciliationResult("s", "t", "shadow_vs_prod", 100, mismatch, left, right)
    assert r.passed(limit) is passed
