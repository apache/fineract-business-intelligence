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

import sys
from pathlib import Path

SUPERSET_DIR = Path(__file__).resolve().parent.parent.parent.parent / "docker" / "superset"

if str(SUPERSET_DIR) not in sys.path:
    sys.path.insert(0, str(SUPERSET_DIR))

from asset_specs import (  # noqa: E402
    DELINQUENCY_COLUMNS,
    DELINQUENCY_METRICS,
    PORTFOLIO_COLUMNS,
    REPAYMENT_COLUMNS,
    adhoc_metric,
    sql_filter,
)


def test_adhoc_metric_returns_sql_expression_type():
    metric = adhoc_metric("PAR 30 %", "SUM(par_30_amount)")

    assert metric["expressionType"] == "SQL"
    assert metric["sqlExpression"] == "SUM(par_30_amount)"


def test_adhoc_metric_sets_custom_label():
    metric = adhoc_metric("PAR 30 %", "SUM(par_30_amount)")

    assert metric["label"] == "PAR 30 %"
    assert metric["hasCustomLabel"] is True


def test_sql_filter_uses_where_clause():
    result = sql_filter("bucket_key = -1")

    assert result["clause"] == "WHERE"
    assert result["expressionType"] == "SQL"
    assert result["sqlExpression"] == "bucket_key = -1"


def test_delinquency_columns_have_no_duplicate_names():
    names = [col["name"] for col in DELINQUENCY_COLUMNS]

    assert len(names) == len(set(names))


def test_portfolio_columns_have_no_duplicate_names():
    names = [col["name"] for col in PORTFOLIO_COLUMNS]

    assert len(names) == len(set(names))


def test_repayment_columns_have_no_duplicate_names():
    names = [col["name"] for col in REPAYMENT_COLUMNS]

    assert len(names) == len(set(names))


def test_repayment_columns_expose_client_hash_not_client_id():
    names = [col["name"] for col in REPAYMENT_COLUMNS]

    assert "client_hash" in names
    assert "client_id" not in names


def test_delinquency_metrics_reference_declared_columns():
    column_names = {col["name"] for col in DELINQUENCY_COLUMNS}

    for metric in DELINQUENCY_METRICS:
        for column_name in ("par_30_amount", "par_60_amount", "par_90_amount", "total_portfolio_amount"):
            if column_name in metric["expression"]:
                assert column_name in column_names


def test_delinquency_metrics_guard_against_division_by_zero():
    for metric in DELINQUENCY_METRICS:
        assert "NULLIF" in metric["expression"]
