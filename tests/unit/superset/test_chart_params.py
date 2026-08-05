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

import re
import sys
from pathlib import Path

SUPERSET_DIR = Path(__file__).resolve().parent.parent.parent.parent / "docker" / "superset"
BOOTSTRAP_SOURCE = (SUPERSET_DIR / "bootstrap_superset_assets.py").read_text(encoding="utf-8")

if str(SUPERSET_DIR) not in sys.path:
    sys.path.insert(0, str(SUPERSET_DIR))


def _chart_block(chart_name: str) -> str:
    pattern = re.compile(
        r'ensure_chart\("' + re.escape(chart_name) + r'".*?\}, owner\)',
        re.DOTALL,
    )
    match = pattern.search(BOOTSTRAP_SOURCE)
    assert match is not None, f"chart {chart_name!r} not found in bootstrap_superset_assets.py"
    return match.group(0)


def test_collection_efficiency_kpi_multiplies_ratio_by_one_hundred():
    block = _chart_block("Collection Efficiency KPI")

    assert "({eff_expr})*100" in block


def test_collection_efficiency_kpi_uses_two_decimal_number_format():
    block = _chart_block("Collection Efficiency KPI")

    assert '"number_format": ",.2f"' in block


def test_par_kpis_have_non_empty_subheaders():
    for chart_name in ("PAR 30 KPI", "PAR 60 KPI", "PAR 90 KPI", "NPA Exposure KPI"):
        block = _chart_block(chart_name)
        match = re.search(r'"subheader":\s*"([^"]*)"', block)

        assert match is not None
        assert match.group(1).strip() != ""


def test_repayment_kpis_have_non_empty_subheaders():
    for chart_name in (
        "Collection Efficiency KPI",
        "Collected Amount KPI",
        "Repayment Transactions KPI",
        "Repaying Borrowers KPI",
    ):
        block = _chart_block(chart_name)
        match = re.search(r'"subheader":\s*"([^"]*)"', block)

        assert match is not None
        assert match.group(1).strip() != ""


def test_repaying_borrowers_kpi_counts_distinct_client_hash():
    block = _chart_block("Repaying Borrowers KPI")

    assert "COUNT(DISTINCT client_hash)" in block


def test_par_kpis_filter_to_all_portfolio_rollup():
    for chart_name in ("PAR 30 KPI", "PAR 60 KPI", "PAR 90 KPI", "NPA Exposure KPI"):
        block = _chart_block(chart_name)

        assert '"adhoc_filters": [ap]' in block
