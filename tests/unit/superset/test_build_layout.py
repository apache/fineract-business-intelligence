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
import sys
from pathlib import Path

import pytest

SUPERSET_DIR = Path(__file__).resolve().parent.parent.parent.parent / "docker" / "superset"

if str(SUPERSET_DIR) not in sys.path:
    sys.path.insert(0, str(SUPERSET_DIR))

from asset_specs import build_layout  # noqa: E402


class FakeChart:
    def __init__(self, chart_id: int, slice_name: str) -> None:
        self.id = chart_id
        self.slice_name = slice_name


def test_build_layout_returns_valid_json():
    lookup = {"KPI A": FakeChart(1, "KPI A")}
    row_specs = [{"id": "ROW-1", "charts": ["KPI A"]}]

    result = build_layout(row_specs, lookup)

    json.loads(result)


def test_build_layout_wires_root_and_grid():
    lookup = {"KPI A": FakeChart(1, "KPI A")}
    row_specs = [{"id": "ROW-1", "charts": ["KPI A"]}]

    position = json.loads(build_layout(row_specs, lookup))

    assert position["ROOT_ID"]["children"] == ["GRID_ID"]
    assert position["GRID_ID"]["parents"] == ["ROOT_ID"]
    assert "ROW-1" in position["GRID_ID"]["children"]


def test_build_layout_includes_every_chart_in_row_specs():
    lookup = {
        "Chart A": FakeChart(1, "Chart A"),
        "Chart B": FakeChart(2, "Chart B"),
    }
    row_specs = [{"id": "ROW-1", "charts": ["Chart A", "Chart B"]}]

    position = json.loads(build_layout(row_specs, lookup))

    assert "CHART-explore-1" in position
    assert "CHART-explore-2" in position


def test_build_layout_component_lists_its_row_in_parents():
    lookup = {"Chart A": FakeChart(1, "Chart A")}
    row_specs = [{"id": "ROW-1", "charts": ["Chart A"]}]

    position = json.loads(build_layout(row_specs, lookup))

    assert position["CHART-explore-1"]["parents"] == ["ROOT_ID", "GRID_ID", "ROW-1"]


def test_build_layout_chart_meta_carries_id_and_name():
    lookup = {"Chart A": FakeChart(42, "Chart A")}
    row_specs = [{"id": "ROW-1", "charts": ["Chart A"]}]

    position = json.loads(build_layout(row_specs, lookup))

    assert position["CHART-explore-42"]["meta"]["chartId"] == 42
    assert position["CHART-explore-42"]["meta"]["sliceName"] == "Chart A"


def test_build_layout_uses_row_default_size_when_no_override():
    lookup = {"Chart A": FakeChart(1, "Chart A")}
    row_specs = [{"id": "ROW-1", "charts": ["Chart A"], "default_width": 6, "default_height": 42}]

    position = json.loads(build_layout(row_specs, lookup))

    assert position["CHART-explore-1"]["meta"]["width"] == 6
    assert position["CHART-explore-1"]["meta"]["height"] == 42


def test_build_layout_chart_sizes_override_default_size():
    lookup = {"Chart A": FakeChart(1, "Chart A")}
    row_specs = [{
        "id": "ROW-1",
        "charts": ["Chart A"],
        "default_width": 6,
        "default_height": 42,
        "chart_sizes": {"Chart A": {"width": 12, "height": 20}},
    }]

    position = json.loads(build_layout(row_specs, lookup))

    assert position["CHART-explore-1"]["meta"]["width"] == 12
    assert position["CHART-explore-1"]["meta"]["height"] == 20


def test_build_layout_falls_back_to_default_width_of_four():
    lookup = {"Chart A": FakeChart(1, "Chart A")}
    row_specs = [{"id": "ROW-1", "charts": ["Chart A"]}]

    position = json.loads(build_layout(row_specs, lookup))

    assert position["CHART-explore-1"]["meta"]["width"] == 4
    assert position["CHART-explore-1"]["meta"]["height"] == 36


def test_build_layout_multiple_rows_each_register_in_grid_children():
    lookup = {
        "Chart A": FakeChart(1, "Chart A"),
        "Chart B": FakeChart(2, "Chart B"),
    }
    row_specs = [
        {"id": "ROW-1", "charts": ["Chart A"]},
        {"id": "ROW-2", "charts": ["Chart B"]},
    ]

    position = json.loads(build_layout(row_specs, lookup))

    assert position["GRID_ID"]["children"] == ["ROW-1", "ROW-2"]


def test_build_layout_raises_when_chart_name_not_in_lookup():
    lookup = {"Chart A": FakeChart(1, "Chart A")}
    row_specs = [{"id": "ROW-1", "charts": ["Missing Chart"]}]

    with pytest.raises(KeyError):
        build_layout(row_specs, lookup)
