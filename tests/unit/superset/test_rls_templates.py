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

from pathlib import Path

import jinja2
import pytest

DATASET_DIR = Path(__file__).resolve().parent.parent.parent.parent / "superset" / "datasets"

DATASET_FILES = [
    "delinquency_par_secure_all_dates.sql",
    "delinquency_par_secure_latest.sql",
    "portfolio_health_secure_all_dates.sql",
    "portfolio_health_secure_latest.sql",
    "repayment_behavior_secure_all_dates.sql",
    "repayment_behavior_secure_latest.sql",
]


def _render(dataset_file: str, username: str | None) -> str:
    template_source = (DATASET_DIR / dataset_file).read_text(encoding="utf-8")
    env = jinja2.Environment()
    env.globals["current_username"] = lambda: username
    return env.from_string(template_source).render()


@pytest.mark.parametrize("dataset_file", DATASET_FILES)
def test_dataset_never_falls_back_to_admin_when_username_is_missing(dataset_file):
    rendered = _render(dataset_file, None)

    assert "'admin'" not in rendered
    assert "'' != ''" in rendered


@pytest.mark.parametrize("dataset_file", DATASET_FILES)
def test_dataset_never_falls_back_to_admin_when_username_is_empty(dataset_file):
    rendered = _render(dataset_file, "")

    assert "'admin'" not in rendered
    assert "'' != ''" in rendered


@pytest.mark.parametrize("dataset_file", DATASET_FILES)
def test_dataset_escapes_embedded_single_quotes_in_username(dataset_file):
    rendered = _render(dataset_file, "x' OR '1'='1")

    assert "x'' OR ''1''=''1" in rendered


@pytest.mark.parametrize("dataset_file", DATASET_FILES)
def test_dataset_renders_a_normal_username_unescaped(dataset_file):
    rendered = _render(dataset_file, "north_manager")

    assert "north_manager" in rendered
    assert "'' != ''" not in rendered
