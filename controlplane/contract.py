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
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

_DEFAULT_CONTRACT = Path(__file__).resolve().parent.parent / "contracts" / "fineract_tables.json"


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str
    cdc: tuple[str, ...] = ()
    synthetic: bool = False
    pii: bool = False

    @property
    def cdc_candidates(self) -> tuple[str, ...]:
        return self.cdc or (self.name,)


@dataclass(frozen=True)
class TableContract:
    table_name: str
    raw_table: str
    primary_key: str
    columns: tuple[ColumnSpec, ...]
    python_cursor: str | None
    kafka_partitions: int
    change_entity: str | None = None
    indexes: tuple[tuple[str, ...], ...] = field(default_factory=tuple)

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    @property
    def business_columns(self) -> tuple[ColumnSpec, ...]:
        return tuple(c for c in self.columns if not c.synthetic)

    def column(self, name: str) -> ColumnSpec:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"{self.table_name} has no column {name}")

    @property
    def cursor_is_usable(self) -> bool:
        return self.python_cursor is not None and not self.column(self.python_cursor).synthetic


@dataclass(frozen=True)
class Contract:
    version: int
    topic_prefix: str
    source_schema: str
    tables: tuple[TableContract, ...]

    def table(self, table_name: str) -> TableContract:
        for t in self.tables:
            if t.table_name == table_name:
                return t
        raise KeyError(f"Unknown contract table {table_name}")

    @property
    def table_names(self) -> tuple[str, ...]:
        return tuple(t.table_name for t in self.tables)

    def topic_for(self, table_name: str) -> str:
        return f"{self.topic_prefix}.{table_name}"


def parse_contract(raw: dict) -> Contract:
    tables = []
    for t in raw["tables"]:
        columns = tuple(
            ColumnSpec(
                name=c["name"],
                type=c["type"],
                cdc=tuple(c.get("cdc", ())),
                synthetic=bool(c.get("synthetic", False)),
                pii=bool(c.get("pii", False)),
            )
            for c in t["columns"]
        )
        change = t.get("change_tracking") or {}
        tables.append(
            TableContract(
                table_name=t["table_name"],
                raw_table=t["raw_table"],
                primary_key=t["primary_key"],
                columns=columns,
                python_cursor=t.get("python_cursor"),
                kafka_partitions=int(t.get("kafka_partitions", 1)),
                change_entity=change.get("entity"),
                indexes=tuple(tuple(i) for i in t.get("indexes", ())),
            )
        )
    return Contract(
        version=int(raw["contract_version"]),
        topic_prefix=raw.get("topic_prefix", "fineract.cdc"),
        source_schema=raw.get("source_schema", "public"),
        tables=tuple(tables),
    )


def load_contract(path: str | Path | None = None) -> Contract:
    resolved = Path(path or os.getenv("FBI_CONTRACT_PATH") or _DEFAULT_CONTRACT)
    return _cached_contract(str(resolved))


@lru_cache(maxsize=4)
def _cached_contract(path: str) -> Contract:
    with open(path, encoding="utf-8") as handle:
        return parse_contract(json.load(handle))
