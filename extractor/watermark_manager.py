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

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Watermark:
    cursor_value: datetime | None
    last_pk: int | None


class WatermarkManager:
    def __init__(self, warehouse_conn, source_id: str) -> None:
        self._conn = warehouse_conn
        self._source_id = source_id

    def get(self, table_name: str, target: str) -> Watermark | None:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT cursor_value, last_pk FROM meta.extract_watermarks "
            "WHERE source_id = %s AND table_name = %s AND target = %s",
            (self._source_id, table_name, target),
        )
        row = cursor.fetchone()
        return Watermark(row[0], row[1]) if row else None

    def update(self, table_name: str, target: str, cursor_value: datetime | None, last_pk: int | None) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            "INSERT INTO meta.extract_watermarks (source_id, table_name, target, cursor_value, last_pk, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, NOW()) "
            "ON CONFLICT (source_id, table_name, target) DO UPDATE "
            "SET cursor_value = EXCLUDED.cursor_value, last_pk = EXCLUDED.last_pk, updated_at = NOW()",
            (self._source_id, table_name, target, cursor_value, last_pk),
        )

    def reset(self, table_name: str, target: str) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            "DELETE FROM meta.extract_watermarks WHERE source_id = %s AND table_name = %s AND target = %s",
            (self._source_id, table_name, target),
        )
