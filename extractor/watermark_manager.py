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

from datetime import datetime

from pg8000.dbapi import Connection


class WatermarkManager:
    def __init__(self, warehouse_conn: Connection, tenant_id: str) -> None:
        self._conn = warehouse_conn
        self._tenant_id = tenant_id

    def get(self, table_name: str) -> datetime | None:
        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT last_cursor_value
            FROM meta.watermarks
            WHERE tenant_id = %s
              AND table_name = %s
            """,
            (self._tenant_id, table_name),
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def update(self, table_name: str, cursor_column: str, last_cursor_value: datetime | None) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            """
            INSERT INTO meta.watermarks (
                tenant_id,
                table_name,
                cursor_column,
                last_cursor_value,
                updated_at
            )
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (tenant_id, table_name)
            DO UPDATE SET
                cursor_column = EXCLUDED.cursor_column,
                last_cursor_value = EXCLUDED.last_cursor_value,
                updated_at = NOW()
            """,
            (self._tenant_id, table_name, cursor_column, last_cursor_value),
        )
        self._conn.commit()
