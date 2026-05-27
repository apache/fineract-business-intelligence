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

from pg8000.dbapi import Connection


def check_replica_connectivity(source_conn: Connection) -> None:
    cursor = source_conn.cursor()
    cursor.execute("SELECT 1")
    cursor.fetchone()


def fetch_replica_lag_seconds(source_conn: Connection) -> int:
    cursor = source_conn.cursor()
    cursor.execute(
        """
        SELECT CASE
            WHEN pg_is_in_recovery() THEN
                COALESCE(EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp()))::INT, 0)
            ELSE 0
        END AS lag_seconds
        """
    )
    row = cursor.fetchone()
    return int(row[0] or 0)


def ensure_replica_safe(source_conn: Connection, threshold_seconds: int) -> int:
    check_replica_connectivity(source_conn)
    lag_seconds = fetch_replica_lag_seconds(source_conn)
    if lag_seconds > threshold_seconds:
        raise RuntimeError(
            f"Replica lag {lag_seconds}s exceeds configured threshold {threshold_seconds}s."
        )
    return lag_seconds
