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

import os
import ssl
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import pg8000.dbapi


@dataclass(frozen=True)
class PgConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    ssl_mode: str = "disable"
    application_name: str = "fbi-controlplane"

    def connect_kwargs(self) -> dict:
        kwargs = {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "application_name": self.application_name,
        }
        if self.ssl_mode in ("require", "verify-full"):
            ctx = ssl.create_default_context()
            if self.ssl_mode == "require":
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            kwargs["ssl_context"] = ctx
        return kwargs


def warehouse_config_from_env(application_name: str = "fbi-controlplane") -> PgConfig:
    return PgConfig(
        host=os.getenv("WAREHOUSE_DB_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_DB_PORT", "5434")),
        database=os.getenv("WAREHOUSE_DB_NAME", "analytics"),
        user=os.getenv("WAREHOUSE_DB_USER") or os.getenv("WAREHOUSE_LOADER_USER", "analytics_loader"),
        password=os.getenv("WAREHOUSE_DB_PASSWORD") or os.getenv("WAREHOUSE_LOADER_PASSWORD", ""),
        ssl_mode=os.getenv("WAREHOUSE_DB_SSL_MODE", "disable"),
        application_name=application_name,
    )


def connect(cfg: PgConfig) -> pg8000.dbapi.Connection:
    conn = pg8000.dbapi.connect(**cfg.connect_kwargs())
    conn.autocommit = False
    return conn


@contextmanager
def transaction(conn: pg8000.dbapi.Connection) -> Iterator[pg8000.dbapi.Cursor]:
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def rows_as_dicts(cursor) -> list[dict]:
    columns = [d[0] for d in cursor.description or ()]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
