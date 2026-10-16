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
import re

_ENV = re.compile(r"^env:([A-Za-z_][A-Za-z0-9_]*)$")
_FILE = re.compile(r"^file:(/[^:]+):([A-Za-z0-9_.-]+)$")


class SecretError(ValueError):
    pass


def resolve(secret_ref: str | None) -> str:
    if not secret_ref:
        raise SecretError("No secret_ref configured for this source")
    m = _ENV.match(secret_ref)
    if m:
        value = os.getenv(m.group(1))
        if value is None:
            raise SecretError(f"Environment variable {m.group(1)} referenced by secret_ref is not set")
        return value
    m = _FILE.match(secret_ref)
    if m:
        path, key = m.groups()
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    continue
                k, v = stripped.split("=", 1)
                if k.strip() == key:
                    return v.strip()
        raise SecretError(f"Key {key} not found in {path}")
    raise SecretError(f"Unsupported secret_ref format: {secret_ref.split(':', 1)[0]}:...")


def connect_placeholder(secret_ref: str | None) -> str:
    if not secret_ref:
        raise SecretError("No secret_ref configured for this source")
    m = _ENV.match(secret_ref)
    if m:
        return "${env:" + m.group(1) + "}"
    m = _FILE.match(secret_ref)
    if m:
        return "${file:" + m.group(1) + ":" + m.group(2) + "}"
    raise SecretError(f"Unsupported secret_ref format: {secret_ref.split(':', 1)[0]}:...")
