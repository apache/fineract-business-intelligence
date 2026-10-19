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

import os
import re

_ENV = re.compile(r"^env:([A-Za-z_][A-Za-z0-9_]*)$")
_FILE = re.compile(r"^file:(/[^:]+):([A-Za-z0-9_.-]+)$")

DEFAULT_ENV_ALLOWLIST = r"FBI_SRC_[A-Za-z0-9_]+"


class SecretError(ValueError):
    pass


def _env_allowed(name: str) -> bool:
    pattern = os.getenv("FBI_SECRET_ENV_ALLOWLIST", DEFAULT_ENV_ALLOWLIST)
    return re.fullmatch(pattern, name) is not None


def _file_allowed(path: str) -> bool:
    base = os.getenv("FBI_SECRET_DIR")
    if not base:
        return False
    real_base = os.path.realpath(base)
    real_path = os.path.realpath(path)
    return os.path.commonpath([real_base, real_path]) == real_base


def check_reference(secret_ref: str | None) -> None:
    if not secret_ref:
        raise SecretError("No secret_ref configured for this source")
    m = _ENV.match(secret_ref)
    if m:
        if not _env_allowed(m.group(1)):
            raise SecretError(
                f"Environment variable {m.group(1)} is not allowed in a secret_ref "
                "(see FBI_SECRET_ENV_ALLOWLIST; source credentials should use the FBI_SRC_ prefix)"
            )
        return
    m = _FILE.match(secret_ref)
    if m:
        if not _file_allowed(m.group(1)):
            raise SecretError(f"Secret file {m.group(1)} is outside FBI_SECRET_DIR or FBI_SECRET_DIR is not set")
        return
    raise SecretError(f"Unsupported secret_ref format: {secret_ref.split(':', 1)[0]}:... (use env: or file:)")


def resolve(secret_ref: str | None) -> str:
    check_reference(secret_ref)
    m = _ENV.match(secret_ref)
    if m:
        value = os.getenv(m.group(1))
        if value is None:
            raise SecretError(f"Environment variable {m.group(1)} referenced by secret_ref is not set")
        return value
    path, key = _FILE.match(secret_ref).groups()
    with open(os.path.realpath(path), encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            k, v = stripped.split("=", 1)
            if k.strip() == key:
                return v.strip()
    raise SecretError(f"Key {key} not found in {path}")


def connect_placeholder(secret_ref: str | None) -> str:
    check_reference(secret_ref)
    m = _ENV.match(secret_ref)
    if m:
        return "${env:" + m.group(1) + "}"
    path, key = _FILE.match(secret_ref).groups()
    return "${file:" + os.path.realpath(path) + ":" + key + "}"
