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

from superset.app import create_app

from superset import security_manager

_BRANCH_USERS: list[dict] = [
    {
        "username": "north_manager",
        "firstname": "North",
        "lastname": "Manager",
        "email": "north_manager@example.com",
        "password_env": "SUPERSET_NORTH_MANAGER_PASSWORD",
        "role": "Gamma",
    },
    {
        "username": "south_manager",
        "firstname": "South",
        "lastname": "Manager",
        "email": "south_manager@example.com",
        "password_env": "SUPERSET_SOUTH_MANAGER_PASSWORD",
        "role": "Gamma",
    },
]


def _ensure_user(user_def: dict) -> None:
    if security_manager.find_user(username=user_def["username"]):
        return

    password = os.environ.get(user_def["password_env"])
    if not password:
        raise RuntimeError(
            f"Required environment variable '{user_def['password_env']}' is not set."
        )

    security_manager.add_user(
        username=user_def["username"],
        first_name=user_def["firstname"],
        last_name=user_def["lastname"],
        email=user_def["email"],
        role=security_manager.find_role(user_def["role"]),
        password=password,
    )
    print(f"[security] Created user: {user_def['username']}")


app = create_app()

with app.app_context():
    for branch_user in _BRANCH_USERS:
        _ensure_user(branch_user)
