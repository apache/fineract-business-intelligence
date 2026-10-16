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

import os


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable '{name}' is not set.")
    return value


SQLALCHEMY_DATABASE_URI = require_env("SUPERSET_METADATA_URI")
SECRET_KEY = require_env("SUPERSET_SECRET_KEY")

WTF_CSRF_ENABLED = True

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

ROW_LIMIT = int(os.getenv("SUPERSET_ROW_LIMIT", "5000"))

_CACHE_TIMEOUT = int(os.getenv("SUPERSET_CACHE_DEFAULT_TIMEOUT", "300"))
_REDIS_URL = os.getenv("SUPERSET_CACHE_REDIS_URL", "")
if _REDIS_URL:
    _cache = {
        "CACHE_TYPE": "RedisCache",
        "CACHE_DEFAULT_TIMEOUT": _CACHE_TIMEOUT,
        "CACHE_REDIS_URL": _REDIS_URL,
    }
else:
    _cache = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": _CACHE_TIMEOUT}
CACHE_CONFIG = {**_cache, "CACHE_KEY_PREFIX": "fbi_cache_"}
DATA_CACHE_CONFIG = {**_cache, "CACHE_KEY_PREFIX": "fbi_data_"}
FILTER_STATE_CACHE_CONFIG = {**_cache, "CACHE_KEY_PREFIX": "fbi_filter_"}
EXPLORE_FORM_DATA_CACHE_CONFIG = {**_cache, "CACHE_KEY_PREFIX": "fbi_explore_"}
SUPERSET_WEBSERVER_ADDRESS = "0.0.0.0"
SUPERSET_WEBSERVER_PORT = 8088
