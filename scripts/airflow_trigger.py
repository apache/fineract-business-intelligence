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

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = os.getenv("AIRFLOW_URL", "http://localhost:8080").rstrip("/")


def _call(method: str, path: str, token: str | None = None, body: dict | None = None) -> dict:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(BASE + path, method=method, headers=headers,
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"{method} {path} -> {exc.code}: {exc.read().decode(errors='replace')[:500]}") from exc


def token() -> str:
    return _call("POST", "/auth/token", body={
        "username": os.getenv("AIRFLOW_ADMIN_USERNAME", "admin"),
        "password": os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin_dev_only"),
    })["access_token"]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("dag_id")
    p.add_argument("--conf", default="{}")
    p.add_argument("--wait", action="store_true")
    p.add_argument("--timeout", type=int, default=900)
    args = p.parse_args()

    t = token()
    _call("PATCH", f"/api/v2/dags/{args.dag_id}", t, {"is_paused": False})
    run = _call("POST", f"/api/v2/dags/{args.dag_id}/dagRuns", t,
                {"logical_date": None, "conf": json.loads(args.conf)})
    run_id = run["dag_run_id"]
    print(f"triggered {args.dag_id} run {run_id}")
    if not args.wait:
        return 0
    deadline = time.time() + args.timeout
    state = run.get("state")
    while time.time() < deadline:
        state = _call("GET", f"/api/v2/dags/{args.dag_id}/dagRuns/{run_id}", t)["state"]
        if state in ("success", "failed"):
            break
        time.sleep(5)
    tis = _call("GET", f"/api/v2/dags/{args.dag_id}/dagRuns/{run_id}/taskInstances", t)
    for ti in tis.get("task_instances", []):
        print(f"  {ti['task_id']}{'[' + str(ti['map_index']) + ']' if ti.get('map_index', -1) >= 0 else ''}: "
              f"{ti['state']}")
        if ti["state"] == "success" and ti.get("map_index", -1) < 0:
            xcom = _call("GET", f"/api/v2/dags/{args.dag_id}/dagRuns/{run_id}/taskInstances/{ti['task_id']}"
                         "/xcomEntries/return_value?deserialize=true", t) if ti["task_id"] else {}
            value = xcom.get("value")
            if value not in (None, "", "None"):
                print("    -> " + (json.dumps(value, default=str) if not isinstance(value, str) else value)[:600])
    print(f"run state: {state}")
    return 0 if state == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
