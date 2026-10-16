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
import logging
import os
import sys

from extractor.extractor import MODES, run_sources


def parse_shard(value: str) -> tuple[int, int]:
    try:
        index, count = (int(p) for p in value.split("/", 1))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("shard must look like INDEX/COUNT, e.g. 0/8") from exc
    if count < 1 or not 0 <= index < count:
        raise argparse.ArgumentTypeError("shard index must be in [0, COUNT)")
    return index, count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fineract BI Python ingestion engine")
    parser.add_argument("command", choices=MODES, help="backfill | incremental | reconcile (delete detection)")
    parser.add_argument("--source-id", action="append", dest="source_ids", help="Limit to source(s); repeatable")
    parser.add_argument("--shard", type=parse_shard, help="Process only registry shard INDEX/COUNT")
    parser.add_argument("--table", action="append", dest="tables", help="Limit to table(s); repeatable")
    return parser


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    results = run_sources(
        args.command,
        source_ids=args.source_ids,
        shard=args.shard,
        tables=set(args.tables) if args.tables else None,
    )
    print(json.dumps(results, indent=2, default=str))
    return 1 if any(r["status"] == "failed" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
