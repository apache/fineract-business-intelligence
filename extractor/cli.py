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

from extractor.config import AppConfig
from extractor.extractor import FineractExtractor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fineract BI Extractor")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("backfill", help="Run a full historical backfill")
    subparsers.add_parser("incremental", help="Run an incremental extraction")
    return parser


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> None:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()

    config = AppConfig.from_env()
    extractor = FineractExtractor(config)

    result = extractor.run(mode=args.command)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
