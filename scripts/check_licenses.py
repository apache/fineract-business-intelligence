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

import importlib.metadata
import re
import sys
from pathlib import Path

_CATEGORY_A: frozenset[str] = frozenset(
    {
        "apache",
        "apache-2.0",
        "apache software license",
        "mit",
        "mit license",
        "mit no attribution",
        "mit-0",
        "bsd",
        "bsd license",
        "bsd-2-clause",
        "bsd-3-clause",
        "3-clause bsd",
        "new bsd license",
        "modified bsd license",
        "simplified bsd license",
        "isc",
        "isc license",
        "python software foundation",
        "psf",
        "psf license",
        "psf-2.0",
        "python license",
        "mpl-2.0",
        "mozilla public license 2.0",
        "public domain",
        "unlicense",
        "the unlicense",
        "cc0",
        "zlib",
        "zlib/libpng",
        "artistic license 2.0",
        "w3c license",
    }
)

_CATEGORY_X: frozenset[str] = frozenset(
    {
        "gpl",
        "gnu general public license",
        "agpl",
        "gnu affero general public license",
        "lgpl",
        "gnu lesser general public license",
        "copyleft",
        "cc-by-sa",
        "cc-by-nc",
        "eupl",
        "european union public licence",
        "cddl",
        "common development and distribution license",
        "cpl",
        "common public license",
        "osl",
        "open software license",
        "non-commercial",
        "noncommercial",
    }
)

_NORM_RE = re.compile(r"[-_.]+")


def _normalize(name: str) -> str:
    return _NORM_RE.sub("-", name).lower()


def _collect_direct_deps() -> frozenset[str]:
    project_root = Path(__file__).resolve().parent.parent
    names: set[str] = set()
    for req_file in sorted(project_root.glob("**/requirements*.txt")):
        parts = req_file.parts
        if any(p.startswith(".") or p in ("venv", ".venv", "env") for p in parts):
            continue
        for line in req_file.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line or line.startswith(("#", "-", ".")):
                continue
            pkg = re.split(r"[>=<!;\s\[]", line)[0].strip()
            if pkg:
                names.add(_normalize(pkg))
    return frozenset(names)


def _license_from_classifiers(meta: importlib.metadata.PackageMetadata) -> str:
    classifiers: list[str] = meta.get_all("Classifier") or []
    license_tags = [c for c in classifiers if c.startswith("License ::")]
    if not license_tags:
        return ""
    parts = license_tags[0].split(" :: ")
    return parts[-1].strip() if len(parts) >= 2 else ""


def _classify(license_text: str) -> str:
    normalized = license_text.lower()
    for keyword in _CATEGORY_A:
        if keyword in normalized:
            return "SAFE"
    for keyword in _CATEGORY_X:
        if keyword in normalized:
            return "RESTRICTED"
    return "UNKNOWN"


def _scan_direct_deps(direct_dep_names: frozenset[str]) -> list[dict]:
    results: list[dict] = []
    for dist in importlib.metadata.distributions():
        meta = dist.metadata
        name: str | None = meta["Name"]
        if name is None:
            continue
        if _normalize(name) not in direct_dep_names:
            continue
        version: str = meta["Version"] or "unknown"
        license_str = (
            _license_from_classifiers(meta) or (meta["License"] or "")
        ).strip() or "UNKNOWN"
        results.append(
            {
                "package": name,
                "version": version,
                "license": license_str,
                "classification": _classify(license_str),
            }
        )
    return sorted(results, key=lambda r: r["package"].lower())


def _print_table(results: list[dict]) -> None:
    W = [36, 12, 46, 14]
    headers = ["Package", "Version", "License", "Status"]
    header_row = "  ".join(h.ljust(w) for h, w in zip(headers, W, strict=False))
    print(header_row)
    print("-" * len(header_row))
    marker_map = {"SAFE": "[OK]  ", "RESTRICTED": "[FAIL]", "UNKNOWN": "[WARN]"}
    for r in results:
        marker = marker_map.get(r["classification"], "[WARN]")
        print(
            "  ".join(
                [
                    r["package"][: W[0] - 1].ljust(W[0]),
                    r["version"][: W[1] - 1].ljust(W[1]),
                    r["license"][: W[2] - 1].ljust(W[2]),
                    f"{marker} {r['classification']}".ljust(W[3]),
                ]
            )
        )


def main() -> int:
    direct_deps = _collect_direct_deps()

    if not direct_deps:
        print("No requirements*.txt files found — nothing to check.")
        return 0

    print(f"Checking {len(direct_deps)} direct dependenc(ies): {', '.join(sorted(direct_deps))}")
    print()

    results = _scan_direct_deps(direct_deps)

    if not results:
        print("None of the declared dependencies are currently installed.")
        return 0

    _print_table(results)
    print()

    restricted = [r for r in results if r["classification"] == "RESTRICTED"]
    unknown = [r for r in results if r["classification"] == "UNKNOWN"]
    safe = [r for r in results if r["classification"] == "SAFE"]

    if restricted:
        print(f"[FAIL] {len(restricted)} Category X (restricted) license(s) detected:")
        for r in restricted:
            print(f"   {r['package']} {r['version']}  ->  {r['license']}")
        print()
        print("   These licenses are forbidden in Apache releases.")
        print("   See: https://www.apache.org/legal/resolved.html#category-x")
        return 1

    if unknown:
        print(f"[WARN] {len(unknown)} package(s) with unrecognized license metadata:")
        for r in unknown:
            print(f"   {r['package']} {r['version']}  ->  {r['license']}")
        print()
        print("   Verify each against https://www.apache.org/legal/resolved.html")
        print("   If safe, add the license string to _CATEGORY_A in this script.")
        print()

    print(
        f"[PASS] {len(safe)} safe  |  {len(unknown)} unrecognized  |  "
        f"0 restricted  ({len(results)} total)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
