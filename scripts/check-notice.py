#!/usr/bin/env python3
#
# Copyright 2026 laughingman7743
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Check a shaded module's hand-written META-INF/NOTICE against what Maven resolved.

The NOTICE of an uber-jar is a legal statement, so it stays hand-written and
human-reviewed rather than generated. What is mechanical is whether it still
*matches* the bundle, and that is what this checks:

  1. every bundled artifact is listed, and nothing else is;
  2. each one is listed under the licence its own POM declares;
  3. every META-INF/licenses/ file the NOTICE points at exists;
  4. no licence file sits there unreferenced.

Only (1) had a guard before; a dependency changing its licence between versions
was invisible.

Deliberately not automated: the licence *texts* under META-INF/licenses/.
license-maven-plugin's download-licenses names files after the licence, so
several artifacts sharing one licence collapse into a single file and the last
download wins. In this bundle that put ThreeTen's copyright line in the file
covering protobuf, gax and google-auth as well — wrong, since the copyright
holder is part of a BSD or MIT text. Those files are collected by hand from each
project's own LICENSE.

Usage:  check-notice.py <module-directory>

Reads target/generated-sources/license/THIRD-PARTY.txt, which `just check-notice`
regenerates first. Standard library only, by design: nothing here needs a
dependency, and adding one would mean a package manager for a single script.
"""

import re
import sys
from pathlib import Path

# license-maven-plugin's normalised names (see licenseMerges in the root POM)
# mapped to a phrase that identifies the matching NOTICE paragraph. This table is
# the seam between machine output and human prose, so it is the one place a new
# licence group has to be registered — the check fails loudly rather than
# silently skipping if a group appears that is not here.
LICENCE_GROUPS = {
    "Apache-2.0": "Apache Software License 2.0",
    "BSD-3-Clause": "BSD 3-Clause",
    "Go License": "Go License",
    "MIT license": "MIT License",
    "CDDL + GPLv2 with classpath exception": "CDDL 1.0",
}

# `    (Licence Name) Artifact Description (groupId:artifactId:version - url)`
THIRD_PARTY_LINE = re.compile(
    r"^\s+\((?P<licence>.+?)\)\s+.*\((?P<ga>[\w.\-]+:[\w.\-]+):(?P<version>[\w.+\-]+)\s"
)

NOTICE_HEADING = "This project bundles"
NOTICE_BULLET = "- "


def read_resolved(module: Path) -> dict[str, str]:
    """Return {groupId:artifactId:version -> licence} from license-maven-plugin."""
    report = module / "target" / "generated-sources" / "license" / "THIRD-PARTY.txt"
    if not report.is_file():
        sys.exit(
            f"{report} is missing. Run `just check-notice {module.name}`, which "
            f"regenerates it before calling this script."
        )
    resolved = {}
    for line in report.read_text(encoding="utf-8").splitlines():
        match = THIRD_PARTY_LINE.match(line)
        if match:
            gav = f"{match['ga']}:{match['version']}"
            resolved[gav] = match["licence"]
    if not resolved:
        sys.exit(f"Parsed no artifacts out of {report}; its format has changed.")
    return resolved


def read_notice(notice: Path) -> tuple[dict[str, str], dict[str, str]]:
    """Return ({gav -> licence}, {gav -> licence file}) from a hand-written NOTICE."""
    listed: dict[str, str] = {}
    pointers: dict[str, str] = {}
    group = None
    for line in notice.read_text(encoding="utf-8").splitlines():
        if line.startswith(NOTICE_HEADING):
            group = next(
                (name for name, phrase in LICENCE_GROUPS.items() if phrase in line),
                None,
            )
            if group is None:
                sys.exit(
                    f"{notice} has a licence paragraph this script does not know:\n"
                    f"  {line}\nAdd it to LICENCE_GROUPS in {Path(__file__).name}."
                )
        elif line.startswith(NOTICE_BULLET) and group:
            # `- groupId:artifactId:version` optionally followed by `(path)`.
            fields = line[len(NOTICE_BULLET) :].split()
            listed[fields[0]] = group
            if len(fields) > 1:
                pointers[fields[0]] = fields[1].strip("()")
    return listed, pointers


def report(title: str, entries: list[str]) -> bool:
    if entries:
        print(f"\n{title}", file=sys.stderr)
        for entry in sorted(entries):
            print(f"  {entry}", file=sys.stderr)
    return bool(entries)


def main() -> int:
    if len(sys.argv) != 2:
        sys.exit(f"usage: {Path(__file__).name} <module-directory>")

    module = Path(sys.argv[1])
    notice = module / "src" / "main" / "resources" / "META-INF" / "NOTICE"
    licence_dir = notice.parent / "licenses"
    if not notice.is_file():
        sys.exit(f"{notice} does not exist.")

    resolved = read_resolved(module)
    listed, pointers = read_notice(notice)
    on_disk = {p.name for p in licence_dir.iterdir()} if licence_dir.is_dir() else set()

    failed = False
    failed |= report(
        "Bundled but missing from META-INF/NOTICE:", sorted(set(resolved) - set(listed))
    )
    failed |= report(
        "Listed in META-INF/NOTICE but not bundled:",
        sorted(set(listed) - set(resolved)),
    )
    failed |= report(
        "Listed under the wrong licence (POM says / NOTICE says):",
        [
            f"{gav}: {licence} / {listed[gav]}"
            for gav, licence in resolved.items()
            if gav in listed and licence != listed[gav]
        ],
    )
    failed |= report(
        "META-INF/NOTICE points at a licence file that does not exist:",
        [
            f"{gav} -> {path}"
            for gav, path in pointers.items()
            if Path(path).name not in on_disk
        ],
    )
    failed |= report(
        "Licence file present but referenced by nothing in META-INF/NOTICE:",
        sorted(on_disk - {Path(p).name for p in pointers.values()}),
    )

    if failed:
        print(
            f"\n{module.name}: META-INF/NOTICE does not match the resolved bundle.\n"
            "Fix the NOTICE, and for a newly added artifact confirm its licence "
            "against its own POM before grouping it — the generated "
            "META-INF/DEPENDENCIES lists licences pre-mediation, so its versions "
            "can differ from what actually resolves.",
            file=sys.stderr,
        )
        return 1

    print(
        f"{module.name}: {len(resolved)} bundled artifacts, "
        f"{len(on_disk)} licence files, all accounted for."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
