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
    "MIT": "MIT License",
    "CDDL + GPLv2 with classpath exception": "CDDL 1.0",
}

# `    (Licence Name) Artifact Description (groupId:artifactId:version - url)`
THIRD_PARTY_LINE = re.compile(
    r"^\s+\((?P<licence>.+?)\)\s+.*\((?P<ga>[\w.\-]+:[\w.\-]+):(?P<version>[\w.+\-]+)\s"
)

# The report's own header: `Lists of 52 third-party dependencies.` Compared against
# the number of lines actually parsed, because a *partial* parse is the dangerous
# one — an artifact the regex cannot read is simply absent, and nothing then
# demands the NOTICE mention it. That is exactly the new-dependency case this
# script exists to catch, and it is the direction that would otherwise be silent
# (an artifact in the NOTICE but not parsed is loud already). A classifier in the
# coordinates is one real way to trip it.
THIRD_PARTY_COUNT = re.compile(r"^Lists of (?P<count>\d+) third-party dependencies")

NOTICE_HEADING = "This project bundles"
NOTICE_BULLET = "- "
# Marks a paragraph that promises a licence file for each of its entries.
NOTICE_PROMISES_FILES = "See bundled license files"


def read_resolved(module: Path) -> dict[str, str]:
    """Return {groupId:artifactId:version -> licence} from license-maven-plugin."""
    report = module / "target" / "generated-sources" / "license" / "THIRD-PARTY.txt"
    if not report.is_file():
        sys.exit(
            f"{report} is missing. Run `just check-notice {module.name}`, which "
            f"regenerates it before calling this script."
        )
    resolved = {}
    declared = None
    for line in report.read_text(encoding="utf-8").splitlines():
        header = THIRD_PARTY_COUNT.match(line)
        if header:
            declared = int(header["count"])
        match = THIRD_PARTY_LINE.match(line)
        if match:
            gav = f"{match['ga']}:{match['version']}"
            resolved[gav] = match["licence"]
    if declared is None:
        sys.exit(f"Found no dependency count in {report}; its format has changed.")
    if declared != len(resolved):
        sys.exit(
            f"{report} says {declared} dependencies but {len(resolved)} could be "
            f"parsed. Every unparsed one is an artifact nothing would require "
            f"META-INF/NOTICE to list, so this is a hard failure rather than a "
            f"partial check. Widen THIRD_PARTY_LINE in {Path(__file__).name}."
        )
    return resolved


class Notice:
    """What a hand-written META-INF/NOTICE claims."""

    def __init__(self) -> None:
        self.listed: dict[str, str] = {}  # gav -> licence group
        self.pointers: dict[str, str] = {}  # gav -> licence file path, as written
        self.duplicates: list[str] = []  # gav listed by more than one bullet
        self.missing_pointers: list[str] = []  # gav in a group that promises a file


def read_notice(notice: Path) -> Notice:
    """Parse a hand-written NOTICE into the claims this script can check."""
    parsed = Notice()
    group = None
    promises_file = False
    for line in notice.read_text(encoding="utf-8").splitlines():
        if line.startswith(NOTICE_HEADING):
            # All matches, not the first: a heading naming two licences this script
            # knows would otherwise be filed under whichever appears earlier in
            # LICENCE_GROUPS, silently.
            matches = [n for n, phrase in LICENCE_GROUPS.items() if phrase in line]
            if len(matches) != 1:
                sys.exit(
                    f"{notice} has a licence paragraph matching {len(matches)} known "
                    f"licences ({', '.join(matches) or 'none'}):\n  {line}\n"
                    f"Adjust LICENCE_GROUPS in {Path(__file__).name} so it matches "
                    f"exactly one."
                )
            group = matches[0]
            promises_file = False
        elif line.startswith(NOTICE_PROMISES_FILES):
            promises_file = True
        elif line.startswith(NOTICE_BULLET) and group:
            # `- groupId:artifactId:version` optionally followed by `(path)`.
            fields = line[len(NOTICE_BULLET) :].split()
            if not fields:
                sys.exit(f"{notice} has a bullet with no coordinate:\n  {line}")
            gav = fields[0]
            if gav in parsed.listed:
                parsed.duplicates.append(gav)
            parsed.listed[gav] = group
            if len(fields) > 1:
                parsed.pointers[gav] = fields[1].strip("()")
            elif promises_file:
                # The paragraph says "See bundled license files for details" and this
                # entry names none. The orphan check below cannot notice, because a
                # licence file shared with a sibling entry is still referenced.
                parsed.missing_pointers.append(gav)
    return parsed


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
    notice_claims = read_notice(notice)
    listed, pointers = notice_claims.listed, notice_claims.pointers
    # Pointers are jar paths (`META-INF/licenses/LICENSE.x`), so they resolve against
    # the resource root, not the licence directory. Checking the whole path rather
    # than the basename is what distinguishes META-INF/licenses/ from gRPC's own
    # META-INF/license/ — a confusion this NOTICE has a paragraph warning about, and
    # which a basename check cannot see.
    resource_root = notice.parent.parent
    # Regular files only, and no dotfiles: macOS drops a .DS_Store into any directory
    # Finder visits, which would otherwise be reported as an unreferenced licence file
    # and fail the check on a developer's machine but never in CI. The repository's
    # apache-rat configuration excludes `**/.*` for the same reason.
    on_disk = (
        {
            p.name
            for p in licence_dir.iterdir()
            if p.is_file() and not p.name.startswith(".")
        }
        if licence_dir.is_dir()
        else set()
    )

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
            if not (resource_root / path).is_file()
        ],
    )
    failed |= report(
        "Licence file present but referenced by nothing in META-INF/NOTICE:",
        sorted(on_disk - {Path(p).name for p in pointers.values()}),
    )
    failed |= report(
        "Listed under a licence that promises a file, but naming none:",
        notice_claims.missing_pointers,
    )
    failed |= report(
        "Listed by more than one bullet in META-INF/NOTICE:",
        notice_claims.duplicates,
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
