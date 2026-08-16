#!/usr/bin/env python3
# Copyright 2026 The flink-gcp authors
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
"""Require every Java source to start with a complete approved header.

Apache RAT identifies a licence family from a distinctive substring.
That answers whether a file names an approved licence, but it cannot prove that
the surrounding notice is complete.
This check holds Java sources to either this project's canonical Apache-2.0
header or the canonical ASF header retained by copied Apache sources.

It also holds the *holder* named on the copyright line, which nothing else does:
apache-rat matches one distinctive licence line and never reads a holder, so
before this a file could name anyone and pass. That mattered when the project's
holder moved to the organisation across ~1,500 files — a sweep that missed one
would have left it naming the founder with every check green.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# The holder every file written for this project names. The artifacts declare the
# same one; see the licensing section of .agents/references/repository-guide.md.
PROJECT_HOLDER = "The flink-gcp authors"

# Holders preserved on files adapted from another project, which may not be
# rewritten. Each is recorded in the provenance section of its module's README —
# add an entry here only alongside that record, because this list is what stops
# an unattributed third-party header from passing as an ordinary one.
PRESERVED_HOLDERS = ("Google LLC",)

COPYRIGHT_HEADER = f"""/*
 * Copyright 2026 {PROJECT_HOLDER}
"""

APACHE_HEADER_SUFFIX = """ *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"""

PROJECT_HEADER = COPYRIGHT_HEADER + APACHE_HEADER_SUFFIX

# Any four-digit year, because a preserved header carries the year it was
# adapted from rather than this project's inception, and the holder is what this
# pins. Built from the constants above so the two cannot drift.
_HOLDERS = "|".join(
    re.escape(holder) for holder in (PROJECT_HOLDER, *PRESERVED_HOLDERS)
)
COPYRIGHT_LINE = rf"/\*\n \* Copyright \d{{4}} (?:{_HOLDERS})\n"

ASF_HEADER = """/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
"""


def java_sources(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*.java")
        if "target" not in path.relative_to(root).parts
        and not any(part.startswith(".") for part in path.relative_to(root).parts)
    )


def invalid_headers(root: Path) -> list[Path]:
    invalid = []
    for path in java_sources(root):
        contents = path.read_text(encoding="utf-8")
        copyright_header = re.match(COPYRIGHT_LINE, contents)
        complete_copyright_header = bool(
            copyright_header
            and contents.startswith(APACHE_HEADER_SUFFIX, copyright_header.end())
        )
        if not (complete_copyright_header or contents.startswith(ASF_HEADER)):
            invalid.append(path.relative_to(root))
    return invalid


# Paths this project does not write the headers of. Two kinds: third-party
# licence texts shipped inside the SQL uber-jars, which carry their own
# projects' copyright lines by law and are pinned by content hash in
# scripts/config/licence-sources.toml; and build output, which contains vendored
# bundles like mermaid.min.js.
#
# Matched as path prefixes rather than as bare directory names, because
# `resources` and `api` are also ordinary source directories — `src/main/resources`
# holds files this project does write. The build directories are named explicitly
# rather than left to the line budget below: measured, mermaid's copyright sits
# 3.1 MB into one minified line, so it passes today for a reason that would
# evaporate the day a bundler emitted a banner instead.
UNOWNED_PREFIXES = (
    "docs/public/",
    "docs/resources/",
    "docs/static/api/",
    "node_modules/",
)
# Any Maven module's build output, at any depth, and the licence texts each SQL
# module ships.
UNOWNED_PARTS = ("target",)
UNOWNED_SUBPATH = "META-INF/licenses/"

# A copyright line in any comment syntax: the prefix is `#`, `//`, ` *`, `<!--`
# or nothing at all depending on the file type, and only the holder is checked.
ANY_COPYRIGHT_LINE = re.compile(
    r"^.{0,8}Copyright (?:\(C\) )?\d{4}(?:-\d{4})? (?P<holder>.+?)\s*(?:-->)?$",
    re.MULTILINE,
)

# The header sits at the top of a file; scanning further would reach prose that
# quotes a licence rather than carries one.
HEADER_LINES = 20


def owned_files(root: Path) -> list[Path]:
    owned = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        as_posix = relative.as_posix()
        if as_posix.startswith(UNOWNED_PREFIXES) or UNOWNED_SUBPATH in as_posix:
            continue
        if any(part in UNOWNED_PARTS for part in relative.parts):
            continue
        # Dot directories are tool state — .git, .venv, .mvn's local repository —
        # except the two this project writes headers into.
        if any(
            part.startswith(".") and part not in (".agents", ".github")
            for part in relative.parts
        ):
            continue
        owned.append(path)
    return sorted(owned)


def unapproved_holders(root: Path) -> list[tuple[Path, str]]:
    """Every file this project owns whose header names an unapproved holder.

    The structural check above reads Java only, but the holder moved across every
    file type at once — markdown, Python, OpenTofu, POMs, workflows. A sweep that
    missed one of those would otherwise be invisible, which is the whole failure
    this exists to make loud.
    """
    approved = {PROJECT_HOLDER, *PRESERVED_HOLDERS}
    offenders = []
    for path in owned_files(root):
        try:
            head = "".join(path.open(encoding="utf-8").readlines()[:HEADER_LINES])
        except (UnicodeDecodeError, OSError):
            continue
        for match in ANY_COPYRIGHT_LINE.finditer(head):
            holder = match.group("holder")
            if holder not in approved:
                offenders.append((path.relative_to(root), holder))
    return offenders


def main() -> int:
    invalid = invalid_headers(ROOT)
    offenders = unapproved_holders(ROOT)
    if not invalid and not offenders:
        return 0
    if invalid:
        print(
            "Java files must start with a complete copyright-bearing or ASF "
            "Apache-2.0 header:"
        )
        for path in invalid:
            print(f"  {path}")
    if offenders:
        print("Files naming a copyright holder this project has not recorded:")
        for path, holder in offenders:
            print(f"  {path}: {holder}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
