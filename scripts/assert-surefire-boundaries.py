#!/usr/bin/env python3
#
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
"""Assert that a surefire report proves the named boundary tests ran.

The documentation SQL suites need this because a validation that silently
stopped running would leave every build green while the docs drift (the
reasoning lives on the ``check-doc-snippets`` recipe). The report is read as
XML rather than grepped: a first bash version of this check was defeated, in
review, by CDATA in ``system-out`` faking a second ``<testsuite>``, by
``<property name=...>`` entries satisfying a boundary name, and by regex
metacharacters in names — every one of which structural parsing removes.

The suite must be a single ``<testsuite>`` root that executed at least one
test and skipped none, and every required name must appear as a
``<testcase>`` name — exactly, or continuing as a parameterized case
(``name(...`` / ``name[...``), so a renamed test cannot keep satisfying its
old boundary.

Usage: assert-surefire-boundaries.py <surefire-report.xml> <test-name>...
"""

from __future__ import annotations

import sys
from pathlib import Path
from xml.etree import ElementTree


def boundary_error(report_xml: str, names: list[str]) -> str | None:
    """The refusal message for a report that proves too little, else None."""
    try:
        root = ElementTree.fromstring(report_xml)
    except ElementTree.ParseError as error:
        return f"the report is not well-formed XML: {error}"
    if root.tag != "testsuite":
        return (
            f"the report's root element is <{root.tag}>, not a single "
            "<testsuite>; the tests/skipped attributes would not describe one suite"
        )
    if int(root.get("tests", "0")) < 1:
        return "the report records no executed tests"
    if int(root.get("skipped", "0")) != 0:
        return "the report records skipped tests; a skipped boundary is a silent pass"
    case_names = [case.get("name", "") for case in root.iter("testcase")]
    for name in names:
        if not any(
            case == name or case.startswith((name + "(", name + "["))
            for case in case_names
        ):
            return f"required boundary test '{name}' is missing"
    return None


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(
            "usage: assert-surefire-boundaries.py <surefire-report.xml> <test-name>...",
            file=sys.stderr,
        )
        return 2
    report = Path(argv[0])
    if not report.is_file():
        print(
            f"assert-surefire-boundaries: {report} does not exist — did the suite run?",
            file=sys.stderr,
        )
        return 1
    error = boundary_error(report.read_text(encoding="utf-8"), argv[1:])
    if error:
        print(f"assert-surefire-boundaries: {error} ({report})", file=sys.stderr)
        return 1
    print(
        f"assert-surefire-boundaries: {report} carries all {len(argv) - 1} required boundaries"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
