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
"""Validate source-backed Java examples in public Javadoc (issue #694).

Every ``<pre>{@code ...}</pre>`` block in a main Java source is classified by
one marker immediately above it:

* ``<!-- javadoc-example file="Example.java" tag="example" -->`` maps runnable
  guidance to an exact tagged region under ``JAVADOC_SOURCES``;
* ``<!-- javadoc-example partial="reason" -->`` exempts an intentionally
  abbreviated fragment whose preceding prose says ``Abbreviated, not compiled``.

The runnable display and source may differ only by their container indentation.
The Java compiler checks the tagged source through ``just check-doc-snippets``;
this script holds the displayed copy to that source and makes every current or
future block choose one of the two forms. It does not decide whether an example
is useful, complete enough for a reader, or correct at runtime.

Exit codes: 0 clean, 1 classification or synchronization violation, 2 missing
input or unreadable source tree.

Java comments are classified through the shared Tree-sitter parser.
"""

from __future__ import annotations

import difflib
import re
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path

from java_ast import JavaSource, JavaSyntaxError
from java_example_regions import (
    SourceRegion,
    collect_source_regions,
    line_at,
)

ROOT = Path(__file__).resolve().parent.parent
JAVADOC_SOURCES = (
    ROOT
    / "flink-connector-gcp-docs-validation"
    / "src/test/java/io/github/flink/gcp/connector/docs/javadoc"
)
MAIN_SOURCE_PATTERN = "flink-*/src/main/java*/**/*.java"

RUNNABLE_MARKER = re.compile(
    r'<!-- javadoc-example file="(?P<file>[^"]+)" tag="(?P<tag>[a-z0-9-]+)" -->'
)
PARTIAL_MARKER = re.compile(r'<!-- javadoc-example partial="(?P<reason>[^"]+)" -->')
ANY_MARKER = re.compile(r"<!--\s*javadoc-example\b.*?-->")
BLOCK = re.compile(r"<pre>\{@code(?P<body>.*?)\}</pre>", re.DOTALL)
VISIBLE_PARTIAL_LABEL = "<b>Abbreviated, not compiled:</b>"


@dataclass(frozen=True)
class LocatedText:
    path: Path
    line: int
    text: str


def relative(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def doc_comments(source: str, path: Path) -> list[LocatedText]:
    """Extract real Java doc comments, ignoring comment-shaped strings."""
    parsed = JavaSource.parse(relative(path), source)
    return [
        LocatedText(
            path,
            parsed.line(node),
            parsed.text(node)[3:-2],
        )
        for node in parsed.nodes("block_comment")
        if parsed.text(node).startswith("/**")
    ]


def normalized_display(body: str) -> str:
    lines = body.splitlines()
    stripped = [re.sub(r"^[ \t]*\*[ ]?", "", line) for line in lines]
    return textwrap.dedent("\n".join(stripped)).strip()


def source_regions() -> tuple[dict[tuple[str, str], SourceRegion], list[str]]:
    return collect_source_regions(
        JAVADOC_SOURCES,
        relative,
        require_tagged_files=True,
        tagged_kind="Javadoc",
    )


def marker_before(comment: LocatedText, block: re.Match[str]):
    markers = list(ANY_MARKER.finditer(comment.text, 0, block.start()))
    if not markers:
        return None
    marker = markers[-1]
    between = comment.text[marker.end() : block.start()]
    if normalized_display(between):
        return None
    return marker


def validate() -> tuple[int, int, list[str], bool]:
    paths = sorted(ROOT.glob(MAIN_SOURCE_PATTERN))
    if not paths:
        return (
            0,
            0,
            [f"{MAIN_SOURCE_PATTERN} matched no Java sources under {ROOT}."],
            True,
        )

    try:
        regions, problems = source_regions()
    except JavaSyntaxError as error:
        return 0, 0, [str(error)], True
    infrastructure = not JAVADOC_SOURCES.is_dir() or not any(
        JAVADOC_SOURCES.rglob("*.java")
    )
    used: set[tuple[str, str]] = set()
    runnable = 0
    partial = 0

    for path in paths:
        source = path.read_text(encoding="utf-8")
        try:
            comments = doc_comments(source, path)
        except JavaSyntaxError as error:
            problems.append(str(error))
            infrastructure = True
            continue
        for comment in comments:
            blocks = list(BLOCK.finditer(comment.text))
            markers = list(ANY_MARKER.finditer(comment.text))
            consumed_markers: set[int] = set()
            previous_end = 0
            for block in blocks:
                block_line = comment.line + line_at(comment.text, block.start()) - 1
                where = f"{relative(path)}:{block_line}"
                marker = marker_before(comment, block)
                if marker is None:
                    problems.append(
                        f"{where}: Javadoc code block has no immediately preceding "
                        "javadoc-example marker."
                    )
                    previous_end = block.end()
                    continue
                consumed_markers.add(marker.start())
                marker_text = marker.group(0)
                runnable_match = RUNNABLE_MARKER.fullmatch(marker_text)
                partial_match = PARTIAL_MARKER.fullmatch(marker_text)
                if runnable_match is not None:
                    runnable += 1
                    key = (runnable_match.group("file"), runnable_match.group("tag"))
                    region = regions.get(key)
                    if region is None:
                        problems.append(
                            f"{where}: marker names {key[0]} tag `{key[1]}`, but no valid "
                            "backing region has that identity."
                        )
                    elif key in used:
                        problems.append(
                            f"{where}: backing region {key[0]} tag `{key[1]}` is referenced "
                            "by more than one Javadoc block."
                        )
                    else:
                        used.add(key)
                        displayed = normalized_display(block.group("body"))
                        if displayed != region.text:
                            diff = "\n".join(
                                difflib.unified_diff(
                                    region.text.splitlines(),
                                    displayed.splitlines(),
                                    fromfile=f"{relative(region.path)}:{region.line}",
                                    tofile=where,
                                    lineterm="",
                                )
                            )
                            problems.append(
                                f"{where}: displayed code differs from {key[0]} tag "
                                f"`{key[1]}`:\n{diff}"
                            )
                elif partial_match is not None:
                    partial += 1
                    prose = comment.text[previous_end : marker.start()]
                    if VISIBLE_PARTIAL_LABEL not in prose:
                        problems.append(
                            f"{where}: partial example must be introduced by "
                            f"`{VISIBLE_PARTIAL_LABEL}` in the visible Javadoc."
                        )
                else:
                    problems.append(
                        f"{where}: malformed javadoc-example marker `{marker_text}`."
                    )
                previous_end = block.end()

            for marker in markers:
                if marker.start() not in consumed_markers:
                    marker_line = (
                        comment.line + line_at(comment.text, marker.start()) - 1
                    )
                    problems.append(
                        f"{relative(path)}:{marker_line}: javadoc-example marker is not "
                        "immediately followed by a code block."
                    )

    for key, region in sorted(regions.items()):
        if key not in used:
            problems.append(
                f"{relative(region.path)}:{region.line}: tag `{region.tag}` is not referenced "
                "by any Javadoc block; delete the stale region or add its marker."
            )

    return runnable, partial, problems, infrastructure


def main() -> int:
    try:
        runnable, partial, problems, infrastructure = validate()
    except OSError as error:
        print(f"Could not read Javadoc validation inputs: {error}", file=sys.stderr)
        return 2

    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        print(
            "See .agents/skills/maintain-javadoc-examples/SKILL.md for the repair "
            "procedure.",
            file=sys.stderr,
        )
        return 2 if infrastructure else 1

    print(
        f"{runnable} runnable Javadoc examples are source-backed; "
        f"{partial} abbreviated examples."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
