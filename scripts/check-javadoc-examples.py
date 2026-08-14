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

Standard library only, like the other repository checkers.
"""

from __future__ import annotations

import difflib
import re
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path

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
START_TAG = re.compile(r"^\s*// tag::(?P<tag>[a-z0-9-]+)\[\]\s*$")
END_TAG = re.compile(r"^\s*// end::(?P<tag>[a-z0-9-]+)\[\]\s*$")
ANY_SOURCE_TAG = re.compile(r"^\s*//\s*(?:tag|end)::")


@dataclass(frozen=True)
class LocatedText:
    path: Path
    line: int
    text: str


@dataclass(frozen=True)
class SourceRegion:
    path: Path
    tag: str
    line: int
    text: str


def relative(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def line_at(source: str, offset: int) -> int:
    return source.count("\n", 0, offset) + 1


def _skip_quoted(source: str, start: int, quote: str) -> int:
    """Return the first offset after a Java string or character literal."""
    index = start + 1
    while index < len(source):
        if source[index] == "\\":
            index += 2
        elif source[index] == quote:
            return index + 1
        else:
            index += 1
    return len(source)


def _skip_text_block(source: str, start: int) -> int:
    index = start + 3
    while index < len(source):
        end = source.find('"""', index)
        if end < 0:
            return len(source)
        backslashes = 0
        cursor = end - 1
        while cursor >= 0 and source[cursor] == "\\":
            backslashes += 1
            cursor -= 1
        if backslashes % 2 == 0:
            return end + 3
        index = end + 3
    return len(source)


def doc_comments(source: str, path: Path) -> list[LocatedText]:
    """Extract real Java doc comments, ignoring comment-shaped strings."""
    comments: list[LocatedText] = []
    index = 0
    while index < len(source):
        if source.startswith('"""', index):
            index = _skip_text_block(source, index)
        elif source[index] == '"':
            index = _skip_quoted(source, index, '"')
        elif source[index] == "'":
            index = _skip_quoted(source, index, "'")
        elif source.startswith("//", index):
            newline = source.find("\n", index + 2)
            index = len(source) if newline < 0 else newline + 1
        elif source.startswith("/*", index):
            end = source.find("*/", index + 2)
            if end < 0:
                end = len(source) - 2
            if source.startswith("/**", index):
                comments.append(
                    LocatedText(path, line_at(source, index), source[index + 3 : end])
                )
            index = end + 2
        else:
            index += 1
    return comments


def line_comments(source: str) -> list[tuple[int, str]]:
    """Extract Java line comments outside strings, text blocks, and block comments."""
    comments: list[tuple[int, str]] = []
    index = 0
    while index < len(source):
        if source.startswith('"""', index):
            index = _skip_text_block(source, index)
        elif source[index] == '"':
            index = _skip_quoted(source, index, '"')
        elif source[index] == "'":
            index = _skip_quoted(source, index, "'")
        elif source.startswith("//", index):
            end = source.find("\n", index + 2)
            if end < 0:
                end = len(source)
            line_start = source.rfind("\n", 0, index) + 1
            comments.append((line_at(source, index), source[line_start:end]))
            index = end
        elif source.startswith("/*", index):
            end = source.find("*/", index + 2)
            index = len(source) if end < 0 else end + 2
        else:
            index += 1
    return comments


def normalized_display(body: str) -> str:
    lines = body.splitlines()
    stripped = [re.sub(r"^[ \t]*\*[ ]?", "", line) for line in lines]
    return textwrap.dedent("\n".join(stripped)).strip()


def normalized_source(lines: list[str]) -> str:
    return textwrap.dedent("\n".join(lines)).strip()


def source_regions() -> tuple[dict[tuple[str, str], SourceRegion], list[str]]:
    if not JAVADOC_SOURCES.is_dir():
        return {}, [f"{relative(JAVADOC_SOURCES)} does not exist."]

    regions: dict[tuple[str, str], SourceRegion] = {}
    problems: list[str] = []
    files = sorted(JAVADOC_SOURCES.rglob("*.java"))
    if not files:
        return {}, [f"{relative(JAVADOC_SOURCES)} contains no Java sources."]

    paths_by_name: dict[str, Path] = {}
    for path in files:
        if path.name in paths_by_name:
            problems.append(
                f"{relative(path)} duplicates backing file name `{path.name}` from "
                f"{relative(paths_by_name[path.name])}; file names must be unique."
            )
        else:
            paths_by_name[path.name] = path

        source = path.read_text(encoding="utf-8")
        lines = source.splitlines()
        markers: dict[str, dict[str, list[int]]] = {}
        for line_number, comment in line_comments(source):
            match = START_TAG.fullmatch(comment)
            kind = "start"
            if match is None:
                match = END_TAG.fullmatch(comment)
                kind = "end"
            if match is not None:
                markers.setdefault(match.group("tag"), {"start": [], "end": []})[
                    kind
                ].append(line_number)
            elif ANY_SOURCE_TAG.match(comment):
                problems.append(
                    f"{relative(path)}:{line_number}: malformed source tag `{comment.strip()}`."
                )

        if not markers:
            problems.append(
                f"{relative(path)} contains no tagged Javadoc example regions; delete the "
                "stale source or add exact tag markers."
            )

        for tag, positions in sorted(markers.items()):
            starts = positions["start"]
            ends = positions["end"]
            where = f"{relative(path)} tag `{tag}`"
            if len(starts) != 1 or len(ends) != 1:
                problems.append(
                    f"{where} needs exactly one start and one end marker; found "
                    f"{len(starts)} start and {len(ends)} end markers."
                )
                continue
            start, end = starts[0], ends[0]
            if end <= start:
                problems.append(f"{where} has its end marker before its start marker.")
                continue
            content = normalized_source(lines[start : end - 1])
            if not content:
                problems.append(f"{where} has an empty tagged region.")
                continue
            key = (path.name, tag)
            if key in regions:
                continue
            regions[key] = SourceRegion(path, tag, start + 1, content)
    return regions, problems


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

    regions, problems = source_regions()
    infrastructure = not JAVADOC_SOURCES.is_dir() or not any(
        JAVADOC_SOURCES.rglob("*.java")
    )
    used: set[tuple[str, str]] = set()
    runnable = 0
    partial = 0

    for path in paths:
        source = path.read_text(encoding="utf-8")
        for comment in doc_comments(source, path):
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
