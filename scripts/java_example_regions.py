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
"""Shared parsing for tagged Java documentation-example regions."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from java_ast import JavaSource

START_TAG = re.compile(r"^\s*// tag::(?P<tag>[a-z0-9-]+)\[\]\s*$")
END_TAG = re.compile(r"^\s*// end::(?P<tag>[a-z0-9-]+)\[\]\s*$")
ANY_SOURCE_TAG = re.compile(r"^\s*//\s*(?:tag|end)::")


@dataclass(frozen=True)
class SourceRegion:
    path: Path
    tag: str
    line: int
    text: str


def line_at(source: str, offset: int) -> int:
    return source.count("\n", 0, offset) + 1


def collect_source_regions(
    source_root: Path,
    relative: Callable[[Path], str],
    *,
    require_tagged_files: bool = False,
    tagged_kind: str = "documentation",
) -> tuple[dict[tuple[str, str], SourceRegion], list[str]]:
    """Index exact tagged regions by unique backing file name and tag."""
    if not source_root.is_dir():
        return {}, [f"{relative(source_root)} does not exist."]

    files = sorted(source_root.rglob("*.java"))
    if not files:
        return {}, [f"{relative(source_root)} contains no Java sources."]

    regions: dict[tuple[str, str], SourceRegion] = {}
    problems: list[str] = []
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
        parsed = JavaSource.parse(relative(path), source)
        lines = source.splitlines()
        markers: dict[str, dict[str, list[int]]] = {}
        for node in parsed.nodes("line_comment"):
            line_number = parsed.line(node)
            comment = parsed.text(node)
            standalone = lines[line_number - 1].strip() == comment.strip()
            match = START_TAG.fullmatch(comment) if standalone else None
            kind = "start"
            if match is None:
                match = END_TAG.fullmatch(comment) if standalone else None
                kind = "end"
            if match is not None:
                markers.setdefault(match.group("tag"), {"start": [], "end": []})[
                    kind
                ].append(line_number)
            elif ANY_SOURCE_TAG.match(comment):
                problems.append(
                    f"{relative(path)}:{line_number}: malformed source tag "
                    f"`{comment.strip()}`."
                )

        if require_tagged_files and not markers:
            problems.append(
                f"{relative(path)} contains no tagged {tagged_kind} example regions; "
                "delete the stale source or add exact tag markers."
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
            content_lines = lines[start : end - 1]
            non_blank_indents = [
                line[: len(line) - len(line.lstrip(" \t"))]
                for line in content_lines
                if line.strip()
            ]
            common_indent = non_blank_indents[0] if non_blank_indents else ""
            for indentation in non_blank_indents[1:]:
                while common_indent and not indentation.startswith(common_indent):
                    common_indent = common_indent[:-1]
            content = "\n".join(
                line.removeprefix(common_indent) if line.strip() else line
                for line in content_lines
            )
            if not content.strip():
                problems.append(f"{where} has an empty tagged region.")
                continue
            key = (path.name, tag)
            if key in regions:
                continue
            regions[key] = SourceRegion(path, tag, start + 1, content)
    return regions, problems
