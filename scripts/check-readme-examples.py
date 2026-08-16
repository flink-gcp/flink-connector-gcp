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
"""Validate source-backed Java examples in module README files (issue #705).

Every Java fenced code block in a ``flink-*/README.md`` chooses one marker on
the immediately preceding line:

* ``<!-- readme-example file="Example.java" tag="example" -->`` maps runnable
  guidance to an exact tagged region under ``SNIPPET_SOURCES``;
* ``<!-- readme-example partial="reason" -->`` classifies an intentionally
  abbreviated fragment whose preceding visible paragraph says
  ``Abbreviated, not compiled:`` and explains the omission.

The runnable display and source may differ only by their container indentation.
The Java compiler checks the tagged source through ``just check-doc-snippets``;
this script keeps the ordinary GitHub-rendered Markdown copy synchronized. It
does not decide whether an example is useful, complete enough, or correct at
runtime.

Exit codes: 0 clean, 1 classification or synchronization violation, 2 missing
input or unreadable source tree.
"""

from __future__ import annotations

import difflib
import re
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from java_example_regions import SourceRegion, collect_source_regions

ROOT = Path(__file__).resolve().parent.parent
SNIPPET_SOURCES = (
    ROOT
    / "flink-connector-gcp-docs-validation"
    / "src/test/java/io/github/flink/gcp/connector/docs"
)
README_PATTERN = "flink-*/README.md"

RUNNABLE_MARKER = re.compile(
    r'\s*<!-- readme-example file="(?P<file>[^"/]+)" '
    r'tag="(?P<tag>[a-z0-9-]+)" -->\s*'
)
PARTIAL_MARKER = re.compile(
    r'\s*<!-- readme-example partial="(?P<reason>[^"\n]+)" -->\s*'
)
ANY_MARKER = re.compile(r"<!--\s*readme-example\b.*?-->")
OPEN_FENCE = re.compile(r"^(?P<indent> {0,3})(?P<fence>`{3,}|~{3,})(?P<info>.*)$")
CONTAINER_FENCE = re.compile(
    r"^ {0,3}(?:(?:(?:> ?)|(?:[-+*]|\d{1,9}[.)]) {1,4}) {0,3})+"
    r"(?P<fence>`{3,}|~{3,})(?P<info>.*)$"
)
INDENTED_FENCE = re.compile(r"^(?P<indent> {4,})(?P<fence>`{3,}|~{3,})(?P<info>.*)$")
SPACED_FENCE = re.compile(r"^(?P<indent> +)(?P<fence>`{3,}|~{3,})(?P<info>.*)$")
INDENTED_CONTAINER_FENCE = re.compile(
    r"^(?P<indent> +)(?:(?:(?:> ?)|(?:[-+*]|\d{1,9}[.)]) {1,4}) {0,3})+"
    r"(?P<fence>`{3,}|~{3,})(?P<info>.*)$"
)
LIST_ITEM = re.compile(
    r"^(?P<indent> {0,3})(?P<marker>[-+*]|\d{1,9}[.)])(?P<spacing> {1,4})"
)
EMPTY_LIST_ITEM = re.compile(r"^(?P<indent> {0,3})(?P<marker>[-+*]|\d{1,9}[.)]) *$")
ATX_HEADING = re.compile(r"^ {0,3}#{1,6}(?:[ \t]+|$)")
BLOCK_QUOTE = re.compile(r"^ {0,3}>")
HTML_BLOCK = re.compile(r"^ {0,3}(?:<!--|<\?|<![A-Z]|<!\[CDATA\[)")
HTML_BLOCK_TAG = re.compile(
    r"^ {0,3}</?(?:address|article|aside|base|basefont|blockquote|body|caption|center|"
    r"col|colgroup|dd|details|dialog|dir|div|dl|dt|fieldset|figcaption|figure|footer|"
    r"form|frame|frameset|h[1-6]|head|header|hr|html|iframe|legend|li|link|main|menu|"
    r"menuitem|nav|noframes|ol|optgroup|option|p|param|pre|script|search|section|style|"
    r"summary|table|tbody|td|textarea|tfoot|th|thead|title|tr|track|ul)"
    r"(?:[ \t]|/?>|$)",
    re.IGNORECASE,
)
LIST_CONTAINER_STEP = re.compile(r"(?P<marker>[-+*]|\d{1,9}[.)]) {1,4}")
HTML_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
VISIBLE_PARTIAL_LABEL = "Abbreviated, not compiled:"


@dataclass(frozen=True)
class JavaFence:
    line: int
    marker_line: int
    marker: str | None
    text: str
    closed: bool
    nested: bool


def relative(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _is_java_info(info: str, fence_character: str) -> bool:
    if fence_character == "`" and "`" in info:
        return False
    words = info.strip().split()
    return bool(words) and words[0].lower() == "java"


def _is_valid_fence_opening(opening: re.Match[str]) -> bool:
    fence = opening.group("fence")
    return fence[0] != "`" or "`" not in opening.group("info")


def _fence_end(
    lines: list[str],
    index: int,
    fence: str,
    candidate_at: Callable[[str], re.Match[str] | None],
    boundary_at: Callable[[str], bool] | None = None,
) -> int:
    end = index + 1
    while end < len(lines):
        if boundary_at is not None and boundary_at(lines[end]):
            return end - 1
        candidate = candidate_at(lines[end])
        if (
            candidate is not None
            and candidate.group("fence")[0] == fence[0]
            and len(candidate.group("fence")) >= len(fence)
            and not candidate.group("info").strip()
        ):
            break
        end += 1
    return end


def _displayed_content(lines: list[str], opening: re.Match[str], end: int) -> str:
    indentation = len(opening.group("indent"))
    content: list[str] = []
    for line in lines[1:end]:
        removed = 0
        while removed < indentation and removed < len(line) and line[removed] == " ":
            removed += 1
        content.append(line[removed:])
    return "\n".join(content)


def java_fences(
    source: str,
    ignored_lines: set[int] | None = None,
    *,
    include_list_continuations: bool = True,
) -> tuple[list[JavaFence], set[int]]:
    """Extract CommonMark Java fences while skipping the content of every fence."""
    lines = source.splitlines()
    ignored_lines = ignored_lines or set()
    fences: list[JavaFence] = []
    fenced_lines: set[int] = set()
    index = 0
    while index < len(lines):
        if index in ignored_lines:
            index += 1
            continue
        opening = OPEN_FENCE.fullmatch(lines[index])
        if opening is None:
            index += 1
            continue
        if not include_list_continuations and _is_list_continuation_fence(lines, index):
            index += 1
            continue

        fence = opening.group("fence")
        if not _is_valid_fence_opening(opening):
            index += 1
            continue
        character = fence[0]
        end = _fence_end(lines, index, fence, OPEN_FENCE.fullmatch)
        fenced_lines.update(range(index, min(end + 1, len(lines))))

        if _is_java_info(opening.group("info"), character):
            marker_line = index - 1
            marker = lines[marker_line] if marker_line >= 0 else None
            fences.append(
                JavaFence(
                    line=index + 1,
                    marker_line=marker_line,
                    marker=marker,
                    text=_displayed_content(
                        lines[index : end + 1], opening, end - index
                    ),
                    closed=end < len(lines),
                    nested=_is_list_continuation_fence(lines, index),
                )
            )
        index = end + 1
    return fences, fenced_lines


def container_fences(
    source: str, ignored_lines: set[int]
) -> tuple[list[int], set[int]]:
    """Find explicit quote/list container fences and shield their contents."""
    lines = source.splitlines()
    java_openings: list[int] = []
    fenced_lines: set[int] = set()
    index = 0
    while index < len(lines):
        if index in ignored_lines:
            index += 1
            continue
        expanded = lines[index].expandtabs(4)
        opening = CONTAINER_FENCE.fullmatch(expanded)
        if opening is None:
            opening = INDENTED_CONTAINER_FENCE.fullmatch(expanded)
        list_indentation: int | None = None
        if opening is None:
            spaced = SPACED_FENCE.fullmatch(expanded)
            if spaced is not None:
                list_indentation = _list_continuation_indentation(
                    lines, index, len(spaced.group("indent"))
                )
            if spaced is not None and list_indentation is not None:
                opening = spaced
        if opening is None or not _is_valid_fence_opening(opening):
            index += 1
            continue

        fence = opening.group("fence")
        character = fence[0]
        if list_indentation is not None:
            steps = [("list", list_indentation, None)]
        else:
            prefix = expanded[: opening.start("fence")]
            steps, prefix_remainder = _container_steps(prefix)
            leading = len(prefix) - len(prefix.lstrip(" "))
            outer_list_indentation = _list_continuation_indentation(
                lines, index, leading
            )
            if outer_list_indentation is not None:
                nested_steps, nested_remainder = _container_steps(
                    prefix[outer_list_indentation:]
                )
                nested_container = [
                    ("list", outer_list_indentation, None),
                    *nested_steps,
                ]
                if _valid_explicit_container(
                    lines, index, nested_container, nested_remainder
                ):
                    steps = nested_container
                    prefix_remainder = nested_remainder
            if not _valid_explicit_container(lines, index, steps, prefix_remainder):
                index += 1
                continue
        if list_indentation is None and steps:
            remainder = _container_remainder(expanded, steps)
            remainder_opening = (
                OPEN_FENCE.fullmatch(remainder) if remainder is not None else None
            )
            if remainder_opening is not None:
                implicit_list_indentation = _nested_list_continuation_indentation(
                    lines,
                    index,
                    steps,
                    len(remainder_opening.group("indent")),
                )
                if implicit_list_indentation is not None:
                    steps.append(("list", implicit_list_indentation, None))
        container_steps = tuple(steps)

        def candidate_at(
            candidate_line: str,
            container_path: Sequence[tuple[str, int, str | None]] = container_steps,
        ) -> re.Match[str] | None:
            remainder = _container_remainder(
                candidate_line.expandtabs(4), container_path
            )
            return OPEN_FENCE.fullmatch(remainder) if remainder is not None else None

        def boundary_at(
            candidate_line: str,
            container_path: Sequence[tuple[str, int, str | None]] = container_steps,
        ) -> bool:
            return (
                _container_remainder(candidate_line.expandtabs(4), container_path)
                is None
            )

        end = _fence_end(lines, index, fence, candidate_at, boundary_at)

        fenced_lines.update(range(index, min(end + 1, len(lines))))
        if _is_java_info(opening.group("info"), character):
            java_openings.append(index)
        index = end + 1
    return java_openings, fenced_lines


def _container_steps(
    prefix: str,
) -> tuple[list[tuple[str, int, str | None]], str]:
    steps: list[tuple[str, int, str | None]] = []
    index = 0
    while index < len(prefix):
        start = index
        while index < len(prefix) and prefix[index] == " " and index - start < 3:
            index += 1
        if index < len(prefix) and prefix[index] == ">":
            index += 1
            if index < len(prefix) and prefix[index] == " ":
                index += 1
            steps.append(("quote", 0, None))
            continue
        item = LIST_CONTAINER_STEP.match(prefix, index)
        if item is None:
            index = start
            break
        index = item.end()
        steps.append(("list", index - start, item.group("marker")))
    return steps, prefix[index:]


def _container_remainder(
    line: str, steps: Sequence[tuple[str, int, str | None]]
) -> str | None:
    index = 0
    for step_index, (kind, width, _) in enumerate(steps):
        if kind == "quote":
            start = index
            while index < len(line) and line[index] == " " and index - start < 3:
                index += 1
            if index >= len(line) or line[index] != ">":
                return None
            index += 1
            if index < len(line) and line[index] == " ":
                index += 1
            continue
        remainder = line[index:]
        available = len(remainder) - len(remainder.lstrip(" "))
        if available < width:
            later_quote = any(kind == "quote" for kind, _, _ in steps[step_index + 1 :])
            if not line[index:].strip() and not later_quote:
                return ""
            return None
        index += width
    return line[index:]


def _valid_explicit_container(
    lines: list[str],
    index: int,
    steps: list[tuple[str, int, str | None]],
    remainder: str,
) -> bool:
    if not steps:
        return False
    if remainder.strip():
        return False
    if steps[-1][0] == "list" and remainder:
        return False
    if len(remainder) > 3:
        return False
    for step_index, (_, _, marker) in enumerate(steps):
        if marker is None:
            continue
        stripped = _contiguous_container_lines(lines, index, steps[:step_index])
        if not _can_start_list_item(stripped, len(stripped) - 1, marker, empty=False):
            return False
    return True


def _contiguous_container_lines(
    lines: list[str], index: int, steps: list[tuple[str, int, str | None]]
) -> list[str]:
    stripped: list[str] = []
    for preceding_index in range(index, -1, -1):
        remainder = _container_remainder(lines[preceding_index].expandtabs(4), steps)
        if remainder is None:
            break
        stripped.append(remainder)
    stripped.reverse()
    return stripped


def _nested_list_continuation_indentation(
    lines: list[str],
    index: int,
    steps: list[tuple[str, int, str | None]],
    indentation: int,
) -> int | None:
    stripped = _contiguous_container_lines(lines, index, steps)
    return _list_continuation_indentation(stripped, len(stripped) - 1, indentation)


def _partial_context(lines: list[str], marker_line: int) -> str:
    index = marker_line - 1
    while index >= 0 and not lines[index].strip():
        index -= 1
    paragraph: list[str] = []
    while index >= 0 and lines[index].strip():
        paragraph.append(lines[index])
        index -= 1
    return "\n".join(reversed(paragraph))


def _has_visible_partial_explanation(
    lines: list[str], marker_line: int, fenced_lines: set[int]
) -> bool:
    prefix = "\n".join(
        "" if index in fenced_lines else line
        for index, line in enumerate(lines[:marker_line])
    )
    visible_prefix = HTML_COMMENT.sub("", prefix)
    if "<!--" in visible_prefix:
        return False
    visible_lines = visible_prefix.splitlines()
    context = _partial_context(visible_lines, len(visible_lines)).strip()
    if not context.startswith(VISIBLE_PARTIAL_LABEL):
        return False
    return bool(context.removeprefix(VISIBLE_PARTIAL_LABEL).strip())


def _list_continuation_indentation(
    lines: list[str], index: int, indentation: int
) -> int | None:
    """Return the content indentation of the owning CommonMark list item."""
    blank_lines = 0
    paragraph_line_seen = False
    paragraph_crossed_blank = False
    paragraph_start_indentation = 0
    for preceding_index in range(index - 1, -1, -1):
        preceding = lines[preceding_index].expandtabs(4)
        if not preceding.strip():
            blank_lines += 1
            if blank_lines >= 2:
                return None
            if paragraph_line_seen:
                paragraph_crossed_blank = True
            continue
        blank_lines = 0
        leading = len(preceding) - len(preceding.lstrip(" "))
        if _is_outer_block_boundary(lines, preceding_index, indentation):
            return None
        if _starts_block(preceding):
            continue
        item = LIST_ITEM.match(preceding)
        empty_item = EMPTY_LIST_ITEM.fullmatch(preceding)
        if empty_item is not None:
            if _can_start_list_item(
                lines, preceding_index, empty_item.group("marker"), empty=True
            ):
                content_indentation = (
                    len(empty_item.group("indent"))
                    + len(empty_item.group("marker"))
                    + 1
                )
                if content_indentation <= indentation and (
                    not paragraph_crossed_blank
                    or paragraph_start_indentation >= content_indentation
                ):
                    return content_indentation
                continue
        elif item is not None and _can_start_list_item(
            lines, preceding_index, item.group("marker"), empty=False
        ):
            content_indentation = _list_item_content_indentation(item, preceding)
            if content_indentation <= indentation and (
                not paragraph_crossed_blank
                or paragraph_start_indentation >= content_indentation
            ):
                return content_indentation
            continue
        if not paragraph_line_seen:
            paragraph_crossed_blank = False
        paragraph_start_indentation = leading
        paragraph_line_seen = True
    return None


def _is_list_continuation(lines: list[str], index: int, indentation: int) -> bool:
    return _list_continuation_indentation(lines, index, indentation) is not None


def _list_item_content_indentation(item: re.Match[str], line: str) -> int:
    spacing = len(item.group("spacing"))
    if spacing == 4 and line[item.end() :].startswith(" "):
        spacing = 1
    return len(item.group("indent")) + len(item.group("marker")) + spacing


def _is_thematic_break(line: str) -> bool:
    expanded = line.expandtabs(4)
    indentation = len(expanded) - len(expanded.lstrip(" "))
    if indentation > 3:
        return False
    compact = expanded.strip().replace(" ", "")
    return len(compact) >= 3 and len(set(compact)) == 1 and compact[0] in "*-_"


def _is_outer_block_boundary(lines: list[str], index: int, indentation: int) -> bool:
    line = lines[index].expandtabs(4)
    leading = len(line) - len(line.lstrip(" "))
    if leading >= indentation or not _starts_block(line):
        return False
    return leading == 0 or not _is_list_continuation(lines, index, leading)


def _starts_block(line: str) -> bool:
    fence = OPEN_FENCE.fullmatch(line)
    return bool(
        _is_thematic_break(line)
        or ATX_HEADING.match(line)
        or BLOCK_QUOTE.match(line)
        or (fence is not None and _is_valid_fence_opening(fence))
        or HTML_BLOCK.match(line)
        or HTML_BLOCK_TAG.match(line)
    )


def _can_start_list_item(
    lines: list[str], index: int, marker: str, *, empty: bool
) -> bool:
    """Apply CommonMark's paragraph-interruption restrictions to a list marker."""
    starting_index = index
    paragraph_text_seen = False
    pending_block_result: bool | None = None
    candidate_paragraph_start_indentation: int | None = None
    if not empty and (not marker[0].isdigit() or int(marker[:-1]) == 1):
        return True
    while index > 0 and lines[index - 1].strip():
        previous = lines[index - 1].expandtabs(4)
        previous_empty_item = EMPTY_LIST_ITEM.fullmatch(previous)
        if previous_empty_item is not None:
            if pending_block_result is True:
                return True
            content_indentation = (
                len(previous_empty_item.group("indent"))
                + len(previous_empty_item.group("marker"))
                + 1
            )
            if (
                pending_block_result is False
                and candidate_paragraph_start_indentation is not None
                and candidate_paragraph_start_indentation < content_indentation
            ):
                return False
            index -= 1
            starting_index = index
            marker = previous_empty_item.group("marker")
            empty = True
            paragraph_text_seen = False
            pending_block_result = None
            candidate_paragraph_start_indentation = None
            continue
        previous_item = LIST_ITEM.match(previous)
        if previous_item is not None:
            if pending_block_result is True:
                return True
            content_indentation = _list_item_content_indentation(
                previous_item, previous
            )
            if (
                pending_block_result is False
                and candidate_paragraph_start_indentation is not None
                and candidate_paragraph_start_indentation < content_indentation
            ):
                return False
            index -= 1
            starting_index = index
            marker = previous_item.group("marker")
            empty = False
            paragraph_text_seen = False
            pending_block_result = None
            candidate_paragraph_start_indentation = None
            continue
        if _starts_block(previous):
            leading = len(previous) - len(previous.lstrip(" "))
            if leading:
                if pending_block_result is None:
                    pending_block_result = not paragraph_text_seen or (
                        not empty and (not marker[0].isdigit() or int(marker[:-1]) == 1)
                    )
                paragraph_text_seen = True
                index -= 1
                continue
            if pending_block_result is not None:
                return pending_block_result
            if not paragraph_text_seen:
                return True
            break
        paragraph_text_seen = True
        leading = len(previous) - len(previous.lstrip(" "))
        if pending_block_result is None:
            candidate_paragraph_start_indentation = leading
        index -= 1
    if pending_block_result is not None:
        return pending_block_result
    if not paragraph_text_seen and (index > 0 or starting_index == 0):
        return True
    return not empty and (not marker[0].isdigit() or int(marker[:-1]) == 1)


def _is_list_continuation_fence(lines: list[str], index: int) -> bool:
    """Return whether a space- or tab-indented fence continues a list item."""
    opening = SPACED_FENCE.fullmatch(lines[index].expandtabs(4))
    if opening is None:
        return False
    fence_indentation = len(opening.group("indent"))
    return _is_list_continuation(lines, index, fence_indentation)


def validate() -> tuple[int, int, list[str], bool]:
    paths = sorted(ROOT.glob(README_PATTERN))
    if not paths:
        return 0, 0, [f"{README_PATTERN} matched no README files under {ROOT}."], True

    regions, problems = collect_source_regions(SNIPPET_SOURCES, relative)
    infrastructure = not SNIPPET_SOURCES.is_dir() or not any(
        SNIPPET_SOURCES.rglob("*.java")
    )
    used: set[tuple[str, str]] = set()
    runnable = 0
    partial = 0

    for path in paths:
        source = path.read_text(encoding="utf-8")
        lines = source.splitlines()
        consumed_markers: set[int] = set()
        _, provisional_fenced_lines = java_fences(
            source, include_list_continuations=False
        )
        nested_openings, container_fenced_lines = container_fences(
            source, provisional_fenced_lines
        )
        blocks, fenced_lines = java_fences(source, container_fenced_lines)
        fenced_lines.update(container_fenced_lines)
        for opening in nested_openings:
            problems.append(
                f"{relative(path)}:{opening + 1}: nested Java code fence is not "
                "supported; place the marker and fence at the README top level."
            )
        for block in blocks:
            where = f"{relative(path)}:{block.line}"
            if block.nested:
                problems.append(
                    f"{where}: nested Java code fence is not supported; place the "
                    "marker and fence at the README top level."
                )
                if block.marker is not None and ANY_MARKER.fullmatch(
                    block.marker.strip()
                ):
                    consumed_markers.add(block.marker_line)
                continue
            if not block.closed:
                problems.append(f"{where}: Java code fence is not closed.")
            marker_text = block.marker or ""
            marker = ANY_MARKER.fullmatch(marker_text.strip())
            if marker is None:
                problems.append(
                    f"{where}: Java code fence has no immediately preceding "
                    "readme-example marker."
                )
                continue
            consumed_markers.add(block.marker_line)

            runnable_match = RUNNABLE_MARKER.fullmatch(marker_text)
            partial_match = PARTIAL_MARKER.fullmatch(marker_text)
            if runnable_match is not None:
                runnable += 1
                key = (runnable_match.group("file"), runnable_match.group("tag"))
                region: SourceRegion | None = regions.get(key)
                if region is None:
                    problems.append(
                        f"{where}: marker names {key[0]} tag `{key[1]}`, but no valid "
                        "backing region has that identity."
                    )
                elif key in used:
                    problems.append(
                        f"{where}: backing region {key[0]} tag `{key[1]}` is referenced "
                        "by more than one README block."
                    )
                else:
                    used.add(key)
                    if block.text != region.text:
                        diff = "\n".join(
                            difflib.unified_diff(
                                region.text.splitlines(),
                                block.text.splitlines(),
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
                if not _has_visible_partial_explanation(
                    lines, block.marker_line, fenced_lines
                ):
                    problems.append(
                        f"{where}: partial example needs visible README prose beginning "
                        f"`{VISIBLE_PARTIAL_LABEL}` and explaining the omission."
                    )
            else:
                problems.append(
                    f"{where}: malformed readme-example marker `{marker_text.strip()}`."
                )

        for line_number, line in enumerate(lines, start=1):
            if line_number - 1 in fenced_lines:
                continue
            expanded_line = line.expandtabs(4)
            indented = INDENTED_FENCE.fullmatch(expanded_line)
            if (
                indented is not None
                and _is_java_info(indented.group("info"), indented.group("fence")[0])
                and _is_list_continuation(
                    lines, line_number - 1, len(indented.group("indent"))
                )
            ):
                problems.append(
                    f"{relative(path)}:{line_number}: nested Java code fence is not "
                    "supported; place the marker and fence at the README top level."
                )
            if ANY_MARKER.search(line) and line_number - 1 not in consumed_markers:
                problems.append(
                    f"{relative(path)}:{line_number}: readme-example marker is not "
                    "immediately followed by a Java code fence."
                )

    return runnable, partial, problems, infrastructure


def main() -> int:
    try:
        runnable, partial, problems, infrastructure = validate()
    except OSError as error:
        print(f"Could not read README validation inputs: {error}", file=sys.stderr)
        return 2

    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        print(
            "See .agents/skills/maintain-doc-java-snippets/SKILL.md for the repair "
            "procedure.",
            file=sys.stderr,
        )
        return 2 if infrastructure else 1

    print(
        f"{runnable} runnable README examples are source-backed; "
        f"{partial} abbreviated examples."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
