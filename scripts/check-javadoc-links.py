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
"""Hold Javadoc member references to the ones Javadoc can actually resolve (issue #897).

A ``{@link Type#member}`` written without a parameter list resolves against
**fields before methods**, and does not disambiguate overloads. Three shapes go
wrong, each measured on this repository rather than inferred:

* a method shadowed by a field of the same name renders the label with **no
  anchor at all** (issue #893, measured on the generated ``PubSubSink.html``);
* an overloaded method renders **one anchor on an overload the reader cannot
  predict** (issue #894, measured on ``BoundedShutdown.html``);
* a field the API reference does not document — private or package-private,
  with no method of the name — renders **no anchor** either (issue #931,
  measured on ``PubSubWriter.html`` and three sibling writers).

``mvn javadoc:aggregate`` exits 0 on all three, ``failOnWarnings`` included, so
nothing else in CI sees them. This script reads every main-tree source, indexes
the types it declares, and fails on a reference of any of the three. The repair
is in the message: the parameter list where there is a method to name, and
``{@code member}`` where the sentence means the state itself.

What it does not do: it judges only references whose target type it can find in
this repository — ``{@link Duration#ZERO}`` belongs to the JDK and is skipped —
and only members the target type declares itself, so a method inherited from a
supertype is left alone. A reference that already carries a parameter list is
not checked against the declared signatures. And it says nothing about whether
a reference points at the *right* member.

Exit codes: 0 clean, 1 an unresolvable reference, 2 no sources to read.

Standard library only, like the other repository checkers.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

from java_example_regions import line_at, skip_quoted, skip_text_block

ROOT = Path(__file__).resolve().parent.parent
MAIN_SOURCE_PATTERN = "flink-*/src/main/java*/**/*.java"

PACKAGE = re.compile(r"^\s*package\s+([\w.]+)\s*;", re.MULTILINE)
IMPORT = re.compile(r"^\s*import\s+(?:static\s+)?([\w.]+)\s*;", re.MULTILINE)
TYPE_KEYWORD = re.compile(r"\b(class|interface|enum|record|@interface)\s+(\w+)")
MODIFIERS = frozenset(
    [
        "public",
        "protected",
        "private",
        "static",
        "final",
        "abstract",
        "default",
        "synchronized",
        "native",
        "strictfp",
        "transient",
        "volatile",
        "sealed",
        "non-sealed",
    ]
)
ANNOTATION = re.compile(r"@\w+(?:\.\w+)*")
IDENTIFIER = re.compile(r"\b(\w+)\s*$")
# `{@link Type#member}` and `@see Type#member`, with the reference split across
# Javadoc lines exactly as the formatter leaves it.
REFERENCE = re.compile(
    r"(?P<tag>\{@link(?:plain)?|@see)\s+(?P<type>[\w.$]*)#(?P<member>\w+)\s*(?P<next>.)"
)


@dataclass
class JavaType:
    """One declared type: what it names, and where its body begins and ends."""

    simple: str
    qualified: str
    kind: str
    decl_start: int
    body_start: int
    body_end: int = -1
    outer: JavaType | None = None
    fields: dict[str, str] = field(default_factory=dict)
    methods: dict[str, list[str]] = field(default_factory=dict)
    nested: dict[str, JavaType] = field(default_factory=dict)


@dataclass
class SourceFile:
    path: Path
    text: str
    masked: str
    package: str
    imports: dict[str, str]
    types: list[JavaType]


def relative(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def mask(source: str) -> str:
    """Blank comments and literals, keeping every offset and newline in place."""
    out = list(source)
    index = 0
    length = len(source)

    def blank(start: int, end: int):
        for position in range(start, min(end, length)):
            if out[position] != "\n":
                out[position] = " "

    while index < length:
        if source.startswith('"""', index):
            end = skip_text_block(source, index)
        elif source[index] == '"':
            end = skip_quoted(source, index, '"')
        elif source[index] == "'":
            end = skip_quoted(source, index, "'")
        elif source.startswith("//", index):
            newline = source.find("\n", index + 2)
            end = length if newline < 0 else newline
        elif source.startswith("/*", index):
            terminator = source.find("*/", index + 2)
            end = length if terminator < 0 else terminator + 2
        else:
            index += 1
            continue
        blank(index, end)
        index = max(end, index + 1)
    return "".join(out)


def skip_annotations(masked: str, index: int) -> int:
    """Step over leading annotations and whitespace of a declaration."""
    while index < len(masked):
        while index < len(masked) and masked[index].isspace():
            index += 1
        match = ANNOTATION.match(masked, index)
        if match is None:
            return index
        index = match.end()
        while index < len(masked) and masked[index].isspace():
            index += 1
        if index < len(masked) and masked[index] == "(":
            depth = 0
            while index < len(masked):
                if masked[index] == "(":
                    depth += 1
                elif masked[index] == ")":
                    depth -= 1
                    if depth == 0:
                        index += 1
                        break
                index += 1
    return index


def parameter_types(masked: str, start: int, end: int) -> list[str]:
    """The erasure-shaped parameter list a Javadoc reference has to name."""
    inner = masked[start:end]
    parameters: list[str] = []
    depth = 0
    current = ""
    for character in inner:
        if character in "<([":
            depth += 1
        elif character in ">)]":
            depth -= 1
        if character == "," and depth == 0:
            parameters.append(current)
            current = ""
        else:
            current += character
    parameters.append(current)

    types: list[str] = []
    for parameter in parameters:
        text = ANNOTATION.sub("", parameter).strip()
        if not text:
            continue
        text = re.sub(r"\bfinal\b", "", text).strip()
        text = re.sub(r"<[^<>]*(?:<[^<>]*>)?[^<>]*>", "", text).strip()
        words = text.split()
        if len(words) < 2:
            continue
        declared = " ".join(words[:-1])
        types.append(declared.replace(" ", ""))
    return types


def skip_initializer(masked: str, index: int) -> int:
    """Step over a field initializer, to the ``;`` that ends the declaration.

    Nothing inside it is a declaration, and everything inside it may be
    bracketed: a call, an array literal, a lambda body. Walking it here is what
    keeps the brace depth honest — resuming just after the ``=`` puts the scan
    inside the expression, where the next unbalanced ``)`` breaks the count for
    the rest of the file and silently drops every member below it.
    """
    depth = 0
    while index < len(masked):
        character = masked[index]
        if character in "([{":
            depth += 1
        elif character in ")]}":
            depth -= 1
        elif character == ";" and depth <= 0:
            return index
        index += 1
    return len(masked)


def find_header(masked: str, index: int) -> tuple[int, int, int] | None:
    """Split a declaration into its header, its parameter list and its end.

    Returns ``(name_end, params_end, terminator)`` where ``name_end`` is the
    offset of the ``(`` that opens a parameter list (``-1`` when there is
    none) and ``terminator`` the offset of the ``;``, ``=`` or ``{`` that ends
    the declaration's header.

    The *first* top-level ``(`` is the one that counts. A list of enum
    constants with arguments is one declaration carrying several, and taking
    the last would name the declaration after its final constant.
    """
    depth = 0
    name_end = -1
    params_end = -1
    while index < len(masked):
        character = masked[index]
        if character == "(":
            if depth == 0 and name_end < 0:
                name_end = index
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0 and params_end < 0:
                params_end = index
        elif depth == 0 and character in ";={}":
            return name_end, params_end, index
        index += 1
    return None


def index_source(path: Path, text: str) -> SourceFile:
    """Index every type a source declares, with its own fields and methods."""
    masked = mask(text)
    package_match = PACKAGE.search(masked)
    package = package_match.group(1) if package_match else ""
    imports = {
        qualified.rsplit(".", 1)[-1]: qualified for qualified in IMPORT.findall(masked)
    }

    types: list[JavaType] = []
    stack: list[tuple[JavaType, int]] = []
    depth = 0
    index = 0
    at_declaration = True
    while index < len(masked):
        character = masked[index]
        if character == "{":
            depth += 1
            index += 1
            at_declaration = True
            continue
        if character == "}":
            depth -= 1
            while stack and stack[-1][1] > depth:
                stack.pop()[0].body_end = index
            index += 1
            at_declaration = True
            continue
        if character == ";":
            index += 1
            at_declaration = True
            continue
        if character.isspace():
            index += 1
            continue
        if not at_declaration:
            index += 1
            continue

        member_depth = stack[-1][1] if stack else 0
        if depth != member_depth:
            at_declaration = False
            index += 1
            continue

        start = skip_annotations(masked, index)
        header = find_header(masked, start)
        if header is None:
            break
        name_end, params_end, terminator = header
        enclosing = stack[-1][0] if stack else None

        keyword = TYPE_KEYWORD.search(masked, start, terminator + 1)
        if keyword is not None and masked[terminator] == "{":
            simple = keyword.group(2)
            qualified = (
                f"{enclosing.qualified}.{simple}"
                if enclosing is not None
                else (f"{package}.{simple}" if package else simple)
            )
            declared = JavaType(
                simple=simple,
                qualified=qualified,
                kind=keyword.group(1),
                decl_start=start,
                body_start=terminator,
                outer=enclosing,
            )
            if enclosing is None:
                types.append(declared)
            else:
                enclosing.nested[simple] = declared
            stack.append((declared, depth + 1))
            index = terminator
            at_declaration = False
            continue

        if enclosing is not None:
            record_member(enclosing, masked, start, name_end, params_end, terminator)
        at_declaration = False
        # Leave a brace or a `;` for the loop above to read, so depth and the
        # next declaration position stay right; an `=` hands over to the
        # initializer walk, which stops on the `;` the loop then reads.
        index = (
            skip_initializer(masked, terminator)
            if masked[terminator] == "="
            else terminator
        )

    for declared, _ in stack:
        declared.body_end = len(masked)
    return SourceFile(path, text, masked, package, imports, types)


def enum_constants(masked: str, start: int, terminator: int) -> list[str]:
    """Every constant a single enum constant-list declaration names."""
    depth = 0
    current = ""
    segments: list[str] = []
    for character in masked[start:terminator]:
        if character in "([{":
            depth += 1
        elif character in ")]}":
            depth -= 1
        if character == "," and depth == 0:
            segments.append(current)
            current = ""
        else:
            current += character
    segments.append(current)

    names = []
    for segment in segments:
        identifier = re.match(r"\s*(\w+)", segment)
        if identifier is not None:
            names.append(identifier.group(1))
    return names


def record_member(
    enclosing: JavaType,
    masked: str,
    start: int,
    name_end: int,
    params_end: int,
    terminator: int,
):
    """Attribute one field, method or enum constant to its declaring type."""
    if 0 <= name_end < terminator:
        identifier = IDENTIFIER.search(masked, start, name_end)
        if identifier is None:
            return
        name = identifier.group(1)
        head = masked[start:name_end].strip()
        words = [word for word in head.split() if word not in MODIFIERS]
        if name == enclosing.simple:
            return
        if len(words) <= 1 and enclosing.kind == "enum":
            # Enum constants with arguments, not methods: `RETRY(3), NEVER(0);`
            # is one declaration carrying every constant. Every other member
            # with a parameter list has a return type before its name, and a
            # constructor left above.
            for constant in enum_constants(masked, start, terminator):
                enclosing.fields.setdefault(constant, "public")
            return
        enclosing.methods.setdefault(name, []).append(
            "(" + ", ".join(parameter_types(masked, name_end + 1, params_end)) + ")"
        )
        return

    head = masked[start:terminator].strip()
    if not head or masked[terminator] == "{":
        return
    identifier = IDENTIFIER.search(head)
    if identifier is None:
        return
    words = [word for word in head.split() if word not in MODIFIERS]
    if len(words) <= 1 and enclosing.kind != "enum":
        return
    visibility = (
        "private"
        if re.search(r"\bprivate\b", head)
        else "protected"
        if re.search(r"\bprotected\b", head)
        else "public"
        if re.search(r"\bpublic\b", head) or enclosing.kind in ("interface", "enum")
        else "package"
    )
    for declarator in head.split(","):
        candidate = IDENTIFIER.search(declarator.strip())
        if candidate is not None:
            enclosing.fields.setdefault(candidate.group(1), visibility)


def all_types(source: SourceFile) -> list[JavaType]:
    found: list[JavaType] = []
    pending = list(source.types)
    while pending:
        current = pending.pop()
        found.append(current)
        pending.extend(current.nested.values())
    return found


def enclosing_type(source: SourceFile, start: int, end: int) -> JavaType | None:
    """The type a Javadoc comment documents from the inside, or announces.

    A comment sitting immediately before a type declaration documents *that*
    type, so its ``#member`` references resolve against it; anything else
    resolves against the type whose body holds the comment.

    "Immediately before" has to step over the type's annotations. Every public
    type in this repository carries a Flink API-tier one, so reading `@Public`
    as something other than whitespace left every class javadoc without a
    context and every bare reference in one unchecked (issue #930).
    """
    for candidate in all_types(source):
        between = source.masked[end : candidate.decl_start]
        if candidate.decl_start >= end and not ANNOTATION.sub("", between).strip():
            return candidate
    containing = [
        candidate
        for candidate in all_types(source)
        if candidate.body_start <= start < candidate.body_end
    ]
    return max(containing, key=lambda candidate: candidate.body_start, default=None)


def resolve(
    reference: str,
    context: JavaType | None,
    source: SourceFile,
    by_qualified: dict[str, JavaType],
) -> JavaType | None:
    """Find the repository type a reference names, or None if it is not ours."""
    if not reference:
        return context
    segments = reference.split(".")
    head, rest = segments[0], segments[1:]

    candidate: JavaType | None = None
    scope = context
    while scope is not None and candidate is None:
        if scope.simple == head:
            candidate = scope
        else:
            candidate = scope.nested.get(head)
        scope = scope.outer
    if candidate is None:
        candidate = next(
            (declared for declared in source.types if declared.simple == head), None
        )
    if candidate is None and head in source.imports:
        candidate = by_qualified.get(source.imports[head])
    if candidate is None and source.package:
        candidate = by_qualified.get(f"{source.package}.{head}")
    if candidate is None:
        # A fully qualified reference. Nothing else is in scope for Java either:
        # `AvoidStarImport` is on, so an unimported simple name is not ours.
        return by_qualified.get(reference)
    for segment in rest:
        if candidate is None:
            return None
        candidate = candidate.nested.get(segment)
    return candidate


def declaring_scope(target: JavaType, member: str, *, bare: bool) -> JavaType | None:
    """Where Javadoc finds a member: the type itself, then its enclosing ones.

    Only a bare ``#member`` searches outwards, which is what Javadoc's own
    search order does; a qualified reference names the type to look in.
    """
    scope: JavaType | None = target
    while scope is not None:
        if member in scope.methods or member in scope.fields:
            return scope
        if not bare:
            return None
        scope = scope.outer
    return None


def doc_comments(source: str) -> list[tuple[int, int, str]]:
    """Every real Javadoc comment as (start offset, end offset, body)."""
    comments: list[tuple[int, int, str]] = []
    index = 0
    while index < len(source):
        if source.startswith('"""', index):
            index = skip_text_block(source, index)
        elif source[index] == '"':
            index = skip_quoted(source, index, '"')
        elif source[index] == "'":
            index = skip_quoted(source, index, "'")
        elif source.startswith("//", index):
            newline = source.find("\n", index + 2)
            index = len(source) if newline < 0 else newline + 1
        elif source.startswith("/*", index):
            terminator = source.find("*/", index + 2)
            end = len(source) if terminator < 0 else terminator + 2
            if source.startswith("/**", index):
                comments.append((index, end, source[index + 3 : end - 2]))
            index = end
        else:
            index += 1
    return comments


LINE_PREFIX = re.compile(r"\n[ \t]*\*?[ \t]?")


def flatten(body: str) -> tuple[str, list[int]]:
    """Join a Javadoc comment's lines so a wrapped reference reads as one.

    Returns the joined text and, for each of its characters, the offset it came
    from, so a reference is still reported at the line it was written on.
    """
    joined: list[str] = []
    offsets: list[int] = []
    index = 0
    while index < len(body):
        prefix = LINE_PREFIX.match(body, index)
        if prefix is not None and prefix.end() > index:
            joined.append(" ")
            offsets.append(index)
            index = prefix.end()
        else:
            joined.append(body[index])
            offsets.append(index)
            index += 1
    return "".join(joined), offsets


def check() -> tuple[int, list[str]]:
    paths = sorted(ROOT.glob(MAIN_SOURCE_PATTERN))
    if not paths:
        raise FileNotFoundError(f"{MAIN_SOURCE_PATTERN} matched no Java sources.")

    sources = [index_source(path, path.read_text(encoding="utf-8")) for path in paths]
    by_qualified: dict[str, JavaType] = {}
    for source in sources:
        for declared in all_types(source):
            by_qualified[declared.qualified] = declared

    problems: list[str] = []
    checked = 0
    for source in sources:
        for start, end, body in doc_comments(source.text):
            context = enclosing_type(source, start, end)
            flat, offsets = flatten(body)
            for match in REFERENCE.finditer(flat):
                if match.group("next") == "(":
                    continue
                target = resolve(match.group("type"), context, source, by_qualified)
                if target is None:
                    continue
                member = match.group("member")
                target = declaring_scope(target, member, bare=not match.group("type"))
                if target is None:
                    continue
                overloads = target.methods.get(member)
                checked += 1
                line = line_at(source.text, start + 3 + offsets[match.start()])
                shown = (
                    f"{match.group('type')}#{member}"
                    if match.group("type")
                    else f"#{member}"
                )
                where = f"{relative(source.path)}:{line}"
                visibility = target.fields.get(member)
                if visibility is not None:
                    # A field wins whatever the methods look like, so an
                    # overload count says nothing here: what matters is whether
                    # the field it binds is one the reference can reach.
                    if visibility in ("public", "protected"):
                        continue
                    repair = (
                        f"Write `{shown}{overloads[0]}`"
                        if overloads
                        else f"Write `{{@code {member}}}` for the state itself, or name a "
                        f"method with its parameter list"
                    )
                    problems.append(
                        f"{where}: `{shown}` resolves to the {visibility} field "
                        f"`{member}` of {target.simple}, which the API reference does "
                        f"not document, so Javadoc renders no anchor. {repair}."
                    )
                elif overloads and len(overloads) > 1:
                    listed = ", ".join(
                        f"`{shown}{signature}`" for signature in overloads
                    )
                    problems.append(
                        f"{where}: `{shown}` names an overloaded method, so Javadoc "
                        f"links whichever overload it picks first. Name the one meant: "
                        f"{listed}."
                    )
    return checked, problems


def main() -> int:
    try:
        checked, problems = check()
    except (OSError, FileNotFoundError) as error:
        print(f"Could not read the Java sources: {error}", file=sys.stderr)
        return 2

    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        print(
            f"{len(problems)} Javadoc references cannot resolve to what they name.",
            file=sys.stderr,
        )
        return 1

    print(
        f"{checked} parameterless Javadoc member references reach a member they can name."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
