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

Issue #1093 adds two rule families on the same index. A type whose nearest
annotated enclosing declaration carries ``@Public``, ``@PublicEvolving`` or
``@Experimental`` needs type-level Javadoc, and so does every public or
protected member it declares — methods, constructors, fields, nested types,
implicitly-public interface members and enum constants — with implicit default
and canonical constructors made explicit so they can carry a comment, and
``@Override`` members as the sole exemption, since their docs inherit. And a
``public static final ConfigOption`` whose declaration builds its description
from ``.withDescription`` string literals must carry Javadoc equal to that
description, so the API reference cannot drift from the option's runtime
description. Each failure carries its repair, which is why there is no allowlist.

Exit codes: 0 clean, 1 a policy failure, 2 no sources to read.

Standard library only, like the other repository checkers.
"""

from __future__ import annotations

import re
import sys
from bisect import bisect_right
from dataclasses import dataclass, field
from pathlib import Path

from java_example_regions import line_at, skip_quoted, skip_text_block

ROOT = Path(__file__).resolve().parent.parent
MAIN_SOURCE_PATTERN = "flink-*/src/main/java*/**/*.java"

PACKAGE = re.compile(r"^\s*package\s+([\w.]+)\s*;", re.MULTILINE)
IMPORT = re.compile(r"^\s*import\s+(?:static\s+)?([\w.]+)\s*;", re.MULTILINE)
# `@interface` needs its own alternative without `\b`: no word boundary exists
# between a space and `@`, so a `\b`-anchored alternation can never match it.
TYPE_KEYWORD = re.compile(r"(@interface|\b(?:class|interface|enum|record))\s+(\w+)")
AT_INTERFACE = re.compile(r"@interface\b")
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
WORD = re.compile(r"\w+")
# The Flink API-tier annotations, in the order surface_tier() reports them.
FLINK_ANNOTATION = "org.apache.flink.annotation"
TIERS = ("Public", "PublicEvolving", "Experimental", "Internal")
DOCUMENTED_TIERS = frozenset(["Public", "PublicEvolving", "Experimental"])
OVERRIDE = re.compile(r"@(?:java\.lang\.)?Override\b")
WITH_DESCRIPTION = re.compile(r"\.\s*withDescription\s*\(")
# `{@link Type#member}` and `@see Type#member`, with the reference split across
# Javadoc lines exactly as the formatter leaves it.
REFERENCE = re.compile(
    r"(?P<tag>\{@link(?:plain)?|@see)\s+(?P<type>[\w.$]*)#(?P<member>\w+)\s*(?P<next>.)"
)


@dataclass
class Member:
    """One declared member, held to the Javadoc-presence rules."""

    name: str
    kind: str  # "method", "constructor", "field" or "enum constant"
    visibility: str
    ann_start: int  # where its annotations begin; Javadoc sits above this
    decl_start: int  # after annotations, the line the report names
    statement_end: int  # the `;` or `{` ending the declaration, initializer walked
    overridden: bool
    head: str  # masked modifiers-and-type text, for the ConfigOption rule
    canonical: bool = False  # whether a record constructor declares its canonical form


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
    ann_start: int = -1
    tier: str | None = None
    visibility: str = "package"
    fields: dict[str, str] = field(default_factory=dict)
    methods: dict[str, list[str]] = field(default_factory=dict)
    nested: dict[str, JavaType] = field(default_factory=dict)
    members: list[Member] = field(default_factory=list)
    record_components: tuple[str, ...] = ()


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
    """Step over leading annotations and whitespace of a declaration.

    ``@interface`` looks like an annotation use but opens an annotation type
    declaration; eating it would drop the type from the index entirely.
    """
    while index < len(masked):
        while index < len(masked) and masked[index].isspace():
            index += 1
        match = ANNOTATION.match(masked, index)
        if match is None or AT_INTERFACE.match(masked, index):
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


def tier_of(annotations: str, imports: dict[str, str]) -> str | None:
    """The Flink API-tier annotation a declaration carries, or None.

    A simple name counts only when the file imports it from
    ``org.apache.flink.annotation``: with ``AvoidStarImport`` on, an unimported
    ``@Experimental`` is somebody else's annotation, not a tier. An annotation
    written after the modifiers — ``public @PublicEvolving class`` — is not
    seen, a documented limit: the formatter always puts annotations first.
    """
    for name in TIERS:
        if re.search(rf"@org\.apache\.flink\.annotation\.{name}\b", annotations):
            return name
        if (
            re.search(rf"@{name}\b", annotations)
            and imports.get(name) == f"{FLINK_ANNOTATION}.{name}"
        ):
            return name
    return None


def member_visibility(head: str, enclosing: JavaType | None) -> str:
    """What Java gives a declaration whose modifiers say nothing.

    An interface member without a modifier is public; everything else without
    one is package-private. Enum constants never reach here — they are recorded
    as public directly — and an enum constructor's implicit ``private`` and the
    returned ``package`` are both off the documented surface, so the
    distinction is not worth carrying.
    """
    if re.search(r"\bprivate\b", head):
        return "private"
    if re.search(r"\bprotected\b", head):
        return "protected"
    if re.search(r"\bpublic\b", head):
        return "public"
    if enclosing is not None and enclosing.kind in ("interface", "@interface"):
        return "public"
    return "package"


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
        if depth == 0 and character == "@":
            following = skip_annotations(masked, index)
            if following > index:
                index = following
                continue
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
                ann_start=index,
                tier=tier_of(masked[index:start], imports),
                visibility=member_visibility(
                    masked[start : keyword.start()], enclosing
                ),
                record_components=(
                    tuple(parameter_types(masked, name_end + 1, params_end))
                    if keyword.group(1) == "record"
                    and name_end >= 0
                    and params_end >= 0
                    else ()
                ),
            )
            if enclosing is None:
                types.append(declared)
            else:
                enclosing.nested[simple] = declared
            stack.append((declared, depth + 1))
            index = terminator
            at_declaration = False
            continue

        # Leave a brace or a `;` for the loop above to read, so depth and the
        # next declaration position stay right; an `=` hands over to the
        # initializer walk, which stops on the `;` the loop then reads.
        statement_end = (
            skip_initializer(masked, terminator)
            if masked[terminator] == "="
            else terminator
        )
        if enclosing is not None:
            resume = record_member(
                enclosing,
                masked,
                index,
                start,
                name_end,
                params_end,
                terminator,
                statement_end,
            )
            if resume is not None:
                # An enum constant list with class bodies runs past the
                # terminator; resuming at its end leaves the closing `;` or
                # `}` for the loop to read, like any other declaration.
                statement_end = resume
        at_declaration = False
        index = statement_end

    for declared, _ in stack:
        declared.body_end = len(masked)
    return SourceFile(path, text, masked, package, imports, types)


def constant_list_end(masked: str, index: int) -> int:
    """The offset ending an enum constant list that opens a class body.

    The declaration scan stops at the first depth-0 ``{``, which for a
    strategy-pattern constant is its body's opening brace, not the list's end.
    This walks on from there — over every body and argument list — to the
    ``;`` closing the list, or to the enum body's ``}`` when the ``;`` is
    omitted.
    """
    depth = 0
    while index < len(masked):
        character = masked[index]
        if character in "([{":
            depth += 1
        elif character == ";" and depth == 0:
            return index
        elif character in ")]}":
            if character == "}" and depth == 0:
                return index
            depth -= 1
        index += 1
    return len(masked)


def enum_constant_list(
    masked: str, start: int, terminator: int
) -> list[tuple[str, int, int]] | None:
    """The constants one declaration lists, or None when it is not the list.

    Returns ``(name, ann_start, name_start)`` per constant, so each carries its
    own Javadoc position: a comma-separated list is one declaration to the
    scan, but every constant in it is documented on its own. A segment that is
    anything other than annotations, an identifier, an optional argument list
    and an optional class body — a field's type, or an initializer's
    ``static`` — makes the declaration not a constant list.
    """
    boundaries: list[tuple[int, int]] = []
    depth = 0
    segment_start = start
    for offset in range(start, terminator):
        character = masked[offset]
        if character in "([{":
            depth += 1
        elif character in ")]}":
            depth -= 1
        elif character == "," and depth == 0:
            boundaries.append((segment_start, offset))
            segment_start = offset + 1
    boundaries.append((segment_start, terminator))

    constants: list[tuple[str, int, int]] = []
    for segment_start, segment_end in boundaries:
        ann_start = segment_start
        while ann_start < segment_end and masked[ann_start].isspace():
            ann_start += 1
        if ann_start == segment_end:
            continue  # a trailing comma
        name_start = skip_annotations(masked, ann_start)
        identifier = WORD.match(masked, name_start)
        if identifier is None or identifier.group(0) in MODIFIERS:
            return None
        rest = masked[identifier.end() : segment_end].strip()
        if rest and not rest.startswith(("(", "{")):
            return None
        constants.append((identifier.group(0), ann_start, name_start))
    return constants or None


def record_enum_constants(
    enclosing: JavaType,
    constants: list[tuple[str, int, int]],
    ann_start: int,
    terminator: int,
):
    """Attribute each listed constant, with its own Javadoc position.

    The first constant's annotations were already consumed by the declaration
    scan, so its position is the declaration's own.
    """
    for position, (name, constant_ann, name_start) in enumerate(constants):
        enclosing.fields.setdefault(name, "public")
        enclosing.members.append(
            Member(
                name=name,
                kind="enum constant",
                visibility="public",
                ann_start=ann_start if position == 0 else constant_ann,
                decl_start=name_start,
                statement_end=terminator,
                overridden=False,
                head="",
            )
        )


def split_declarators(head: str) -> list[str]:
    """Split ``a, b`` declarators at top-level commas only.

    A comma inside a generic argument, ``ConfigOption<Map<String, String>>``,
    separates type arguments, not declarators; splitting there would mint a
    public member named ``String``. The head never reaches past the first
    depth-0 ``=``, so ``int a = 1, b = 2;`` records only ``a`` — no main source
    declares one. Family 2 separately rejects that shape for ``ConfigOption``
    constants, because one shared Javadoc cannot equal two distinct runtime
    descriptions; its repair is to split the declaration.
    """
    declarators: list[str] = []
    depth = 0
    current = ""
    for character in head:
        if character in "<([":
            depth += 1
        elif character in ">)]":
            depth -= 1
        if character == "," and depth == 0:
            declarators.append(current)
            current = ""
        else:
            current += character
    declarators.append(current)
    return declarators


def record_member(
    enclosing: JavaType,
    masked: str,
    ann_start: int,
    start: int,
    name_end: int,
    params_end: int,
    terminator: int,
    statement_end: int,
) -> int | None:
    """Attribute one field, method or enum constant to its declaring type.

    Returns the offset the scan should resume at when the declaration was an
    enum constant list running past its terminator — a body-opening constant —
    and None otherwise, where the caller's position already stands.
    """
    overridden = OVERRIDE.search(masked, ann_start, start) is not None
    if 0 <= name_end < terminator:
        identifier = IDENTIFIER.search(masked, start, name_end)
        if identifier is None:
            return None
        name = identifier.group(1)
        head = masked[start:name_end].strip()
        if name == enclosing.simple:
            parameters = tuple(parameter_types(masked, name_end + 1, params_end))
            enclosing.members.append(
                Member(
                    name=name,
                    kind="constructor",
                    visibility=member_visibility(head, enclosing),
                    ann_start=ann_start,
                    decl_start=start,
                    statement_end=statement_end,
                    overridden=overridden,
                    head=head,
                    canonical=(
                        enclosing.kind == "record"
                        and parameters == enclosing.record_components
                    ),
                )
            )
            return
        if enclosing.kind == "enum":
            # Enum constants with arguments, not methods: `DEFAULT, RETRY(3);`
            # is one declaration carrying every constant. Let the full-list
            # grammar below distinguish it from a method: checking only the
            # text before the first `(` loses bare constants preceding the
            # first argument-bearing one.
            list_end = (
                constant_list_end(masked, terminator)
                if masked[terminator] == "{"
                else terminator
            )
            constants = enum_constant_list(masked, start, list_end)
            if constants is not None:
                record_enum_constants(enclosing, constants, ann_start, list_end)
                return list_end if list_end != terminator else None
        enclosing.methods.setdefault(name, []).append(
            "(" + ", ".join(parameter_types(masked, name_end + 1, params_end)) + ")"
        )
        enclosing.members.append(
            Member(
                name=name,
                kind="method",
                visibility=member_visibility(head, enclosing),
                ann_start=ann_start,
                decl_start=start,
                statement_end=statement_end,
                overridden=overridden,
                head=head,
            )
        )
        return

    head = masked[start:terminator].strip()
    if head and enclosing.kind == "enum" and masked[terminator] != "=":
        # Enum constants without arguments: `MAX_AGE, UNION` up to the `;` —
        # or the body's closing `}`, since the `;` is optional there. A
        # constant opening a class body — the strategy-pattern shape — is one
        # too: its body belongs to the constant's anonymous class, not the
        # enum, so the returned offset jumps the scan over every body to the
        # list's end.
        list_end = (
            constant_list_end(masked, terminator)
            if masked[terminator] == "{"
            else terminator
        )
        constants = enum_constant_list(masked, start, list_end)
        if constants is not None:
            record_enum_constants(enclosing, constants, ann_start, list_end)
            return list_end if list_end != terminator else None
    if not head or masked[terminator] == "{":
        identifier = IDENTIFIER.search(head) if head else None
        if (
            enclosing.kind == "record"
            and identifier is not None
            and identifier.group(1) == enclosing.simple
        ):
            # A compact record constructor: `[modifiers] Name {` with no
            # parameter list is the canonical constructor's shorthand — a
            # public constructor like any other to the presence rule.
            enclosing.members.append(
                Member(
                    name=identifier.group(1),
                    kind="constructor",
                    visibility=member_visibility(head, enclosing),
                    ann_start=ann_start,
                    decl_start=start,
                    statement_end=terminator,
                    overridden=overridden,
                    head=head,
                    canonical=True,
                )
            )
        return None
    identifier = IDENTIFIER.search(head)
    if identifier is None:
        return None
    words = [word for word in head.split() if word not in MODIFIERS]
    if len(words) <= 1 and enclosing.kind != "enum":
        return None
    visibility = (
        "private"
        if re.search(r"\bprivate\b", head)
        else "protected"
        if re.search(r"\bprotected\b", head)
        else "public"
        if re.search(r"\bpublic\b", head)
        or enclosing.kind in ("interface", "@interface", "enum")
        else "package"
    )
    for declarator in split_declarators(head):
        candidate = IDENTIFIER.search(declarator.strip())
        if candidate is not None:
            enclosing.fields.setdefault(candidate.group(1), visibility)
            enclosing.members.append(
                Member(
                    name=candidate.group(1),
                    kind="field",
                    visibility=member_visibility(head, enclosing),
                    ann_start=ann_start,
                    decl_start=start,
                    statement_end=statement_end,
                    overridden=overridden,
                    head=head,
                )
            )


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

    "Immediately before" has to step over the type's annotations, including
    their arguments. Every public type in this repository carries a Flink
    API-tier annotation, and types with custom type information add an
    argument-bearing annotation after it. Reading either as something other
    than part of the declaration leaves the class javadoc without a context and
    every bare reference in it unchecked (issues #930 and #992).
    """
    following_declaration = skip_annotations(source.masked, end)
    for candidate in all_types(source):
        if candidate.decl_start == following_declaration:
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


def javadoc_above(source: SourceFile):
    """A lookup for the Javadoc body sitting immediately above an offset.

    "Immediately above" means nothing but whitespace between the comment's end
    and the offset — which is the declaration's first annotation where it has
    any, so annotations sit between the Javadoc and the declaration exactly as
    Javadoc itself reads them.
    """
    comments = doc_comments(source.text)
    ends = [end for _, end, _ in comments]

    def above(ann_start: int) -> str | None:
        position = bisect_right(ends, ann_start) - 1
        if position < 0:
            return None
        _, end, body = comments[position]
        if source.text[end:ann_start].strip():
            return None
        return body

    return above


def surface_tier(declared: JavaType) -> str | None:
    """The tier putting a type's own members on the documented surface, or None.

    The nearest annotated enclosing declaration decides: a nested type with its
    own tier annotation answers for its own members, ``@Internal`` on the
    nearest one takes the type off the surface, and an unannotated nested type
    inherits. A type the reader cannot reach — package-private or private
    anywhere in its enclosing chain — is not on the surface either, because the
    generated API reference never shows it.
    """
    annotated = declared
    while annotated is not None and annotated.tier is None:
        annotated = annotated.outer
    if annotated is None or annotated.tier not in DOCUMENTED_TIERS:
        return None
    scope = declared
    while scope is not None:
        if scope.visibility not in ("public", "protected"):
            return None
        scope = scope.outer
    return annotated.tier


def presence_problems(source: SourceFile, above, counts: Counts) -> list[str]:
    """Rule family 1: Javadoc presence on the tier-annotated surface."""
    problems: list[str] = []
    for declared in sorted(all_types(source), key=lambda entry: entry.decl_start):
        tier = surface_tier(declared)
        if tier is None:
            continue
        body = above(declared.ann_start)
        if body is not None and javadoc_text(body):
            counts.documented += 1
        else:
            # An empty `/** */` — or a margin-only multiline one, which is why
            # emptiness is judged on the rendered text — satisfies no reader;
            # it is a distinct failure from a missing comment only in what the
            # repair asks for.
            missing = (
                "has an empty Javadoc; write"
                if body is not None
                else "has no Javadoc; add"
            )
            where = (
                f"{relative(source.path)}:{line_at(source.text, declared.decl_start)}"
            )
            problems.append(
                f"{where}: @{tier} {declared.kind} '{declared.simple}' {missing} "
                f"a type-level comment above its annotations."
            )
        constructors = [
            member for member in declared.members if member.kind == "constructor"
        ]
        has_implicit_constructor = (declared.kind == "class" and not constructors) or (
            declared.kind == "record"
            and not any(member.canonical for member in constructors)
        )
        if has_implicit_constructor:
            where = (
                f"{relative(source.path)}:{line_at(source.text, declared.decl_start)}"
            )
            problems.append(
                f"{where}: {declared.visibility} {declared.kind} "
                f"'{declared.simple}' exposes an implicit constructor with no "
                "Javadoc; declare it explicitly and add one."
            )
        for member in declared.members:
            if member.visibility not in ("public", "protected") or member.overridden:
                continue
            body = above(member.ann_start)
            if body is not None and javadoc_text(body):
                counts.documented += 1
                continue
            line = line_at(source.text, member.decl_start)
            display = (
                f"{member.name}()"
                if member.kind in ("method", "constructor")
                else member.name
            )
            missing = (
                "has an empty Javadoc; write one"
                if body is not None
                else "has no Javadoc; add one"
            )
            description = None
            if member.kind == "field" and member.visibility == "public":
                option = declares_config_option(member.head)
                constant = option and (
                    (
                        re.search(r"\bstatic\b", member.head)
                        and re.search(r"\bfinal\b", member.head)
                    )
                    or declared.kind in ("interface", "@interface")
                )
                if constant:
                    description = literal_description(source, member)
            if description is not None:
                missing += f' equal to its withDescription text "{clip(description)}"'
            exemption = (
                " (an @Override member inherits its docs and is exempt)"
                if member.kind == "method"
                else ""
            )
            problems.append(
                f"{relative(source.path)}:{line}: {member.visibility} "
                f"{member.kind} '{display}' of @{tier} type '{declared.simple}' "
                f"{missing}{exemption}."
            )
    return problems


def matching_paren(masked: str, open_paren: int) -> int:
    """The offset of the ``)`` closing the ``(`` at ``open_paren``."""
    depth = 0
    for offset in range(open_paren, len(masked)):
        if masked[offset] == "(":
            depth += 1
        elif masked[offset] == ")":
            depth -= 1
            if depth == 0:
                return offset
    return len(masked)


JAVA_ESCAPES = {
    "n": "\n",
    "t": "\t",
    "r": "\r",
    "b": "\b",
    "f": "\f",
    "s": " ",
    '"': '"',
    "'": "'",
    "\\": "\\",
}


def unescape(literal: str) -> str:
    """A Java string literal's content with its escape sequences decoded.

    An escape this does not decode is kept literally, backslash included,
    rather than guessed at or crashed on — a malformed ``\\uZZZZ`` would not
    compile, but the checker also reads trees that do not compile yet. Octal
    escapes are not decoded; no main source uses one.
    """
    out: list[str] = []
    index = 0
    while index < len(literal):
        character = literal[index]
        if character == "\\" and index + 1 < len(literal):
            following = literal[index + 1]
            if following == "u" and index + 6 <= len(literal):
                try:
                    out.append(chr(int(literal[index + 2 : index + 6], 16)))
                    index += 6
                    continue
                except ValueError:
                    pass
            decoded = JAVA_ESCAPES.get(following)
            if decoded is None:
                out.append(character)
                index += 1
                continue
            out.append(decoded)
            index += 2
        else:
            out.append(character)
            index += 1
    return "".join(out)


def string_literal_expression(text: str, start: int, end: int) -> list[str] | None:
    """Raw literals when an expression contains only strings, ``+`` and parentheses."""
    index = start

    def skip_trivia():
        nonlocal index
        while index < end:
            if text[index].isspace():
                index += 1
            elif text.startswith("//", index):
                newline = text.find("\n", index + 2, end)
                index = end if newline < 0 else newline + 1
            elif text.startswith("/*", index):
                terminator = text.find("*/", index + 2, end)
                index = end if terminator < 0 else terminator + 2
            else:
                return

    def expression() -> list[str] | None:
        nonlocal index
        values = term()
        if values is None:
            return None
        while True:
            skip_trivia()
            if index >= end or text[index] != "+":
                return values
            index += 1
            following = term()
            if following is None:
                return None
            values.extend(following)

    def term() -> list[str] | None:
        nonlocal index
        skip_trivia()
        if text.startswith('"""', index):
            stop = skip_text_block(text, index)
            if stop > end:
                return None
            value = text[index + 3 : stop - 3]
            index = stop
            return [value]
        if index < end and text[index] == '"':
            stop = skip_quoted(text, index, '"')
            if stop > end:
                return None
            value = text[index + 1 : stop - 1]
            index = stop
            return [value]
        if index < end and text[index] == "(":
            index += 1
            values = expression()
            skip_trivia()
            if values is None or index >= end or text[index] != ")":
                return None
            index += 1
            return values
        return None

    literals = expression()
    skip_trivia()
    return literals if literals is not None and index == end else None


def javadoc_text(body: str) -> str:
    """A Javadoc comment's prose with only the comment margin removed.

    The margin is a leading ``*`` followed by a space, or a bare ``*`` line.
    A content-leading ``*`` — ``*.googleapis.com``, say — is prose, not margin,
    and stays.
    """
    lines: list[str] = []
    for line in body.split("\n"):
        stripped = line.strip()
        if stripped == "*":
            stripped = ""
        elif stripped.startswith("* "):
            stripped = stripped[2:]
        lines.append(stripped)
    return " ".join(" ".join(lines).split())


def literal_description(source: SourceFile, member: Member) -> str | None:
    """The description a constant's statement builds from string literals.

    Returns the concatenated, unescaped, whitespace-normalized text, or None
    when the statement carries no ``withDescription`` call or its argument is
    not built from string literals alone — a ``Description`` object has no one
    flat text to hold the Javadoc to. The statement's end was found with a
    string-aware scan, so a description containing ``;`` does not end it.
    """
    found = None
    for candidate in WITH_DESCRIPTION.finditer(
        source.masked, member.decl_start, member.statement_end
    ):
        found = candidate
    if found is None:
        return None
    open_paren = found.end() - 1
    close = matching_paren(source.masked, open_paren)
    literals = string_literal_expression(source.text, open_paren + 1, close)
    if not literals:
        return None
    return " ".join("".join(unescape(literal) for literal in literals).split())


def has_multiple_initialized_declarators(source: SourceFile, member: Member) -> bool:
    """Whether a field initializer is followed by another top-level declarator."""
    equals = source.masked.find("=", member.decl_start, member.statement_end)
    if equals < 0:
        return False
    depth = 0
    for character in source.masked[equals + 1 : member.statement_end]:
        if character in "([{":
            depth += 1
        elif character in ")]}":
            depth -= 1
        elif character == "," and depth == 0:
            return True
    return False


def clip(text: str, limit: int = 100) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def divergence_clips(javadoc: str, description: str) -> tuple[str, str]:
    """Both texts clipped to a window around their first difference.

    A clipped head alone can show two identical-looking strings when the
    difference sits past the clip, which tells the reader nothing; the window
    starts shortly before the first differing character instead.
    """
    divergence = next(
        (
            offset
            for offset, (ours, theirs) in enumerate(zip(javadoc, description))
            if ours != theirs
        ),
        min(len(javadoc), len(description)),
    )
    start = max(0, divergence - 40)

    def window(text: str) -> str:
        return ("..." if start else "") + clip(text[start:])

    return window(javadoc), window(description)


def without_annotations(head: str) -> str:
    """A declaration head with type-use annotations replaced by whitespace."""
    stripped = list(head)
    index = 0
    while found := ANNOTATION.search(head, index):
        end = found.end()
        while end < len(head) and head[end].isspace():
            end += 1
        if end < len(head) and head[end] == "(":
            end = matching_paren(head, end) + 1
        stripped[found.start() : end] = " " * (end - found.start())
        index = end
    return "".join(stripped)


def declares_config_option(head: str) -> bool:
    """True when the field's declared type is itself ``ConfigOption``.

    A container of options — ``List<ConfigOption<?>> ALL`` — carries the name
    only inside a type argument; a ``withDescription`` in its initializer
    belongs to the nested options, not to the field, so comparing the field's
    Javadoc against it would be a false failure. The first non-modifier word
    of the head is the declared type; a dotted qualification is stripped, so
    the fully qualified spelling counts too, while a type merely *containing*
    the name — ``MyConfigOptionHolder`` — does not.
    """
    for word in without_annotations(head).split():
        if word in MODIFIERS:
            continue
        base = word.split("<", 1)[0]
        return base.rsplit(".", 1)[-1] == "ConfigOption"
    return False


def option_description_problems(source: SourceFile, above, counts: Counts) -> list[str]:
    """Rule family 2: a ConfigOption's Javadoc equals its withDescription text."""
    problems: list[str] = []
    for declared in sorted(all_types(source), key=lambda entry: entry.decl_start):
        for member in declared.members:
            if member.kind != "field" or member.visibility != "public":
                continue
            if not declares_config_option(member.head):
                continue
            constant = (
                re.search(r"\bstatic\b", member.head)
                and re.search(r"\bfinal\b", member.head)
            ) or declared.kind in ("interface", "@interface")
            if not constant:
                continue
            if has_multiple_initialized_declarators(source, member):
                where = (
                    f"{relative(source.path)}:{line_at(source.text, member.decl_start)}"
                )
                problems.append(
                    f"{where}: ConfigOption declaration starting at '{member.name}' "
                    "has multiple declarators; split each constant into its own "
                    "declaration so each Javadoc can equal its withDescription text."
                )
                continue
            description = literal_description(source, member)
            if description is None:
                continue
            body = above(member.ann_start)
            if body is not None and not javadoc_text(body):
                # An empty comment is no more a description than none at all.
                body = None
            if body is None:
                if surface_tier(declared) is None:
                    # On the tier-annotated surface the presence rule already
                    # asks for the comment; off it, this rule does.
                    where = (
                        f"{relative(source.path)}:"
                        f"{line_at(source.text, member.decl_start)}"
                    )
                    problems.append(
                        f"{where}: ConfigOption '{member.name}' has no Javadoc; "
                        f"add one equal to its withDescription text "
                        f'"{clip(description)}".'
                    )
                continue
            javadoc = javadoc_text(body)
            if javadoc == description:
                counts.options += 1
            else:
                shown_javadoc, shown_description = divergence_clips(
                    javadoc, description
                )
                where = (
                    f"{relative(source.path)}:{line_at(source.text, member.decl_start)}"
                )
                problems.append(
                    f"{where}: make the Javadoc of '{member.name}' equal to its "
                    f'withDescription text; the Javadoc reads "{shown_javadoc}" '
                    f'but withDescription says "{shown_description}".'
                )
    return problems


@dataclass
class Counts:
    """What the summary line reports when every rule holds."""

    references: int = 0
    documented: int = 0
    options: int = 0


def check() -> tuple[Counts, list[str]]:
    paths = sorted(ROOT.glob(MAIN_SOURCE_PATTERN))
    if not paths:
        raise FileNotFoundError(f"{MAIN_SOURCE_PATTERN} matched no Java sources.")

    sources = [index_source(path, path.read_text(encoding="utf-8")) for path in paths]
    by_qualified: dict[str, JavaType] = {}
    for source in sources:
        for declared in all_types(source):
            by_qualified[declared.qualified] = declared

    problems: list[str] = []
    counts = Counts()
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
                counts.references += 1
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
    for source in sources:
        above = javadoc_above(source)
        problems.extend(presence_problems(source, above, counts))
        problems.extend(option_description_problems(source, above, counts))
    return counts, problems


def main() -> int:
    try:
        counts, problems = check()
    except (OSError, FileNotFoundError) as error:
        print(f"Could not read the Java sources: {error}", file=sys.stderr)
        return 2

    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        print(
            f"{len(problems)} Javadoc problems; every message above names its repair.",
            file=sys.stderr,
        )
        return 1

    print(
        f"{counts.references} parameterless Javadoc member references reach a member "
        f"they can name; {counts.documented} tier-annotated declarations carry "
        f"Javadoc; {counts.options} ConfigOption Javadocs equal their "
        f"withDescription text."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
