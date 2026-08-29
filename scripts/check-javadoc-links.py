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

Exit codes: 0 clean, 1 a policy failure, 2 invalid or unavailable sources.
"""

from __future__ import annotations

import re
import sys
from bisect import bisect_right
from dataclasses import dataclass, field
from pathlib import Path

from java_ast import (
    TYPE_DECLARATIONS,
    JavaSource,
    JavaSyntaxError,
    annotation_name,
    annotations,
    code_named_children,
    declaration_target,
    modifiers,
    string_literal_content,
)
from java_example_regions import line_at
from tree_sitter import Node

ROOT = Path(__file__).resolve().parent.parent
MAIN_SOURCE_PATTERN = "flink-*/src/main/java*/**/*.java"

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
# The Flink API-tier annotations, in the order surface_tier() reports them.
FLINK_ANNOTATION = "org.apache.flink.annotation"
TIERS = ("Public", "PublicEvolving", "Experimental", "Internal")
DOCUMENTED_TIERS = frozenset(["Public", "PublicEvolving", "Experimental"])
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
    overridden: bool
    head: str  # masked modifiers-and-type text, for the ConfigOption rule
    canonical: bool = False  # whether a record constructor declares its canonical form
    node: Node | None = None


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
    header_javadocs: set[tuple[int, int]] = field(default_factory=set)


@dataclass
class SourceFile:
    path: Path
    text: str
    masked: str
    package: str
    imports: dict[str, str]
    types: list[JavaType]
    parsed: JavaSource


def relative(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def mask(source: str, parsed: JavaSource) -> str:
    """Blank AST comments and literals, preserving offsets and newlines."""
    out = list(source)
    for node in parsed.nodes(
        "block_comment",
        "character_literal",
        "line_comment",
        "string_literal",
    ):
        for position in range(parsed.start(node), min(parsed.end(node), len(out))):
            if out[position] != "\n":
                out[position] = " "
    return "".join(out)


def tier_of(annotations: str, imports: dict[str, str]) -> str | None:
    """The Flink API-tier annotation a declaration carries, or None.

    A simple name counts only when the file imports it from
    ``org.apache.flink.annotation``: with ``AvoidStarImport`` on, an unimported
    ``@Experimental`` is somebody else's annotation, not a tier. An annotation
    written after a visibility modifier still counts because the Java grammar
    attaches it to the declaration's modifiers node.
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


def index_source(path: Path, text: str) -> SourceFile:
    """Index every type a source declares, with its own fields and methods."""
    parsed = JavaSource.parse(relative(path), text)
    masked = mask(text, parsed)
    package_node = next(parsed.nodes("package_declaration"), None)
    package = declaration_target(parsed, package_node) if package_node else ""
    imports: dict[str, str] = {}
    for import_node in parsed.nodes("import_declaration"):
        qualified = declaration_target(parsed, import_node)
        imports[qualified.rsplit(".", 1)[-1]] = qualified

    types: list[JavaType] = []

    def modifier_text(node: Node) -> str:
        modifiers = next(
            (child for child in node.children if child.type == "modifiers"), None
        )
        return (
            masked[parsed.start(modifiers) : parsed.end(modifiers)]
            if modifiers is not None
            else ""
        )

    def is_override(node: Node) -> bool:
        return any(
            annotation_name(parsed, annotation).rsplit(".", 1)[-1] == "Override"
            for annotation in annotations(node)
        )

    def signatures(parameters: Node | None) -> tuple[str, ...]:
        if parameters is None:
            return ()
        return tuple(
            parameter_types(
                masked, parsed.start(parameters) + 1, parsed.end(parameters) - 1
            )
        )

    def direct_members(body: Node) -> list[Node]:
        found: list[Node] = []
        for child in body.named_children:
            if child.type == "enum_body_declarations":
                found.extend(child.named_children)
            elif child.type not in ("block_comment", "line_comment"):
                found.append(child)
        return found

    def add_type(node: Node, outer: JavaType | None) -> JavaType:
        name_node = node.child_by_field_name("name")
        body = node.child_by_field_name("body")
        assert name_node is not None and body is not None
        simple = parsed.text(name_node)
        qualified = (
            f"{outer.qualified}.{simple}"
            if outer is not None
            else (f"{package}.{simple}" if package else simple)
        )
        kinds = {
            "annotation_type_declaration": "@interface",
            "class_declaration": "class",
            "enum_declaration": "enum",
            "interface_declaration": "interface",
            "record_declaration": "record",
        }
        declaration_modifiers = modifiers(node)
        declared = JavaType(
            simple=simple,
            qualified=qualified,
            kind=kinds[node.type],
            decl_start=parsed.start(name_node),
            body_start=parsed.start(body),
            body_end=parsed.end(body),
            outer=outer,
            ann_start=parsed.start(node),
            tier=tier_of(
                " ".join(
                    f"@{annotation_name(parsed, annotation)}"
                    for annotation in annotations(node)
                ),
                imports,
            ),
            visibility=member_visibility(modifier_text(node), outer),
            record_components=(
                signatures(node.child_by_field_name("parameters"))
                if node.type == "record_declaration"
                else ()
            ),
            header_javadocs=(
                {
                    (parsed.start(child), parsed.end(child))
                    for child in declaration_modifiers.named_children
                    if child.type == "block_comment"
                    and parsed.text(child).startswith("/**")
                }
                if declaration_modifiers is not None
                else set()
            ),
        )
        if outer is None:
            types.append(declared)
        else:
            outer.nested[simple] = declared

        for child in direct_members(body):
            if child.type in TYPE_DECLARATIONS:
                add_type(child, declared)
                continue
            if child.type == "enum_constant":
                constant_name = child.child_by_field_name("name")
                if constant_name is None:
                    continue
                name = parsed.text(constant_name)
                declared.fields[name] = "public"
                declared.members.append(
                    Member(
                        name,
                        "enum constant",
                        "public",
                        parsed.start(child),
                        parsed.start(constant_name),
                        False,
                        "",
                        node=child,
                    )
                )
                continue
            if child.type in ("constant_declaration", "field_declaration"):
                head = f"{modifier_text(child)} {parsed.text(child.child_by_field_name('type'))}"
                visibility = member_visibility(head, declared)
                for declarator in (
                    item
                    for item in child.named_children
                    if item.type == "variable_declarator"
                ):
                    field_name = declarator.child_by_field_name("name")
                    if field_name is None:
                        continue
                    name = parsed.text(field_name)
                    declared.fields.setdefault(name, visibility)
                    declared.members.append(
                        Member(
                            name,
                            "field",
                            visibility,
                            parsed.start(child),
                            parsed.start(field_name),
                            False,
                            head,
                            node=child,
                        )
                    )
                continue
            if child.type not in (
                "annotation_type_element_declaration",
                "compact_constructor_declaration",
                "constructor_declaration",
                "method_declaration",
            ):
                continue
            name_node = child.child_by_field_name("name")
            if name_node is None:
                continue
            name = parsed.text(name_node)
            constructor = child.type in (
                "compact_constructor_declaration",
                "constructor_declaration",
            )
            kind = "constructor" if constructor else "method"
            head = modifier_text(child)
            visibility = member_visibility(head, declared)
            if not constructor:
                parameters = child.child_by_field_name("parameters")
                declared.methods.setdefault(name, []).append(
                    "(" + ", ".join(signatures(parameters)) + ")"
                )
            parameters = child.child_by_field_name("parameters")
            canonical = child.type == "compact_constructor_declaration" or (
                declared.kind == "record"
                and signatures(parameters) == declared.record_components
            )
            declared.members.append(
                Member(
                    name,
                    kind,
                    visibility,
                    parsed.start(child),
                    parsed.start(name_node),
                    is_override(child),
                    head,
                    canonical,
                    child,
                )
            )
        return declared

    for child in parsed.root.named_children:
        if child.type in TYPE_DECLARATIONS:
            add_type(child, None)
    return SourceFile(path, text, masked, package, imports, types, parsed)


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

    A comment inside a type declaration's modifiers, or immediately before the
    declaration, documents *that* type, so its ``#member`` references resolve
    against it; anything else resolves against the type whose body holds the
    comment.

    Tree-sitter puts a comment written after an annotation inside the modifiers
    node, while a comment above the annotations remains outside the declaration.
    Both forms must keep the type context. Every public type in this repository
    carries a Flink API-tier annotation, and types with custom type information
    add an argument-bearing annotation after it. Losing either form leaves the
    class Javadoc without a context and every bare reference in it unchecked
    (issues #930 and #992).
    """
    for candidate in all_types(source):
        if (start, end) in candidate.header_javadocs:
            return candidate
        if (
            candidate.ann_start >= end
            and not source.masked[end : candidate.ann_start].strip()
        ):
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


def doc_comments(source: SourceFile) -> list[tuple[int, int, str]]:
    """Every real Javadoc comment as (start offset, end offset, body)."""
    return [
        (
            source.parsed.start(node),
            source.parsed.end(node),
            source.text[source.parsed.start(node) + 3 : source.parsed.end(node) - 2],
        )
        for node in source.parsed.nodes("block_comment")
        if source.parsed.text(node).startswith("/**")
    ]


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
    comments = doc_comments(source)
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

    Tree-sitter rejects malformed Java before this function runs.
    An escape the grammar accepts but this function does not decode is kept
    literally, backslash included, rather than guessed at.
    Octal escapes are not decoded; no main source uses one.
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
    flat text to hold the Javadoc to. The field and invocation boundaries come
    from the syntax tree, so punctuation inside a description stays literal.
    """
    if member.node is None:
        return None

    calls = [
        call
        for call in source.parsed.nodes("method_invocation", below=member.node)
        if (name := call.child_by_field_name("name")) is not None
        and source.parsed.text(name) == "withDescription"
    ]
    if not calls:
        return None
    # A fluent chain nests the earlier receiver call under the outer, runtime
    # call. Pre-order therefore puts the last invocation first.
    arguments = calls[0].child_by_field_name("arguments")
    values = code_named_children(arguments) if arguments is not None else ()
    if len(values) != 1:
        return None

    def literals_of(node: Node) -> list[str] | None:
        if node.type == "string_literal":
            literal = string_literal_content(source.parsed, node)
            return [literal] if literal is not None else None
        children = code_named_children(node)
        if node.type == "parenthesized_expression" and len(children) == 1:
            return literals_of(children[0])
        if node.type == "binary_expression" and any(
            child.type == "+" for child in node.children
        ):
            operands = children
            if len(operands) != 2:
                return None
            left = literals_of(operands[0])
            right = literals_of(operands[1])
            return None if left is None or right is None else [*left, *right]
        return None

    literals = literals_of(values[0])
    if not literals:
        return None
    return " ".join("".join(unescape(literal) for literal in literals).split())


def has_multiple_declarators(source: SourceFile, member: Member) -> bool:
    """Whether one field declaration declares more than one variable."""
    if member.node is None:
        return False
    declarators = [
        child
        for child in member.node.named_children
        if child.type == "variable_declarator"
    ]
    return len(declarators) > 1


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
    reported_multi_declarations: set[int] = set()
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
            if has_multiple_declarators(source, member):
                declaration_id = (
                    member.node.start_byte if member.node is not None else -1
                )
                if declaration_id in reported_multi_declarations:
                    continue
                reported_multi_declarations.add(declaration_id)
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
        for start, end, body in doc_comments(source):
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
    except (JavaSyntaxError, OSError, FileNotFoundError) as error:
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
