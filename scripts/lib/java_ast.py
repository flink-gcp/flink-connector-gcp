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
"""Shared Tree-sitter plumbing for repository Java source checkers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import tree_sitter_java
from tree_sitter import Language, Node, Parser, Tree

JAVA_LANGUAGE = Language(tree_sitter_java.language())
JAVA_PARSER = Parser(JAVA_LANGUAGE)

TYPE_DECLARATIONS = frozenset(
    {
        "annotation_type_declaration",
        "class_declaration",
        "enum_declaration",
        "interface_declaration",
        "record_declaration",
    }
)
ANNOTATIONS = frozenset({"annotation", "marker_annotation"})
# The main source roots the two Flink API tier checkers read: every connector
# module with its Flink-version compatibility roots (java-flink1, java-flink2),
# and the Tier-3 job modules. Other checkers define their own roots.
API_TIER_SOURCE_PATTERNS = (
    "*/src/main/java*/**/*.java",
    "kubernetes/apps/*/src/main/java*/**/*.java",
)
FLINK_ANNOTATION_PACKAGE = "org.apache.flink.annotation"
# Flink's API stability tiers, stablest first.
FLINK_TIERS = ("Public", "PublicEvolving", "Experimental", "Internal")
COMMENTS = frozenset({"block_comment", "line_comment"})


class JavaSyntaxError(ValueError):
    """A Java source contains a Tree-sitter ERROR or MISSING node."""


def walk(node: Node):
    """Yield one node and all descendants in source order."""
    yield node
    for child in node.children:
        yield from walk(child)


@dataclass(frozen=True)
class JavaSource:
    """One UTF-8 Java compilation unit and its syntax tree."""

    path: Path | str
    bytes: bytes
    tree: Tree

    @classmethod
    def parse(cls, path: Path | str, source: str | bytes) -> JavaSource:
        data = source.encode("utf-8") if isinstance(source, str) else source
        tree = JAVA_PARSER.parse(data)
        parsed = cls(path, data, tree)
        parsed.require_valid_syntax()
        return parsed

    def require_valid_syntax(self) -> None:
        if not self.tree.root_node.has_error:
            return
        broken = next(
            node
            for node in walk(self.tree.root_node)
            if node.type == "ERROR" or node.is_missing
        )
        line = broken.start_point.row + 1
        column = broken.start_point.column + 1
        kind = "MISSING" if broken.is_missing else "ERROR"
        raise JavaSyntaxError(
            f"{self.path}:{line}:{column}: Java parser produced a {kind} syntax "
            "node; fix the source before trusting a partial checker inventory."
        )

    @property
    def root(self) -> Node:
        return self.tree.root_node

    def text(self, node: Node) -> str:
        return self.bytes[node.start_byte : node.end_byte].decode("utf-8")

    def line(self, node: Node) -> int:
        return node.start_point.row + 1

    def char_offset(self, byte_offset: int) -> int:
        """Translate a Tree-sitter UTF-8 byte offset to a Python string offset."""
        return len(self.bytes[:byte_offset].decode("utf-8"))

    def start(self, node: Node) -> int:
        return self.char_offset(node.start_byte)

    def end(self, node: Node) -> int:
        return self.char_offset(node.end_byte)

    def nodes(self, *types: str, below: Node | None = None):
        accepted = frozenset(types)
        start = self.root if below is None else below
        for node in walk(start):
            if node.type in accepted:
                yield node


def modifiers(node: Node) -> Node | None:
    """Return a declaration's modifiers node, when it has one."""
    return next((child for child in node.children if child.type == "modifiers"), None)


def annotations(node: Node):
    """Yield annotations attached directly to a declaration."""
    declaration_modifiers = modifiers(node)
    if declaration_modifiers is None:
        return
    yield from (
        child
        for child in declaration_modifiers.named_children
        if child.type in ANNOTATIONS
    )


def annotation_name(source: JavaSource, annotation: Node) -> str:
    """Return an annotation's possibly-qualified name without the leading @."""
    name = annotation.child_by_field_name("name")
    if name is None and annotation.named_child_count:
        name = annotation.named_children[0]
    return source.text(name) if name is not None else ""


def api_tier_sources(root: Path) -> list[Path]:
    """Every Java source under ``root`` that API_TIER_SOURCE_PATTERNS matches, sorted."""
    return sorted(
        {path for pattern in API_TIER_SOURCE_PATTERNS for path in root.glob(pattern)}
    )


def type_imports(source: JavaSource) -> dict[str, str]:
    """Map each single-type import's simple name to its qualified name.

    Static and on-demand imports are left out: neither names an annotation
    type, and a static member sharing a type's simple name must not shadow it.
    """
    imports: dict[str, str] = {}
    for node in source.nodes("import_declaration"):
        if any(child.type in ("static", "asterisk") for child in node.children):
            continue
        qualified = declaration_target(source, node)
        imports[qualified.rsplit(".", 1)[-1]] = qualified
    return imports


def flink_tier(
    source: JavaSource, declaration: Node, imports: dict[str, str]
) -> str | None:
    """The Flink API-tier annotation a declaration itself carries, or None.

    A qualified name counts when it names the annotation in
    ``org.apache.flink.annotation``; a simple name counts only when ``imports``
    (from ``type_imports``) maps it there, because with ``AvoidStarImport`` on,
    an unimported ``@Internal`` is somebody else's annotation. An annotation
    written after a modifier counts too, since the grammar attaches it to the
    same modifiers node. A declaration carrying two tiers reports the stablest.
    """
    names = {annotation_name(source, node) for node in annotations(declaration)}
    for tier in FLINK_TIERS:
        qualified = f"{FLINK_ANNOTATION_PACKAGE}.{tier}"
        if qualified in names or (tier in names and imports.get(tier) == qualified):
            return tier
    return None


def code_named_children(node: Node) -> tuple[Node, ...]:
    """Return named syntax children without comments, Java's AST trivia."""
    return tuple(child for child in node.named_children if child.type not in COMMENTS)


def string_literal_content(source: JavaSource, node: Node) -> str | None:
    """Return a string literal's raw content without its one- or three-quote delimiter."""
    if node.type != "string_literal":
        return None
    literal = source.text(node)
    width = 3 if literal.startswith('"""') else 1
    return literal[width:-width]


def declaration_target(source: JavaSource, node: Node) -> str:
    """Return a package or import declaration's identifier from its AST child."""
    children = code_named_children(node)
    target = next(
        (
            child
            for child in children
            if child.type in ("identifier", "scoped_identifier")
        ),
        None,
    )
    if target is None:
        return ""
    wildcard = ".*" if any(child.type == "asterisk" for child in children) else ""
    return source.text(target) + wildcard
