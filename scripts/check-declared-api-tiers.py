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
"""Require a Flink API tier annotation on every top-level main type (issue #1567).

AGENTS.md requires every main-tree class to carry @Public, @PublicEvolving,
@Experimental or @Internal from org.apache.flink.annotation. Nothing held the
rule: check-flink-api-tiers.py audits the tiers of the Flink types the main
sources *import*, not the tiers this repository's own declarations carry.

The rule reads top-level types only — classes, interfaces, enums, records and
annotation types declared directly in a compilation unit. A nested type
inherits its enclosing type's tier, which is how check-javadoc-links.py reads
the surface too; that checker's Javadoc-presence rule also skips a type with no
tier anywhere in its enclosing chain, so an unannotated top-level type escapes
both. A file declaring no type, such as package-info.java, has nothing to
annotate. What counts as a tier is java_ast.flink_tier, which both checkers
share: a simple name counts only when the file imports it from
org.apache.flink.annotation, so a wildcard import of that package (which
AvoidStarImport already forbids) reads as no tier.

There is no allowlist, because the rule has no exceptions, and so no curate-*
skill: the failure message names the file and the annotation to add.

Exit codes: 0 clean, 1 an unannotated top-level type, 2 a source that cannot
be read or parsed, or no source at all.
"""

import sys
from pathlib import Path

from java_ast import (
    API_TIER_SOURCE_PATTERNS,
    JavaSource,
    JavaSyntaxError,
    api_tier_sources,
    flink_tier,
    type_imports,
)

ROOT = Path(__file__).resolve().parent.parent
KINDS = {
    "annotation_type_declaration": "annotation type",
    "class_declaration": "class",
    "enum_declaration": "enum",
    "interface_declaration": "interface",
    "record_declaration": "record",
}


def unannotated(parsed: JavaSource) -> list[str]:
    """One problem per top-level type in a compilation unit that carries no tier."""
    imports = type_imports(parsed)
    problems: list[str] = []
    for declaration in parsed.root.named_children:
        if declaration.type not in KINDS or flink_tier(parsed, declaration, imports):
            continue
        name = declaration.child_by_field_name("name")
        problems.append(
            f"{parsed.path}:{parsed.line(declaration)}: top-level "
            f"{KINDS[declaration.type]} '{parsed.text(name)}' carries no Flink API "
            f"tier annotation. Add @Internal (import "
            f"org.apache.flink.annotation.Internal) unless the type belongs on the "
            f"published surface; a public entry point takes @Public, "
            f"@PublicEvolving or @Experimental instead (ADR-0124, ADR-0141)."
        )
    return problems


def check(root: Path) -> tuple[int, list[str]]:
    """Return how many source files were read, and every problem found."""
    files = api_tier_sources(root)
    problems: list[str] = []
    for source in files:
        parsed = JavaSource.parse(source.relative_to(root), source.read_bytes())
        problems.extend(unannotated(parsed))
    return len(files), problems


def main() -> int:
    try:
        count, problems = check(ROOT)
    except (JavaSyntaxError, OSError) as error:
        print(error, file=sys.stderr)
        return 2
    if count == 0:
        print(
            f"No main sources match {', '.join(API_TIER_SOURCE_PATTERNS)} under {ROOT}.",
            file=sys.stderr,
        )
        return 2
    if problems:
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        print(
            f"\n{len(problems)} top-level main types carry no Flink API tier "
            "annotation; every message above names its repair.",
            file=sys.stderr,
        )
        return 1
    print(f"Every top-level type in {count} main sources carries a Flink API tier.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
