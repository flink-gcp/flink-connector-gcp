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
"""Hold the configuration reference to the options the connectors actually take (issue #89).

Two directions, both required, for each mapping in scripts/config/option-docs.toml:

* **Coverage** — every public builder setter of a module's `*Options` /
  `*SinkBuilder` / `*SourceBuilder` classes, plus any class its `sources` list
  names, is named in that module's reference page, and every `ConfigOption` key
  of the Table API surface is named in the page documenting it. A knob added
  without a doc row fails here.
* **Staleness** — every option an *option table* names exists in the source. A
  renamed or deleted knob fails here rather than lingering as a row nobody can
  act on.
* **Reach** — a public builder no mapping can see is itself a failure, since
  neither direction above says anything about a class it never reads (#328).

An option table is one whose first column header is exactly `Option`. That is
the whole selection rule, and it is what keeps this check off the metadata,
type-mapping and policy tables the same pages carry — naming a table's first
column `Option` is how you opt it in.

The reference pages are hand-written rather than generated, decided on #89:
their tables group knobs (one Pub/Sub row covers eight `retry*` setters) and
carry defaults the sources do not hold (an unset knob's default belongs to the
client library). Generation would lose both. This script buys the property
generation would have given for free — that the set of options cannot drift —
and nothing else.

Exit codes: 0 clean, 1 policy violation (undocumented or stale option),
2 infrastructure or config authoring error (missing file, invalid Java, a
source that parses to no options at all, malformed config).
"""

import fnmatch
import os.path
import re
import sys
from pathlib import Path

from java_ast import (
    TYPE_DECLARATIONS,
    JavaSource,
    JavaSyntaxError,
    annotation_name,
    annotations,
    code_named_children,
    string_literal_content,
)

try:
    import tomllib  # stdlib since 3.11
except ModuleNotFoundError:  # pragma: no cover - version guard, not logic
    sys.exit(
        "This script needs Python 3.11+ (tomllib). mise.toml pins a suitable "
        "python; run `mise x -- just check-option-docs`, or any python3 >= 3.11. "
        "CI installs one with actions/setup-python."
    )

ROOT = Path(__file__).resolve().parent.parent
CONFIG = Path(__file__).resolve().parent / "config" / "option-docs.toml"

# The three source shapes a module's builder options live in. A class named for
# what it *is* rather than for the options it takes is reached by a `sources`
# entry instead, and `unmapped_public_builders` below is what makes finding one
# somebody's problem rather than nobody's.
#
# `*SerializationSchema.java` is deliberately absent, and the boundary is worth
# stating because seven `with*` methods sit just outside it (`withAttributes`,
# `withOrderingKey`, `withMethod`, `withUrl`, `withHeaders`, `withOidcToken`,
# `withOAuthToken`). Those configure a *record*, not the sink: they compose a
# schema value the builder then takes as one option, which is why the reference
# pages document `serializer` and point at the connector page and the JavaDoc
# for what a schema can be told to do. Widening the globs to cover them would
# also mean this script deciding which `with*` on which fluent type is an
# option, which is a judgement it has no way to make.
#
# Widening them to reach `PubSubDeadLetterQueue` was the other candidate on
# #328, and it was measured rather than argued. `*Queue.java` also matches
# `base/failure/DeadLetterQueue.java`, an interface with no setters in an
# unmapped module — exit 1 from the stray-module guard, then exit 2 from the
# zero-setter guard once the module is mapped. A wider pattern reaching every
# class with a nested builder adds `BigQueryDynamicSink` and `SubscriptionInfo`,
# 19 setters that are `@Internal` and would each demand a row or an [exempt]
# entry. Naming the one class costs one config line and no false demands.
SOURCE_GLOBS = ("*Options.java", "*SinkBuilder.java", "*SourceBuilder.java")

# A table row's first cell, and the backticked identifiers inside it. The cell
# may name several options (`subscription` / `subscriptions`), and an entry may
# carry an argument list to distinguish overloads (`timePartitioning(type)`).
BACKTICKED = re.compile(r"`([A-Za-z][\w.\-]*)(?:\([^`]*\))?`")


def fail(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(1)


def infra(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(2)


def parse_java(path: Path, text: str | None = None) -> JavaSource:
    """Parse one Java source or terminate before using a partial inventory."""
    try:
        return JavaSource.parse(
            path.relative_to(ROOT),
            path.read_bytes() if text is None else text,
        )
    except JavaSyntaxError as error:
        infra(str(error))


def builder_setter_names(parsed: JavaSource) -> set[str]:
    """Public methods whose AST return type is a connector builder type."""
    found: set[str] = set()
    for method in parsed.nodes("method_declaration"):
        modifiers = next(
            (child for child in method.children if child.type == "modifiers"), None
        )
        modifier_tokens = (
            {child.type for child in modifiers.children}
            if modifiers is not None
            else set()
        )
        if "public" not in modifier_tokens or "static" in modifier_tokens:
            continue
        if method.child_by_field_name("type_parameters") is not None:
            continue
        return_type = method.child_by_field_name("type")
        name = method.child_by_field_name("name")
        if return_type is None or name is None:
            continue
        raw_type = re.sub(r"\s+", "", parsed.text(return_type))
        raw_base = raw_type.split("<", 1)[0]
        if "." in raw_base:
            continue
        simple_type = raw_base
        if simple_type == "Builder" or simple_type.endswith("Builder"):
            found.add(parsed.text(name))
    return found


def config_option_keys(parsed: JavaSource) -> set[str]:
    """String-literal arguments of ConfigOptions.key(...) invocations."""
    found: set[str] = set()
    for call in parsed.nodes("method_invocation"):
        name = call.child_by_field_name("name")
        owner = call.child_by_field_name("object")
        arguments = call.child_by_field_name("arguments")
        if (
            name is None
            or owner is None
            or arguments is None
            or parsed.text(name) != "key"
            or parsed.text(owner) != "ConfigOptions"
        ):
            continue
        values = code_named_children(arguments)
        if len(values) == 1 and values[0].type == "string_literal":
            literal = string_literal_content(parsed, values[0])
            if literal is not None:
                found.add(literal)
    return found


def top_level_is_internal(parsed: JavaSource) -> bool:
    """Whether the compilation unit's first top-level type is @Internal."""
    declaration = next(
        (
            child
            for child in parsed.root.named_children
            if child.type in TYPE_DECLARATIONS
        ),
        None,
    )
    return declaration is not None and any(
        annotation_name(parsed, annotation).rsplit(".", 1)[-1] == "Internal"
        for annotation in annotations(declaration)
    )


def read(path: Path) -> str:
    if not path.is_file():
        infra(f"{path.relative_to(ROOT)} does not exist; {CONFIG.name} names it.")
    return path.read_text(encoding="utf-8")


def option_table_entries(page: Path) -> dict[str, int]:
    """Options named in the first column of the page's `Option`-headed tables.

    Returns each name mapped to the 1-based line it was found on, so a failure
    can point at the row rather than at the file.
    """
    entries: dict[str, int] = {}
    in_table = False
    fenced = False
    for number, line in enumerate(read(page).splitlines(), start=1):
        if line.lstrip().startswith(("```", "~~~")):
            # An example table in a snippet earns no coverage credit.
            fenced = not fenced
            in_table = False
            continue
        if fenced or not line.startswith("|"):
            in_table = False
            continue
        cells = line.split("|")
        first = cells[1].strip() if len(cells) > 1 else ""
        if first == "Option":
            in_table = True
            continue
        if not in_table or set(first) <= set("- :"):
            continue
        for name in BACKTICKED.findall(first):
            entries.setdefault(name, number)
    return entries


def main_source_trees() -> list[Path]:
    """Every module's main source roots — the one place this location is spelled.

    `java*` rather than `java`, because `java-flink1` / `java-flink2` hold the
    per-major `CrossVersionSink` seam (ADR-0054). Nothing there declares an
    option today; reading them anyway is what keeps one from being invisible if
    something ever does, and is what check-metric-docs.py already does.

    Four things need this — the glob scan, the reach guard, the stray-module
    guard and the per-module lookup below — and spelling it four times means a
    fifth source root has four edit sites and three chances to be missed.
    """
    return sorted(ROOT.glob("*/src/main/java*"))


def main_source_roots(module: str) -> list[Path]:
    """The subset of the above belonging to one module."""
    return [
        tree
        for tree in main_source_trees()
        if tree.relative_to(ROOT).parts[0] == module
    ]


def glob_matched() -> set[str]:
    """Repo-relative paths of every main source SOURCE_GLOBS reaches, any module.

    Every module and not only the mapped ones, because this feeds the
    unmapped-builder guard: an unmapped module's options are the stray-module
    guard's business, and reporting them a second time by a message saying they
    match no pattern would be false as well as noisy.
    """
    return {
        str(source.relative_to(ROOT))
        for tree in main_source_trees()
        for pattern in SOURCE_GLOBS
        for source in tree.rglob(pattern)
    }


def setters_of(path: Path, text: str, whence: str, remedy: str) -> set[str]:
    """The public builder setters one source declares; none of them is an error.

    A class this script was told to read and cannot parse makes the whole run
    untrustworthy rather than that one class merely absent, so it is exit 2 in
    both cases and only the wording differs.
    """
    setters = builder_setter_names(parse_java(path, text))
    if not setters:
        infra(
            f"{path.relative_to(ROOT)} {whence} but declares no builder setter this "
            f"script recognises. Either it is not an options class — {remedy} — or "
            f"its builder no longer follows the builder-returning method shape, "
            f"which would "
            f"make every other class's result untrustworthy too."
        )
    return setters


def builder_setters(
    module: str, claimed: set[str], sources: list[str]
) -> dict[str, set[str]]:
    """Public builder setters per class under one module's main sources.

    An explicitly named `sources` class is parsed, keyed and checked in both
    directions exactly as a glob-matched one is.

    Sources listed under [[config_options]] are skipped: their options are
    ConfigOptions rather than builder setters, so finding none in them is
    correct rather than the parse failure it would be anywhere else.

    Setters are merged per class name rather than assigned, because two source
    roots hold the same file name by design — every connector has a
    `CrossVersionSink.java` in both `java-flink1` and `java-flink2` (ADR-0054) —
    and assigning would let the root sorted last hide the other's options behind
    an exit 0. The name is the key because `[exempt]` is keyed `Class.setter`.
    """
    found: dict[str, set[str]] = {}
    roots = main_source_roots(module)
    if not roots:
        infra(f"{module}/src/main has no java* source root; {CONFIG.name} names it.")
    for root in roots:
        for pattern in SOURCE_GLOBS:
            for source in sorted(root.rglob(pattern)):
                if str(source.relative_to(ROOT)) in claimed:
                    continue
                found.setdefault(source.stem, set()).update(
                    setters_of(
                        source,
                        source.read_text("utf-8"),
                        f"matches {pattern}",
                        "narrow SOURCE_GLOBS",
                    )
                )
    for name in sources:
        path = ROOT / name
        # Existence before placement: a typo'd path is the likelier authoring
        # error of the two, and "does not exist" locates it where "does not live
        # in one of its main source roots" only puzzles.
        if not path.is_file():
            infra(f"{name} does not exist; {CONFIG.name} names it under {module}.")
        if not any(root in path.parents for root in roots):
            infra(
                f"{name} is named under {module}'s sources but does not live in one "
                f"of its main source roots. A sources entry names a class of the "
                f"module it is listed under, or the page it is mapped to means "
                f"nothing."
            )
        if any(fnmatch.fnmatch(path.name, pattern) for pattern in SOURCE_GLOBS):
            infra(
                f"{name} is named under {module}'s sources but already matches "
                f"SOURCE_GLOBS, so it is scanned either way. Delete the entry — a "
                f"mapping that changes nothing is a claim nobody can check."
            )
        found.setdefault(path.stem, set()).update(
            setters_of(
                path,
                read(path),
                f"is named under {module}'s sources",
                "delete the entry",
            )
        )
    if not found:
        infra(f"No options sources found under {module}/src/main.")
    return found


def unmapped_public_builders(
    seen: set[str], value_builders: dict[str, str], used: set[str]
) -> list[str]:
    """Public builders that no mapping in the config reaches.

    The gap #328 closed. `PubSubDeadLetterQueue.Builder`'s five knobs matched no
    SOURCE_GLOBS pattern, so *both* directions skipped the whole class and
    neither said so — a knob could be added, renamed or deleted, and a row could
    go stale, with nothing failing either way. A `sources` entry reaches one
    class; this is what keeps the next one from being invisible for as long.

    It is the stray-module guard one level down. Two things exempt a class, and
    they answer different questions. `@Internal` says the builder is not a user
    surface at all; the top-level type's AST annotations decide it. A
    `[value_builders]` entry says the opposite — it *is* public, and it builds a
    value rather than a configuration, so no reference row could exist for it.
    `DataChangeRecord` is the case that forced the distinction: a `@PublicEvolving`
    record a deserializer receives, whose builder sets thirteen record fields
    and not one option. Marking it `@Internal` to quieten this would have
    demoted a published type to satisfy a check.
    """
    problems: list[str] = []
    for tree in main_source_trees():
        for source in sorted(tree.rglob("*.java")):
            name = str(source.relative_to(ROOT))
            if name in seen:
                continue
            parsed = parse_java(source)
            setters = sorted(builder_setter_names(parsed))
            if not setters:
                continue
            if top_level_is_internal(parsed):
                continue
            if name in value_builders:
                used.add(name)
                continue
            problems.append(
                f"{name} declares public builder setters ({', '.join(setters)}) "
                f"but nothing maps it: it matches no SOURCE_GLOBS pattern and no "
                f"[[builders]] sources entry names it, so both directions of this "
                f"check skip the whole class. Map it in {CONFIG.name}; or mark the "
                f"class @Internal if it is not a user-facing surface; or add a "
                f'[value_builders] entry "{name}" if it is public but builds a '
                f"value rather than a configuration."
            )
    return problems


def load_config() -> dict:
    """The parsed config, its required keys checked so a typo is exit 2."""
    if not CONFIG.is_file():
        infra(f"{CONFIG} is missing.")
    try:
        config = tomllib.loads(CONFIG.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as error:
        infra(f"{CONFIG.name} is not valid TOML: {error}")
    if not config.get("builders"):
        infra(f"{CONFIG.name} names no [[builders]] mapping.")
    for entry in config["builders"]:
        if "module" not in entry or "page" not in entry:
            infra(f"a [[builders]] entry in {CONFIG.name} lacks module or page.")
        sources = entry.get("sources", [])
        if not isinstance(sources, list) or not all(
            isinstance(source, str) for source in sources
        ):
            infra(
                f"the sources of the [[builders]] entry for {entry['module']} in "
                f"{CONFIG.name} is not a list of paths."
            )
        # Normalised here, once, because a path is both joined to ROOT and
        # compared as a string against `str(source.relative_to(ROOT))`. Written
        # `./a/b.java`, the join reads it and the comparison does not, so the
        # class would be fully checked *and* reported as reached by nothing.
        entry["sources"] = [os.path.normpath(source) for source in sources]
    for entry in config.get("config_options", []):
        if "source" not in entry or "page" not in entry:
            infra(f"a [[config_options]] entry in {CONFIG.name} lacks source or page.")
        entry["source"] = os.path.normpath(entry["source"])
    return config


def main() -> int:
    config = load_config()
    exempt = config.get("exempt", {})
    extra = config.get("extra", {})
    value_builders = config.get("value_builders", {})
    problems: list[str] = []
    counts: list[tuple[str, int]] = []
    # Which allowlist entries actually did something. An entry that never fires
    # is a claim nobody can check, and it accumulates silently — the four
    # [exempt] entries this check shipped with were all dead on arrival,
    # because the pages named the bulk overloads in the same row as their
    # singular. check-flink-api-tiers.py fails on a stale entry for the same
    # reason; so does this now.
    used: set[str] = set()

    claimed = {entry["source"] for entry in config.get("config_options", [])}

    # What is left over after this is what unmapped_public_builders looks at.
    seen = set(claimed) | glob_matched()
    for entry in config["builders"]:
        seen.update(entry.get("sources", []))

    # A module that grows options and is never mapped would be checked by
    # nothing, silently — the failure mode a per-module mapping otherwise has,
    # and the one a new connector walks straight into. Bigtable and Spanner are
    # the known candidates.
    mapped = {entry["module"] for entry in config["builders"]}
    for tree in main_source_trees():
        module = tree.relative_to(ROOT).parts[0]
        if module in mapped:
            continue
        stray = sorted(
            str(source.relative_to(ROOT))
            for pattern in SOURCE_GLOBS
            for source in tree.rglob(pattern)
            if str(source.relative_to(ROOT)) not in claimed
        )
        if stray:
            problems.append(
                f"{module} declares options ({stray[0]}"
                f"{f', and {len(stray) - 1} more' if len(stray) > 1 else ''}) but no "
                f"[[builders]] entry in {CONFIG.name} maps it to a page, so nothing "
                f"checks them. Add the module and its reference page."
            )

    problems += unmapped_public_builders(seen, value_builders, used)

    for entry in config["builders"]:
        module, page = entry["module"], ROOT / entry["page"]
        documented = option_table_entries(page)
        by_class = builder_setters(module, claimed, entry.get("sources", []))
        real: set[str] = set()
        for klass, setters in by_class.items():
            real |= setters
            for setter in sorted(setters):
                if setter in documented:
                    continue
                if f"{klass}.{setter}" in exempt:
                    used.add(f"{klass}.{setter}")
                    continue
                problems.append(
                    f"{entry['page']}: {klass}.{setter} is a builder option but no "
                    f"`Option`-headed table names it. Add a row, or an [exempt] "
                    f'entry "{klass}.{setter}" in {CONFIG.name} saying why not.'
                )
        for name, line in sorted(documented.items(), key=lambda kv: kv[1]):
            if name in real:
                continue
            if name in extra:
                used.add(name)
                continue
            problems.append(
                f"{entry['page']}:{line}: the option table names `{name}`, "
                f"which no builder in {module} declares. Remove the row, or "
                f"correct it to the setter's current name."
            )
        counts.append((entry["page"], len(real)))

    for entry in config.get("config_options", []):
        source, page = ROOT / entry["source"], ROOT / entry["page"]
        keys = config_option_keys(parse_java(source, read(source)))
        if not keys:
            infra(f"{entry['source']} declares no ConfigOptions.key(...) entries.")
        documented = option_table_entries(page)
        for key in sorted(keys - set(documented)):
            problems.append(
                f"{entry['page']}: `{key}` is a ConfigOption but no `Option`-headed "
                f"table names it."
            )
        for name, line in sorted(documented.items(), key=lambda kv: kv[1]):
            if name in keys:
                continue
            if name in extra:
                used.add(name)
                continue
            problems.append(
                f"{entry['page']}:{line}: the option table names `{name}`, "
                f"which {Path(entry['source']).name} does not declare. Remove "
                f'the row, or add an [extra] entry "{name}" saying where it '
                f"comes from."
            )
        counts.append((entry["page"], len(keys)))

    for table, entries in (
        ("exempt", exempt),
        ("extra", extra),
        ("value_builders", value_builders),
    ):
        for key in sorted(set(entries) - used):
            problems.append(
                f'[{table}] entry "{key}" in {CONFIG.name} never fires: the check '
                f"passes without it. Delete it — an allowlist entry that forgives "
                f"nothing is a claim nobody can check."
            )

    if problems:
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        fail(f"\n{len(problems)} problem(s) between the sources and the reference.")

    total = sum(n for _, n in counts)
    print(f"{total} options documented:")
    for page, n in counts:
        print(f"  {n:>3}  {page}")
    if exempt or extra or value_builders:
        print(
            f"  {len(exempt)} exempt, {len(extra)} declared elsewhere, "
            f"{len(value_builders)} value builder(s) (see {CONFIG.name}); every one "
            f"of them fires, or this would have failed"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
