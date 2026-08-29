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
"""Hold the metrics tables to what the connectors actually register (issue #296).

Two directions, both required, for each mapping in scripts/config/metric-docs.toml:

* **Coverage** — every metric name a module's `*MetricNames` inventory declares
  is named in a `Metric`-headed table row on that module's page, with the row's
  Type column leading with the kind (`counter` / `gauge`) the source registers
  it as. A metric added without a doc row, or documented as the wrong kind,
  fails here.
* **Staleness** — every name those tables carry is either registered by the
  module, one of the subgroup templates the module actually wires (`errorClass`,
  `destination`), or marked `(Flink standard)` in its Type column. A renamed or
  deleted metric fails here rather than lingering as a row nobody can act on.

A metrics table is one whose first column header is exactly `Metric` — the same
opt-in `check-option-docs.py` uses for `Option`, and what keeps this check off
every other table the same pages carry. Fenced code blocks are skipped, so an
example table in a snippet earns no coverage credit.

Around those two directions, three facts of the #280 naming convention are held
mechanically:

* every registration goes through a `*MetricNames` constant, so the inventory
  class *is* the inventory (a registration by any other name fails);
* every inventory constant is registered somewhere (a dead entry is a claim the
  inventory cannot back);
* no name this repository registers itself takes Flink's `num` prefix. The rest
  of the convention — counter names the event, gauge names the state — needs
  English morphology to decide and stays with review, on purpose.

Flink's standard names (`numRecordsSend` and friends) come from metric-group
accessors rather than from a name in this tree, so the tables mark them
`(Flink standard)` in the Type column and this script makes that marker
load-bearing: it exempts the row from the registration requirement, and a row
so marked whose name the module *does* register fails, so the marker cannot be
used to hide a stale row. Subgroup rows (`errorClass.CODE.errors`,
`destination.TABLE.recordsSend`) are templated — the middle segment is an
all-caps placeholder — and their group and leaf names are read from the
`base.metrics` sources that register them, listed under [[subgroups]].

Exit codes: 0 clean, 1 policy violation (undocumented, stale or mis-typed
metric), 2 infrastructure or config authoring error (missing file, invalid
Java, a source that parses to nothing, malformed config).
"""

import re
import sys
from pathlib import Path
from typing import NamedTuple

from java_ast import (
    JavaSource,
    JavaSyntaxError,
    code_named_children,
    string_literal_content,
)

try:
    import tomllib  # stdlib since 3.11
except ModuleNotFoundError:  # pragma: no cover - version guard, not logic
    sys.exit(
        "This script needs Python 3.11+ (tomllib). mise.toml pins a suitable "
        "python; run `mise x -- just check-metric-docs`, or any python3 >= 3.11. "
        "CI installs one with actions/setup-python."
    )

ROOT = Path(__file__).resolve().parent.parent
CONFIG = Path(__file__).resolve().parent / "config" / "metric-docs.toml"

# The per-connector inventory classes issue #280 introduced: every name the
# connector registers, as `static final String` constants.
INVENTORY_GLOB = "*MetricNames.java"

# A table row's first cell, and the backticked metric names inside it. One cell
# may name a pair that shares a meaning (`destination.TABLE.recordsSend`,
# `destination.TABLE.sendErrors`).
BACKTICKED = re.compile(r"`([A-Za-z][\w.]*)`")

# The middle segment of a templated subgroup row: `CODE`, `TABLE`, `TOPIC`,
# `QUEUE` — a stand-in for the runtime value, never a registered name.
PLACEHOLDER = re.compile(r"^[A-Z][A-Z0-9_]*$")

FLINK_STANDARD = "(Flink standard)"

# Flink's own style (`numRecordsSend`); issue #280 keeps it off every name this
# repository registers itself, so a connector metric cannot masquerade as a
# standard one.
NUM_PREFIX = re.compile(r"^num[A-Z]")


class Subgroup(NamedTuple):
    """A base.metrics registrar: the group it opens and the leaves it registers."""

    class_name: str
    group: str
    leaves: dict[str, str]  # leaf name -> kind


def fail(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(1)


def infra(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(2)


def parse_java(path: Path) -> JavaSource:
    """Parse one Java source or terminate before using a partial inventory."""
    try:
        return JavaSource.parse(path.relative_to(ROOT), path.read_bytes())
    except JavaSyntaxError as error:
        infra(str(error))


def read(path: Path) -> str:
    if not path.is_file():
        infra(f"{path.relative_to(ROOT)} does not exist; {CONFIG.name} names it.")
    return path.read_text(encoding="utf-8")


def parsed_sources(module: str) -> dict[Path, JavaSource]:
    """Every main-tree source of the module, parsed once.

    `src/main/java*` rather than `src/main/java`: the per-major compat roots
    (`java-flink1`/`java-flink2`) are main sources too, so a registration
    added there is scanned rather than invisible.
    """
    main = ROOT / module / "src" / "main"
    if not (main / "java").is_dir():
        infra(f"{module}/src/main/java does not exist; {CONFIG.name} names it.")
    return {
        source: parse_java(source)
        for tree in sorted(main.glob("java*"))
        for source in sorted(tree.rglob("*.java"))
    }


def metric_table_rows(page: Path) -> tuple[list[tuple[list[str], str, int]], list[str]]:
    """Rows of the page's `Metric`-headed tables: (names, Type cell, line).

    Also returns the authoring problems found on the way: a metrics table whose
    second column is not `Type` has nowhere to say what kind each name is, and
    this check is what makes that column load-bearing.
    """
    rows: list[tuple[list[str], str, int]] = []
    problems: list[str] = []
    in_table = False
    fenced = False
    for number, line in enumerate(read(page).splitlines(), start=1):
        if line.lstrip().startswith(("```", "~~~")):
            fenced = not fenced
            in_table = False
            continue
        if fenced or not line.startswith("|"):
            in_table = False
            continue
        cells = [cell.strip() for cell in line.split("|")]
        first = cells[1] if len(cells) > 1 else ""
        if first == "Metric":
            in_table = True
            if len(cells) < 3 or cells[2] != "Type":
                problems.append(
                    f"{page.relative_to(ROOT)}:{number}: a `Metric`-headed table "
                    f"needs `Type` as its second column; this one has nowhere to "
                    f"say what kind each name is."
                )
                in_table = False
            continue
        if not in_table or set(first) <= set("- :"):
            continue
        names = BACKTICKED.findall(first)
        type_cell = cells[2] if len(cells) > 2 else ""
        rows.append((names, type_cell, number))
    return rows, problems


def string_constants(parsed: JavaSource) -> dict[str, str]:
    """Static-final String declarations initialized by string literals."""
    found: dict[str, str] = {}
    for field in parsed.nodes("constant_declaration", "field_declaration"):
        modifiers = next(
            (child for child in field.children if child.type == "modifiers"), None
        )
        tokens = {child.type for child in modifiers.children} if modifiers else set()
        declared_type = field.child_by_field_name("type")
        if (
            field.type != "constant_declaration"
            and ("static" not in tokens or "final" not in tokens)
        ) or (declared_type is None or parsed.text(declared_type) != "String"):
            continue
        for declarator in (
            child
            for child in field.named_children
            if child.type == "variable_declarator"
        ):
            name = declarator.child_by_field_name("name")
            value = declarator.child_by_field_name("value")
            if (
                name is not None
                and value is not None
                and value.type == "string_literal"
            ):
                literal = string_literal_content(parsed, value)
                if literal is not None:
                    found[parsed.text(name)] = literal
    return found


def metric_calls(parsed: JavaSource):
    """Yield counter and gauge method invocations in source order."""
    for call in parsed.nodes("method_invocation"):
        name = call.child_by_field_name("name")
        if (
            name is not None
            and call.child_by_field_name("object") is not None
            and parsed.text(name) in ("counter", "gauge")
        ):
            yield parsed.text(name), call


def inventory(
    module: str, sources: dict[Path, JavaSource]
) -> dict[str, dict[str, str]]:
    """Constant -> name literal, per `*MetricNames` class of the module."""
    found: dict[str, dict[str, str]] = {}
    for source, parsed in sources.items():
        if not source.match(INVENTORY_GLOB):
            continue
        constants = string_constants(parsed)
        if not constants:
            infra(
                f"{source.relative_to(ROOT)} matches {INVENTORY_GLOB} but declares "
                f"no `static final String` name this script recognises — either it "
                f"is not an inventory class, or the declaration shape changed, "
                f"which would make every other class's result untrustworthy too."
            )
        found[source.stem] = constants
    if not found:
        infra(
            f"{module} is mapped in {CONFIG.name} but has no {INVENTORY_GLOB} "
            f"inventory class under its main sources."
        )
    return found


def registrations(
    module: str,
    sources: dict[Path, JavaSource],
    constants: dict[str, dict[str, str]],
) -> tuple[dict[str, str], set[tuple[str, str]], list[str]]:
    """Registered kinds and constants, from every registration in the module.

    Returns (name literal -> kind, the (class, constant) pairs actually
    registered, problems). Every `.counter(` / `.gauge(` call must name a
    `*MetricNames` constant — that is what makes the inventory the inventory —
    so a call that does not is returned as a problem rather than silently
    skipped.
    """
    kinds: dict[str, str] = {}
    used_constants: set[tuple[str, str]] = set()
    conflicted: set[str] = set()
    problems: list[str] = []
    for source, parsed in sources.items():
        for kind, call in metric_calls(parsed):
            arguments = call.child_by_field_name("arguments")
            values = code_named_children(arguments) if arguments is not None else ()
            first = values[0] if values else None
            if first is None or first.type != "field_access":
                problems.append(
                    f"{source.relative_to(ROOT)}:{parsed.line(call)}: registers a "
                    f"{kind} by a name outside the module's "
                    f"{INVENTORY_GLOB} inventory. Declare the name there and "
                    f"register it through the constant."
                )
                continue
            owner = first.child_by_field_name("object")
            field = first.child_by_field_name("field")
            klass = parsed.text(owner) if owner is not None else ""
            constant = parsed.text(field) if field is not None else ""
            if not klass.endswith("MetricNames"):
                problems.append(
                    f"{source.relative_to(ROOT)}:{parsed.line(call)}: registers a "
                    f"{kind} by a name outside the module's {INVENTORY_GLOB} "
                    f"inventory. Declare the name there and register it through "
                    f"the constant."
                )
                continue
            if klass not in constants or constant not in constants[klass]:
                infra(
                    f"{source.relative_to(ROOT)} registers {klass}.{constant}, "
                    f"which no {INVENTORY_GLOB} class in {module} declares as a "
                    f"string constant this script can read."
                )
            used_constants.add((klass, constant))
            name = constants[klass][constant]
            if kinds.setdefault(name, kind) != kind and name not in conflicted:
                conflicted.add(name)
                problems.append(
                    f"{module} registers `{name}` as both a counter and a gauge; "
                    f"one registration is wrong, and no Type cell can be right "
                    f"until it is."
                )
    return kinds, used_constants, problems


def subgroup_template(source: Path) -> Subgroup:
    """The group and leaves a [[subgroups]] source registers, read from it."""
    read(source)
    parsed = parse_java(source)
    constants = string_constants(parsed)
    groups: set[str | None] = set()
    leaves: dict[str, str] = {}
    consistent = True
    for call in parsed.nodes("method_invocation"):
        name = call.child_by_field_name("name")
        arguments = call.child_by_field_name("arguments")
        values = code_named_children(arguments) if arguments is not None else ()
        method = parsed.text(name) if name is not None else ""
        if method == "addGroup" and values and values[0].type == "identifier":
            groups.add(constants.get(parsed.text(values[0])))
        if method in ("counter", "gauge") and values and values[0].type == "identifier":
            leaf = constants.get(parsed.text(values[0]))
            consistent &= leaf is not None and leaves.setdefault(leaf, method) == method
    if len(groups) != 1 or None in groups or not leaves or not consistent:
        infra(
            f"{source.relative_to(ROOT)}: could not read one addGroup segment "
            f"and its registered leaves, one kind each, as string constants. "
            f"The Java registration shape changed; fix the checker, not the config."
        )
    return Subgroup(source.stem, groups.pop(), leaves)


def module_uses(sources: dict[Path, JavaSource], class_name: str) -> bool:
    return any(
        parsed.text(node) == class_name
        for parsed in sources.values()
        for node in parsed.nodes("identifier", "type_identifier")
    )


def check_rows(
    page_ref: str,
    rows: list[tuple[list[str], str, int]],
    registered: dict[str, str],
    module: str,
    active: list[Subgroup],
    templates: list[Subgroup],
    extra: dict[str, str],
) -> tuple[set[str], set[tuple[str, str]], set[str], list[str]]:
    """The staleness direction, row by row.

    Returns (registered names covered, (class, leaf) pairs covered, [extra]
    entries that fired, problems).
    """
    covered: set[str] = set()
    covered_leaves: set[tuple[str, str]] = set()
    used: set[str] = set()
    problems: list[str] = []
    for names, type_cell, line in rows:
        parts = type_cell.split()
        kind = parts[0] if parts else ""
        where = f"{page_ref}:{line}"
        if kind not in ("counter", "gauge"):
            problems.append(
                f"{where}: the Type cell must lead with `counter` or "
                f"`gauge`; it reads {type_cell!r}."
            )
            continue
        standard = FLINK_STANDARD in type_cell
        for name in names:
            segments = name.split(".")
            if name in registered:
                covered.add(name)
                if standard:
                    problems.append(
                        f"{where}: `{name}` is marked {FLINK_STANDARD} but "
                        f"{module} registers it itself; the marker is for "
                        f"names that come from Flink's metric-group "
                        f"accessors, and nothing else."
                    )
                elif kind != registered[name]:
                    problems.append(
                        f"{where}: `{name}` is documented as a {kind} but "
                        f"{module} registers it as a {registered[name]}."
                    )
            elif len(segments) == 3 and PLACEHOLDER.match(segments[1]):
                group, leaf = segments[0], segments[2]
                template = next(
                    (t for t in active if t.group == group and leaf in t.leaves),
                    None,
                )
                if template is None:
                    wired = next(
                        (t for t in templates if t.group == group and leaf in t.leaves),
                        None,
                    )
                    problems.append(
                        f"{where}: names `{name}`, "
                        + (
                            f"but {module} does not use {wired.class_name}, "
                            f"which is what registers it. Remove the row, or "
                            f"it documents a metric that never appears."
                            if wired
                            else "which no [[subgroups]] source registers. "
                            "Remove the row, or correct the group and leaf."
                        )
                    )
                    continue
                covered_leaves.add((template.class_name, leaf))
                if standard:
                    problems.append(
                        f"{where}: `{name}` is marked {FLINK_STANDARD} but "
                        f"this repository registers it, through "
                        f"{template.class_name}."
                    )
                elif kind != template.leaves[leaf]:
                    problems.append(
                        f"{where}: `{name}` is documented as a {kind} but "
                        f"{template.class_name} registers it as a "
                        f"{template.leaves[leaf]}."
                    )
            elif standard:
                continue
            elif name in extra:
                used.add(name)
            else:
                problems.append(
                    f"{where}: names `{name}`, which {module} does not "
                    f"register. Remove the row, correct it to the current "
                    f"name, or — if Flink itself provides it — mark its "
                    f"Type cell {FLINK_STANDARD}."
                )
    return covered, covered_leaves, used, problems


def load_config() -> dict:
    """The parsed config, its required keys checked so a typo is exit 2."""
    if not CONFIG.is_file():
        infra(f"{CONFIG} is missing.")
    try:
        config = tomllib.loads(CONFIG.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as error:
        infra(f"{CONFIG.name} is not valid TOML: {error}")
    if not config.get("connectors"):
        infra(f"{CONFIG.name} names no [[connectors]] mapping.")
    for entry in config["connectors"]:
        if "module" not in entry or "page" not in entry:
            infra(f"a [[connectors]] entry in {CONFIG.name} lacks module or page.")
    for entry in config.get("subgroups", []):
        if "source" not in entry:
            infra(f"a [[subgroups]] entry in {CONFIG.name} lacks source.")
    return config


def main() -> int:
    config = load_config()
    exempt = config.get("exempt", {})
    extra = config.get("extra", {})
    problems: list[str] = []
    counts: list[tuple[str, int]] = []
    # Which allowlist entries actually did something: an entry that never fires
    # fails, exactly as it does in check-option-docs.py and for the same reason.
    used: set[str] = set()

    subgroup_entries = config.get("subgroups", [])
    templates = [
        subgroup_template(ROOT / entry["source"]) for entry in subgroup_entries
    ]
    claimed = {entry["source"] for entry in subgroup_entries}

    # The subgroup leaves are names this repository registers, so the #280
    # prefix rule holds for them too — checked here, once per registrar, not
    # once per consuming module.
    for template in templates:
        for leaf in sorted(template.leaves):
            if NUM_PREFIX.match(leaf):
                problems.append(
                    f"{template.class_name} registers `{leaf}`: no name this "
                    f"repository registers itself takes Flink's `num` prefix "
                    f"(issue #280). Rename it — a counter names the event, a "
                    f"gauge the state."
                )

    # A module that registers metrics and is never mapped would be checked by
    # nothing, silently — the failure mode a per-module mapping otherwise has,
    # and the one a new connector walks straight into.
    mapped = {entry["module"] for entry in config["connectors"]}
    stray_by_module: dict[str, list[str]] = {}
    for tree in sorted(ROOT.glob("*/src/main/java*")):
        module = tree.relative_to(ROOT).parts[0]
        if module in mapped:
            continue
        for source in sorted(tree.rglob("*.java")):
            relative = str(source.relative_to(ROOT))
            if relative in claimed:
                continue
            parsed = parse_java(source)
            if source.match(INVENTORY_GLOB) or next(metric_calls(parsed), None):
                stray_by_module.setdefault(module, []).append(relative)
    for module, stray in sorted(stray_by_module.items()):
        problems.append(
            f"{module} registers metrics ({stray[0]}"
            f"{f', and {len(stray) - 1} more' if len(stray) > 1 else ''}) but no "
            f"[[connectors]] entry in {CONFIG.name} maps it to a page, so "
            f"nothing checks them. Add the module and its documentation page."
        )

    for entry in config["connectors"]:
        module, page = entry["module"], ROOT / entry["page"]
        sources = parsed_sources(module)
        constants = inventory(module, sources)
        registered, used_constants, reg_problems = registrations(
            module, sources, constants
        )
        problems.extend(reg_problems)
        if not registered:
            infra(
                f"{module} has an inventory but this script found no "
                f"registration through it at all. Either the registration shape "
                f"changed — which would make every module's result "
                f"untrustworthy — or the inventory belongs to sources that "
                f"moved."
            )
        # Constant-granular on purpose: two constants carrying the same
        # literal must each be registered, or the dead one hides behind the
        # live one's name.
        for klass, consts in sorted(constants.items()):
            for constant, name in sorted(consts.items()):
                if (klass, constant) not in used_constants:
                    problems.append(
                        f"{module}: {klass}.{constant} names `{name}` but "
                        f"nothing registers it. An inventory entry nothing "
                        f"backs is a claim nobody can check; delete it, or "
                        f"restore the registration."
                    )
        declaring = {
            name: klass
            for klass, consts in constants.items()
            for name in consts.values()
        }
        for name in sorted(registered):
            if NUM_PREFIX.match(name):
                problems.append(
                    f"{module} registers `{name}`: no name this repository "
                    f"registers itself takes Flink's `num` prefix (issue #280). "
                    f"Rename it — a counter names the event, a gauge the state."
                )
        active = [t for t in templates if module_uses(sources, t.class_name)]

        rows, row_problems = metric_table_rows(page)
        problems.extend(row_problems)
        covered, covered_leaves, used_extra, page_problems = check_rows(
            entry["page"], rows, registered, module, active, templates, extra
        )
        used |= used_extra
        problems.extend(page_problems)

        for name in sorted(set(registered) - covered):
            key = f"{declaring[name]}.{name}"
            if key in exempt:
                used.add(key)
                continue
            problems.append(
                f"{entry['page']}: {module} registers `{name}` "
                f"({registered[name]}) but no `Metric`-headed table names it. "
                f'Add a row, or an [exempt] entry "{key}" in {CONFIG.name} '
                f"saying why not."
            )
        for template in active:
            documented = {
                leaf for cls, leaf in covered_leaves if cls == template.class_name
            }
            for leaf in sorted(set(template.leaves) - documented):
                key = f"{template.class_name}.{leaf}"
                if key in exempt:
                    used.add(key)
                    continue
                problems.append(
                    f"{entry['page']}: {module} uses {template.class_name} but "
                    f"no `Metric`-headed table documents "
                    f"`{template.group}.….{leaf}`. Add the templated row, or "
                    f'an [exempt] entry "{key}" in {CONFIG.name} saying why '
                    f"not."
                )
        counts.append(
            (entry["page"], len(registered) + sum(len(t.leaves) for t in active))
        )

    for table, entries in (("exempt", exempt), ("extra", extra)):
        for key in sorted(set(entries) - used):
            problems.append(
                f'[{table}] entry "{key}" in {CONFIG.name} never fires: the check '
                f"passes without it. Delete it — an allowlist entry that forgives "
                f"nothing is a claim nobody can check."
            )

    if problems:
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        fail(f"\n{len(problems)} problem(s) between the sources and the tables.")

    total = sum(n for _, n in counts)
    print(f"{total} metrics documented:")
    for page, n in counts:
        print(f"  {n:>3}  {page}")
    if exempt or extra:
        print(
            f"  {len(exempt)} exempt, {len(extra)} declared elsewhere "
            f"(see {CONFIG.name}); every one of them fires, or this would have failed"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
