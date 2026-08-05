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
"""Hold the configuration reference to the options the connectors actually take (issue #89).

Two directions, both required, for each mapping in scripts/option-docs.toml:

* **Coverage** — every public builder setter of a module's `*Options` /
  `*SinkBuilder` / `*SourceBuilder` classes is named in that module's reference
  page, and every `ConfigOption` key of the Table API surface is named in the
  page documenting it. A knob added without a doc row fails here.
* **Staleness** — every option an *option table* names exists in the source. A
  renamed or deleted knob fails here rather than lingering as a row nobody can
  act on.

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
2 infrastructure or config authoring error (missing file, a source that parses
to no options at all, malformed config).

Standard library only, like its siblings in this directory.
"""

import re
import sys
from pathlib import Path

try:
    import tomllib  # stdlib since 3.11
except ModuleNotFoundError:  # pragma: no cover - version guard, not logic
    sys.exit(
        "This script needs Python 3.11+ (tomllib). mise.toml pins a suitable "
        "python; run `mise x -- just check-option-docs`, or any python3 >= 3.11. "
        "CI installs one with actions/setup-python."
    )

ROOT = Path(__file__).resolve().parent.parent
CONFIG = Path(__file__).resolve().parent / "option-docs.toml"

# The three source shapes a module's builder options live in.
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
SOURCE_GLOBS = ("*Options.java", "*SinkBuilder.java", "*SourceBuilder.java")

# `public Builder maxInFlightBytes(long ...)` on a nested options builder, and
# `public PubSubSinkBuilder<T> topic(TopicDestination ...)` on a sink/source
# builder. Anchored at line start with leading whitespace so a match inside an
# expression cannot count.
SETTER = re.compile(
    r"^[ \t]+public\s+(?:Builder|\w+Builder<\w*>)\s+(\w+)\s*\(", re.MULTILINE
)

CONFIG_OPTION_KEY = re.compile(r'ConfigOptions\.key\(\s*"([^"]+)"\s*\)')

# Comments are blanked before both scans above run, so a setter named in javadoc
# — `{@link #maxInFlightMessages(int)}` is everywhere in these files — cannot be
# read as a declaration.
#
# String literals are deliberately left intact, unlike in check-flink-api-tiers.py:
# a ConfigOption's key *is* a string literal, so blanking them would leave the
# Table API surface looking empty. They are matched first and kept, so a `//`
# inside one (`"http://…"`) cannot be read as a comment opener and blank the
# rest of its line — the same mechanism check-metric-docs.py uses.
COMMENT_OR_STRING = re.compile(
    r'"(?:\\.|[^"\\\n])*"'  # string literal, kept intact
    r"|//[^\n]*"  # line comment
    r"|/\*.*?\*/",  # block comment, incl. javadoc
    re.DOTALL,
)

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


def blank_comments(source: str) -> str:
    """Blank every comment, preserving newlines and columns so anchors still hold."""
    return COMMENT_OR_STRING.sub(
        lambda m: (
            m.group(0)
            if m.group(0).startswith('"')
            else re.sub(r"[^\n]", " ", m.group(0))
        ),
        source,
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
        if line.lstrip().startswith("```"):
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


def builder_setters(module: str, claimed: set[str]) -> dict[str, set[str]]:
    """Public builder setters per class under one module's main sources.

    Sources listed under [[config_options]] are skipped: their options are
    ConfigOptions rather than builder setters, so finding none in them is
    correct rather than the parse failure it would be anywhere else.
    """
    found: dict[str, set[str]] = {}
    root = ROOT / module / "src" / "main" / "java"
    if not root.is_dir():
        infra(f"{module}/src/main/java does not exist; {CONFIG.name} names it.")
    for pattern in SOURCE_GLOBS:
        for source in sorted(root.rglob(pattern)):
            if str(source.relative_to(ROOT)) in claimed:
                continue
            setters = set(SETTER.findall(blank_comments(source.read_text("utf-8"))))
            if not setters:
                infra(
                    f"{source.relative_to(ROOT)} matches {pattern} but declares no "
                    f"builder setter this script recognises. Either it is not an "
                    f"options class — narrow SOURCE_GLOBS — or its builder no "
                    f"longer follows the shape SETTER matches, which would make "
                    f"every other class's result untrustworthy too."
                )
            found[source.stem] = setters
    if not found:
        infra(f"No options sources found under {module}/src/main/java.")
    return found


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
    for entry in config.get("config_options", []):
        if "source" not in entry or "page" not in entry:
            infra(f"a [[config_options]] entry in {CONFIG.name} lacks source or page.")
    return config


def main() -> int:
    config = load_config()
    exempt = config.get("exempt", {})
    extra = config.get("extra", {})
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

    # A module that grows options and is never mapped would be checked by
    # nothing, silently — the failure mode a per-module mapping otherwise has,
    # and the one a new connector walks straight into. Bigtable and Spanner are
    # the known candidates.
    mapped = {entry["module"] for entry in config["builders"]}
    for tree in sorted(ROOT.glob("*/src/main/java")):
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

    for entry in config["builders"]:
        module, page = entry["module"], ROOT / entry["page"]
        documented = option_table_entries(page)
        by_class = builder_setters(module, claimed)
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
        keys = set(CONFIG_OPTION_KEY.findall(blank_comments(read(source))))
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
        fail(f"\n{len(problems)} problem(s) between the sources and the reference.")

    total = sum(n for _, n in counts)
    print(f"{total} options documented:")
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
