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
"""Audit the Flink API stability tiers the main sources depend on (issue #103).

Every `org.apache.flink` type the main sources import is classified by its
class-level annotation (@Public / @PublicEvolving / @Experimental / @Internal,
or none), read from the -sources.jars of the artifacts listed in
scripts/config/flink-api-tiers.toml at the pom-pinned flink.version. A type on an
unstable tier — @Internal, @Experimental or unannotated — must have an
allowlist entry with a reason in that file, and a stale entry (import gone, or
tier changed) fails too, so the list stays an exact record.

Sources jars, never class files: a class file's constant pool lists every
annotation referenced anywhere in the class, including on its methods, so
reading it misclassifies a @Public class with one @Internal method — the exact
bug that produced the wrong numbers this script replaces (see issue #103).

Parsing degrades fail-closed: a class-level annotation the parser misses
demotes the type to "unannotated", which demands an allowlist entry, which
fails loudly; a declaration it cannot find at all is a hard error. The one
accepted blind spot is a declaration-lookalike inside a Java text block, which
survives comment/string stripping.

Exit codes: 0 clean, 1 policy violation (unlisted type, stale entry, unused
artifact), 2 infrastructure or config authoring error (download failure,
unresolvable import, unparseable declaration, malformed allowlist).

Standard library only, deliberately: nothing here justifies a package manager.
"""

import argparse
import http.client
import re
import sys
import urllib.request
import zipfile
from pathlib import Path

try:
    import tomllib  # stdlib since 3.11
except ModuleNotFoundError:  # pragma: no cover - version guard, not logic
    sys.exit(
        "This script needs Python 3.11+ (tomllib). mise.toml pins a suitable "
        "python; run `mise x -- just check-flink-api-tiers`, or any python3 "
        ">= 3.11. CI installs one with actions/setup-python."
    )

ROOT = Path(__file__).resolve().parent.parent
CONFIG = Path(__file__).resolve().parent / "config" / "flink-api-tiers.toml"
CACHE = ROOT / "target" / "flink-api-tiers"
MAVEN = "https://repo1.maven.org/maven2/org/apache/flink"

TIERS = ("Public", "PublicEvolving", "Experimental", "Internal")
UNANNOTATED = "unannotated"
# The TOML tables, keyed by the tier name classify() produces.
ALLOWLISTED = {
    "Internal": "internal",
    "Experimental": "experimental",
    UNANNOTATED: "unannotated",
}

# Single-type imports only, and that is complete: checkstyle's AvoidStarImport
# (tools/maven/checkstyle.xml) keeps wildcard imports out of the tree, so a
# star import cannot slip a type past this scan.
IMPORT = re.compile(
    r"^import\s+(?:static\s+)?(org\.apache\.flink[\w.]+)\s*;", re.MULTILINE
)

# One alternation over string | char | line comment | block comment, applied in
# document order. Single-pass on purpose: stripping strings and comments in two
# passes mis-pairs javadoc apostrophes ("Guava's", "it's") into phantom char
# literals that swallow the annotated declaration — measured on six of the
# Flink 2.2.1 sources this script reads.
LEXEME = re.compile(
    r'"(?:\\.|[^"\\])*"'  # string literal
    r"|'(?:\\.|[^'\\])*'"  # char literal
    r"|//[^\n]*"  # line comment
    r"|/\*.*?\*/",  # block comment, incl. javadoc
    re.DOTALL,
)


def strip_comments(source: str) -> str:
    """Blank comments and string/char bodies, leaving declarations intact."""
    return LEXEME.sub(
        lambda m: '""' if m.group(0)[0] in "\"'" else " ",
        source,
    )


def declaration(simple: str) -> re.Pattern[str]:
    """The type declaration of `simple` with its class-level annotations in group 1.

    The annotation-argument alternative allows one level of nested parens:
    `@ConfigGroups(groups = @ConfigGroup(...))` (TaskManagerOptions in
    flink-core) would otherwise break the anchor at the first `)` and demote
    the type to unannotated.
    """
    return re.compile(
        r"^[ \t]*("
        r"(?:@\w+(?:\s*\((?:[^()]|\([^()]*\))*\))?\s*"
        r"|(?:public|protected|private|abstract|final|static|sealed|non-sealed|strictfp)\s+)*"
        r")"
        r"(?:class|interface|enum|record|@interface)\s+" + re.escape(simple) + r"\b",
        re.MULTILINE,
    )


def fail(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(1)


def infra(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(2)


def flink_version() -> str:
    pom = (ROOT / "pom.xml").read_text(encoding="utf-8")
    match = re.search(r"<flink\.version>([^<]+)</flink\.version>", pom)
    if not match:
        infra("pom.xml no longer declares <flink.version>; this script reads it.")
    return match.group(1)


def collect_imports() -> set[str]:
    """Distinct org.apache.flink imports across every module's main source roots."""
    found: set[str] = set()
    for source in ROOT.glob("*/src/main/java*/**/*.java"):
        found.update(IMPORT.findall(source.read_text(encoding="utf-8")))
    if not found:
        infra(f"No org.apache.flink imports found under {ROOT}/*/src/main/java*.")
    return found


def sources_jar(artifact: str, version: str) -> Path:
    """Download (or reuse) one -sources.jar into the cache directory."""
    jar = CACHE / f"{artifact}-{version}-sources.jar"
    if jar.is_file():
        return jar
    CACHE.mkdir(parents=True, exist_ok=True)
    url = f"{MAVEN}/{artifact}/{version}/{artifact}-{version}-sources.jar"
    request = urllib.request.Request(
        url, headers={"User-Agent": "flink-connector-gcp-api-tiers"}
    )
    # HTTPException covers what OSError does not: a truncated body raises
    # http.client.IncompleteRead, which is not an OSError.
    try:
        body = urllib.request.urlopen(request, timeout=30).read()
    except (OSError, http.client.HTTPException) as error:
        infra(
            f"Downloading {url} failed ({error}). A 404 usually means the "
            f"artifacts list in {CONFIG.name} names an artifact that does not "
            f"exist at this flink.version."
        )
    partial = jar.with_suffix(".part")
    partial.write_bytes(body)
    partial.rename(jar)
    return jar


def build_index(
    artifacts: list[str], version: str
) -> dict[str, tuple[str, zipfile.ZipFile]]:
    """Map each .java entry path to (owning artifact, open jar); first jar wins."""
    index: dict[str, tuple[str, zipfile.ZipFile]] = {}
    for artifact in artifacts:
        path = sources_jar(artifact, version)
        try:
            jar = zipfile.ZipFile(path)
        except zipfile.BadZipFile:
            # A complete HTTP 200 body that was not a zip (an outage page, a
            # middlebox). Without the unlink the bad file would satisfy the
            # cache check on every later run.
            path.unlink()
            infra(f"{path.name} was not a valid zip; removed from the cache, rerun.")
        for name in jar.namelist():
            if name.endswith(".java"):
                index.setdefault(name, (artifact, jar))
    return index


def resolve(
    fqcn: str, index: dict[str, tuple[str, zipfile.ZipFile]]
) -> tuple[str, list[str]]:
    """Return (entry path, nested simple names dropped) for an imported type.

    An import of a nested type has no .java entry of its own, so trailing
    segments are dropped until one matches; the dropped names are the nested
    chain to classify inside that file. None exist today.
    """
    parts = fqcn.split(".")
    nested: list[str] = []
    while len(parts) > 1:
        entry = "/".join(parts) + ".java"
        if entry in index:
            return entry, nested
        nested.insert(0, parts.pop())
    infra(
        f"{fqcn} resolves to no .java entry in any configured sources jar. "
        f"Either the type moved between Flink artifacts or a new package "
        f"family arrived: extend the artifacts list in {CONFIG.name}."
    )


def classify(source: str, entry: str, nested: list[str]) -> str:
    """The class-level tier of the imported type inside one stripped source."""
    stripped = strip_comments(source)
    # For a nested import, the innermost declared name wins if it carries a
    # tier; otherwise the file's primary type speaks for it.
    for simple in [*reversed(nested), Path(entry).stem]:
        match = declaration(simple).search(stripped)
        if not match:
            continue
        tiers = [t for t in re.findall(r"@(\w+)", match.group(1)) if t in TIERS]
        if tiers:
            # Flink does dual-annotate (ExternallyInducedSourceReader is
            # @Experimental @PublicEvolving in 2.2.1): the weaker guarantee
            # governs, and TIERS is ordered stablest-first.
            return max(tiers, key=TIERS.index)
        if simple == Path(entry).stem:
            return UNANNOTATED
    infra(
        f"{entry}: no type declaration found for {Path(entry).stem}. The "
        f"parser in this script cannot read this source shape; fix it rather "
        f"than allowlisting around it."
    )


def main() -> int:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()

    with CONFIG.open("rb") as handle:
        try:
            config = tomllib.load(handle)
        except tomllib.TOMLDecodeError as error:
            infra(f"{CONFIG.name} is not valid TOML: {error}")
    # A typo'd table name would otherwise sit ignored while its types get
    # reported as unlisted — fail on the typo itself, which is the fixable end.
    unknown = set(config) - {"artifacts", *ALLOWLISTED.values()}
    if unknown:
        infra(f"{CONFIG.name} has unknown top-level entries: {sorted(unknown)}.")
    if not isinstance(config.get("artifacts"), list) or not config["artifacts"]:
        infra(f"{CONFIG.name} needs a non-empty artifacts list.")
    for table in ALLOWLISTED.values():
        for fqcn, entry in config.get(table, {}).items():
            if not isinstance(entry, dict) or not str(entry.get("reason", "")).strip():
                infra(
                    f"{CONFIG.name}: [{table}] entry {fqcn} needs a table with a "
                    f"reason. The reason is the point of the allowlist; write one."
                )
    version = flink_version()
    index = build_index(config["artifacts"], version)

    by_tier: dict[str, set[str]] = {tier: set() for tier in (*TIERS, UNANNOTATED)}
    used_artifacts: set[str] = set()
    for fqcn in collect_imports():
        entry, nested = resolve(fqcn, index)
        artifact, jar = index[entry]
        used_artifacts.add(artifact)
        source = jar.read(entry).decode("utf-8")
        by_tier[classify(source, entry, nested)].add(fqcn)

    problems: list[str] = []
    for tier, table in ALLOWLISTED.items():
        allowed = set(config.get(table, {}))
        for fqcn in sorted(by_tier[tier] - allowed):
            problems.append(
                f"{fqcn} is {tier} but has no [{table}] entry in {CONFIG.name}. "
                f"Prefer a stable alternative; if unavoidable, add an entry "
                f"whose reason says why."
            )
        for fqcn in sorted(allowed - by_tier[tier]):
            problems.append(
                f"[{table}] entry {fqcn} is stale: the main sources no longer "
                f"import it at that tier. Delete the entry (or re-file it "
                f"under the tier it moved to)."
            )
    for artifact in config["artifacts"]:
        if artifact not in used_artifacts:
            problems.append(
                f"{artifact} owns no imported type; remove it from the "
                f"artifacts list in {CONFIG.name}."
            )
    if problems:
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        fail(f"\nFlink API tier audit failed against {version}.")

    total = sum(len(types) for types in by_tier.values())
    print(f"{total} distinct org.apache.flink imports, classified against {version}:")
    for tier in (*TIERS, UNANNOTATED):
        label = tier if tier == UNANNOTATED else f"@{tier}"
        print(f"  {label:<16} {len(by_tier[tier]):>3}")
    for tier in ALLOWLISTED:
        for fqcn in sorted(by_tier[tier]):
            print(f"    {tier}: {fqcn}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
