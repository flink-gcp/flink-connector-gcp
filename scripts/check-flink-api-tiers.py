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
"""Audit the Flink API stability tiers the main sources depend on (issue #103).

Every `org.apache.flink` type the main sources import is classified by its
class-level annotation (@Public / @PublicEvolving / @Experimental / @Internal,
or none), read from the -sources.jars of the artifacts listed in
scripts/config/flink-api-tiers.toml. A type on an unstable tier — @Internal,
@Experimental or unannotated — must have an allowlist entry with a reason in
that file, and a stale entry (import gone, or tier changed) fails too, so the
list stays an exact record.

An import is classified at each of the two versions a published line compiles
its source root at, and the weakest tier governs (issue #1714): the shared
src/main/java roots at both pom.xml's flink.version (the 2.x floor) and the 1.x
LTS pinned by weekly.yaml's FLINK_LTS, java-flink1 at the LTS alone, java-flink2
at flink.version alone. A Tier-3 job module is built at flink.version, and at
the LTS too when the config's lts_tier3_modules names it; its roots are read
only at the versions it is built at. The weekly ceiling and snapshot builds are
not classified here.

Sources jars, never class files: a class file's constant pool lists every
annotation referenced anywhere in the class, including on its methods, so
reading it misclassifies a @Public class with one @Internal method — the exact
bug that produced the wrong numbers this script replaces (see issue #103).

Parsing is syntax-aware and fail-closed: every Java compilation unit must form
a complete Tree-sitter syntax tree before its imports or declarations can
contribute to the inventory.

Exit codes: 0 clean, 1 policy violation (unlisted type, stale entry, unused
artifact), 2 infrastructure or config authoring error (download failure,
unresolvable import, unparseable declaration, malformed allowlist, an unknown
source root or version).

Tree-sitter is shared with the repository's other Java-aware checkers.
"""

import argparse
import http.client
import re
import sys
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

from java_ast import (
    API_TIER_SOURCE_PATTERNS,
    TYPE_DECLARATIONS,
    JavaSource,
    JavaSyntaxError,
    annotation_name,
    annotations,
    api_tier_sources,
    declaration_target,
)

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

# Maven Central rate-limits the shared egress IPs of GitHub-hosted runners, so
# a download can fail with HTTP 429 on a change that touched nothing this
# script reads (issue #769). Retries sleep these delays in order — at most
# ~14 s per download against the CI job's ten-minute timeout, and only for
# responses that arrived: a hung connection is an OSError, which is not
# retried at all.
RETRY_DELAYS = (2.0, 4.0, 8.0)
RETRY_ATTEMPTS = len(RETRY_DELAYS) + 1

TIERS = ("Public", "PublicEvolving", "Experimental", "Internal")
UNANNOTATED = "unannotated"
# Stablest first; across the versions an import is classified at, the last wins.
STABILITY = (*TIERS, UNANNOTATED)
# The TOML tables, keyed by the tier name classify() produces.
ALLOWLISTED = {
    "Internal": "internal",
    "Experimental": "experimental",
    UNANNOTATED: "unannotated",
}


def fail(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(1)


def infra(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(2)


def flink_version() -> str:
    # Stripped here, at the single owner: the regex tolerates whitespace
    # inside the tags, and the jar URLs, the cache filenames and the CI cache
    # key all want the bare version.
    pom = (ROOT / "pom.xml").read_text(encoding="utf-8")
    match = re.search(r"<flink\.version>([^<]+)</flink\.version>", pom)
    version = match.group(1).strip() if match else ""
    if not version:
        infra("pom.xml no longer declares <flink.version>; this script reads it.")
    return version


# release.yaml's LTS step reads the same line with the sed equivalent of this
# pattern; a test pins that the two still match.
FLINK_LTS_LINE = re.compile(r"^  FLINK_LTS: '(.*)'$", re.MULTILINE)


def lts_version() -> str:
    """weekly.yaml's FLINK_LTS, the one place the 1.x patch is pinned.

    Read as release.yaml's sed reads it: the first line of that exact shape,
    no newline translation and no stripping, so a shape one of them rejects
    the other rejects too.
    """
    path = ROOT / ".github" / "workflows" / "weekly.yaml"
    try:
        text = path.read_bytes().decode("utf-8")
    except (OSError, UnicodeDecodeError) as error:
        infra(f"{path.relative_to(ROOT)} could not be read: {error}")
    match = FLINK_LTS_LINE.search(text)
    version = match.group(1) if match else ""
    if not version:
        infra(
            f"{path.relative_to(ROOT)} no longer pins FLINK_LTS; this script reads it."
        )
    return version


def source_versions(
    source: Path, root_version: str, lts: str, lts_modules: set[str]
) -> frozenset[str]:
    """The Flink versions one main source compiles against."""
    parts = source.relative_to(ROOT).parts
    tier3 = parts[:2] == ("kubernetes", "apps")
    # <module>/src/main/<root>/... or kubernetes/apps/<module>/src/main/<root>/...
    tree = parts[5] if tier3 else parts[3]
    built = (
        {root_version} if tier3 and parts[2] not in lts_modules else {root_version, lts}
    )
    by_root = {"java": built, "java-flink1": {lts}, "java-flink2": {root_version}}
    if tree not in by_root:
        infra(
            f"{source.relative_to(ROOT)}: no Flink version is known for the source "
            f"root {tree}; teach source_versions() which versions build it."
        )
    versions = by_root[tree] & built
    if not versions:
        infra(
            f"{source.relative_to(ROOT)}: nothing builds the {tree} root of this "
            f"module; name the module in lts_tier3_modules, or remove the root."
        )
    return frozenset(versions)


def pinned_versions(root_version: str) -> list[str]:
    """Each Tier-3 job module whose own flink.version differs from the root's.

    A Tier-3 module's imports are classified at the root pom's version, so a
    module compiled against another would be audited against the wrong tiers:
    a type Flink demoted there would pass here.
    """
    mismatched: list[str] = []
    for pom in sorted(ROOT.glob("kubernetes/apps/*/pom.xml")):
        match = re.search(
            r"<flink\.version>([^<]+)</flink\.version>", pom.read_text(encoding="utf-8")
        )
        if match and match.group(1).strip() != root_version:
            mismatched.append(
                f"{pom.relative_to(ROOT)} pins flink.version {match.group(1).strip()}"
            )
    return mismatched


def collect_imports(
    root_version: str, lts: str, lts_modules: set[str]
) -> dict[str, set[str]]:
    """Each concrete Flink import across java_ast.API_TIER_SOURCE_PATTERNS,
    with every Flink version a source importing it compiles against."""
    found: dict[str, set[str]] = {}
    for source in api_tier_sources(ROOT):
        versions = source_versions(source, root_version, lts, lts_modules)
        try:
            parsed = JavaSource.parse(source.relative_to(ROOT), source.read_bytes())
        except JavaSyntaxError as error:
            infra(str(error))
        except OSError as error:
            # Exit 2, not the policy-violation 1: an unreadable source is the
            # tree's state, not a tier finding.
            infra(f"{source.relative_to(ROOT)} could not be read: {error}")
        for node in parsed.nodes("import_declaration"):
            imported = declaration_target(parsed, node)
            if imported.startswith("org.apache.flink.") and imported.endswith(".*"):
                infra(
                    f"{parsed.path}:{parsed.line(node)}: wildcard import {imported} "
                    "cannot be classified by API tier; replace it with explicit "
                    "type imports."
                )
            if imported.startswith("org.apache.flink."):
                found.setdefault(imported, set()).update(versions)
    if not found:
        infra(
            f"No org.apache.flink imports found under {ROOT} in "
            f"{', '.join(API_TIER_SOURCE_PATTERNS)}."
        )
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
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        # HTTPError before the broad handler below, which would otherwise
        # swallow it: HTTPError is an OSError subclass. It is also the only
        # failure worth retrying — a 429 or 5xx is Maven Central's state, not
        # this repository's. HTTPException covers what OSError does not: a
        # truncated body raises http.client.IncompleteRead, which is not an
        # OSError.
        try:
            body = urllib.request.urlopen(request, timeout=30).read()
            break
        except urllib.error.HTTPError as error:
            error.close()  # an HTTPError carries an open response body
            if not (error.code == 429 or 500 <= error.code < 600):
                # Fatal without retry, and the hint only where it applies: a
                # retried 404 would hide a wrong entry in the artifacts list,
                # and the same hint on a 429 sent an earlier reader hunting a
                # config problem that was not there.
                hint = (
                    f" A 404 usually means the artifacts list in "
                    f"{CONFIG.name} names an artifact that does not exist at "
                    f"{version}; every listed artifact must exist at both the "
                    f"floor and the LTS."
                    if error.code == 404
                    else ""
                )
                infra(f"Downloading {url} failed ({error}).{hint}")
            if attempt == RETRY_ATTEMPTS:
                infra(
                    f"Downloading {url} failed ({error}) after "
                    f"{RETRY_ATTEMPTS} attempts."
                )
            delay = RETRY_DELAYS[attempt - 1]
            print(
                f"HTTP {error.code} from Maven Central for {url}; retrying "
                f"in {delay:.0f} s (attempt {attempt} of {RETRY_ATTEMPTS}).",
                file=sys.stderr,
            )
            time.sleep(delay)
        except (OSError, http.client.HTTPException) as error:
            infra(f"Downloading {url} failed ({error}).")
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
    fqcn: str, index: dict[str, tuple[str, zipfile.ZipFile]], version: str = ""
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
    at = f" at {version}" if version else ""
    infra(
        f"{fqcn} resolves to no .java entry in any configured sources jar{at}. "
        f"Either the type moved between Flink artifacts or a new package "
        f"family arrived: extend the artifacts list in {CONFIG.name}. If "
        f"nothing builds the importing source at that version, the version "
        f"mapping is wrong instead: check source_versions() and "
        f"lts_tier3_modules."
    )


def classify(source: str, entry: str, nested: list[str]) -> str:
    """The class-level tier of the imported type in one Java syntax tree."""
    try:
        parsed = JavaSource.parse(entry, source)
    except JavaSyntaxError as error:
        infra(str(error))
    # For a nested import, the innermost declared name wins if it carries a
    # tier; otherwise the file's primary type speaks for it.
    for simple in [*reversed(nested), Path(entry).stem]:
        declaration = next(
            (
                node
                for node in parsed.nodes(*TYPE_DECLARATIONS)
                if (name := node.child_by_field_name("name")) is not None
                and parsed.text(name) == simple
            ),
            None,
        )
        if declaration is None:
            continue
        tiers = [
            name.rsplit(".", 1)[-1]
            for annotation in annotations(declaration)
            if (name := annotation_name(parsed, annotation)).rsplit(".", 1)[-1] in TIERS
        ]
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
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    prints = parser.add_mutually_exclusive_group()
    prints.add_argument(
        "--print-flink-version",
        action="store_true",
        help="print pom.xml's flink.version and exit; CI keys the sources-jar "
        "cache on it, and going through this script keeps one owner of the "
        "parsing",
    )
    prints.add_argument(
        "--print-lts-version",
        action="store_true",
        help="print weekly.yaml's FLINK_LTS and exit; CI keys the sources-jar "
        "cache on it beside flink.version",
    )
    arguments = parser.parse_args()
    if arguments.print_flink_version:
        print(flink_version())
        return 0
    if arguments.print_lts_version:
        print(lts_version())
        return 0

    with CONFIG.open("rb") as handle:
        try:
            config = tomllib.load(handle)
        except tomllib.TOMLDecodeError as error:
            infra(f"{CONFIG.name} is not valid TOML: {error}")
    # A typo'd table name would otherwise sit ignored while its types get
    # reported as unlisted — fail on the typo itself, which is the fixable end.
    unknown = set(config) - {"artifacts", "lts_tier3_modules", *ALLOWLISTED.values()}
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
    lts_modules = config.get("lts_tier3_modules", [])
    if not isinstance(lts_modules, list) or not all(
        isinstance(name, str) for name in lts_modules
    ):
        infra(f"{CONFIG.name}: lts_tier3_modules must be a list of module names.")
    # Matched against the module directory names themselves, not resolved as a
    # path: "cloudtasks/" would find the directory and then never equal the
    # name source_versions() compares, silently dropping the LTS.
    tier3_modules = {
        path.name
        for path in ROOT.glob("kubernetes/apps/*")
        if (path / "src" / "main").is_dir()
    }
    for name in lts_modules:
        if name not in tier3_modules:
            infra(
                f"{CONFIG.name}: lts_tier3_modules names {name}, but "
                f"kubernetes/apps/{name} has no main sources. Remove the entry."
            )
    version = flink_version()
    lts = lts_version()
    mismatched = pinned_versions(version)
    if mismatched:
        infra(
            f"{'; '.join(mismatched)}, but this audit classifies a Tier-3 "
            f"module's imports at pom.xml's {version}. Align the versions, or teach the audit to "
            f"classify that module at its own; the artifacts comment in "
            f"{CONFIG.name} records the coupling."
        )
    imports = collect_imports(version, lts, set(lts_modules))
    imported_versions = set().union(*imports.values())
    versions = [v for v in dict.fromkeys([version, lts]) if v in imported_versions]
    indexes = {v: build_index(config["artifacts"], v) for v in versions}
    # The CI cache restores across an FLINK_LTS bump, and an artifact can
    # leave the list: drop every cached jar this run did not open, so neither
    # the cache nor target/ keeps dead versions.
    wanted = {f"{a}-{v}-sources.jar" for a in config["artifacts"] for v in versions}
    for cached in CACHE.glob("*-sources.jar"):
        if cached.name not in wanted:
            cached.unlink()

    by_tier: dict[str, set[str]] = {tier: set() for tier in STABILITY}
    # The imports whose tier differs between versions, for the messages.
    split: dict[str, dict[str, str]] = {}
    used_artifacts: set[str] = set()
    for fqcn, imported_at in imports.items():
        tiers: dict[str, str] = {}
        for v in (v for v in versions if v in imported_at):
            entry, nested = resolve(fqcn, indexes[v], v)
            artifact, jar = indexes[v][entry]
            used_artifacts.add(artifact)
            tiers[v] = classify(jar.read(entry).decode("utf-8"), entry, nested)
        by_tier[max(tiers.values(), key=STABILITY.index)].add(fqcn)
        if len(set(tiers.values())) > 1:
            split[fqcn] = tiers

    def by_version(fqcn: str) -> str:
        if fqcn not in split:
            return ""
        return " (" + ", ".join(f"{t} at {v}" for v, t in split[fqcn].items()) + ")"

    problems: list[str] = []
    for tier, table in ALLOWLISTED.items():
        allowed = set(config.get(table, {}))
        for fqcn in sorted(by_tier[tier] - allowed):
            problems.append(
                f"{fqcn} is {tier}{by_version(fqcn)} but has no [{table}] entry "
                f"in {CONFIG.name}. "
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
        fail(f"\nFlink API tier audit failed against {' and '.join(versions)}.")

    total = sum(len(types) for types in by_tier.values())
    print(
        f"{total} distinct org.apache.flink imports, classified against "
        f"{' and '.join(versions)}:"
    )
    for tier in STABILITY:
        label = tier if tier == UNANNOTATED else f"@{tier}"
        print(f"  {label:<16} {len(by_tier[tier]):>3}")
    for tier in ALLOWLISTED:
        for fqcn in sorted(by_tier[tier]):
            print(f"    {tier}: {fqcn}")
    for fqcn in sorted(split):
        print(f"    differs by version: {fqcn}{by_version(fqcn)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
