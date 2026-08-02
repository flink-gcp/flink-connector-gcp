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
"""Decide which Maven modules a change builds (issue #243).

CI's Build-and-test job used to run the full reactor (~8 minutes measured on
PR #241) on every pull request, whatever the change touched. This script turns
a changed-file list into the smallest `-pl` argument list that still verifies
everything the change can affect, so the common case — a pull request touching
one connector — builds that connector and its neighbours instead of the world.

The mapping is derived, never configured, in the e2e-gated-its.sh tradition
(joining is a decision, not an accident):

* The module set — and the reactor order the `-pl` list preserves — comes from
  the root pom's `<modules>`.
* The inter-module edges come from each module pom's `io.github.flink-gcp`
  dependencies. A changed module pulls in its dependents transitively, because
  they consume the change (a pubsub change rebuilds the SQL uber-jar that
  bundles it), and its dependencies ride along in the `-pl` list, because a
  `-pl` subset cannot resolve a reactor sibling the list omits — the same fact
  the justfile's binary-compat and e2e install lists document.
* Because everything is derived, a new module is covered from the moment the
  root pom names it; there is no filter file to forget to update.

Each changed file is classified by the first matching rule:

1. **Ignored** — any `README.md` or `CLAUDE.md` (apache-rat's exclude list
   carries exactly those patterns, so skipping them loses no licence-header
   check), `opentofu/**`, `tfaction-root.yaml` and the three tofu workflows
   (the tfaction workflows check those). This is the pull_request half of the
   old workflow-level paths-ignore; the push trigger keeps the real one, so
   merges that cannot affect the Maven build stay free on main too.
2. **A module directory** — that module.
3. **`docs/**`** — the root module alone (`-pl .`): its apache-rat execution
   scans the whole working tree, and CI is the docs markdown's only pre-merge
   licence check, so a docs-only change pays ~a minute for rat instead of the
   full reactor.
4. **Anything else** — the full reactor: the root pom, `mise.toml`, the
   justfile, `scripts/`, the workflows, a new top-level directory. Unknown
   means everything; that is the safe direction.

Modes (CI passes the first two, see ci.yaml; the third reproduces CI's
decision by hand):

  --files '<json array>'  pull_request: dorny/paths-filter's list-files output
                          for a catch-all filter. The action is the
                          changed-file *provider* — it reads the pull
                          request's file list through the API — and this
                          script is the single place the decision logic
                          lives, in CI and locally alike.
  --full                  push / workflow_dispatch: the full reactor.
  --diff <base-ref>       classify `git diff --name-only <base-ref>...HEAD`
                          exactly as CI would, e.g. `just ci-maven-args
                          --diff origin/main` on a feature branch.

Output, one `$GITHUB_OUTPUT`-style line each:

  run_build=true|false    false when nothing Maven-relevant changed; the gate
                          job turns that into an explicit green.
  maven_args=...          empty = full reactor; else `-pl .,<modules>` in
                          reactor order (`.` always included: the subset
                          needs the parent pom, and its rat execution is what
                          covers root-level and docs files).
  check_notice=true|false whether the built set contains a shaded module —
                          one whose directory carries a NOTICE.template —
                          which is what decides whether ci.yaml runs
                          `just check-notice`.

Exit codes: 0 clean, 1 a module pom names an `io.github.flink-gcp` dependency
that is no reactor module (fix the pom), 2 infrastructure error (unreadable
pom, malformed input). Standard library only, like its siblings in this
directory — and nothing newer than any runner image's python3, so CI needs no
setup-python for it.
"""

import argparse
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

GROUP_ID = "io.github.flink-gcp"

POM_NS = {"m": "http://maven.apache.org/POM/4.0.0"}

# Rule 1 above: the pull_request half of the old ci.yaml paths-ignore. Keep in
# sync with the push trigger's paths-ignore in .github/workflows/ci.yaml.
IGNORED_BASENAMES = {"README.md", "CLAUDE.md"}
IGNORED_PREFIXES = ("opentofu/",)
IGNORED_FILES = {
    "tfaction-root.yaml",
    ".github/workflows/tofu-plan.yaml",
    ".github/workflows/tofu-apply.yaml",
    ".github/workflows/tofu-list.yaml",
}


def fail(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(1)


def infra(message: str) -> "sys.NoReturn":
    print(message, file=sys.stderr)
    sys.exit(2)


def pom_modules() -> list[str]:
    """The reactor modules, in root-pom (= reactor) order."""
    pom = ROOT / "pom.xml"
    try:
        tree = ET.parse(pom)
    except (OSError, ET.ParseError) as e:
        infra(f"cannot parse {pom}: {e}")
    modules = [
        el.text.strip()
        for el in tree.getroot().findall("m:modules/m:module", POM_NS)
        if el.text and el.text.strip()
    ]
    if not modules:
        infra(f"{pom} declares no <modules>; refusing to derive anything from it")
    return modules


def module_dependencies(modules: list[str]) -> dict[str, set[str]]:
    """Direct io.github.flink-gcp edges, from each module pom's <dependencies>.

    Only the project's own <dependencies> block is read — never
    <dependencyManagement>, and never plugin configuration — because only a
    real dependency makes a sibling's output part of this module's build.
    """
    known = set(modules)
    edges: dict[str, set[str]] = {}
    for module in modules:
        pom = ROOT / module / "pom.xml"
        try:
            tree = ET.parse(pom)
        except (OSError, ET.ParseError) as e:
            infra(f"cannot parse {pom}: {e}")
        deps = set()
        for dep in tree.getroot().findall("m:dependencies/m:dependency", POM_NS):
            group = dep.findtext("m:groupId", "", POM_NS).strip()
            artifact = dep.findtext("m:artifactId", "", POM_NS).strip()
            if group != GROUP_ID:
                continue
            if artifact not in known:
                fail(
                    f"{pom} depends on {GROUP_ID}:{artifact}, which is not a "
                    f"module of the root pom — a typo there would silently "
                    f"drop the dependency edge this script builds from"
                )
            deps.add(artifact)
        edges[module] = deps
    return edges


def classify(
    files: list[str], modules: list[str]
) -> tuple[list[str], set[str], list[str], list[str]]:
    """Split changed paths into (ignored, module set, docs, full-reactor)."""
    ignored: list[str] = []
    selected: set[str] = set()
    docs: list[str] = []
    everything: list[str] = []
    for f in files:
        f = f.strip().lstrip("/")
        if not f:
            continue
        if (
            f.rsplit("/", 1)[-1] in IGNORED_BASENAMES
            or f.startswith(IGNORED_PREFIXES)
            or f in IGNORED_FILES
        ):
            ignored.append(f)
            continue
        module = next((m for m in modules if f.startswith(m + "/")), None)
        if module is not None:
            selected.add(module)
        elif f.startswith("docs/"):
            docs.append(f)
        else:
            everything.append(f)
    return ignored, selected, docs, everything


def close_over(changed: set[str], edges: dict[str, set[str]]) -> set[str]:
    """The build set for a set of changed modules.

    Two directions with deliberately different rules. *Dependents* are added
    transitively from the changed modules only — they consume the change, so
    their tests must run. *Dependencies* of that affected set then ride along
    for reactor resolution, but they trigger no further expansion: bigtable
    pulling in base must not drag base's other dependents (every module) into
    the build — base did not change.
    """
    affected = set(changed)
    while True:
        dependents = {m for m, deps in edges.items() if deps & affected}
        if dependents <= affected:
            break
        affected |= dependents
    build = set(affected)
    while True:
        upstream = set().union(*(edges[m] for m in build)) if build else set()
        if upstream <= build:
            return build
        build |= upstream


def changed_files(args: argparse.Namespace) -> list[str]:
    if args.files is not None:
        raw = args.files.strip()
        if not raw:
            return []
        try:
            files = json.loads(raw)
        except json.JSONDecodeError as e:
            infra(f"--files is not valid JSON: {e}")
        if not isinstance(files, list) or not all(isinstance(f, str) for f in files):
            infra("--files must be a JSON array of path strings")
        return files
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{args.diff}...HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        infra(
            f"git diff --name-only {args.diff}...HEAD failed: {result.stderr.strip()}"
        )
    return result.stdout.splitlines()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--files", help="JSON array of changed paths (CI, pull_request)")
    mode.add_argument("--full", action="store_true", help="full reactor (CI, push)")
    mode.add_argument(
        "--diff", metavar="BASE_REF", help="classify git diff BASE...HEAD"
    )
    args = parser.parse_args()

    modules = pom_modules()
    edges = module_dependencies(modules)

    if args.full:
        emit(run_build=True, maven_args="", check_notice=True, reason="--full")
        return

    files = changed_files(args)
    ignored, selected, docs, everything = classify(files, modules)
    print(
        f"changed: {len(files)} file(s) — {len(ignored)} ignored, "
        f"{len(docs)} docs, {len(everything)} full-reactor, "
        f"modules: {', '.join(sorted(selected)) or 'none'}",
        file=sys.stderr,
    )

    if everything:
        emit(
            run_build=True,
            maven_args="",
            check_notice=True,
            reason=f"full reactor, forced by e.g. {everything[0]}",
        )
        return

    selected = close_over(selected, edges)
    if selected >= set(modules):
        emit(run_build=True, maven_args="", check_notice=True, reason="all modules")
        return
    if not selected and not docs:
        emit(
            run_build=False,
            maven_args="",
            check_notice=False,
            reason="nothing here can affect the Maven build",
        )
        return

    ordered = [m for m in modules if m in selected]
    maven_args = "-pl " + ",".join(["."] + ordered)
    check_notice = any((ROOT / m / "NOTICE.template").is_file() for m in ordered)
    reason = (
        "modules " + ", ".join(ordered) if ordered else "docs only (root rat check)"
    )
    emit(
        run_build=True,
        maven_args=maven_args,
        check_notice=check_notice,
        reason=reason,
    )


def emit(*, run_build: bool, maven_args: str, check_notice: bool, reason: str) -> None:
    print(f"building: {reason}", file=sys.stderr)
    print(f"run_build={'true' if run_build else 'false'}")
    print(f"maven_args={maven_args}")
    print(f"check_notice={'true' if check_notice else 'false'}")


if __name__ == "__main__":
    main()
