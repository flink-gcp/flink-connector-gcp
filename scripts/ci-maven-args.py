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
"""Decide which Maven modules a change builds (issue #243).

A full-reactor `just verify` costs ~8 minutes of CI wall clock (measured on
PR #241, 2026-08-01, one run; the breakdown is on #243). This script turns a
changed-file list into the smallest `-pl` argument list that still verifies
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

1. **Ignored** — any `README.md`, `AGENTS.md` or `CLAUDE.md` (apache-rat's exclude list
   carries exactly those patterns, so skipping them loses no licence-header
   check), `opentofu/**`, `tfaction-root.yaml` and the three tofu workflows
   (the tfaction workflows check those), and everything under `.github/` that
   is not a workflow or a composite action — templates, CODEOWNERS,
   dependabot.yml — since GitHub reads those and no build does. This is the
   pull_request half of the old workflow-level paths-ignore; the push trigger
   keeps the real one, so merges that cannot affect the Maven build stay free
   on main too. The push list stays an explicit handful rather than mirroring
   the `.github/` rule: GitHub's `!` negation in paths-ignore is order-sensitive
   and getting it wrong would silently stop CI on a real workflow change, and
   the cost of not mirroring it is one full build per merge of an inert file.
2. **A module directory** — that module.
3. **Root-only** — the root module alone (`-pl .`). These are the paths whose
   only Maven-relevant consumer is the root module's apache-rat execution,
   which scans the whole working tree (`inherited=false`,
   `excludeSubProjects=false`) and is their only pre-merge licence check:
   `docs/**`, `scripts/**`, the uv project behind `just test-scripts`
   (`pyproject.toml`, `uv.lock` — the lockfile is rat-excluded and rides along
   with its pyproject rather than earning a rule of its own), and
   `CONTRIBUTING.md` (rat scans it: the pom excludes `**/README.md` but not
   the contributor guide). No pom wires
   anything under `scripts/` into the build and no source reads a file from
   it, so such a change pays ~a minute for rat instead of the full reactor
   (issue #253; measured at 7m41s on PR #252, which changed `scripts/tests/`
   alone). That `-pl .` really does licence-check these paths is measured too,
   not inferred from the rat configuration: deleting the header block from
   the gate script (then scripts/ci-gate.sh) fails `just verify -pl .` with "Unapproved: 1"
   (2026-08-02, one run).
   The exception is NOTICE_INPUTS below, and the reason is mechanical: the
   NOTICE check is a step *inside* the build job, gated on `check_notice`,
   which is true only when the built set carries a NOTICE.template. Routing
   those two here would compute `false` and silently skip the licence check on
   exactly the change that edits the licence pins. The other two checkers run
   as unconditional verify.yaml jobs, so their inputs are genuinely Maven-free.
4. **Anything else** — the full reactor: the root pom, `mise.toml`, the
   justfile (it carries the Maven invocations themselves), the workflows, a
   new top-level directory. Unknown means everything; that is the safe
   direction.

The three classification modes (verify.yaml's changes job passes the last two;
no third-party changed-files action is involved — a pull_request checkout is
the base-into-head merge commit, so its own git history already carries the
changed-file list, decided on PR #247 after weighing the supply-chain
surface such actions add):

  --files '<json array>'  classify an explicit changed-path list — the
                          synthetic-input seam for exercising the
                          classification by hand and from tests.
  --full                  push / workflow_dispatch: the full reactor.
  --diff <base-ref>       classify `git diff --no-renames --name-only
                          <base-ref>...HEAD`. CI passes `HEAD^1` — on the
                          merge commit that is the current base tip, so the
                          diff is the pull request's net change. Locally,
                          `just ci-maven-args --diff origin/main` predicts
                          what a pull request with the current branch's
                          committed diff would build.

And one mode that answers a different question from the same pom graph,
classifying nothing (issue #932):

  --install-modules       the modules `just binary-compat` must install before
                          its goal-only rerun — every module another module
                          depends on, root first, as one comma-separated `-pl`
                          value on stdout and nothing else. ADR-0053 states the
                          rule; the recipe used to restate it and went stale.
                          This mode skips the NOTICE_INPUTS guard below, which
                          has nothing to say about a module list.

Output of the three classification modes, one `$GITHUB_OUTPUT`-style line each:

  run_build=true|false    false when nothing Maven-relevant changed; the gate
                          job turns that into an explicit green.
  lanes=<json array>      the build matrix: one object per lane, each with a
                          `name`, the `args` it builds (`-pl .,<modules>` in
                          reactor order; `.` is always included, since the
                          subset needs the parent pom and its rat execution is
                          what covers root-level and docs files), and the
                          `notice` modules — the shaded modules **that lane**
                          built, space-separated, which is what decides whether
                          it runs `just check-notice`.
                          One lane per connector the selection builds, so an
                          ordinary single-connector pull request still pays for
                          one runner; a selection with no connector in it is one
                          `root` lane.
  check_notice_sources=true|false   whether the change touches an input that
                          can move a pinned licence source — a pom.xml (the
                          resolved versions feed the {version} url templates),
                          a NOTICE_INPUTS file, a NOTICE.template, or a
                          checked-in META-INF NOTICE/licences file — which is
                          what decides whether the build job also runs
                          `just check-notice-sources`, the network-fetching
                          sibling of the offline NOTICE check (issue #343).
                          --full emits false: with no diff there is no
                          licence-input signal, and the weekly notice_sources
                          job owns the fetch that needs no change to trigger.

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

# Rule 1 above: the pull-request half of the Maven workflow's paths-ignore. Keep in
# sync with the push trigger's paths-ignore in .github/workflows/verify.yaml.
# The dot-directories are agent harness configuration — shared skills, MCP,
# Serena, settings and worktrees. No Maven build reads them, and rat excludes
# every dot-directory (`**/.*/**` in the root pom), so like .github/ below they
# do not even buy the root-only rat run. Named here because editing agent
# guidance is a routine change and must not force the full reactor.
IGNORED_BASENAMES = {"README.md", "AGENTS.md", "CLAUDE.md"}
# .claude/ is the compatibility surface; .agents/ is the canonical guidance.
# .codex/ and .serena/ configure their respective local tools.
IGNORED_PREFIXES = ("opentofu/", ".agents/", ".claude/", ".codex/", ".serena/")
IGNORED_FILES = {
    ".mcp.json",
    "tfaction-root.yaml",
    ".github/workflows/tofu-plan.yaml",
    ".github/workflows/tofu-apply.yaml",
    ".github/workflows/tofu-list.yaml",
}

# Everything under .github/ is inert to the Maven build — GitHub reads it, no
# build does, and it is rat-excluded so it does not even buy the root-only rat
# run — EXCEPT the two directories that decide what CI itself does. Stated as a
# rule rather than as a file list that grows by one entry per template: the
# templates, CODEOWNERS, dependabot.yml and whatever GitHub adds next are all
# the same case, and the case that is not is small and named.
GITHUB_BUILD_RELEVANT = (".github/workflows/", ".github/actions/")

# Rule 3: nothing Maven builds from these, but the root module's rat run does
# scan them, so they select `-pl .` rather than nothing at all.
# The `flink-connector-gcp-*` modules that are not connectors: every lane needs them,
# so they are spine rather than a lane of their own.
SPINE_SUFFIXES = frozenset({"base", "test-utils"})

ROOT_ONLY_PREFIXES = ("docs/", "scripts/")
ROOT_ONLY_FILES = {"pyproject.toml", "uv.lock", "CONTRIBUTING.md"}

# ...except the inputs of the one checker whose CI step the deriver can switch
# off. `just check-notice` runs inside the build job behind check_notice, so a
# change to these must keep the full reactor — where a shaded module is built,
# check_notice is true, and the module's own packaging tests run too. Not a
# list of "scripts that matter": it is exactly the Maven-gated checker's
# inputs, which is why check-option-docs.py and flink-api-tiers.toml are
# absent (their verify.yaml jobs run unconditionally).
NOTICE_INPUTS = {"scripts/config/licence-sources.toml", "scripts/check-notice.py"}


def moves_a_licence_source(path: str) -> bool:
    """Can this change move what a pinned licence source serves or resolves to?

    A pom decides the resolved versions the {version} url templates fetch at,
    NOTICE_INPUTS is the pin file and its interpreter, and the NOTICE.template
    and checked-in META-INF files are what the re-fetch is compared against. A
    change touching any of these additionally runs `just check-notice-sources`
    inside the build job (issue #343); everything else leaves the fetch to the
    weekly notice_sources job, so ordinary pull requests stay off the network.
    """
    return (
        path.rsplit("/", 1)[-1] == "pom.xml"
        or path in NOTICE_INPUTS
        or path.endswith("/NOTICE.template")
        or (
            "/src/main/resources/META-INF/" in path
            and (path.endswith("/NOTICE") or "/META-INF/licenses/" in path)
        )
    )


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
    """Split changed paths into (ignored, module set, root-only, full-reactor)."""
    ignored: list[str] = []
    selected: set[str] = set()
    root_only: list[str] = []
    everything: list[str] = []
    for f in files:
        f = f.strip().lstrip("/")
        if not f:
            continue
        if (
            f.rsplit("/", 1)[-1] in IGNORED_BASENAMES
            or f.startswith(IGNORED_PREFIXES)
            or f in IGNORED_FILES
            or (f.startswith(".github/") and not f.startswith(GITHUB_BUILD_RELEVANT))
        ):
            ignored.append(f)
            continue
        module = next((m for m in modules if f.startswith(m + "/")), None)
        if module is not None:
            selected.add(module)
        elif f in NOTICE_INPUTS:
            everything.append(f)
        elif f.startswith(ROOT_ONLY_PREFIXES) or f in ROOT_ONLY_FILES:
            root_only.append(f)
        else:
            everything.append(f)
    return ignored, selected, root_only, everything


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
    # --no-renames so a rename is one deleted plus one added path: git's
    # default rename detection would show only the new name, letting a file
    # moved out of a module hide that module from the build.
    result = subprocess.run(
        ["git", "diff", "--no-renames", "--name-only", f"{args.diff}...HEAD"],
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


def check_notice_inputs_exist() -> None:
    """Refuse to run if a NOTICE_INPUTS path is gone.

    The two allowlists this script carries go stale in opposite directions, so
    only one needs a guard. A stale ROOT_ONLY_FILES entry stops matching and
    its path falls through to the full reactor: over-building, which is the
    safe direction and announces itself in the wall clock. A stale
    NOTICE_INPUTS entry falls through to the *root-only* class instead, and the
    licence check silently stops running on the change that edits the licence
    pins — the failure this set exists to prevent, reintroduced by a rename
    nobody would connect to it.
    """
    missing = sorted(path for path in NOTICE_INPUTS if not (ROOT / path).is_file())
    if missing:
        infra(
            f"NOTICE_INPUTS names {missing}, which no longer exist. A rename "
            f"here does not fail loudly on its own: the path would quietly "
            f"rejoin the root-only class and stop pulling in the reactor that "
            f"makes check_notice true. Update the set in {Path(__file__).name}."
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--files", help="JSON array of changed paths (the synthetic-input seam)"
    )
    mode.add_argument(
        "--full", action="store_true", help="full reactor (CI: push, dispatch)"
    )
    mode.add_argument(
        "--diff", metavar="BASE_REF", help="classify git diff BASE...HEAD"
    )
    mode.add_argument(
        "--install-modules",
        action="store_true",
        help="the comma-separated -pl *value* `just binary-compat` installs (ADR-0053)",
    )
    args = parser.parse_args()

    # Before check_notice_inputs_exist(), and on purpose: that guard protects the
    # classification path, where a stale licence-source path silently skips the NOTICE
    # check. It has nothing to say about a module list, and running it here would fail a
    # build recipe for an unrelated reason. Nothing but the value goes to stdout — the
    # recipe reads it into a shell variable — and it is the value alone rather than the
    # `-pl `-prefixed form pl() returns, so the recipe can quote it as one word.
    if args.install_modules:
        modules = pom_modules()
        print(
            ",".join(
                ["."]
                + reactor_dependency_modules(modules, module_dependencies(modules))
            )
        )
        return

    check_notice_inputs_exist()
    modules = pom_modules()
    edges = module_dependencies(modules)

    if args.full:
        emit(
            run_build=True,
            built=modules,
            check_notice_sources=False,
            reason="--full",
        )
        return

    files = changed_files(args)
    fetch = any(moves_a_licence_source(f.strip().lstrip("/")) for f in files)
    ignored, selected, root_only, everything = classify(files, modules)
    print(
        f"changed: {len(files)} file(s) — {len(ignored)} ignored, "
        f"{len(root_only)} root-only, {len(everything)} full-reactor, "
        f"modules: {', '.join(sorted(selected)) or 'none'}",
        file=sys.stderr,
    )

    if everything:
        emit(
            run_build=True,
            built=modules,
            check_notice_sources=fetch,
            reason=f"full reactor, forced by e.g. {everything[0]}",
        )
        return

    selected = close_over(selected, edges)
    if selected >= set(modules):
        emit(
            run_build=True,
            built=modules,
            check_notice_sources=fetch,
            reason="all modules",
        )
        return
    if not selected and not root_only:
        emit(
            run_build=False,
            built=[],
            check_notice_sources=fetch,
            reason="nothing here can affect the Maven build",
        )
        return

    ordered = [m for m in modules if m in selected]
    reason = (
        "modules " + ", ".join(ordered) if ordered else "root module only (rat check)"
    )
    emit(
        run_build=True,
        built=ordered,
        check_notice_sources=fetch,
        reason=reason,
    )


def shaded_modules(modules: list[str]) -> list[str]:
    """The shaded modules among `modules`, in reactor order.

    A module carrying a NOTICE.template is a shaded module: that file is the human half of a
    generated META-INF/NOTICE and nothing else has one. Derived rather than listed, so a third
    flink-sql-connector-gcp-* is checked from the commit that adds it — and derived *here* rather
    than in the workflow, so the set checked is the set built. A workflow-side rule cannot see the
    selection, and would re-check a module the change never touched.
    """
    return [m for m in modules if (ROOT / m / "NOTICE.template").is_file()]


def reactor_dependency_modules(
    modules: list[str], edges: dict[str, set[str]]
) -> list[str]:
    """The modules that some other module depends on, in reactor order.

    This is the set `just binary-compat` installs between its two runs. The second run is
    goal-only, so it resolves every `io.github.flink-gcp` sibling from `~/.m2` instead of from
    the reactor, and ADR-0053 states which modules that requires as a rule: the root pom, each
    connector a SQL uber-jar bundles, the base module every connector compiles against, and the
    test-utils module every module's tests depend on. Being depended upon *is* that rule — an
    uber-jar is absent because nothing depends on it, which is right: the rerun builds it rather
    than resolving it.

    Derived rather than enumerated in the recipe, because the recipe's hand-written copy of this
    same rule went stale when the fifth uber-jar landed, and the weekly `binary_compat` job died
    resolving the connector it bundles (issue #932). A sixth connector is now covered from the
    commit that adds it.

    The root module is not among them: it is every module's `<parent>`, never a `<dependency>`,
    so nothing here reports it and the caller names it.
    """
    depended_on = {dep for deps in edges.values() for dep in deps}
    return [m for m in modules if m in depended_on]


def pl(built: list[str]) -> str:
    """The `-pl` argument for `built`, always explicit.

    Never the empty string, which Maven reads as the whole reactor: an empty `built` is
    the root-only case (`-pl .`, the rat check covering docs/ and scripts/, issue #253),
    so emitting "" for it would silently turn the cheapest selection into the most
    expensive one. The whole reactor is expressed by naming every module instead.
    """
    return "-pl " + ",".join(["."] + built)


def lane_of(module: str) -> str | None:
    """The lane a module belongs to: its connector's short name, or None for the spine.

    A `flink-sql-connector-gcp-x` rides with `flink-connector-gcp-x`, which it must --
    the uber-jar shades the connector it bundles. Everything else (the parent,
    test-utils, base) is spine and rides in every lane.
    """
    for prefix in ("flink-sql-connector-gcp-", "flink-connector-gcp-"):
        if module.startswith(prefix):
            name = module[len(prefix) :]
            return None if name in SPINE_SUFFIXES else name
    return None


def split_into_lanes(built: list[str]) -> list[dict[str, object]]:
    """One lane per connector, in reactor order; the spine rides in each.

    Derived, never configured -- the same rule the rest of this script follows. An
    earlier draft carried a hand-balanced pair of groups, which meant a judgement about
    which connectors to put together, a test to prove none had been forgotten, and a
    balance that nothing pinned and that the first measured run was 17% off. Deriving
    the lanes from the module list deletes all three: a connector added to the root pom
    gets a lane from that commit, and there is no balance to get wrong.

    Separate runners rather than a parallel reactor inside one: `-T 1C` was measured on
    2026-08-15 and declined, because it made every module three to four times slower at
    once -- four of them start testcontainers and the runner has four vCPUs. Runners do
    not share cores.

    A selection with no connector in it -- the root-only rat check -- is one lane.
    """
    lanes: dict[str, list[str]] = {}
    spine: list[str] = []
    for module in built:
        name = lane_of(module)
        if name is None:
            spine.append(module)
        else:
            lanes.setdefault(name, []).append(module)

    if not lanes:
        return [{"name": "root", "args": pl(built), "notice": built}]
    return [
        {"name": name, "args": pl(spine + members), "notice": spine + members}
        for name, members in lanes.items()
    ]


def emit(
    *,
    run_build: bool,
    built: list[str],
    check_notice_sources: bool,
    reason: str,
) -> None:
    lanes = [
        {
            "name": lane["name"],
            "args": lane["args"],
            "notice": " ".join(shaded_modules(lane["notice"])),
        }
        for lane in (split_into_lanes(built) if run_build else [])
    ]
    print(f"building: {reason}", file=sys.stderr)
    for lane in lanes:
        print(
            f"  lane {lane['name']}: {lane['args'] or 'whole reactor'}", file=sys.stderr
        )
    print(f"run_build={'true' if run_build else 'false'}")
    # One line of JSON, read by verify.yaml through fromJSON as the build matrix. Each
    # lane carries its own NOTICE list rather than the workflow reading a shared one:
    # with two lanes a shared list has each lane checking modules the other built, and
    # the failure shape there is a lane that checks nothing while staying green.
    print(f"lanes={json.dumps(lanes, separators=(',', ':'))}")
    print(f"check_notice_sources={'true' if check_notice_sources else 'false'}")


if __name__ == "__main__":
    main()
