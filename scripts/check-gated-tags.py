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
"""Hold environment-gated and JUnit-tagged Java test classes together."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from lib.java_ast import (
    ANNOTATIONS,
    JavaSource,
    JavaSyntaxError,
    annotation_name,
    code_named_children,
    string_literal_content,
)


def annotation_string_argument(
    parsed: JavaSource, annotation, key: str | None = None
) -> str | None:
    arguments = annotation.child_by_field_name("arguments")
    if arguments is None:
        return None
    if key is None:
        values = code_named_children(arguments)
        value = values[0] if len(values) == 1 else None
    else:
        value = None
        for pair in code_named_children(arguments):
            if pair.type != "element_value_pair":
                continue
            pair_key = pair.child_by_field_name("key")
            if pair_key is not None and parsed.text(pair_key) == key:
                value = pair.child_by_field_name("value")
                break
    if value is None or value.type != "string_literal":
        return None
    return string_literal_content(parsed, value)


# The order `just e2e` runs them in. Cloud Tasks comes first, as it did when the other suites
# waited for its fixture; nothing waits for it now, but its short run surfaces a fixture problem
# early.
E2E_GATES = (
    "CLOUDTASKS_IT_PROJECT",
    "BQ_IT_PROJECT",
    "PUBSUB_IT_PROJECT",
    "BIGTABLE_IT_PROJECT",
    "SPANNER_IT_PROJECT",
    "FIRESTORE_IT_PROJECT",
)


# Connector modules, and the Tier-3 applications, which sit outside the default reactor behind
# a `tier3-<name>` profile each but whose gated classes join the same E2E run.
TEST_SOURCES = (
    "*/src/test/java*/**/*.java",
    "kubernetes/apps/*/src/test/java*/**/*.java",
)


def test_sources(root: Path) -> list[Path]:
    return sorted({path for pattern in TEST_SOURCES for path in root.glob(pattern)})


def module_of(source: Path) -> str:
    """The module directory of a test source: everything before its `src/test`."""
    parts = source.parts
    index = next(
        i for i in range(len(parts) - 1) if parts[i : i + 2] == ("src", "test")
    )
    return "/".join(parts[:index])


def tags(path: Path, root: Path) -> tuple[set[str], bool, bool]:
    parsed = JavaSource.parse(path.relative_to(root), path.read_bytes())
    gated = False
    slow = False
    environment_gates: set[str] = set()
    for annotation in parsed.nodes(*ANNOTATIONS):
        name = annotation_name(parsed, annotation).rsplit(".", 1)[-1]
        if name == "Tag":
            value = annotation_string_argument(parsed, annotation)
            gated |= value == "gated"
            slow |= value == "slow"
        elif name == "EnabledIfEnvironmentVariable":
            gate = annotation_string_argument(parsed, annotation, "named")
            if gate is not None:
                environment_gates.add(gate)
    return environment_gates, gated, slow


def check(root: Path) -> tuple[int, int, list[str]]:
    sources = test_sources(root)
    gated_sources: list[Path] = []
    slow_sources: list[Path] = []
    problems: list[str] = []
    for source in sources:
        environment_gates, gated, slow = tags(source, root)
        environment_gate = bool(environment_gates)
        relative = source.relative_to(root)
        if environment_gate:
            gated_sources.append(relative)
        if slow:
            slow_sources.append(relative)
        if environment_gate and not gated:
            problems.append(
                f"{relative} is gated on an environment variable but carries no "
                '@Tag("gated"), so any build in a shell where that variable is '
                "set runs it — for the real-GCP suites, at the cost of billed "
                'resources. Add @Tag("gated") beside the gate, or remove the gate '
                "if the class is not part of a gated suite."
            )
        e2e_gates = [gate for gate in E2E_GATES if gate in environment_gates]
        if len(e2e_gates) > 1:
            problems.append(
                f"{relative} is gated on {' and '.join(e2e_gates)}, so the E2E run, "
                "which runs each gate on its own, would run it more than once. Gate it "
                "on one of them."
            )
        if gated and not environment_gate:
            problems.append(
                f'{relative} carries @Tag("gated") but no '
                '@EnabledIfEnvironmentVariable(named = "…"), so nothing runs it: '
                "ordinary builds exclude the tag and `just e2e` selects classes "
                "by the environment gate. Add the gate, or remove the tag."
            )
    if not gated_sources:
        problems.append(
            'no test class carries @EnabledIfEnvironmentVariable(named = "…"); '
            "the gating annotation moved or the tree layout changed, and this "
            "check would pass vacuously"
        )

    weekly = root / ".github" / "workflows" / "weekly.yaml"
    weekly_text = weekly.read_text(encoding="utf-8") if weekly.is_file() else ""
    active_weekly = "\n".join(
        line for line in weekly_text.splitlines() if not line.lstrip().startswith("#")
    )
    opts_into_slow = bool(
        re.search(r"-Dtest\.excluded\.groups=gated(?:\s|$)", active_weekly)
    )
    if slow_sources and not opts_into_slow:
        names = " ".join(str(path) for path in slow_sources)
        problems.append(
            f'classes carry @Tag("slow") but {weekly.relative_to(root)} no longer '
            "passes -Dtest.excluded.groups=gated, so nothing runs them: the root "
            "pom excludes the tag and no lane opts back in. Restore the opt-in, "
            f"or remove the tag from: {names}"
        )
    elif not slow_sources and opts_into_slow:
        problems.append(
            f'{weekly.relative_to(root)} opts into the "slow" lane but no test '
            'class carries @Tag("slow"), so the flag selects nothing and this '
            "check would pass vacuously from here on. Drop the flag, or tag the "
            "classes it was added for."
        )
    return len(gated_sources), len(slow_sources), problems


def gated_sources(
    root: Path, only_gate: str | None, except_gate: str | None
) -> list[Path]:
    return sorted(
        path
        for gate, path in inventory(root)
        if (only_gate is None or gate == only_gate)
        and (except_gate is None or gate != except_gate)
    )


def inventory(root: Path) -> list[tuple[str, Path]]:
    """Every gated class with the one E2E gate it runs under, in E2E_GATES order."""
    by_gate: dict[str, list[Path]] = {gate: [] for gate in E2E_GATES}
    for source in test_sources(root):
        gates, _, _ = tags(source, root)
        selected = [gate for gate in E2E_GATES if gate in gates]
        if len(selected) > 1:
            raise ValueError(
                f"{source.relative_to(root)} is gated on {' and '.join(selected)}, so the E2E "
                "run, which runs each gate on its own, would run it more than once; gate it "
                "on one of them"
            )
        if selected:
            by_gate[selected[0]].append(source.relative_to(root))
    missing = next((gate for gate, paths in by_gate.items() if not paths), None)
    if missing is not None:
        raise ValueError(
            f"no test class is gated on {missing}; the gating annotation moved "
            "or the tree layout changed"
        )
    return [(gate, path) for gate, paths in by_gate.items() for path in sorted(paths)]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--for-gate", choices=E2E_GATES)
    mode.add_argument("--except-gate", choices=E2E_GATES)
    mode.add_argument("--gates", action="store_true")
    mode.add_argument("--inventory", action="store_true")
    mode.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        if args.inventory:
            # One `GATE<TAB>MODULE<TAB>SOURCE` line per gated class: e2e-gated-its.sh runs
            # each module's classes from it, and e2e-reports.py validates their reports.
            for gate, path in inventory(root):
                print(f"{gate}\t{module_of(path)}\t{path}")
            return 0
        if args.gates:
            # The E2E gates, one per line, once every one of them has been seen to select a
            # class; e2e.yaml builds its job matrix from this.
            inventory(root)
            print("\n".join(E2E_GATES))
            return 0
        if not args.check:
            sources = gated_sources(root, args.for_gate, args.except_gate)
            print("\n".join(str(source) for source in sources))
            return 0
        gated, slow, problems = check(root)
    except (JavaSyntaxError, OSError) as error:
        print(f"::error::{error}", file=sys.stderr)
        return 2
    except ValueError as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1
    if problems:
        for problem in problems:
            print(f"::error::{problem}", file=sys.stderr)
        return 1
    print(f"gated classes carrying both markers: {gated}")
    print(f"slow classes the weekly lane runs: {slow}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
