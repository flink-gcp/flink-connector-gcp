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

from java_ast import (
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


E2E_GATES = (
    "BQ_IT_PROJECT",
    "PUBSUB_IT_PROJECT",
    "BIGTABLE_IT_PROJECT",
    "SPANNER_IT_PROJECT",
    "CLOUDTASKS_IT_PROJECT",
)


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
    sources = sorted(root.glob("*/src/test/java/**/*.java"))
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
    selected = [
        gate
        for gate in E2E_GATES
        if (only_gate is None or gate == only_gate)
        and (except_gate is None or gate != except_gate)
    ]
    by_gate: dict[str, list[Path]] = {gate: [] for gate in selected}
    for source in sorted(root.glob("*/src/test/java/**/*.java")):
        gates, _, _ = tags(source, root)
        for gate in gates & by_gate.keys():
            by_gate[gate].append(source.relative_to(root))
    missing = next((gate for gate, paths in by_gate.items() if not paths), None)
    if missing is not None:
        raise ValueError(
            f"no test class is gated on {missing}; the gating annotation moved "
            "or the tree layout changed"
        )
    return sorted({path for paths in by_gate.values() for path in paths})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--for-gate", choices=E2E_GATES)
    mode.add_argument("--except-gate", choices=E2E_GATES)
    mode.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    try:
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
