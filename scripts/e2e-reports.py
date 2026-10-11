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
"""Validate fresh E2E reports and export only selected test metadata.

Each gate is prepared and asserted on its own, so its suite can run in a job of its own; its
evidence lands in target/e2e/evidence/<gate>/. `aggregate` then reads every gate's evidence back
against the whole inventory, which is what proves that no gate's run went missing.
"""

import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

FRAME = re.compile(r"\s*at ([\w.$/]+\([\w.$]+\.java:[0-9]+\))\s*")
IDENTIFIER = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")
# A gate names a directory, so it is held to the environment-variable shape before it is one.
GATE = re.compile(r"[A-Z][A-Z0-9_]*")


def checked_gate(gate):
    if not GATE.fullmatch(gate):
        raise ValueError("invalid gate name")
    return gate


def manifest_path(root, gate):
    return root / "target/e2e/runs" / f"{checked_gate(gate)}.json"


def evidence_directory(root, gate=None):
    directory = root / "target/e2e/evidence"
    return directory if gate is None else directory / checked_gate(gate)


def module_depth(parts):
    """How many leading parts name the module: a connector, or a Tier-3 application."""
    return 3 if parts[:2] == ("kubernetes", "apps") else 1


def module(source):
    parts = Path(source).parts
    return "/".join(parts[: module_depth(parts)])


def report_path(root, source):
    parts = Path(source).parts
    depth = module_depth(parts)
    if (
        len(parts) < depth + 5
        or parts[depth : depth + 2] != ("src", "test")
        or not parts[depth + 2].startswith("java")
        or ".." in parts
        or not parts[-1].endswith(".java")
    ):
        raise ValueError("invalid gated source path")
    fqcn = ".".join((*parts[depth + 3 : -1], parts[-1][:-5]))
    if not all(IDENTIFIER.fullmatch(part) for part in fqcn.split(".")):
        raise ValueError("invalid gated class name")
    return (
        root.joinpath(*parts[:depth]) / "target/surefire-reports" / f"TEST-{fqcn}.xml",
        fqcn,
    )


def prepare(root, gate, sources):
    if not sources or len(set(sources)) != len(sources):
        raise ValueError("empty or duplicate E2E inventory")
    reports = [report_path(root, source)[0] for source in sources]
    for report in reports:
        report.unlink(missing_ok=True)
    manifest = manifest_path(root, gate)
    manifest.parent.mkdir(parents=True, exist_ok=True)
    # A combined result from an earlier run must not outlive the start of a new one.
    for name in ("results.json", "summary.md"):
        (evidence_directory(root) / name).unlink(missing_ok=True)
    # Compare filesystem timestamps with the same resolution, including coarse Linux clocks.
    manifest.touch()
    started_ns = manifest.stat().st_mtime_ns
    manifest.write_text(json.dumps({"started_ns": started_ns, "sources": sources}))
    write_evidence(
        evidence_directory(root, gate),
        [
            {
                "module": module(source),
                "class": report_path(root, source)[1],
                "status": "NOT_RUN",
                "problems": ["no result yet"],
                "cases": [],
            }
            for source in sources
        ],
    )


def number(element, name):
    value = element.get(name, "")
    if not re.fullmatch(r"[0-9]+", value):
        raise ValueError(f"missing or invalid {name} count")
    return int(value)


def read_report(path, fqcn, started_ns):
    if path.stat().st_mtime_ns < started_ns:
        raise ValueError("report predates this run")
    suite = ET.parse(path).getroot()
    if suite.tag != "testsuite" or suite.get("name") != fqcn:
        raise ValueError("report suite identity does not match the selected class")
    counts = {
        name: number(suite, name) for name in ("tests", "failures", "errors", "skipped")
    }
    cases = suite.findall("testcase")
    if len(cases) != counts["tests"] or counts["tests"] == 0:
        raise ValueError("testcase count is zero or disagrees with the report")
    observed = {"failures": 0, "errors": 0, "skipped": 0}
    exported = []
    for case in cases:
        if case.get("classname") != fqcn:
            raise ValueError(
                "testcase class identity does not match the selected class"
            )
        raw_name = case.get("name")
        name = IDENTIFIER.match(raw_name or "")
        if name is not None:
            method = name.group()
        elif raw_name == "" and (
            case.find("error") is not None or case.find("failure") is not None
        ):
            # Surefire 3.2.5 emits an empty method name for a JUnit class lifecycle failure.
            method = "class_lifecycle"
        else:
            raise ValueError("missing testcase method name")
        result = {"method": method, "status": "PASSED"}
        for tag, count, status in (
            ("failure", "failures", "FAILED"),
            ("error", "errors", "ERROR"),
            ("skipped", "skipped", "SKIPPED"),
        ):
            children = case.findall(tag)
            observed[count] += len(children)
            if children:
                result["status"] = status
                result["frames"] = [
                    match.group(1)
                    for child in children
                    for line in (child.text or "").splitlines()
                    if (match := FRAME.fullmatch(line))
                ][:50]
        if any(
            node.tag in ("flakyFailure", "flakyError", "rerunFailure", "rerunError")
            for node in case.iter()
        ):
            raise ValueError("retried testcase cannot establish an E2E pass")
        exported.append(result)
    if any(observed[name] != counts[name] for name in observed):
        raise ValueError("outcome counts disagree with testcase results")
    problems = [f"{name}={counts[name]}" for name in observed if counts[name]]
    return counts, exported, problems


def collect(root, gate, sources):
    state = json.loads(manifest_path(root, gate).read_text())
    if state.get("sources") != sources or not isinstance(state.get("started_ns"), int):
        raise ValueError("E2E inventory differs from the prepared run")
    results = []
    for source in sources:
        path, fqcn = report_path(root, source)
        result = {
            "module": module(source),
            "class": fqcn,
            "status": "INVALID",
            "problems": [],
            "cases": [],
        }
        try:
            counts, cases, problems = read_report(path, fqcn, state["started_ns"])
            result.update(
                counts,
                cases=cases,
                problems=problems,
                status="FAILED" if problems else "PASSED",
            )
        except FileNotFoundError:
            result.update(
                status="NOT_RUN", problems=["gated ITCase did not run: missing report"]
            )
        except (ValueError, ET.ParseError, OSError) as error:
            # Parser/OS exception text may contain report contents or local paths.
            message = (
                str(error)
                if isinstance(error, ValueError)
                else "unreadable or truncated XML report"
            )
            result["problems"] = [message]
        results.append(result)
    write_evidence(evidence_directory(root, gate), results)
    for result in results:
        print_result(result)
    return int(any(result["status"] != "PASSED" for result in results))


def aggregate(root, inventory):
    """Combine every gate's evidence and fail unless each covers its own classes, all passing.

    A gate whose run was cancelled, skipped or never uploaded leaves no evidence, and that has to
    read as a failure here; otherwise its classes would simply be absent from a green summary.
    It reads whatever evidence is present and cannot tell this run's from an earlier one's, and a
    gate can fail after its tests passed (its fixture's stop, say); e2e.yaml therefore also
    requires every gate job to have succeeded, and `--run` returns each gate's own status.
    """
    results = []
    problems = []
    for gate, sources in inventory.items():
        expected = {
            (module(source), report_path(root, source)[1]): source for source in sources
        }
        reported = {}
        path = evidence_directory(root, gate) / "results.json"
        try:
            rows = json.loads(path.read_text())
        except FileNotFoundError:
            problems.append(
                f"{gate}: no evidence; its run did not start or did not finish"
            )
            rows = []
        except (OSError, json.JSONDecodeError):
            problems.append(f"{gate}: unreadable evidence")
            rows = []
        if not isinstance(rows, list) or not all(map(well_formed, rows)):
            problems.append(f"{gate}: malformed evidence")
            rows = []
        for row in rows:
            key = (row["module"], row["class"])
            if key not in expected:
                problems.append(
                    f"{gate}: evidence for {row['class']}, which is not one of its classes"
                )
            elif key in reported:
                problems.append(f"{gate}: {row['class']} is reported twice")
            reported[key] = row
        for key in expected:
            results.append(
                reported.get(key)
                or {
                    "module": key[0],
                    "class": key[1],
                    "status": "NOT_RUN",
                    "problems": ["no evidence from its gate's run"],
                    "cases": [],
                }
            )
    write_evidence(evidence_directory(root), results, problems)
    for result in results:
        if result["status"] != "PASSED":
            print_result(result)
    for problem in problems:
        print(problem, file=sys.stderr)
    passed = sum(result["status"] == "PASSED" for result in results)
    print(
        f"E2E: {passed} of {len(results)} gated classes passed "
        f"across {len(inventory)} gates"
    )
    return int(bool(problems) or passed != len(results))


def well_formed(row):
    """Whether an evidence row has the fields the summary and the checks read."""
    return (
        isinstance(row, dict)
        and all(isinstance(row.get(key), str) for key in ("module", "class", "status"))
        and isinstance(row.get("problems"), list)
        and all(isinstance(problem, str) for problem in row["problems"])
    )


def read_inventory(lines):
    """Group `GATE<TAB>MODULE<TAB>SOURCE` lines, as check-gated-tags.py --inventory prints them."""
    inventory = {}
    for line in lines:
        fields = line.split("\t")
        # The runner selects a module by the middle column and this reads its report from the
        # module the path names; the two must be the same module.
        if len(fields) != 3 or fields[1] != module(fields[2]):
            raise ValueError("malformed E2E inventory")
        inventory.setdefault(checked_gate(fields[0]), []).append(fields[2])
    sources = [source for gate_sources in inventory.values() for source in gate_sources]
    if not sources or len(set(sources)) != len(sources):
        raise ValueError("empty or duplicate E2E inventory")
    return inventory


def print_result(result):
    print(
        f"{result['class']}: {result['status']} " + "; ".join(result["problems"]),
        file=sys.stdout if result["status"] == "PASSED" else sys.stderr,
    )


def write_evidence(directory, results, problems=()):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    lines = [
        "# E2E results",
        "",
        "| Connector | Class | Result | Details |",
        "| --- | --- | --- | --- |",
    ]
    for result in results:
        lines.append(
            f"| {result['module']} | {result['class']} | {result['status']} | {'; '.join(result['problems'])} |"
        )
    if problems:
        lines.extend(["", *(f"- {problem}" for problem in problems)])
    (directory / "summary.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "assert", "aggregate"))
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    try:
        inventory = read_inventory(sys.stdin.read().splitlines())
        if args.mode == "prepare":
            for gate, sources in inventory.items():
                prepare(args.root, gate, sources)
            return 0
        if args.mode == "assert":
            return max(
                collect(args.root, gate, sources) for gate, sources in inventory.items()
            )
        return aggregate(args.root, inventory)
    except (ValueError, OSError, json.JSONDecodeError) as error:
        print(
            f"E2E report validation failed ({type(error).__name__}); prepare the run before executing tests",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
