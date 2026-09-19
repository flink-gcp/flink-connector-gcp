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
"""Validate fresh E2E reports and export only selected test metadata."""

import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

FRAME = re.compile(r"\s*at ([\w.$/]+\([\w.$]+\.java:[0-9]+\))\s*")
IDENTIFIER = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")


def report_path(root, source):
    parts = Path(source).parts
    if (
        len(parts) < 6
        or parts[1:3] != ("src", "test")
        or not parts[3].startswith("java")
        or ".." in parts
        or not parts[-1].endswith(".java")
    ):
        raise ValueError("invalid gated source path")
    fqcn = ".".join((*parts[4:-1], parts[-1][:-5]))
    if not all(IDENTIFIER.fullmatch(part) for part in fqcn.split(".")):
        raise ValueError("invalid gated class name")
    return root / parts[0] / "target/surefire-reports" / f"TEST-{fqcn}.xml", fqcn


def prepare(root, sources):
    if not sources or len(set(sources)) != len(sources):
        raise ValueError("empty or duplicate E2E inventory")
    reports = [report_path(root, source)[0] for source in sources]
    for report in reports:
        report.unlink(missing_ok=True)
    directory = root / "target/e2e"
    directory.mkdir(parents=True, exist_ok=True)
    manifest = directory / "run.json"
    # Compare filesystem timestamps with the same resolution, including coarse Linux clocks.
    manifest.touch()
    started_ns = manifest.stat().st_mtime_ns
    manifest.write_text(json.dumps({"started_ns": started_ns, "sources": sources}))
    write_evidence(
        root,
        [
            {
                "module": source.split("/")[0],
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


def collect(root, sources):
    state = json.loads((root / "target/e2e/run.json").read_text())
    if state.get("sources") != sources or not isinstance(state.get("started_ns"), int):
        raise ValueError("E2E inventory differs from the prepared run")
    results = []
    for source in sources:
        path, fqcn = report_path(root, source)
        result = {
            "module": source.split("/")[0],
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
    write_evidence(root, results)
    for result in results:
        print(
            f"{result['class']}: {result['status']} " + "; ".join(result["problems"]),
            file=sys.stdout if result["status"] == "PASSED" else sys.stderr,
        )
    return int(any(result["status"] != "PASSED" for result in results))


def write_evidence(root, results):
    directory = root / "target/e2e/evidence"
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
    (directory / "summary.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "assert"))
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    sources = sys.stdin.read().splitlines()
    try:
        if args.mode == "prepare":
            prepare(args.root, sources)
            return 0
        return collect(args.root, sources)
    except (ValueError, OSError, json.JSONDecodeError) as error:
        print(
            f"E2E report validation failed ({type(error).__name__}); prepare the run before executing tests",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
