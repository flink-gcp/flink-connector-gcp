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
"""Exercise suite orchestration against synthetic Maven and fixture processes."""

import json
import os
import signal
import subprocess
import sys
import time

import pytest
from conftest import SCRIPTS


@pytest.fixture()
def suite(tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    for name in ("e2e-gated-its.sh", "check-gated-tags.py", "e2e-reports.py"):
        (scripts / name).symlink_to(SCRIPTS / name)
    # The parser imports its shared helpers relative to its real file.
    (tmp_path / ".venv").symlink_to(SCRIPTS.parent / ".venv", target_is_directory=True)
    modules = ("cloudtasks", "bigquery", "pubsub", "bigtable", "spanner")
    for module in modules:
        gate = "BQ" if module == "bigquery" else module.upper()
        source = (
            tmp_path
            / f"flink-connector-gcp-{module}/src/test/java/p/{module.title()}ITCase.java"
        )
        source.parent.mkdir(parents=True)
        source.write_text(
            f'package p;\n@Tag("gated")\n@EnabledIfEnvironmentVariable(named="{gate}_IT_PROJECT", matches=".+")\n'
            f"class {module.title()}ITCase {{}}\n"
        )
    fixture = scripts / "appengine-e2e-fixture.sh"
    fixture.write_text(
        '#!/bin/bash\nset -eu\necho "fixture-$1" >> "$CALLS"\n'
        'if [ "$1" = stop ]; then exit "${STOP_STATUS:-0}"; fi\n'
        'shift 2\nexec "$@"\n'
    )
    fixture.chmod(0o755)
    maven = tmp_path / "maven"
    maven.write_text(
        f"#!{sys.executable}\n"
        + r"""
import os, pathlib, signal, sys, time
args = sys.argv[1:]
module = args[args.index("-pl") + 1]
phase = "install" if "install" in args else "compile" if "test-compile" in args else module.removeprefix("flink-connector-gcp-")
with open(os.environ["CALLS"], "a") as out:
    out.write(phase + "\n")
if os.environ.get("BLOCK") == phase:
    def stop(*args):
        pathlib.Path("child-stopped").write_text("yes")
        sys.exit(143)
    signal.signal(signal.SIGTERM, stop)
    pathlib.Path("child-pid").write_text(str(os.getpid()))
    while True:
        time.sleep(.01)
status = 9 if os.environ.get("FAIL") == phase else 0
if phase not in ("install", "compile"):
    assert "-Dsurefire.rerunFailingTestsCount=0" in args
    name = phase.title() + "ITCase"
    assert "-Dtest=" + name in args
    report = pathlib.Path(module) / "target/surefire-reports" / ("TEST-p." + name + ".xml")
    report.parent.mkdir(parents=True, exist_ok=True)
    failure = '<failure message="synthetic"/>' if status else ''
    report.write_text(f'<testsuite name="p.{name}" tests="1" failures="{int(bool(status))}" errors="0" skipped="0"><testcase classname="p.{name}" name="test">{failure}</testcase></testsuite>')
sys.exit(status)
"""
    )
    maven.chmod(0o755)
    env = {
        "PATH": os.environ["PATH"],
        "VIRTUAL_ENV": str(tmp_path / ".venv"),
        "UV_RUN_RECURSION_DEPTH": "1",
        "CALLS": str(tmp_path / "calls"),
        **dict.fromkeys(
            (
                "BQ_IT_PROJECT",
                "BQ_IT_DATASET",
                "BQ_IT_GCS_BUCKET",
                "PUBSUB_IT_PROJECT",
                "BIGTABLE_IT_PROJECT",
                "SPANNER_IT_PROJECT",
                "CLOUDTASKS_IT_PROJECT",
            ),
            "synthetic",
        ),
    }
    command = [str(scripts / "e2e-gated-its.sh"), "--run", "--", str(maven)]

    def run(**overrides):
        return subprocess.run(
            command,
            cwd=tmp_path,
            env=env | overrides,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

    run.root = tmp_path
    run.env = env
    run.command = command
    run.calls = lambda: (tmp_path / "calls").read_text().splitlines()
    run.results = lambda: json.loads(
        (tmp_path / "target/e2e/evidence/results.json").read_text()
    )
    return run


@pytest.mark.parametrize(
    "failed", ["", "cloudtasks", "bigquery", "pubsub", "bigtable", "spanner"]
)
def test_independent_connectors_run_even_after_a_test_failure(suite, failed):
    result = suite(FAIL=failed)
    assert result.returncode == (1 if failed else 0), result.stderr
    assert suite.calls() == [
        "install",
        "compile",
        "fixture-run",
        "cloudtasks",
        "fixture-stop",
        "bigquery",
        "pubsub",
        "bigtable",
        "spanner",
    ]
    results = suite.results()
    assert len(results) == 5
    assert sum(row["status"] == "FAILED" for row in results) == bool(failed)


def test_unconfirmed_idle_fixture_prevents_later_billed_suites(suite):
    result = suite(STOP_STATUS="8")
    assert result.returncode == 8
    assert suite.calls() == [
        "install",
        "compile",
        "fixture-run",
        "cloudtasks",
        "fixture-stop",
    ]
    assert sum(row["status"] == "NOT_RUN" for row in suite.results()) == 4


@pytest.mark.parametrize("phase", ["install", "compile"])
def test_build_failure_still_reports_unexecuted_classes(suite, phase):
    result = suite(FAIL=phase)
    assert result.returncode == 9
    assert all(row["status"] == "NOT_RUN" for row in suite.results())
    assert "fixture-run" not in suite.calls()


def test_an_earlier_pass_cannot_cover_a_failed_build(suite):
    assert suite().returncode == 0
    assert suite(FAIL="install").returncode == 9
    assert all(row["status"] == "NOT_RUN" for row in suite.results())


def test_termination_reaches_active_child_and_finalizes_reports(suite):
    process = subprocess.Popen(
        suite.command,
        cwd=suite.root,
        env=suite.env | {"BLOCK": "pubsub"},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 15
        while not (suite.root / "child-pid").exists():
            assert process.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.02)
        process.send_signal(signal.SIGTERM)
        process.communicate(timeout=10)
        assert process.returncode == 143
        assert (suite.root / "child-stopped").exists()
        assert "bigtable" not in suite.calls()
        assert sum(row["status"] == "NOT_RUN" for row in suite.results()) == 3
    finally:
        child_pid = suite.root / "child-pid"
        if child_pid.exists() and not (suite.root / "child-stopped").exists():
            try:
                os.kill(int(child_pid.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)
