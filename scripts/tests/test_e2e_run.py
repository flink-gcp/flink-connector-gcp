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

# The connector modules the synthetic tree holds gated classes in, one per E2E gate.
MODULES = ("cloudtasks", "bigquery", "pubsub", "bigtable", "spanner", "firestore")


@pytest.fixture()
def suite(tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    for name in ("e2e-gated-its.sh", "check-gated-tags.py", "e2e-reports.py"):
        (scripts / name).symlink_to(SCRIPTS / name)
    # The parser imports its shared helpers relative to its real file.
    (tmp_path / ".venv").symlink_to(SCRIPTS.parent / ".venv", target_is_directory=True)
    for module in MODULES:
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
import json
args = sys.argv[1:]
module = args[args.index("-pl") + 1]
with open(os.environ["ARGV"], "a") as out:
    out.write(json.dumps(args) + "\n")
application = module.startswith("kubernetes/apps/")
phase = (
    "install" if "install" in args
    else "app-" + module.rsplit("/", 1)[1] if application
    else "compile" if "test-compile" in args
    else module.removeprefix("flink-connector-gcp-")
)
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
# FAIL names a phase, or a phase and the -pl it was given ("compile:flink-connector-gcp-pubsub").
status = 9 if os.environ.get("FAIL") in (phase, phase + ":" + module) else 0
if phase not in ("install", "compile"):
    assert "-Dsurefire.rerunFailingTestsCount=0" in args
    names = next(a for a in args if a.startswith("-Dtest=")).removeprefix("-Dtest=").split(",")
    if not application:
        assert names[0] == phase.title() + "ITCase"
    for name in names:
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
        "ARGV": str(tmp_path / "argv.jsonl"),
        **dict.fromkeys(
            (
                "BQ_IT_PROJECT",
                "BQ_IT_DATASET",
                "BQ_IT_GCS_BUCKET",
                "PUBSUB_IT_PROJECT",
                "BIGTABLE_IT_PROJECT",
                "SPANNER_IT_PROJECT",
                "FIRESTORE_IT_PROJECT",
                "CLOUDTASKS_IT_PROJECT",
            ),
            "synthetic",
        ),
    }
    command = [str(scripts / "e2e-gated-its.sh"), "--run", "--", str(maven)]

    def run(*mode, **overrides):
        return subprocess.run(
            [command[0], *mode, "--", str(maven)] if mode else command,
            cwd=tmp_path,
            env=env | overrides,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

    def add(module, name, gate):
        source = tmp_path / module / "src/test/java/p" / f"{name}.java"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(
            f'package p;\n@Tag("gated")\n@EnabledIfEnvironmentVariable(named="{gate}", matches=".+")\n'
            f"class {name} {{}}\n"
        )

    run.add = add
    run.argv = lambda: [
        json.loads(line) for line in (tmp_path / "argv.jsonl").read_text().splitlines()
    ]
    run.root = tmp_path
    run.env = env
    run.command = command
    run.calls = lambda: (tmp_path / "calls").read_text().splitlines()
    run.results = lambda gate=None: json.loads(
        (tmp_path / "target/e2e/evidence" / (gate or "") / "results.json").read_text()
    )
    run.evidence = tmp_path / "target/e2e/evidence"
    return run


# The gates in check-gated-tags.py's order, which is the order --run takes them in, each with
# the phases the synthetic Maven and fixture record for it.
GATE_PHASES = (
    ("CLOUDTASKS_IT_PROJECT", ["compile", "fixture-run", "cloudtasks", "fixture-stop"]),
    ("BQ_IT_PROJECT", ["compile", "bigquery"]),
    ("PUBSUB_IT_PROJECT", ["compile", "pubsub"]),
    ("BIGTABLE_IT_PROJECT", ["compile", "bigtable"]),
    ("SPANNER_IT_PROJECT", ["compile", "spanner"]),
    ("FIRESTORE_IT_PROJECT", ["compile", "firestore"]),
)
EVERY_PHASE = ["install"] + [phase for _, phases in GATE_PHASES for phase in phases]


@pytest.mark.parametrize(
    "failed",
    ["", "cloudtasks", "bigquery", "pubsub", "bigtable", "spanner", "firestore"],
)
def test_independent_connectors_run_even_after_a_test_failure(suite, failed):
    result = suite(FAIL=failed)
    assert result.returncode == (1 if failed else 0), result.stderr
    assert suite.calls() == EVERY_PHASE
    results = suite.results()
    assert len(results) == 6
    assert sum(row["status"] == "FAILED" for row in results) == bool(failed)


def test_an_unconfirmed_idle_fixture_fails_the_run_but_holds_back_no_gate(suite):
    # Each billed fixture has a lifecycle of its own, so the App Engine stop no longer gates the
    # other connectors, which run after it; it fails the Cloud Tasks gate, and with it the run.
    result = suite(STOP_STATUS="8")
    assert result.returncode == 1, result.stderr
    assert suite.calls() == EVERY_PHASE
    assert all(row["status"] == "PASSED" for row in suite.results())


def test_a_failed_shared_install_stops_the_run_and_reports_every_class(suite):
    result = suite(FAIL="install")
    assert result.returncode == 9
    assert suite.calls() == ["install"]
    assert all(row["status"] == "NOT_RUN" for row in suite.results())


def test_a_failed_compile_fails_its_gate_before_any_fixture_starts(suite):
    result = suite(FAIL="compile")
    assert result.returncode == 1, result.stderr
    assert all(row["status"] == "NOT_RUN" for row in suite.results())
    assert "fixture-run" not in suite.calls()


def test_a_failed_compile_fails_only_its_own_gate(suite):
    result = suite(FAIL="compile:flink-connector-gcp-pubsub")

    assert result.returncode == 1, result.stderr
    assert "pubsub" not in suite.calls()
    assert {row["class"]: row["status"] for row in suite.results()} == {
        f"p.{module.title()}ITCase": "NOT_RUN" if module == "pubsub" else "PASSED"
        for module in MODULES
    }


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
        # Pub/Sub and every gate after it.
        assert sum(row["status"] == "NOT_RUN" for row in suite.results()) == 4
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


@pytest.mark.parametrize(("gate", "phases"), GATE_PHASES)
def test_a_gate_runs_alone_and_leaves_only_its_own_evidence(suite, gate, phases):
    result = suite("--run-gate", gate)

    assert result.returncode == 0, result.stderr
    assert suite.calls() == ["install", *phases]
    [row] = suite.results(gate)
    assert row["status"] == "PASSED"
    assert [path.name for path in suite.evidence.iterdir()] == [gate]


def test_a_gate_demands_only_its_own_variables(suite):
    without_bigtable = {"BIGTABLE_IT_PROJECT": ""}
    result = suite("--run-gate", "PUBSUB_IT_PROJECT", **without_bigtable)
    assert result.returncode == 0, result.stderr
    (suite.root / "calls").unlink()

    result = suite("--run-gate", "BQ_IT_PROJECT", BQ_IT_GCS_BUCKET="")

    assert result.returncode == 1, result.stderr
    assert "BQ_IT_GCS_BUCKET is not set" in result.stderr
    assert suite.results("BQ_IT_PROJECT")[0]["status"] == "NOT_RUN"
    # Refused before any build minutes are spent.
    assert not (suite.root / "calls").exists()


def test_a_gate_failure_is_its_own(suite):
    result = suite("--run-gate", "SPANNER_IT_PROJECT", FAIL="spanner")

    assert result.returncode == 1, result.stderr
    assert suite.results("SPANNER_IT_PROJECT")[0]["status"] == "FAILED"


def test_an_unknown_gate_is_a_usage_error(suite):
    result = suite("--run-gate", "NOPE_IT_PROJECT")

    assert result.returncode == 2
    assert not (suite.root / "calls").exists()


def run_every_gate_apart(suite):
    """What e2e.yaml's matrix leaves behind: each gate's evidence, from a run of its own."""
    for gate, _ in GATE_PHASES:
        result = suite("--run-gate", gate)
        assert result.returncode == 0, f"{gate}: {result.stderr}"


def aggregate(suite):
    return subprocess.run(
        [suite.command[0], "--aggregate"],
        cwd=suite.root,
        env=suite.env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def test_aggregation_accepts_every_gate_passing_apart(suite):
    run_every_gate_apart(suite)

    result = aggregate(suite)

    assert result.returncode == 0, result.stderr
    assert [row["status"] for row in suite.results()] == ["PASSED"] * 6
    assert "6 of 6 gated classes passed across 6 gates" in result.stdout


def test_aggregation_fails_when_a_gate_left_no_evidence(suite):
    # A cancelled or skipped matrix job uploads nothing; its classes must not simply vanish.
    # --run leaves the same per-gate evidence a job per gate does.
    assert suite().returncode == 0
    for path in (suite.evidence / "SPANNER_IT_PROJECT").iterdir():
        path.unlink()
    (suite.evidence / "SPANNER_IT_PROJECT").rmdir()

    result = aggregate(suite)

    assert result.returncode == 1, result.stderr
    assert "SPANNER_IT_PROJECT: no evidence" in result.stderr
    assert "p.SpannerITCase: NOT_RUN" in result.stderr
    assert (
        "SPANNER_IT_PROJECT: no evidence" in (suite.evidence / "summary.md").read_text()
    )


def test_aggregation_fails_on_a_gate_that_prepared_but_never_ran(suite):
    # A job that dies after preparing still uploads its initial NOT_RUN inventory.
    # --run leaves the same per-gate evidence a job per gate does.
    assert suite().returncode == 0
    assert suite("--run-gate", "PUBSUB_IT_PROJECT", FAIL="install").returncode == 9

    result = aggregate(suite)

    assert result.returncode == 1, result.stderr
    assert "p.PubsubITCase: NOT_RUN" in result.stderr


def test_aggregation_fails_when_a_class_joined_after_its_gate_ran(suite):
    # --run leaves the same per-gate evidence a job per gate does.
    assert suite().returncode == 0
    suite.add("flink-connector-gcp-bigtable", "LateITCase", "BIGTABLE_IT_PROJECT")

    result = aggregate(suite)

    assert result.returncode == 1, result.stderr
    assert "p.LateITCase: NOT_RUN" in result.stderr


def test_aggregation_fails_on_a_gate_that_ran_and_failed(suite):
    # The common case: the gate's job uploaded evidence, and it shows a failure.
    assert suite(FAIL="spanner").returncode == 1

    result = aggregate(suite)

    assert result.returncode == 1, result.stderr
    assert "p.SpannerITCase: FAILED" in result.stderr
    assert "5 of 6 gated classes passed" in result.stdout


def surefire_calls(argv):
    """Every module's test invocation as a (module, -Dtest) pair, repeats kept."""
    return sorted(
        (call[call.index("-pl") + 1], next(a for a in call if a.startswith("-Dtest=")))
        for call in argv
        if "surefire:test@integration-tests" in call
    )


def test_every_module_holding_gated_classes_is_compiled_before_it_runs(suite):
    # Each gate compiles the modules it derives before running them; a module missing from that
    # derivation has its gated classes run uncompiled, which no other test here notices,
    # because the synthetic Maven writes the reports either way.
    suite.add("flink-connector-gcp-extra", "ExtraITCase", "PUBSUB_IT_PROJECT")

    assert suite().returncode == 0
    argv = suite.argv()
    for module in [f"flink-connector-gcp-{name}" for name in MODULES] + [
        "flink-connector-gcp-extra"
    ]:
        compiled = next(
            (
                i
                for i, call in enumerate(argv)
                if "test-compile" in call
                and "surefire:test@integration-tests" not in call
                and module in option(call, "-pl").split(",")
            ),
            None,
        )
        ran = next(
            (
                i
                for i, call in enumerate(argv)
                if "surefire:test@integration-tests" in call
                and option(call, "-pl") == module
            ),
            None,
        )
        assert compiled is not None, f"{module} was never compiled"
        assert ran is not None, f"{module} never ran"
        assert compiled < ran, f"{module} ran before it was compiled"


def option(call, name):
    return call[call.index(name) + 1]


def test_each_module_receives_only_its_own_classes(suite):
    suite.add("kubernetes/apps/bigquery", "AppITCase", "BQ_IT_PROJECT")

    result = suite()

    assert result.returncode == 0, result.stderr
    assert surefire_calls(suite.argv()) == [
        ("flink-connector-gcp-bigquery", "-Dtest=BigqueryITCase"),
        ("flink-connector-gcp-bigtable", "-Dtest=BigtableITCase"),
        ("flink-connector-gcp-cloudtasks", "-Dtest=CloudtasksITCase"),
        ("flink-connector-gcp-firestore", "-Dtest=FirestoreITCase"),
        ("flink-connector-gcp-pubsub", "-Dtest=PubsubITCase"),
        ("flink-connector-gcp-spanner", "-Dtest=SpannerITCase"),
        ("kubernetes/apps/bigquery", "-Dtest=AppITCase"),
    ]


def test_an_application_runs_behind_its_profile_after_its_connector_installs(suite):
    suite.add("kubernetes/apps/bigquery", "AppITCase", "BQ_IT_PROJECT")

    assert suite().returncode == 0
    argv = suite.argv()

    [app] = [i for i, call in enumerate(argv) if "kubernetes/apps/bigquery" in call]
    assert option(argv[app], "-P") == "tier3-bigquery"
    assert "test-compile" in argv[app]
    # The application resolves its connector from the local repository, so this tree's
    # connector must be installed after the shared build and before the application runs.
    compiled = next(i for i, call in enumerate(argv) if "test-compile" in call)
    assert any(
        "install" in call and option(call, "-pl") == "flink-connector-gcp-bigquery"
        for call in argv[compiled + 1 : app]
    )


def test_a_cloud_tasks_class_outside_its_fixture_module_is_refused(suite):
    suite.add("flink-connector-gcp-pubsub", "StrayITCase", "CLOUDTASKS_IT_PROJECT")

    result = suite()

    assert result.returncode == 1, result.stderr
    assert "CLOUDTASKS_IT_PROJECT gates classes outside" in result.stderr
    assert "fixture-run" not in suite.calls()
    ran = {module for module, _ in surefire_calls(suite.argv())}
    assert "flink-connector-gcp-cloudtasks" not in ran
    # The refusal fails its own gate; the gates after it still run.
    assert "flink-connector-gcp-firestore" in ran
