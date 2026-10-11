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
"""The offline analyzer's discovery, reports and command, without a checkout."""

import json
import subprocess
import sys
from array import array
from fractions import Fraction
from pathlib import Path

import pytest
from flink_tier3 import analyze, cli, runs
from flink_tier3.common import Failure

SRC_DIR = Path(analyze.__file__).resolve().parent


def write_run(root, run_id="smoke-1", scenario="smoke", events=()):
    """A downloaded run directory holding only what every scenario exports."""
    run_dir = Path(root) / "runs" / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "approval.json").write_text(
        json.dumps({"run_id": run_id, "scenario": scenario})
    )
    (run_dir / "result.json").write_text(
        json.dumps({"success": True, "idle": True, "plans": {}})
    )
    supervisor = run_dir / "supervisor"
    supervisor.mkdir()
    for index, event in enumerate(events):
        (supervisor / f"{index:05d}.json").write_text(json.dumps(event))
    return run_dir


def test_a_run_directory_or_a_directory_of_them_is_discovered(tmp_path):
    second = write_run(tmp_path, "smoke-2")
    first = write_run(tmp_path, "smoke-1")
    assert [run.run_id for run in runs.discover_runs(first)] == ["smoke-1"]
    assert [run.run_id for run in runs.discover_runs(tmp_path / "runs")] == [
        "smoke-1",
        "smoke-2",
    ]
    assert runs.discover_runs(second)[0].result["success"] is True
    with pytest.raises(Failure, match="No run directory"):
        runs.discover_runs(tmp_path)
    with pytest.raises(Failure, match="does not exist"):
        runs.discover_runs(tmp_path / "nowhere")


def test_events_carry_any_payload_and_need_a_named_event(tmp_path):
    """The supervisor records list payloads, and payload-less events, too."""
    run_dir = write_run(
        tmp_path,
        events=[
            {"event": "inventory", "at": "2026-09-19T00:00:02Z", "payload": [{}]},
            {"event": "stop-requested", "at": "2026-09-19T00:00:01Z"},
            {"event": "idle", "at": "2026-09-19T00:00:03Z", "payload": {"k": 1}},
        ],
    )
    [run] = runs.discover_runs(run_dir)
    assert [entry["event"] for entry in run.events] == [
        "stop-requested",
        "inventory",
        "idle",
    ]
    # Only a dict payload is offered to an analysis that reads one.
    assert [entry["payload"] for entry in run.events_of("idle")] == [{"k": 1}]
    assert run.events_of("inventory") == []
    (run_dir / "supervisor" / "99999.json").write_text(json.dumps({"payload": {}}))
    with pytest.raises(Failure, match="Malformed evidence event"):
        runs.discover_runs(run_dir)


def test_reports_are_byte_identical(tmp_path):
    run_dir = write_run(tmp_path)
    first = analyze.write_reports(analyze.analyze(run_dir), tmp_path / "one")
    second = analyze.write_reports(analyze.analyze(run_dir), tmp_path / "two")
    for name in ("report.json", "report.md"):
        assert (first / name).read_bytes() == (second / name).read_bytes()
    report = json.loads((first / "report.json").read_text())
    assert list(report) == sorted(report)
    assert report["runs"] == [
        {
            "run_id": "smoke-1",
            "scenario": "smoke",
            "events": 0,
            "result": {"success": True, "idle": True},
        }
    ]
    assert report["bigquery"] == [] and report["pubsub"] == []
    assert (first / "report.md").read_text() == (
        "# Tier-3 evidence analysis\n\nThe analyzer read 1 run directories.\n"
    )


def test_command_runs_without_a_checkout_and_prints_one_line(
    tmp_path, monkeypatch, capsys
):
    run_dir = write_run(tmp_path)
    monkeypatch.chdir(tmp_path)
    assert cli.main(["analyze", "--evidence", str(run_dir)]) == 0
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [f"analyzed 1 runs; reports in {run_dir / 'analysis'}"]
    assert (run_dir / "analysis/report.json").is_file()
    assert cli.main(["analyze", "--evidence", str(tmp_path / "nowhere")]) == 1
    assert "analysis failed" in capsys.readouterr().out


def test_help_works_without_a_checkout(tmp_path):
    result = subprocess.run(
        [sys.executable, "-m", "flink_tier3", "analyze", "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )
    assert "usage: flink-tier3 analyze" in result.stdout
    assert "--evidence" in result.stdout


def test_analyze_module_does_not_import_google_cloud_or_kubernetes(tmp_path):
    # The package __init__ eagerly re-exports the cluster clients, so the module
    # is loaded under a bare package shim: what it imports is what it needs.
    code = """
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
source = Path(sys.argv[1])
spec = spec_from_file_location(
    "flink_tier3", source / "__init__.py", submodule_search_locations=[str(source)]
)
sys.modules["flink_tier3"] = module_from_spec(spec)  # never executed
import flink_tier3.analyze
loaded = sorted(
    m for m in sys.modules
    if m == "kubernetes" or m.startswith(("google.cloud", "kubernetes."))
)
print(sys.modules["flink_tier3.runs"].__name__, loaded)
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(SRC_DIR)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "flink_tier3.runs []"


def test_a_fraction_renders_as_its_exact_value_and_a_float():
    rendered = analyze.plain(
        {"ratio": Fraction(38, 9), 1: [Fraction(1, 4)], "rows": array("q", [3, 1])}
    )
    assert rendered == {
        "ratio": {"fraction": "38/9", "float": 38 / 9},
        "1": [{"fraction": "1/4", "float": 0.25}],
        "rows": [3, 1],
    }
