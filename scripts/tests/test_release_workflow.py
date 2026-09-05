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
"""Run the release workflow's shell programs with synthetic inputs and commands."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def steps():
    # The workflow is the program under test, rather than a checker's input tree.
    workflow = yaml.safe_load((ROOT / ".github/workflows/release.yaml").read_text())
    return {
        step["name"]: step
        for step in workflow["jobs"]["stage"]["steps"]
        if "name" in step
    }


def run(step, tmp_path, **env):
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", step["run"]],
        env={**os.environ, **env},
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize("event", ["push", "workflow_dispatch"])
@pytest.mark.parametrize("version", ["1.1.0", "0.0.0", "10.20.30"])
def test_version_resolution(steps, tmp_path, event, version):
    output = tmp_path / "output"
    result = run(
        steps["Resolve the release version"],
        tmp_path,
        GITHUB_OUTPUT=str(output),
        GITHUB_EVENT_NAME=event,
        GITHUB_REF_NAME="v" + version,
        INPUT_VERSION=version if event == "workflow_dispatch" else "",
    )
    assert result.returncode == 0, result.stderr
    assert output.read_text() == f"version={version}\n"


@pytest.mark.parametrize(
    "version", ["v1.1.0", "1.1.0-1.20", "01.1.0", "1.1.0\nother=value", ""]
)
@pytest.mark.parametrize("event", ["push", "workflow_dispatch"])
def test_invalid_version_cannot_inject_outputs(steps, tmp_path, version, event):
    output = tmp_path / "output"
    result = run(
        steps["Resolve the release version"],
        tmp_path,
        GITHUB_OUTPUT=str(output),
        GITHUB_EVENT_NAME=event,
        GITHUB_REF_NAME="v" + version,
        INPUT_VERSION=version if event == "workflow_dispatch" else "",
    )
    assert result.returncode != 0
    assert "expected bare X.Y.Z" in result.stderr
    assert not output.exists()


@pytest.fixture
def fake_just(tmp_path):
    script = tmp_path / "just"
    script.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "with open(os.environ['CALLS'], 'a') as f: f.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "sys.exit(int(os.environ.get('JUST_EXIT', '0')))\n"
    )
    script.chmod(0o755)
    return {
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "CALLS": str(tmp_path / "calls"),
    }


@pytest.mark.parametrize("event", ["push", "workflow_dispatch"])
def test_both_builds_use_capture_and_correct_version_line(
    steps, tmp_path, fake_just, event
):
    for name in ("Stage the Flink 2.x line", "Stage the Flink 1.20 LTS line"):
        assert "if" not in steps[name], "both events must stage both lines"
        result = run(
            steps[name],
            tmp_path,
            **fake_just,
            VERSION="1.1.0",
            FLINK_LTS="1.20.4",
            GITHUB_EVENT_NAME=event,
            GITHUB_RUN_ID="12",
            GITHUB_RUN_ATTEMPT="2",
            RUNNER_TEMP=str(tmp_path),
        )
        assert result.returncode == 0, result.stderr
    calls = [json.loads(line) for line in (tmp_path / "calls").read_text().splitlines()]
    for call, suffix, filename in zip(
        calls, ("", "-1.20"), ("central-2x.id", "central-1x.id")
    ):
        assert call[:6] == [
            "central-portal",
            "capture",
            str(tmp_path / filename),
            "just",
            "stage-release",
            "1.1.0" + suffix,
        ]
        assert ("-Dflink.version=1.20.4" in call) == bool(suffix)
        assert ("dry run" in call[-1]) == (event == "workflow_dispatch")
        assert "run 12 attempt 2" in call[-1]


def test_staging_failure_reaches_workflow(steps, tmp_path, fake_just):
    result = run(
        steps["Stage the Flink 1.20 LTS line"],
        tmp_path,
        **fake_just,
        JUST_EXIT="7",
        VERSION="1.1.0",
        FLINK_LTS="1.20.4",
        GITHUB_EVENT_NAME="push",
        GITHUB_RUN_ID="12",
        GITHUB_RUN_ATTEMPT="2",
        RUNNER_TEMP=str(tmp_path),
    )
    assert result.returncode == 7


def test_publish_requires_successful_prior_steps_and_reads_both_ids(
    steps, tmp_path, fake_just
):
    publish = steps["Publish both Central deployments"]
    release = steps["Publish the GitHub Release"]
    # An if without a status function retains Actions' implicit success() gate.
    assert publish["if"] == release["if"] == "github.event_name == 'push'"
    names = list(steps)
    assert (
        names.index("Collect the 1.20 uber-jars")
        < names.index("Publish both Central deployments")
        < names.index("Publish the GitHub Release")
    )
    assert release["with"]["draft"] is False
    (tmp_path / "central-2x.id").write_text("first\n")
    (tmp_path / "central-1x.id").write_text("second\n")
    result = run(publish, tmp_path, **fake_just, RUNNER_TEMP=str(tmp_path))
    assert result.returncode == 0, result.stderr
    assert json.loads((tmp_path / "calls").read_text()) == [
        "central-portal",
        "publish",
        "first",
        "second",
    ]


@pytest.mark.parametrize("captured", [0, 1, 2])
def test_dry_cleanup_handles_missing_and_partial_capture(
    steps, tmp_path, fake_just, captured
):
    cleanup = steps["Drop dry-run deployments"]
    assert cleanup["if"] == "always() && github.event_name == 'workflow_dispatch'"
    for filename, value in list(
        zip(("central-2x.id", "central-1x.id"), ("first", "second"))
    )[:captured]:
        (tmp_path / filename).write_text(value + "\n")
    result = run(cleanup, tmp_path, **fake_just, RUNNER_TEMP=str(tmp_path))
    assert result.returncode == 0, result.stderr
    if captured:
        assert json.loads((tmp_path / "calls").read_text()) == [
            "central-portal",
            "drop",
            *["first", "second"][:captured],
        ]
    else:
        assert not (tmp_path / "calls").exists()


def test_dry_cleanup_failure_is_not_green(steps, tmp_path, fake_just):
    (tmp_path / "central-2x.id").write_text("first\n")
    result = run(
        steps["Drop dry-run deployments"],
        tmp_path,
        **fake_just,
        RUNNER_TEMP=str(tmp_path),
        JUST_EXIT="3",
    )
    assert result.returncode == 3
