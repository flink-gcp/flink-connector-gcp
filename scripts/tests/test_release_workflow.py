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
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from conftest import load_script

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
        GITHUB_REF_NAME="v" + version if event == "push" else "main",
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
        GITHUB_REF_NAME="v" + version if event == "push" else "main",
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
        # Only a rehearsal dispatch skips staging; a push has no inputs.
        assert steps[name]["if"] == "inputs.mode != 'rehearse'"
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


FIRST = "cbdbc784-e398-4996-8082-b9cbff37460c"
SECOND = "bb77b450-7129-495d-8ad2-fb09fc1bfae2"


@pytest.fixture
def workflow():
    return yaml.safe_load((ROOT / ".github/workflows/release.yaml").read_text())


def named_steps(job):
    return {step["name"]: step for step in job["steps"] if "name" in step}


def test_publication_follows_successful_staging_in_its_own_jobs(workflow):
    jobs = workflow["jobs"]
    assert list(jobs) == ["stage", "publish-central", "github-release"]
    publish, release = jobs["publish-central"], jobs["github-release"]
    # A bare event guard keeps Actions' implicit success() over every need, so
    # neither job starts unless staging, and then Publish, succeeded.
    assert publish["if"] == release["if"] == "github.event_name == 'push'"
    assert publish["needs"] == "stage"
    assert release["needs"] == ["stage", "publish-central"]
    for name, job in jobs.items():
        for step in job["steps"]:
            assert "if" not in step or name == "stage", (name, step)
    staging = str(jobs["stage"]["steps"])
    for later in ("central-portal publish", "release-assets", "action-gh-release"):
        assert later not in staging, later


@pytest.mark.parametrize("ids", [(FIRST, SECOND), (FIRST.upper(), SECOND)])
def test_ids_reach_the_publication_only_as_two_validated_uuids(steps, tmp_path, ids):
    hand = steps["Hand both deployment IDs to the publication"]
    assert hand["if"] == "github.event_name == 'push'"
    names = list(steps)
    assert (
        names.index("Collect the 1.20 uber-jars")
        < names.index("Hand both deployment IDs to the publication")
        < names.index("Drop dry-run deployments")
    )
    (tmp_path / "central-2x.id").write_text(ids[0] + "\n")
    (tmp_path / "central-1x.id").write_text(ids[1] + "\n")
    output = tmp_path / "output"
    result = run(hand, tmp_path, RUNNER_TEMP=str(tmp_path), GITHUB_OUTPUT=str(output))
    if ids[0] == FIRST:
        assert result.returncode == 0, result.stderr
        assert output.read_text() == (
            f"deployment-2x={FIRST}\ndeployment-1x={SECOND}\n"
        )
    else:
        # capture writes lower case; anything else did not come from it.
        assert result.returncode != 0
        assert not output.exists()


@pytest.mark.parametrize(
    "first,second",
    [
        (FIRST + "\n" + SECOND + "\n", SECOND + "\n"),
        ("", SECOND + "\n"),
        (FIRST + "\n", None),
        (FIRST + "\n", "x=1\n"),
        (FIRST + "\nother=value\n", SECOND + "\n"),
    ],
)
def test_malformed_ids_write_no_output(steps, tmp_path, first, second):
    (tmp_path / "central-2x.id").write_text(first)
    if second is not None:
        (tmp_path / "central-1x.id").write_text(second)
    output = tmp_path / "output"
    result = run(
        steps["Hand both deployment IDs to the publication"],
        tmp_path,
        RUNNER_TEMP=str(tmp_path),
        GITHUB_OUTPUT=str(output),
    )
    assert result.returncode != 0
    assert not output.exists()


def test_job_outputs_carry_the_ids_and_the_version(workflow):
    stage = workflow["jobs"]["stage"]
    assert stage["outputs"] == {
        "version": "${{ steps.version.outputs.version }}",
        "deployment-2x": "${{ steps.ids.outputs.deployment-2x }}",
        "deployment-1x": "${{ steps.ids.outputs.deployment-1x }}",
    }
    assert (
        named_steps(stage)["Hand both deployment IDs to the publication"]["id"] == "ids"
    )
    assert named_steps(stage)["Resolve the release version"]["id"] == "version"
    publish = named_steps(workflow["jobs"]["publish-central"])
    step = publish["Publish or wait for both Central deployments"]
    assert step["env"]["DEPLOYMENT_2X"] == "${{ needs.stage.outputs.deployment-2x }}"
    assert step["env"]["DEPLOYMENT_1X"] == "${{ needs.stage.outputs.deployment-1x }}"
    release = named_steps(workflow["jobs"]["github-release"])
    for step in release.values():
        assert step["env"]["VERSION"] == "${{ needs.stage.outputs.version }}"


def test_publish_reads_both_ids_in_line_order(workflow, tmp_path, fake_just):
    step = named_steps(workflow["jobs"]["publish-central"])[
        "Publish or wait for both Central deployments"
    ]
    result = run(step, tmp_path, **fake_just, DEPLOYMENT_2X=FIRST, DEPLOYMENT_1X=SECOND)
    assert result.returncode == 0, result.stderr
    assert json.loads((tmp_path / "calls").read_text()) == [
        "central-portal",
        "publish",
        FIRST,
        SECOND,
    ]
    failed = run(
        step,
        tmp_path,
        **fake_just,
        JUST_EXIT="1",
        DEPLOYMENT_2X=FIRST,
        DEPLOYMENT_1X=SECOND,
    )
    assert failed.returncode == 1


# Each publication job's whole program. A new step must be added here
# deliberately, after checking it stages, signs and uploads nothing to Central.
PUBLICATION_JOBS = {
    "publish-central": (
        ["actions/checkout", "taiki-e/install-action", "run"],
        ["Publish or wait for both Central deployments"],
        {"contents": "read"},
    ),
    "github-release": (
        ["actions/checkout", "run", "run"],
        [
            "Download and verify the published uber-jars",
            "Create or complete the GitHub Release",
        ],
        {"contents": "write"},
    ),
}


@pytest.mark.parametrize("name", PUBLICATION_JOBS)
def test_publication_jobs_reach_no_staging_signing_or_upload(workflow, name):
    job = workflow["jobs"][name]
    kinds, names, permissions = PUBLICATION_JOBS[name]
    # Every step, named or not: an unnamed run step counts as much as a named one.
    assert [
        step["uses"].split("@")[0] if "uses" in step else "run" for step in job["steps"]
    ] == kinds
    assert [step["name"] for step in job["steps"] if "run" in step] == names
    assert job["permissions"] == permissions
    assert "env" not in job
    for step in job["steps"]:
        text = str(step)
        for forbidden in (
            "stage-release",
            "rehearse-release",
            "capture",
            "collect-release-jars",
            "write-central-settings",
            "drop",
            "mvn",
            "MAVEN_GPG",
            "setup-java",
            "mise-action",
        ):
            assert forbidden not in text, (name, forbidden)
        # Maven's deploy goal; the deployment-* outputs are not it.
        assert not re.search(r"\bdeploy\b", text), name
        if "uses" in step and step["uses"].startswith("actions/checkout@"):
            assert step["with"]["persist-credentials"] is False


def test_only_the_publication_job_holds_the_central_token(workflow):
    # No job holds both a release secret and a write scope (#1980).
    for name, job in workflow["jobs"].items():
        writes = "write" in job["permissions"].values()
        assert not (writes and reads_secrets(job)), name
    release = workflow["jobs"]["github-release"]
    assert "environment" not in release
    assert not reads_secrets(release)
    publish = workflow["jobs"]["publish-central"]
    assert [step["name"] for step in publish["steps"] if reads_secrets(step)] == [
        "Publish or wait for both Central deployments"
    ]


def test_the_release_is_built_from_central_with_the_tag_checked_out(workflow):
    release = named_steps(workflow["jobs"]["github-release"])
    assert release["Download and verify the published uber-jars"]["run"] == (
        'python3 scripts/release-assets.py download "$VERSION" "$RUNNER_TEMP/dist"'
    )
    complete = release["Create or complete the GitHub Release"]
    assert complete["run"] == (
        'python3 scripts/release-assets.py complete "$VERSION" "$RUNNER_TEMP/dist"'
    )
    assert complete["env"]["GH_TOKEN"] == "${{ github.token }}"
    checkout = workflow["jobs"]["github-release"]["steps"][0]
    assert checkout["with"]["ref"] == "${{ github.sha }}"


@pytest.fixture
def fake_gh(tmp_path):
    # Answers each attempt's jobs query from GH_ANSWERS, keyed by attempt number.
    script = tmp_path / "gh"
    script.write_text(
        f"#!{sys.executable}\n"
        "import json, os, re, sys\n"
        "with open(os.environ['GH_CALLS'], 'a') as f: f.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "if os.environ.get('GH_EXIT', '0') != '0': sys.exit(int(os.environ['GH_EXIT']))\n"
        "attempt = re.search(r'/attempts/([0-9]+)/jobs$', sys.argv[3])[1]\n"
        "sys.stdout.write(json.loads(os.environ['GH_ANSWERS']).get(attempt, ''))\n"
    )
    script.chmod(0o755)
    return {
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "GH_CALLS": str(tmp_path / "gh-calls"),
    }


def guard_step(steps):
    return steps["Refuse to restage a staged release"]


def run_guard(steps, tmp_path, fake_gh, attempt, answers, gh_exit="0"):
    return run(
        guard_step(steps),
        tmp_path,
        **fake_gh,
        GITHUB_RUN_ATTEMPT=attempt,
        GITHUB_RUN_ID="41269916162",
        GITHUB_REPOSITORY="flink-gcp/flink-connector-gcp",
        STAGE_JOB=guard_step(steps)["env"]["STAGE_JOB"],
        GH_ANSWERS=json.dumps(answers),
        GH_EXIT=gh_exit,
    )


def guard_calls(tmp_path):
    return [
        json.loads(line) for line in (tmp_path / "gh-calls").read_text().splitlines()
    ]


def test_rerun_guard_matches_this_job_and_runs_first(workflow, steps):
    guard = guard_step(steps)
    # First of all steps, named or not: before the checkout and anything signed.
    assert workflow["jobs"]["stage"]["steps"][0] == guard
    assert guard["if"] == "github.event_name == 'push' && github.run_attempt != '1'"
    # A renamed job would make the guard match nothing and pass silently.
    assert guard["env"]["STAGE_JOB"] == workflow["jobs"]["stage"]["name"]
    assert workflow["jobs"]["stage"]["permissions"] == {
        "contents": "read",
        "actions": "read",
    }


@pytest.mark.parametrize(
    "attempt,answers,staged",
    [
        # Re-run all jobs straight after a successful staging.
        ("2", {"1": "success\n"}, "1"),
        # Re-run failed jobs after that re-run was refused: attempt 2's failure
        # must not hide attempt 1's success.
        ("3", {"1": "success\n", "2": "failure\n"}, "1"),
        # Re-run all jobs after a Re-run failed jobs attempt, whose job list
        # carries the reused staging job as a success.
        ("3", {"1": "success\n", "2": "success\n"}, "1"),
        ("4", {"1": "failure\n", "2": "success\n", "3": "failure\n"}, "2"),
    ],
)
def test_rerun_guard_refuses_after_any_successful_staging(
    steps, tmp_path, fake_gh, attempt, answers, staged
):
    result = run_guard(steps, tmp_path, fake_gh, attempt, answers)
    assert result.returncode == 1
    assert f"attempt {staged} already staged this release" in result.stderr
    assert "by hand" in result.stderr
    assert guard_calls(tmp_path)[0] == [
        "api",
        "--paginate",
        "repos/flink-gcp/flink-connector-gcp/actions/runs/41269916162/attempts/1/jobs",
        "--jq",
        ".jobs[] | select(.name == env.STAGE_JOB) | .conclusion",
    ]


@pytest.mark.parametrize(
    "attempt,answers",
    [
        ("2", {"1": "failure\n"}),
        ("2", {"1": "cancelled\n"}),
        ("2", {}),
        ("3", {"1": "failure\n", "2": "cancelled\n"}),
    ],
)
def test_rerun_guard_lets_a_release_never_staged_restage(
    steps, tmp_path, fake_gh, attempt, answers
):
    result = run_guard(steps, tmp_path, fake_gh, attempt, answers)
    assert result.returncode == 0, result.stderr
    queried = [call[2].rsplit("/", 2)[-2] for call in guard_calls(tmp_path)]
    assert queried == [str(n) for n in range(1, int(attempt))]


def test_rerun_guard_fails_closed_when_github_does_not_answer(steps, tmp_path, fake_gh):
    result = run_guard(steps, tmp_path, fake_gh, "2", {}, gh_exit="1")
    assert result.returncode != 0


def test_job_limits_cover_their_waits(workflow):
    # A job stopped by timeout-minutes cannot report a timed-out wait, so each
    # limit holds the waits its job runs, inside the hosted runners' 360.
    portal = load_script("central-portal.py")
    assets = load_script("release-assets.py")
    jobs = workflow["jobs"]
    validation = 30  # central-publishing-maven-plugin's waitMaxTime default
    assert portal.WAIT_TIMEOUT // 60 == 180
    assert 2 * validation + 15 < jobs["stage"]["timeout-minutes"] <= 360
    assert portal.WAIT_TIMEOUT // 60 < jobs["publish-central"]["timeout-minutes"] <= 360
    assert assets.FETCH_WAIT // 60 < jobs["github-release"]["timeout-minutes"] <= 360


def test_docs_follow_a_successful_tag_run(workflow):
    # A successful re-run attempt completes the run with event push, which is
    # what docs.yaml publishes on; a rename here would unhook it silently.
    docs = yaml.safe_load((ROOT / ".github/workflows/docs.yaml").read_text())
    assert workflow["name"] in docs[True]["workflow_run"]["workflows"]
    guard = (
        "github.event.workflow_run.conclusion == 'success' && "
        "github.event.workflow_run.event == 'push'"
    )
    assert guard in docs["jobs"]["plan"]["if"]
    assert guard in docs["concurrency"]["group"]


@pytest.mark.parametrize("captured", [0, 1, 2])
def test_dry_cleanup_handles_missing_and_partial_capture(
    steps, tmp_path, fake_just, captured
):
    cleanup = steps["Drop dry-run deployments"]
    assert cleanup["if"] == (
        "always() && github.event_name == 'workflow_dispatch' && inputs.mode != 'rehearse'"
    )
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


# Every step a rehearsal skips: staging, collection, and anything that reads a
# Central token or writes Central settings.
NOT_IN_REHEARSAL = (
    "Resolve the LTS patch from the weekly matrix",
    "Point Maven at the Central Portal credentials",
    "Stage the Flink 2.x line",
    "Collect the staged uber-jars",
    "Stage the Flink 1.20 LTS line",
    "Collect the 1.20 uber-jars",
)


def test_rehearsal_skips_staging_and_holds_no_central_credentials(
    steps, tmp_path, fake_just
):
    for name in NOT_IN_REHEARSAL:
        assert steps[name]["if"] == "inputs.mode != 'rehearse'", name
    workflow = yaml.safe_load((ROOT / ".github/workflows/release.yaml").read_text())
    job = workflow["jobs"]["stage"]
    assert "CENTRAL_TOKEN" not in str(workflow.get("env", {})) + str(job.get("env", {}))
    for name, step in steps.items():
        if "CENTRAL_TOKEN" in str(step.get("env", {})):
            assert name in NOT_IN_REHEARSAL or step["if"] in (
                "github.event_name == 'push'",
                (
                    "always() && github.event_name == 'workflow_dispatch'"
                    " && inputs.mode != 'rehearse'"
                ),
            ), name
    rehearse = steps["Rehearse both lines without uploading"]
    assert rehearse["if"] == "inputs.mode == 'rehearse'"
    assert set(rehearse["env"]) == {"VERSION", "MAVEN_GPG_KEY", "MAVEN_GPG_PASSPHRASE"}
    result = run(rehearse, tmp_path, **fake_just, VERSION="1.3.0")
    assert result.returncode == 0, result.stderr
    assert json.loads((tmp_path / "calls").read_text()) == ["rehearse-release", "1.3.0"]


def reads_secrets(node):
    # Only expressions count: a run script that merely says "secrets" reads none.
    expressions = re.findall(r"\$\{\{(.*?)\}\}", str(node), flags=re.DOTALL)
    return any(re.search(r"\bsecrets\b", e, flags=re.IGNORECASE) for e in expressions)


def test_every_secret_reader_names_the_release_environment():
    # The release secrets live in the release environment, whose ref policy
    # admits only main and v* tags (#1971). A job without that environment
    # reads the repository's secret store, which the ref policy does not guard.
    workflow = yaml.safe_load((ROOT / ".github/workflows/release.yaml").read_text())
    assert not reads_secrets(workflow.get("env", {})), (
        "workflow-level env reads a secret"
    )
    jobs = workflow["jobs"]
    # A calling job cannot name an environment, so inherit hands the called
    # workflow the repository's secrets without passing through the gate.
    assert [n for n, job in jobs.items() if job.get("secrets") == "inherit"] == []
    readers = {name: job for name, job in jobs.items() if reads_secrets(job)}
    assert readers, "no job reads a secret; the scan no longer sees the release job"
    for name, job in readers.items():
        environment = job.get("environment")
        if isinstance(environment, dict):
            environment = environment.get("name")
        assert environment == "release", (
            f"{name} reads secrets outside the release environment"
        )


def test_dispatch_rehearses_by_default():
    workflow = yaml.safe_load((ROOT / ".github/workflows/release.yaml").read_text())
    # PyYAML reads the bare `on` key as True.
    mode = workflow[True]["workflow_dispatch"]["inputs"]["mode"]
    assert mode["default"] == "rehearse"
    assert mode["options"] == ["rehearse", "validate"]
