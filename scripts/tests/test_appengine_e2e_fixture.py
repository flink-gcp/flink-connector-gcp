# Copyright 2026 laughingman7743
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
"""Tests for the fixed App Engine E2E fixture lifecycle wrapper."""

import shutil
import subprocess

import pytest
from conftest import SCRIPTS

FIXTURE_HCL = """\
locals {
  cloudtasks_appengine_e2e_service = "default"
  cloudtasks_appengine_e2e_version = "flink-e2e"
}
"""

GCLOUD_STUB = """\
#!/usr/bin/env bash
set -eu

next_observation() {
    kind=$1
    counter="$GCLOUD_STUB_DIR/${kind}-counter"
    index=0
    if [ -f "$counter" ]; then
        index=$(cat "$counter")
    fi
    file="$GCLOUD_STUB_DIR/${kind}-${index}.txt"
    if [ ! -f "$file" ]; then
        echo "missing stub observation: ${file}" >&2
        exit 1
    fi
    cat "$file"
    echo $(( index + 1 )) > "$counter"
}

if [ "$1" = app ] && [ "$2" = versions ]; then
    case "$3" in
        describe)
            [ -z "${GCLOUD_STUB_DESCRIBE_FAIL:-}" ] || exit 1
            next_observation status
            ;;
        start)
            echo "$*" >> "$GCLOUD_STUB_LOG"
            [ -z "${GCLOUD_STUB_START_FAIL:-}" ] || exit 1
            ;;
        stop)
            echo "$*" >> "$GCLOUD_STUB_LOG"
            [ -z "${GCLOUD_STUB_STOP_FAIL:-}" ] || exit 1
            ;;
    esac
    exit 0
fi

if [ "$1" = app ] && [ "$2" = instances ] && [ "$3" = list ]; then
    [ -z "${GCLOUD_STUB_LIST_FAIL:-}" ] || exit 1
    next_observation instances
    exit 0
fi

echo "unexpected gcloud invocation: $*" >&2
exit 1
"""


@pytest.fixture()
def fixture_lifecycle(tmp_path):
    """Run a copied lifecycle script against a sequenced gcloud stub."""
    (tmp_path / "scripts").mkdir()
    script = tmp_path / "scripts" / "appengine-e2e-fixture.sh"
    shutil.copy(SCRIPTS / "appengine-e2e-fixture.sh", script)

    tofu = tmp_path / "opentofu" / "flink-gcp"
    tofu.mkdir(parents=True)
    fixture = tofu / "appengine-e2e.tf"
    fixture.write_text(FIXTURE_HCL)

    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    gcloud = stub_dir / "gcloud"
    gcloud.write_text(GCLOUD_STUB)
    gcloud.chmod(0o755)
    log = tmp_path / "gcloud.log"

    def run(
        command,
        *args,
        statuses=("STOPPED",),
        instance_observations=((),),
        project="flink-gcp",
        attempts=None,
        describe_fails=False,
        list_fails=False,
        start_fails=False,
        stop_fails=False,
        extra_env=None,
    ):
        for old in tmp_path.glob("status-*.txt"):
            old.unlink()
        for old in tmp_path.glob("instances-*.txt"):
            old.unlink()
        for counter in (tmp_path / "status-counter", tmp_path / "instances-counter"):
            counter.unlink(missing_ok=True)
        log.write_text("")

        for index, status in enumerate(statuses):
            (tmp_path / f"status-{index}.txt").write_text(f"{status}\n")
        for index, instances in enumerate(instance_observations):
            (tmp_path / f"instances-{index}.txt").write_text(
                "".join(f"{instance}\n" for instance in instances)
            )

        env = {
            "PATH": f"{stub_dir}:/usr/bin:/bin",
            "GCLOUD_STUB_DIR": str(tmp_path),
            "GCLOUD_STUB_LOG": str(log),
            "APPENGINE_E2E_POLL_ATTEMPTS": str(attempts or len(statuses)),
            "APPENGINE_E2E_POLL_SECONDS": "0",
        }
        if project is not None:
            env["CLOUDTASKS_IT_PROJECT"] = project
        if describe_fails:
            env["GCLOUD_STUB_DESCRIBE_FAIL"] = "1"
        if list_fails:
            env["GCLOUD_STUB_LIST_FAIL"] = "1"
        if start_fails:
            env["GCLOUD_STUB_START_FAIL"] = "1"
        if stop_fails:
            env["GCLOUD_STUB_STOP_FAIL"] = "1"
        if extra_env:
            env.update(extra_env)

        result = subprocess.run(
            [str(script), command, *args],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        result.gcloud = log.read_text().splitlines()
        return result

    run.fixture = fixture
    return run


def test_start_waits_for_serving_with_exactly_one_instance(fixture_lifecycle):
    result = fixture_lifecycle(
        "start",
        statuses=("STOPPED", "SERVING"),
        instance_observations=((), ("instance-1",)),
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "instance-1\n"
    assert result.gcloud == [
        "app versions start flink-e2e --service=default --project=flink-gcp --quiet"
    ]


def test_start_refuses_to_accept_more_than_one_instance(fixture_lifecycle):
    result = fixture_lifecycle(
        "start",
        statuses=("SERVING", "SERVING"),
        instance_observations=(("instance-1", "instance-2"),) * 2,
    )

    assert result.returncode == 1
    assert "observed SERVING with 2" in result.stderr


def test_stop_waits_for_stopped_with_zero_instances(fixture_lifecycle):
    result = fixture_lifecycle(
        "stop",
        statuses=("SERVING", "STOPPED"),
        instance_observations=(("instance-1",), ()),
    )

    assert result.returncode == 0, result.stderr
    assert result.gcloud == [
        "app versions stop flink-e2e --service=default --project=flink-gcp --quiet"
    ]
    assert "stopped with zero instances" in result.stdout


def test_stop_waits_for_instances_to_disappear_after_status_changes(fixture_lifecycle):
    result = fixture_lifecycle(
        "stop",
        statuses=("SERVING", "STOPPED", "STOPPED"),
        instance_observations=(("instance-1",), ("instance-1",), ()),
    )

    assert result.returncode == 0, result.stderr


def test_an_already_stopped_fixture_is_idempotent(fixture_lifecycle):
    result = fixture_lifecycle("stop")

    assert result.returncode == 0, result.stderr
    assert result.gcloud == []
    assert "stopped with zero instances" in result.stdout


def test_dry_run_observes_but_does_not_stop(fixture_lifecycle):
    result = fixture_lifecycle(
        "stop",
        "--dry-run",
        statuses=("SERVING",),
        instance_observations=(("instance-1",),),
    )

    assert result.returncode == 0, result.stderr
    assert result.gcloud == []
    assert "would stop" in result.stdout


@pytest.mark.parametrize(
    ("command", "failure"),
    [("start", "start_fails"), ("stop", "stop_fails")],
)
def test_a_lifecycle_operation_failure_is_reported(fixture_lifecycle, command, failure):
    observations = {}
    if command == "stop":
        observations = {
            "statuses": ("SERVING",),
            "instance_observations": (("instance-1",),),
        }
    result = fixture_lifecycle(command, **observations, **{failure: True})

    assert result.returncode == 1
    assert f"Failed to {command}" in result.stderr


@pytest.mark.parametrize("failure", ["describe_fails", "list_fails"])
def test_an_observation_failure_is_not_reported_as_a_state(fixture_lifecycle, failure):
    result = fixture_lifecycle("stop", **{failure: True})

    assert result.returncode == 2
    assert "stopped with zero instances" not in result.stdout


def test_a_missing_project_is_an_infrastructure_error(fixture_lifecycle):
    result = fixture_lifecycle("stop", project=None)

    assert result.returncode == 2
    assert "CLOUDTASKS_IT_PROJECT is not set" in result.stderr


@pytest.mark.parametrize(
    "missing",
    ["cloudtasks_appengine_e2e_service", "cloudtasks_appengine_e2e_version"],
)
def test_a_fixture_identifier_that_cannot_be_read_is_an_error(
    fixture_lifecycle, missing
):
    fixture_lifecycle.fixture.write_text(
        "\n".join(line for line in FIXTURE_HCL.splitlines() if missing not in line)
    )

    result = fixture_lifecycle("stop")

    assert result.returncode == 2
    assert "single source of truth" in result.stderr


@pytest.mark.parametrize(
    "args",
    [
        ("start", "--dry-run"),
        ("stop", "--force"),
        ("stop", "--dry-run", "extra"),
        ("status",),
    ],
)
def test_arguments_it_does_not_understand_are_rejected(fixture_lifecycle, args):
    result = fixture_lifecycle(*args)

    assert result.returncode == 2
    assert result.gcloud == []


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("APPENGINE_E2E_POLL_ATTEMPTS", "0"),
        ("APPENGINE_E2E_POLL_ATTEMPTS", "abc"),
        ("APPENGINE_E2E_POLL_SECONDS", "-1"),
    ],
)
def test_invalid_poll_configuration_is_rejected(fixture_lifecycle, name, value):
    result = fixture_lifecycle("stop", extra_env={name: value})

    assert result.returncode == 2
    assert result.gcloud == []


def test_the_real_fixture_identifiers_still_parse(tmp_path):
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    gcloud = stub_dir / "gcloud"
    gcloud.write_text(GCLOUD_STUB)
    gcloud.chmod(0o755)
    (tmp_path / "status-0.txt").write_text("STOPPED\n")
    (tmp_path / "instances-0.txt").write_text("")
    log = tmp_path / "gcloud.log"
    log.write_text("")

    result = subprocess.run(
        [str(SCRIPTS / "appengine-e2e-fixture.sh"), "stop", "--dry-run"],
        env={
            "PATH": f"{stub_dir}:/usr/bin:/bin",
            "CLOUDTASKS_IT_PROJECT": "flink-gcp",
            "GCLOUD_STUB_DIR": str(tmp_path),
            "GCLOUD_STUB_LOG": str(log),
            "APPENGINE_E2E_POLL_ATTEMPTS": "1",
            "APPENGINE_E2E_POLL_SECONDS": "0",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "stopped with zero instances" in result.stdout
