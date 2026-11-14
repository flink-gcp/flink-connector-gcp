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


"""The generic smoke application, rendered from disposable manifests."""

import shutil

import pytest
import yaml
from test_manifests import (
    KUBERNETES,
    assert_pod_policy,
    cue,
    leaf,
    workload,
)
from test_manifests import module as module  # noqa: PLC0414


@pytest.mark.parametrize("phase", ["initial", "upgrade"])
def test_smoke_application_preserves_state_and_bounds_resources(module, phase):
    inputs = workload()["run"]
    inputs["phase"] = phase
    inputs["image"] = workload()["delivery"]["resources"]["job"]["spec"]["image"]
    path = leaf(module, "runs/smoke", {"run": inputs})
    shutil.copyfile(
        KUBERNETES / "tests/fixtures/smoke.cue", module / path / "smoke.cue"
    )
    rendered = cue(module, "cmd", "render", path)
    assert rendered.returncode == 0, rendered.stderr
    [application] = list(yaml.safe_load_all(rendered.stdout))
    assert (
        application["metadata"]["annotations"]["flink-gcp.io/expires-at"]
        == inputs["expiresAt"]
    )
    spec = application["spec"]
    assert spec["serviceAccount"] == "smoke"
    assert spec["image"] == inputs["image"]
    assert spec["job"]["upgradeMode"] == "savepoint"
    assert spec["job"]["allowNonRestoredState"] is False
    assert spec["job"]["args"] == [
        "--run-id",
        "smoke-001",
        "--phase",
        phase,
        "--records",
        "18000",
        "--records-per-second",
        "10",
        "--require-restored",
        str(phase == "upgrade").lower(),
    ]
    config = spec["flinkConfiguration"]
    assert config["high-availability.type"] == "kubernetes"
    # Two consecutive failed checkpoints, such as storage timeouts, restart nothing.
    assert config["execution.checkpointing.tolerable-failed-checkpoints"] == "2"
    for option, suffix in [
        ("execution.checkpointing.dir", "checkpoints"),
        ("execution.checkpointing.savepoint-dir", "savepoints"),
        ("high-availability.storageDir", "ha"),
    ]:
        assert config[option] == f"gs://flink-gcp-tier3-smoke/runs/smoke-001/{suffix}"
    for manager in ["jobManager", "taskManager"]:
        assert spec[manager]["replicas"] == 1
        assert spec[manager]["resource"] == {"cpu": 1, "memory": "2Gi"}
    assert_pod_policy(spec)
    pod = spec["podTemplate"]["spec"]
    [container] = pod["containers"]
    assert container["name"] == "flink-main-container"
    assert (
        container["resources"]["requests"]
        == container["resources"]["limits"]
        == {
            "cpu": "1",
            "memory": "2Gi",
            "ephemeral-storage": "1Gi",
        }
    )


def test_smoke_cannot_use_the_flink_base_without_an_application(module):
    inputs = workload()["run"]
    inputs.update(
        phase="initial",
        image="us-central1-docker.pkg.dev/flink-gcp/flink-tier3/flink@sha256:"
        + "a" * 64,
    )
    path = leaf(module, "runs/smoke", {"run": inputs})
    shutil.copyfile(
        KUBERNETES / "tests/fixtures/smoke.cue", module / path / "smoke.cue"
    )
    assert cue(module, "cmd", "render", path).returncode != 0


@pytest.mark.parametrize(
    ("location", "value"),
    [
        (("spec", "job", "parallelism"), 3),
        (("spec", "job", "parallelism"), 4),
        (("spec", "flinkVersion"), "v1_20"),
        (("spec", "serviceAccount"), "cloudtasks"),
    ],
)
def test_smoke_namespace_policy_admits_only_its_own_shapes(module, location, value):
    document = workload()
    target = document["delivery"]["resources"]["job"]
    for key in location[:-1]:
        target = target.setdefault(key, {})
    target[location[-1]] = value
    path = leaf(module, "runs/invalid", document)
    result = cue(module, "cmd", "render", path)
    # The smoke namespace admits any non-empty service account, so only that
    # case renders; the version and parallelism rules stay smoke's own.
    if location == ("spec", "serviceAccount"):
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode != 0, result.stdout
        assert not result.stdout.strip()
