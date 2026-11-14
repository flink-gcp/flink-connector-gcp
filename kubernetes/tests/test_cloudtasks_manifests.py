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


"""The Cloud Tasks recovery delivery, rendered from disposable manifests."""

import json
import re
import shutil

import pytest
import yaml
from flink_tier3.bundle import delivered_sources
from test_manifests import (
    CLOUDTASKS_IMAGE,
    KUBERNETES,
    SYNTHETIC_DIGEST,
    assert_pod_policy,
    cue,
    leaf,
    lifecycle_export,
    workload,
)
from test_manifests import module as module  # noqa: PLC0414


def cloudtasks_leaf(module, **changes):
    inputs = {
        "id": "ct-contract",
        "expiresAt": "2026-10-13T08:00:00Z",
        "namespace": "tier3-cloudtasks",
        "image": f"{CLOUDTASKS_IMAGE}@{SYNTHETIC_DIGEST}",
        "phase": "initial",
        "sinkParallelism": 2,
        **changes,
    }
    inputs = {key: value for key, value in inputs.items() if value is not None}
    path = leaf(module, "runs/cloudtasks", {"run": inputs})
    shutil.copyfile(
        KUBERNETES / "tests/fixtures/cloudtasks.cue", module / path / "delivery.cue"
    )
    return path


def cloudtasks_args(run_id, phase, records=180000, rate=100, parallelism=2):
    return [
        "--run-id",
        run_id,
        "--queue",
        "ct1362-" + run_id,
        "--phase",
        phase,
        "--records",
        str(records),
        "--records-per-second",
        str(rate),
        "--require-restored",
        str(phase == "upgrade").lower(),
        "--sink-parallelism",
        str(parallelism),
    ]


def cloudtasks_configuration(run_id):
    storage = f"gs://flink-gcp-tier3-cloudtasks/runs/{run_id}"
    return {
        "kubernetes.operator.job.upgrade.last-state-fallback.enabled": "false",
        "kubernetes.operator.snapshot.resource.enabled": "false",
        "job.autoscaler.enabled": "false",
        "taskmanager.numberOfTaskSlots": "1",
        "state.backend.type": "hashmap",
        "execution.checkpointing.storage": "filesystem",
        "execution.checkpointing.interval": "30 s",
        "execution.checkpointing.timeout": "120 s",
        "execution.checkpointing.max-concurrent-checkpoints": "1",
        "execution.checkpointing.num-retained": "2",
        "execution.checkpointing.tolerable-failed-checkpoints": "2",
        "execution.checkpointing.dir": f"{storage}/checkpoints",
        "execution.checkpointing.savepoint-dir": f"{storage}/savepoints",
        "high-availability.type": "kubernetes",
        "high-availability.storageDir": f"{storage}/ha",
        "restart-strategy.type": "fixed-delay",
        "restart-strategy.fixed-delay.attempts": "3",
        "restart-strategy.fixed-delay.delay": "10 s",
    }


def assert_cloudtasks_application(
    app, run_id, expires_at, phase, records, rate, parallelism=2
):
    """The whole delivered contract of one phase but its approval annotations."""
    assert app["metadata"]["name"] == run_id
    assert app["metadata"]["namespace"] == "tier3-cloudtasks"
    assert app["metadata"]["labels"]["flink-gcp.io/run-id"] == run_id
    assert app["metadata"]["annotations"]["flink-gcp.io/expires-at"] == expires_at
    spec = app["spec"]
    assert spec["flinkVersion"] == "v2_2"
    assert spec["serviceAccount"] == "cloudtasks"
    assert spec["mode"] == "native"
    assert spec["image"] == f"{CLOUDTASKS_IMAGE}@{SYNTHETIC_DIGEST}"
    assert spec["job"] == {
        "jarURI": "local:///opt/flink/usrlib/cloudtasks-recovery.jar",
        "entryClass": "io.github.flink.gcp.connector.tier3.cloudtasks.CloudTasksRecoveryJob",
        "parallelism": parallelism,
        "state": "running",
        "upgradeMode": "savepoint",
        "allowNonRestoredState": False,
        "args": cloudtasks_args(run_id, phase, records, rate, parallelism),
    }
    assert spec["flinkConfiguration"] == cloudtasks_configuration(run_id)
    for manager, replicas in (("jobManager", 1), ("taskManager", parallelism)):
        assert spec[manager]["replicas"] == replicas
        assert spec[manager]["resource"] == {"cpu": 1, "memory": "2Gi"}
        assert "volumes" not in spec[manager]["podTemplate"]["spec"]
        [container] = spec[manager]["podTemplate"]["spec"]["containers"]
        assert container["name"] == "flink-main-container"
        assert (
            container["resources"]["requests"]
            == container["resources"]["limits"]
            == {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "1Gi"}
        )
    assert_pod_policy(spec)


def test_cloudtasks_initial_and_upgrade_preserve_the_trial(module):
    applications = []
    for phase in ("initial", "upgrade"):
        rendered = cue(module, "cmd", "render", cloudtasks_leaf(module, phase=phase))
        assert rendered.returncode == 0, rendered.stderr
        [app] = list(yaml.safe_load_all(rendered.stdout))
        assert_cloudtasks_application(
            app, "ct-contract", "2026-10-13T08:00:00Z", phase, 180000, 100
        )
        applications.append(app)
    # Updating the same deployment changes only the phase and restored-state requirement.
    for app in applications:
        app["spec"]["job"].pop("args")
    assert applications[0] == applications[1]


@pytest.mark.parametrize("parallelism", [1, 2])
def test_cloudtasks_sink_parallelism_sizes_the_job_and_its_taskmanagers(
    module, parallelism
):
    path = cloudtasks_leaf(module, sinkParallelism=parallelism)
    result = cue(module, "cmd", "render", path)
    assert result.returncode == 0, result.stderr
    [app] = list(yaml.safe_load_all(result.stdout))
    assert_cloudtasks_application(
        app,
        "ct-contract",
        "2026-10-13T08:00:00Z",
        "initial",
        180000,
        100,
        parallelism,
    )


@pytest.mark.parametrize(
    "records,rate", [(2, 1), (1800, 1), (180000, 100), (90000, 50)]
)
def test_cloudtasks_input_boundaries_render(module, records, rate):
    path = cloudtasks_leaf(module, records=records, recordsPerSecond=rate)
    result = cue(module, "cmd", "render", path)
    assert result.returncode == 0, result.stderr
    [app] = list(yaml.safe_load_all(result.stdout))
    assert app["spec"]["job"]["args"] == cloudtasks_args(
        "ct-contract", "initial", records, rate
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"phase": "resume"},
        {"records": 1},
        {"records": "100"},
        # Each within the application's bounds, but longer than 30 minutes of input.
        {"records": 180001},
        {"records": 1801, "recordsPerSecond": 1},
        {"records": 360000, "recordsPerSecond": 100},
        # The default count at a lower rate, omitted or given.
        {"recordsPerSecond": 50},
        {"records": 180000, "recordsPerSecond": 50},
        {"recordsPerSecond": 0},
        {"recordsPerSecond": 101},
        # One sink subtask on each of at most two single-slot TaskManagers, and
        # no default: the Flink package's own parallelism default would
        # contradict one.
        {"sinkParallelism": None},
        {"sinkParallelism": 0},
        {"sinkParallelism": 3},
        {"sinkParallelism": "2"},
        # The smoke policy admits this image, account and topology; only the package's
        # namespace refuses it.
        {"namespace": "tier3-smoke"},
        {"id": "../another"},
        {"image": f"{CLOUDTASKS_IMAGE}:latest"},
        {
            "image": "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/pubsub-recovery@"
            + SYNTHETIC_DIGEST
        },
    ],
)
def test_cloudtasks_rejects_invalid_trial_inputs(module, changes):
    result = cue(module, "cmd", "render", cloudtasks_leaf(module, **changes))
    assert result.returncode != 0
    assert not result.stdout.strip()


@pytest.mark.parametrize(
    "override",
    [
        {"serviceAccount": "bigquery"},
        {"job": {"upgradeMode": "stateless"}},
        {
            "job": {
                "entryClass": "io.github.flink.gcp.connector.tier3.cloudtasks.OtherJob"
            }
        },
        {
            "job": {
                "args": cloudtasks_args("ct-contract", "initial")[:3] + ["ct1362-other"]
            }
        },
        {"taskManager": {"replicas": 1}},
        {"job": {"parallelism": 1}},
        {"flinkConfiguration": {"taskmanager.numberOfTaskSlots": "2"}},
        {"flinkConfiguration": {"job.autoscaler.enabled": "true"}},
        {
            "flinkConfiguration": {
                "kubernetes.operator.job.upgrade.last-state-fallback.enabled": "true"
            }
        },
        {
            "flinkConfiguration": {
                "kubernetes.operator.snapshot.resource.enabled": "true"
            }
        },
        {"flinkConfiguration": {"execution.checkpointing.dir": "gs://other/state"}},
        {
            "taskManager": {
                "podTemplate": {
                    "spec": {
                        "containers": [
                            {
                                "name": "flink-main-container",
                                "resources": {"limits": {"memory": "4Gi"}},
                            }
                        ]
                    }
                }
            }
        },
        {
            "jobManager": {
                "podTemplate": {
                    "spec": {"nodeSelector": {"cloud.google.com/gke-spot": "true"}}
                }
            }
        },
    ],
)
def test_cloudtasks_delivery_cannot_change_fixed_topology(module, override):
    path = cloudtasks_leaf(module)
    (module / path / "override.cue").write_text(
        "package tier3\n"
        + json.dumps({"delivery": {"resources": {"app": {"spec": override}}}})
    )
    result = cue(module, "cmd", "render", path)
    assert result.returncode != 0
    assert not result.stdout.strip()


@pytest.mark.parametrize(
    "override",
    [
        {"flinkVersion": "v1_20"},
        {"serviceAccount": "bigquery"},
        {"job": {"parallelism": 3}},
        {
            "image": "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/smoke@"
            + SYNTHETIC_DIGEST
        },
    ],
)
def test_cloudtasks_namespace_policy_without_application_package(module, override):
    # A plain resource prevents package constraints from masking run policy.
    document = workload()
    document["run"]["namespace"] = "tier3-cloudtasks"
    spec = document["delivery"]["resources"]["job"]["spec"]
    spec.update(
        flinkVersion="v2_2",
        serviceAccount="cloudtasks",
        image=f"{CLOUDTASKS_IMAGE}@{SYNTHETIC_DIGEST}",
    )
    spec["job"]["parallelism"] = 2
    path = leaf(module, "runs/plain-cloudtasks", document)
    valid = cue(module, "cmd", "render", path)
    assert valid.returncode == 0, valid.stderr
    [app] = list(yaml.safe_load_all(valid.stdout))
    assert app["metadata"]["namespace"] == "tier3-cloudtasks"
    spec.update(override)
    leaf(module, "runs/plain-cloudtasks", document)
    invalid = cue(module, "cmd", "render", path)
    assert invalid.returncode != 0, invalid.stdout
    assert not invalid.stdout.strip()


@pytest.mark.parametrize(
    "inputs,records,rate,parallelism",
    [
        ({}, 180000, 100, (2, 2)),
        (
            {"cloudtasks_records": "90000", "cloudtasks_records_per_second": "50"},
            90000,
            50,
            (2, 2),
        ),
        ({"cloudtasks_trial": "jm-replacement"}, 180000, 100, (2, 2)),
        ({"cloudtasks_trial": "tm-replacement"}, 180000, 100, (2, 2)),
        ({"cloudtasks_trial": "rescale-out"}, 180000, 100, (1, 2)),
        ({"cloudtasks_trial": "rescale-in"}, 180000, 100, (2, 1)),
    ],
)
def test_cloudtasks_proposal_delivery_binds_both_phases(
    module, inputs, records, rate, parallelism
):
    result = lifecycle_export(
        module,
        "[application, upgradeApplication, delivery.resources]",
        "cloudtasks-recovery",
        run_id="proposal-1362",
        active_seconds="5220",
        application_image=f"{CLOUDTASKS_IMAGE}@{SYNTHETIC_DIGEST}",
        **inputs,
    )
    assert result.returncode == 0, result.stderr
    initial, upgrade, delivery = json.loads(result.stdout)
    for app, phase, size in zip(
        (initial, upgrade), ("initial", "upgrade"), parallelism, strict=True
    ):
        assert_cloudtasks_application(
            app, "proposal-1362", "2026-09-20T00:00:00Z", phase, records, rate, size
        )
        annotations = app["metadata"]["annotations"]
        assert annotations["flink-gcp.io/approval"] == "a" * 32
        assert annotations["flink-gcp.io/scenario"] == "cloudtasks-recovery"
    config = delivery["config"]
    assert config["immutable"] is True
    assert json.loads(config["data"]["approval.json"]) == {}
    assert json.loads(config["data"]["proposal.json"]) == {}
    assert json.loads(config["data"]["application.json"]) == initial
    assert json.loads(config["data"]["upgrade-application.json"]) == upgrade
    supervisor = delivery["supervisor"]
    assert supervisor["spec"]["activeDeadlineSeconds"] == 5220
    projection = supervisor["spec"]["template"]["spec"]["volumes"][0]["configMap"][
        "items"
    ]
    mounted = {item["path"]: config["data"][item["key"]] for item in projection}
    assert len(mounted) == len(projection)
    assert mounted.keys() == {
        "approval.json",
        "application.json",
        "upgrade-application.json",
        "proposal.json",
    } | {"flink_tier3/" + name for name in delivered_sources()}


@pytest.mark.parametrize("trial", ["upgrade-failover", "rescale", "RESCALE-IN"])
def test_cloudtasks_proposal_refuses_an_unknown_trial(module, trial):
    result = lifecycle_export(
        module,
        "[application, upgradeApplication, delivery.resources]",
        "cloudtasks-recovery",
        run_id="proposal-1362",
        active_seconds="5220",
        application_image=f"{CLOUDTASKS_IMAGE}@{SYNTHETIC_DIGEST}",
        cloudtasks_trial=trial,
    )
    assert result.returncode != 0
    assert not result.stdout.strip()
    # The size table refuses it. The tag's own disjunction does not: compared
    # rather than indexed, `rescale` rendered two subtasks in both phases.
    # A label that is not an identifier is quoted.
    assert re.search(rf'undefined field: "?{re.escape(trial)}"?:', result.stderr)
