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

"""The Bigtable recovery delivery, rendered from disposable manifests."""

import json
import re
import shutil

import pytest
import yaml
from flink_tier3.exercise import DEFAULT_FAULT, FAULTS
from test_manifests import (
    KUBERNETES,
    SYNTHETIC_DIGEST,
    assert_pod_policy,
    cue,
    leaf,
    lifecycle_export,
    workload,
)


@pytest.fixture
def module(tmp_path):
    """A disposable copy of the CUE module, as test_manifests.py's fixture makes."""
    destination = tmp_path / "module"
    shutil.copytree(
        KUBERNETES, destination, ignore=shutil.ignore_patterns("tests", "target")
    )
    return destination


BIGTABLE_IMAGE = "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/bigtable-recovery"
END = "2026-10-20T01:30:00Z"


def bigtable_leaf(module, **changes):
    inputs = {
        "id": "bt-contract",
        "expiresAt": "2026-10-20T02:00:00Z",
        "namespace": "tier3-bigtable",
        "image": f"{BIGTABLE_IMAGE}@{SYNTHETIC_DIGEST}",
        "phase": "initial",
        "parallelism": 2,
        "changeStreamEnd": END,
        **changes,
    }
    inputs = {key: value for key, value in inputs.items() if value is not None}
    path = leaf(module, "runs/bigtable", {"run": inputs})
    shutil.copyfile(
        KUBERNETES / "tests/fixtures/bigtable.cue", module / path / "delivery.cue"
    )
    return path


def bigtable_args(run_id, phase, records=180000, rate=100, parallelism=2, end=END):
    return [
        "--run-id",
        run_id,
        "--instance",
        "bt1316-" + run_id,
        "--phase",
        phase,
        "--records",
        str(records),
        "--records-per-second",
        str(rate),
        "--change-stream-end",
        end,
        "--require-restored",
        str(phase == "upgrade").lower(),
        "--parallelism",
        str(parallelism),
    ]


def bigtable_configuration(run_id):
    storage = f"gs://flink-gcp-tier3-bigtable/runs/{run_id}"
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
        "execution.checkpointing.dir": f"{storage}/checkpoints",
        "execution.checkpointing.savepoint-dir": f"{storage}/savepoints",
        "high-availability.type": "kubernetes",
        "high-availability.storageDir": f"{storage}/ha",
        "restart-strategy.type": "fixed-delay",
        "restart-strategy.fixed-delay.attempts": "3",
        "restart-strategy.fixed-delay.delay": "10 s",
    }


def assert_bigtable_application(
    app, run_id, expires_at, phase, records=180000, rate=100, parallelism=2, end=END
):
    """The whole delivered contract of one phase but its approval annotations."""
    assert app["metadata"]["name"] == run_id
    assert app["metadata"]["namespace"] == "tier3-bigtable"
    assert app["metadata"]["labels"]["flink-gcp.io/run-id"] == run_id
    assert app["metadata"]["annotations"]["flink-gcp.io/expires-at"] == expires_at
    spec = app["spec"]
    assert spec["flinkVersion"] == "v2_2"
    assert spec["serviceAccount"] == "bigtable"
    assert spec["mode"] == "native"
    assert spec["image"] == f"{BIGTABLE_IMAGE}@{SYNTHETIC_DIGEST}"
    assert spec["job"] == {
        "jarURI": "local:///opt/flink/usrlib/bigtable-recovery.jar",
        "entryClass": "io.github.flink.gcp.connector.tier3.bigtable.BigtableRecoveryJob",
        "parallelism": parallelism,
        "state": "running",
        "upgradeMode": "savepoint",
        "allowNonRestoredState": False,
        "args": bigtable_args(run_id, phase, records, rate, parallelism, end),
    }
    assert spec["flinkConfiguration"] == bigtable_configuration(run_id)
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


def render(module, path):
    result = cue(module, "cmd", "render", path)
    assert result.returncode == 0, result.stderr
    [app] = list(yaml.safe_load_all(result.stdout))
    return app


def test_bigtable_initial_and_upgrade_preserve_the_trial(module):
    for phase in ("initial", "upgrade"):
        app = render(module, bigtable_leaf(module, phase=phase))
        assert_bigtable_application(app, "bt-contract", "2026-10-20T02:00:00Z", phase)


@pytest.mark.parametrize("parallelism", [1, 2])
def test_bigtable_parallelism_sizes_the_job_and_its_taskmanagers(module, parallelism):
    app = render(module, bigtable_leaf(module, parallelism=parallelism))
    assert_bigtable_application(
        app, "bt-contract", "2026-10-20T02:00:00Z", "initial", parallelism=parallelism
    )


@pytest.mark.parametrize(
    "records,rate", [(2, 1), (1800, 1), (180000, 100), (90000, 50)]
)
def test_bigtable_input_boundaries_render(module, records, rate):
    app = render(module, bigtable_leaf(module, records=records, recordsPerSecond=rate))
    assert app["spec"]["job"]["args"] == bigtable_args(
        "bt-contract", "initial", records, rate
    )


def test_bigtable_instance_id_fits_bigtable_at_the_longest_run_id(module):
    # bt1316- and 26 characters is Bigtable's 33-character instance ID limit.
    run_id = "a" * 26
    app = render(module, bigtable_leaf(module, id=run_id))
    instance = app["spec"]["job"]["args"][3]
    assert instance == "bt1316-" + run_id
    assert len(instance) == 33


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
        # One subtask on each of at most two single-slot TaskManagers, and no
        # default: the Flink package's own parallelism default would contradict one.
        {"parallelism": None},
        {"parallelism": 0},
        {"parallelism": 3},
        {"parallelism": "2"},
        # The source has no other end, and the application parses only an instant.
        {"changeStreamEnd": None},
        {"changeStreamEnd": "2026-10-20T10:30:00+09:00"},
        {"changeStreamEnd": "2026-10-20"},
        {"changeStreamEnd": "later"},
        # Ends in Z but is not an instant: only the time.Time half refuses it.
        {"changeStreamEnd": "laterZ"},
        # The smoke policy admits this image, account and topology; only the
        # package's namespace refuses it.
        {"namespace": "tier3-smoke"},
        {"id": "../another"},
        # One character past the instance ID limit.
        {"id": "a" * 27},
        {"image": f"{BIGTABLE_IMAGE}:latest"},
        {
            "image": "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/cloudtasks-recovery@"
            + SYNTHETIC_DIGEST
        },
    ],
)
def test_bigtable_rejects_invalid_trial_inputs(module, changes):
    result = cue(module, "cmd", "render", bigtable_leaf(module, **changes))
    assert result.returncode != 0
    assert not result.stdout.strip()


@pytest.mark.parametrize(
    "override",
    [
        {"serviceAccount": "bigquery"},
        {"job": {"upgradeMode": "stateless"}},
        {"job": {"entryClass": "io.github.flink.gcp.connector.tier3.bigtable.Other"}},
        {
            "job": {
                # Every argument but the instance, so only the instance differs.
                "args": bigtable_args("bt-contract", "initial")[:3]
                + ["bt1316-other"]
                + bigtable_args("bt-contract", "initial")[4:]
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
def test_bigtable_delivery_cannot_change_fixed_topology(module, override):
    path = bigtable_leaf(module)
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
def test_bigtable_namespace_policy_without_application_package(module, override):
    # A plain resource prevents package constraints from masking run policy.
    document = workload()
    document["run"]["namespace"] = "tier3-bigtable"
    spec = document["delivery"]["resources"]["job"]["spec"]
    spec.update(
        flinkVersion="v2_2",
        serviceAccount="bigtable",
        image=f"{BIGTABLE_IMAGE}@{SYNTHETIC_DIGEST}",
    )
    spec["job"]["parallelism"] = 2
    path = leaf(module, "runs/plain-bigtable", document)
    valid = cue(module, "cmd", "render", path)
    assert valid.returncode == 0, valid.stderr
    [app] = list(yaml.safe_load_all(valid.stdout))
    assert app["metadata"]["namespace"] == "tier3-bigtable"
    spec.update(override)
    leaf(module, "runs/plain-bigtable", document)
    invalid = cue(module, "cmd", "render", path)
    assert invalid.returncode != 0, invalid.stdout
    assert not invalid.stdout.strip()


def proposal(module, **inputs):
    tags = {
        "run_id": "proposal-1316",
        "active_seconds": "5220",
        "application_image": f"{BIGTABLE_IMAGE}@{SYNTHETIC_DIGEST}",
        "bigtable_change_stream_end": END,
        **inputs,
    }
    tags = {key: value for key, value in tags.items() if value is not None}
    return lifecycle_export(
        module,
        "[application, upgradeApplication, delivery.resources]",
        "bigtable-recovery",
        **tags,
    )


@pytest.mark.parametrize("name", list(FAULTS))
def test_bigtable_proposal_binds_both_phases_of_each_fault_kind(module, name):
    """Each phase's job parallelism and TaskManagers are the kind's."""
    fault = FAULTS[name]
    result = proposal(module, bigtable_trial=name)
    assert result.returncode == 0, result.stderr
    initial, upgrade, delivery = json.loads(result.stdout)
    for app, phase, size in zip(
        (initial, upgrade),
        ("initial", "upgrade"),
        (fault.initial, fault.upgrade),
        strict=True,
    ):
        assert_bigtable_application(
            app, "proposal-1316", "2026-09-20T00:00:00Z", phase, parallelism=size
        )
        annotations = app["metadata"]["annotations"]
        assert annotations["flink-gcp.io/approval"] == "a" * 32
        assert annotations["flink-gcp.io/scenario"] == "bigtable-recovery"
    config = delivery["config"]
    assert json.loads(config["data"]["application.json"]) == initial
    assert json.loads(config["data"]["upgrade-application.json"]) == upgrade
    assert delivery["supervisor"]["spec"]["activeDeadlineSeconds"] == 5220


def test_bigtable_proposal_defaults_to_the_default_fault_and_takes_the_input_tags(
    module,
):
    result = proposal(
        module, bigtable_records="90000", bigtable_records_per_second="50"
    )
    assert result.returncode == 0, result.stderr
    initial, upgrade, _ = json.loads(result.stdout)
    fault = FAULTS[DEFAULT_FAULT]
    for app, phase, size in (
        (initial, "initial", fault.initial),
        (upgrade, "upgrade", fault.upgrade),
    ):
        assert_bigtable_application(
            app, "proposal-1316", "2026-09-20T00:00:00Z", phase, 90000, 50, size
        )


@pytest.mark.parametrize("trial", ["upgrade-failover", "rescale", "RESCALE-IN"])
def test_bigtable_proposal_refuses_an_unknown_trial(module, trial):
    result = proposal(module, bigtable_trial=trial)
    assert result.returncode != 0
    assert not result.stdout.strip()
    # The size table refuses it, as it does for the other scenarios.
    assert re.search(rf'undefined field: "?{re.escape(trial)}"?:', result.stderr)


def test_bigtable_proposal_requires_the_change_stream_end(module):
    result = proposal(module, bigtable_change_stream_end=None)
    assert result.returncode != 0
    assert not result.stdout.strip()


@pytest.mark.parametrize(("active_seconds", "admitted"), [(5220, True), (5221, False)])
def test_bigtable_window_is_ninety_minutes(module, active_seconds, admitted):
    result = proposal(module, active_seconds=str(active_seconds))
    assert (result.returncode == 0) is admitted, result.stderr
    if not admitted:
        assert f"invalid value {active_seconds} (out of bound" in result.stderr
