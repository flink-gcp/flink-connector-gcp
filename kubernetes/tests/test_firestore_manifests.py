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

"""The Firestore recovery delivery, rendered from disposable manifests."""

import json
import re
import shutil

import pytest
import yaml
from flink_tier3.bundle import delivered_sources
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
from test_manifests import module as module  # noqa: PLC0414

FIRESTORE_IMAGE = "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/firestore-recovery"


def firestore_leaf(module, **changes):
    inputs = {
        "id": "fs-contract",
        "expiresAt": "2026-10-20T02:00:00Z",
        "namespace": "tier3-firestore",
        "image": f"{FIRESTORE_IMAGE}@{SYNTHETIC_DIGEST}",
        "phase": "initial",
        "parallelism": 2,
        **changes,
    }
    inputs = {key: value for key, value in inputs.items() if value is not None}
    path = leaf(module, "runs/firestore", {"run": inputs})
    shutil.copyfile(
        KUBERNETES / "tests/fixtures/firestore.cue", module / path / "delivery.cue"
    )
    return path


def firestore_args(run_id, phase, records=180000, rate=100, parallelism=2, seed=50):
    return [
        "--run-id",
        run_id,
        "--native-database",
        f"fs1879-{run_id}-n",
        "--datastore-database",
        f"fs1879-{run_id}-d",
        "--phase",
        phase,
        "--records",
        str(records),
        "--records-per-second",
        str(rate),
        "--seed-records-per-second",
        str(seed),
        "--require-restored",
        str(phase == "upgrade").lower(),
        "--parallelism",
        str(parallelism),
    ]


def firestore_configuration(run_id):
    storage = f"gs://flink-gcp-tier3-firestore/runs/{run_id}"
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
        "restart-strategy.fixed-delay.attempts": "2147483647",
        "restart-strategy.fixed-delay.delay": "10 s",
    }


def assert_firestore_application(
    app,
    run_id,
    expires_at,
    phase,
    records=180000,
    rate=100,
    parallelism=2,
    seed=50,
):
    """The whole delivered contract of one phase but its approval annotations."""
    assert app["metadata"]["name"] == run_id
    assert app["metadata"]["namespace"] == "tier3-firestore"
    assert app["metadata"]["labels"]["flink-gcp.io/run-id"] == run_id
    assert app["metadata"]["annotations"]["flink-gcp.io/expires-at"] == expires_at
    spec = app["spec"]
    assert spec["flinkVersion"] == "v2_2"
    assert spec["serviceAccount"] == "firestore"
    assert spec["mode"] == "native"
    assert spec["image"] == f"{FIRESTORE_IMAGE}@{SYNTHETIC_DIGEST}"
    assert spec["job"] == {
        "jarURI": "local:///opt/flink/usrlib/firestore-recovery.jar",
        "entryClass": "io.github.flink.gcp.connector.tier3.firestore.FirestoreRecoveryJob",
        "parallelism": parallelism,
        "state": "running",
        "upgradeMode": "savepoint",
        "allowNonRestoredState": False,
        "args": firestore_args(run_id, phase, records, rate, parallelism, seed),
    }
    assert spec["flinkConfiguration"] == firestore_configuration(run_id)
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


def test_firestore_initial_and_upgrade_preserve_the_trial(module):
    for phase in ("initial", "upgrade"):
        app = render(module, firestore_leaf(module, phase=phase))
        assert_firestore_application(app, "fs-contract", "2026-10-20T02:00:00Z", phase)


@pytest.mark.parametrize("parallelism", [1, 2])
def test_firestore_parallelism_sizes_the_job_and_its_taskmanagers(module, parallelism):
    app = render(module, firestore_leaf(module, parallelism=parallelism))
    assert_firestore_application(
        app, "fs-contract", "2026-10-20T02:00:00Z", "initial", parallelism=parallelism
    )


@pytest.mark.parametrize(
    "records,rate,seed",
    [(2, 1, 1), (1800, 1, 200), (180000, 100, 50), (90000, 50, 150)],
)
def test_firestore_input_boundaries_render(module, records, rate, seed):
    app = render(
        module,
        firestore_leaf(
            module, records=records, recordsPerSecond=rate, seedRecordsPerSecond=seed
        ),
    )
    assert app["spec"]["job"]["args"] == firestore_args(
        "fs-contract", "initial", records, rate, seed=seed
    )


def test_firestore_database_ids_fit_firestore_at_the_longest_run_id(module):
    # fs1879-, the 40 characters of the longest run label and a mode suffix stay
    # within Firestore's 63-character database IDs, so the package takes every label.
    run_id = "a" * 40
    args = render(module, firestore_leaf(module, id=run_id))["spec"]["job"]["args"]
    databases = [args[3], args[5]]
    # 49 characters each, within 63.
    assert databases == [f"fs1879-{run_id}-n", f"fs1879-{run_id}-d"]


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
        # The seed copy's pace, per reading subtask.
        {"seedRecordsPerSecond": 0},
        {"seedRecordsPerSecond": 201},
        {"seedRecordsPerSecond": "50"},
        # One subtask on each of at most two single-slot TaskManagers, and no
        # default: the Flink package's own parallelism default would contradict one.
        {"parallelism": None},
        {"parallelism": 0},
        {"parallelism": 3},
        {"parallelism": "2"},
        # The smoke policy admits this image, account and topology; only the
        # package's namespace refuses it.
        {"namespace": "tier3-smoke"},
        {"id": "../another"},
        # One character past the run label grammar.
        {"id": "a" * 41},
        {"image": f"{FIRESTORE_IMAGE}:latest"},
        {
            "image": "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/bigtable-recovery@"
            + SYNTHETIC_DIGEST
        },
    ],
)
def test_firestore_rejects_invalid_trial_inputs(module, changes):
    result = cue(module, "cmd", "render", firestore_leaf(module, **changes))
    assert result.returncode != 0
    assert not result.stdout.strip()


@pytest.mark.parametrize(
    "override",
    [
        {"serviceAccount": "bigquery"},
        {"job": {"upgradeMode": "stateless"}},
        {"job": {"entryClass": "io.github.flink.gcp.connector.tier3.firestore.Other"}},
        {
            "job": {
                # Every argument but a database, so only that database differs.
                "args": firestore_args("fs-contract", "initial")[:3]
                + ["fs1879-other-n"]
                + firestore_args("fs-contract", "initial")[4:]
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
def test_firestore_delivery_cannot_change_fixed_topology(module, override):
    path = firestore_leaf(module)
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
def test_firestore_namespace_policy_without_application_package(module, override):
    # A plain resource prevents package constraints from masking run policy.
    document = workload()
    document["run"]["namespace"] = "tier3-firestore"
    spec = document["delivery"]["resources"]["job"]["spec"]
    spec.update(
        flinkVersion="v2_2",
        serviceAccount="firestore",
        image=f"{FIRESTORE_IMAGE}@{SYNTHETIC_DIGEST}",
    )
    spec["job"]["parallelism"] = 2
    path = leaf(module, "runs/plain-firestore", document)
    valid = cue(module, "cmd", "render", path)
    assert valid.returncode == 0, valid.stderr
    [app] = list(yaml.safe_load_all(valid.stdout))
    assert app["metadata"]["namespace"] == "tier3-firestore"
    spec.update(override)
    leaf(module, "runs/plain-firestore", document)
    invalid = cue(module, "cmd", "render", path)
    assert invalid.returncode != 0, invalid.stdout
    assert not invalid.stdout.strip()


def proposal(module, **inputs):
    tags = {
        "run_id": "proposal-1879",
        "active_seconds": "5220",
        "application_image": f"{FIRESTORE_IMAGE}@{SYNTHETIC_DIGEST}",
        **inputs,
    }
    tags = {key: value for key, value in tags.items() if value is not None}
    return lifecycle_export(
        module,
        "[application, upgradeApplication, delivery.resources]",
        "firestore-recovery",
        **tags,
    )


@pytest.mark.parametrize("name", list(FAULTS))
def test_firestore_proposal_binds_both_phases_of_each_fault_kind(module, name):
    """Each phase's job parallelism and TaskManagers are the kind's."""
    fault = FAULTS[name]
    result = proposal(module, firestore_trial=name)
    assert result.returncode == 0, result.stderr
    initial, upgrade, delivery = json.loads(result.stdout)
    for app, phase, size in zip(
        (initial, upgrade),
        ("initial", "upgrade"),
        (fault.initial, fault.upgrade),
        strict=True,
    ):
        assert_firestore_application(
            app, "proposal-1879", "2026-09-20T00:00:00Z", phase, parallelism=size
        )
        annotations = app["metadata"]["annotations"]
        assert annotations["flink-gcp.io/approval"] == "a" * 32
        assert annotations["flink-gcp.io/scenario"] == "firestore-recovery"
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
    assert {item["key"] for item in projection} == config["data"].keys()
    assert mounted.keys() == {
        "approval.json",
        "application.json",
        "upgrade-application.json",
        "proposal.json",
    } | {"flink_tier3/" + name for name in delivered_sources()}


def test_firestore_proposal_defaults_to_the_default_fault_and_takes_the_input_tags(
    module,
):
    result = proposal(
        module,
        firestore_records="90000",
        firestore_records_per_second="50",
        firestore_seed_records_per_second="150",
    )
    assert result.returncode == 0, result.stderr
    initial, upgrade, _ = json.loads(result.stdout)
    fault = FAULTS[DEFAULT_FAULT]
    for app, phase, size in (
        (initial, "initial", fault.initial),
        (upgrade, "upgrade", fault.upgrade),
    ):
        assert_firestore_application(
            app,
            "proposal-1879",
            "2026-09-20T00:00:00Z",
            phase,
            90000,
            50,
            size,
            seed=150,
        )


@pytest.mark.parametrize("trial", ["upgrade-failover", "rescale", "RESCALE-IN"])
def test_firestore_proposal_refuses_an_unknown_trial(module, trial):
    result = proposal(module, firestore_trial=trial)
    assert result.returncode != 0
    assert not result.stdout.strip()
    # The size table refuses it, as it does for the other scenarios.
    assert re.search(rf'undefined field: "?{re.escape(trial)}"?:', result.stderr)


@pytest.mark.parametrize(("active_seconds", "admitted"), [(5220, True), (5221, False)])
def test_firestore_window_is_ninety_minutes(module, active_seconds, admitted):
    result = proposal(module, active_seconds=str(active_seconds))
    assert (result.returncode == 0) is admitted, result.stderr
    if not admitted:
        assert f"invalid value {active_seconds} (out of bound" in result.stderr


def test_the_sequence_trial_starts_and_upgrades_at_two(module):
    """The fault sequence (ADR-0204) patches its own rescales from the upgrade."""
    result = proposal(
        module,
        firestore_trial="sequence",
        firestore_records="6000",
        firestore_records_per_second="10",
        firestore_seed_records_per_second="5",
    )
    assert result.returncode == 0, result.stderr
    initial, upgrade, _ = json.loads(result.stdout)
    for app, phase in ((initial, "initial"), (upgrade, "upgrade")):
        assert_firestore_application(
            app,
            "proposal-1879",
            "2026-09-20T00:00:00Z",
            phase,
            records=6000,
            rate=10,
            parallelism=2,
            seed=5,
        )
