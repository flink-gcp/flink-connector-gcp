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


"""The Pub/Sub recovery delivery, rendered from disposable manifests."""

import copy
import itertools
import json
import re
import shutil
from types import SimpleNamespace

import pytest
import yaml
from flink_tier3 import repository
from flink_tier3.bundle import delivered_sources
from flink_tier3.common import Failure, digest, timestamp, verify_pod
from flink_tier3.lifecycle import bind
from flink_tier3.model import Approval, validate_approval
from flink_tier3.policy import GAR, POD_RESOURCES
from flink_tier3.pubsub import plan as pubsub_plan
from flink_tier3.pubsub import verdict as pubsub_verdict
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

PUBSUB_IMAGE = "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/pubsub-recovery"


def pubsub_delivery(module, **inputs):
    run = {
        "id": "pubsub-probe",
        "expiresAt": "2026-09-20T12:00:00Z",
        "image": f"{PUBSUB_IMAGE}@{SYNTHETIC_DIGEST}",
        "phase": "initial",
        "recordsPerSubscription": 1000,
        "parallelism": 1,
        "entryPoint": "datastream",
        **inputs,
    }
    path = leaf(module, "runs/pubsub", {"run": run})
    shutil.copyfile(
        KUBERNETES / "tests/fixtures/pubsub.cue", module / path / "application.cue"
    )
    return path


@pytest.mark.parametrize("phase", ["initial", "upgrade"])
@pytest.mark.parametrize("parallelism", [1, 2])
@pytest.mark.parametrize("records", [1, 10000])
@pytest.mark.parametrize("entry_point", ["datastream", "table"])
def test_pubsub_recovery_deployment_contract(
    module, phase, parallelism, records, entry_point
):
    path = pubsub_delivery(
        module,
        phase=phase,
        parallelism=parallelism,
        recordsPerSubscription=records,
        entryPoint=entry_point,
    )
    result = cue(module, "cmd", "render", path)
    assert result.returncode == 0, result.stderr
    [app] = list(yaml.safe_load_all(result.stdout))
    assert app["metadata"]["name"] == "pubsub-probe"
    assert app["metadata"]["namespace"] == "tier3-pubsub"
    assert app["metadata"]["labels"]["flink-gcp.io/run-id"] == "pubsub-probe"
    assert app["metadata"]["annotations"]["flink-gcp.io/expires-at"] == (
        "2026-09-20T12:00:00Z"
    )
    spec = app["spec"]
    assert spec["image"] == f"{PUBSUB_IMAGE}@{SYNTHETIC_DIGEST}"
    assert spec["flinkVersion"] == "v2_2"
    assert spec["serviceAccount"] == "pubsub"
    assert spec["mode"] == "native"
    job = spec["job"]
    assert job["parallelism"] == parallelism
    assert job["upgradeMode"] == "savepoint"
    assert job["allowNonRestoredState"] is False
    assert job["jarURI"] == "local:///opt/flink/usrlib/pubsub-recovery.jar"
    assert job["entryClass"] == (
        "io.github.flink.gcp.connector.tier3.pubsub.PubSubRecoveryJob"
    )
    assert job["args"] == [
        "--run-id=pubsub-probe",
        f"--phase={phase}",
        f"--records-per-subscription={records}",
        f"--parallelism={parallelism}",
        f"--require-restored={str(phase == 'upgrade').lower()}",
        f"--entry-point={entry_point}",
        "--restore-deduplication=false",
        "--replay-delay-millis=0",
        "--max-outstanding-messages=1000",
        "--hold-after-checkpoint-millis=0",
    ]
    config = spec["flinkConfiguration"]
    assert config["taskmanager.numberOfTaskSlots"] == "1"
    # Status-based savepoint reporting, and no last-state fallback, as the
    # recovery exercise reads the upgrade savepoint from the application.
    assert config["kubernetes.operator.snapshot.resource.enabled"] == "false"
    assert (
        config["kubernetes.operator.job.upgrade.last-state-fallback.enabled"] == "false"
    )
    assert config["execution.checkpointing.interval"] == "120 s"
    assert config["execution.checkpointing.max-concurrent-checkpoints"] == "1"
    assert config["execution.checkpointing.tolerable-failed-checkpoints"] == "2"
    assert config["execution.checkpointing.storage"] == "filesystem"
    assert config["high-availability.type"] == "kubernetes"
    assert config["execution.checkpointing.externalized-checkpoint-retention"] == (
        "RETAIN_ON_CANCELLATION"
    )
    for key, suffix in [
        ("execution.checkpointing.dir", "checkpoints"),
        ("execution.checkpointing.savepoint-dir", "savepoints"),
        ("high-availability.storageDir", "ha"),
    ]:
        assert config[key] == f"gs://flink-gcp-tier3-pubsub/runs/pubsub-probe/{suffix}"
    for manager, replicas in [("jobManager", 1), ("taskManager", parallelism)]:
        assert spec[manager]["replicas"] == replicas
        assert spec[manager]["resource"] == {"cpu": 1, "memory": "2Gi"}
    for owner in [spec, spec["jobManager"], spec["taskManager"]]:
        pod = owner["podTemplate"]
        assert pod["metadata"]["labels"]["flink-gcp.io/run-id"] == "pubsub-probe"
        [container] = pod["spec"]["containers"]
        assert container["name"] == "flink-main-container"
        assert container["resources"]["requests"] == container["resources"]["limits"]
        assert container["resources"]["limits"] == {
            "cpu": "1",
            "memory": "2Gi",
            "ephemeral-storage": "1Gi",
        }


@pytest.mark.parametrize(
    "inputs",
    [
        {"id": "../foreign"},
        {"id": "x" * 41},
        {"phase": "resume"},
        {"recordsPerSubscription": 0},
        {"recordsPerSubscription": 10001},
        {"recordsPerSubscription": "1"},
        {"parallelism": 0},
        {"parallelism": 4},
        {"parallelism": "1"},
        {"entryPoint": "sql"},
        {"entryPoint": "Table"},
        {"image": f"{PUBSUB_IMAGE}:latest"},
        {"image": f"{PUBSUB_IMAGE}@sha256:{'a' * 63}"},
        {"image": f"{PUBSUB_IMAGE}@sha256:{'A' * 64}"},
        {"image": f"{CLOUDTASKS_IMAGE}@{SYNTHETIC_DIGEST}"},
        {
            "image": f"{PUBSUB_IMAGE.replace('/flink-gcp/', '/other/')}@{SYNTHETIC_DIGEST}"
        },
        {"namespace": "tier3-smoke"},
    ],
)
def test_pubsub_rejects_invalid_run_inputs(module, inputs):
    path = pubsub_delivery(module, **inputs)
    result = cue(module, "cmd", "render", path)
    assert result.returncode != 0, result.stdout
    assert not result.stdout.strip()


@pytest.mark.parametrize(
    "override",
    [
        {"flinkVersion": "v1_20"},
        {"serviceAccount": "smoke"},
        {"mode": "standalone"},
        {"job": {"parallelism": 2}},
        {"job": {"upgradeMode": "stateless"}},
        {"job": {"allowNonRestoredState": True}},
        {"job": {"args": ["--run-id=foreign"]}},
        {"job": {"entryClass": "io.github.flink.gcp.connector.tier3.pubsub.Other"}},
        {"jobManager": {"replicas": 2}},
        {"taskManager": {"replicas": 2}},
        {"flinkConfiguration": {"taskmanager.numberOfTaskSlots": "2"}},
        {"flinkConfiguration": {"execution.checkpointing.dir": "gs://foreign/state"}},
        *[
            {manager: {"resource": {"cpu": 2}}}
            for manager in ["jobManager", "taskManager"]
        ],
        # Architecture is pinned on every template; the capacity class is
        # pinned only on the TaskManager, which is the one that selects one.
        *[
            {owner: {"podTemplate": {"spec": {"nodeSelector": {key: value}}}}}
            if owner
            else {"podTemplate": {"spec": {"nodeSelector": {key: value}}}}
            for owner in [None, "jobManager", "taskManager"]
            for key, value in [("kubernetes.io/arch", "arm64")]
        ],
        {
            "taskManager": {
                "podTemplate": {
                    "spec": {"nodeSelector": {"cloud.google.com/gke-spot": "false"}}
                }
            }
        },
    ],
)
def test_pubsub_delivery_cannot_override_recovery_contract(module, override):
    path = pubsub_delivery(module)
    valid = cue(module, "cmd", "render", path)
    assert valid.returncode == 0, valid.stderr
    (module / path / "override.cue").write_text(
        "package tier3\n"
        + json.dumps({"delivery": {"resources": {"app": {"spec": override}}}})
    )
    invalid = cue(module, "cmd", "render", path)
    assert invalid.returncode != 0, invalid.stdout
    assert not invalid.stdout.strip()


@pytest.mark.parametrize("parallelism", [1, 2, 3])
def test_pubsub_namespace_policy_without_application_package(module, parallelism):
    document = workload()
    document["run"]["namespace"] = "tier3-pubsub"
    spec = document["delivery"]["resources"]["job"]["spec"]
    spec["serviceAccount"] = "pubsub"
    spec["job"]["parallelism"] = parallelism
    path = leaf(module, "runs/pubsub-policy", document)
    valid = cue(module, "cmd", "render", path)
    assert valid.returncode == 0, valid.stderr
    [app] = list(yaml.safe_load_all(valid.stdout))
    assert app["metadata"]["namespace"] == "tier3-pubsub"
    assert app["spec"]["flinkVersion"] == "v2_2"
    for override in [
        {"serviceAccount": "smoke"},
        {"flinkVersion": "v1_20"},
        {"job": {"parallelism": 4}},
    ]:
        leaf(
            module,
            "runs/pubsub-policy",
            {
                **document,
                "delivery": {
                    "resources": {
                        "job": {
                            **document["delivery"]["resources"]["job"],
                            "spec": {**spec, **override},
                        }
                    }
                },
            },
        )
        invalid = cue(module, "cmd", "render", path)
        assert invalid.returncode != 0, (override, invalid.stdout)
        assert not invalid.stdout.strip()


@pytest.mark.parametrize("explicit_namespace", [False, True])
def test_pubsub_companion_and_application_use_the_selected_run_namespace(
    module, explicit_namespace
):
    path = pubsub_delivery(module)
    fixture = module / path / "application.cue"
    if not explicit_namespace:
        fixture.write_text(
            fixture.read_text().replace('run: namespace: "tier3-pubsub"', "")
        )
    (module / path / "companion.cue").write_text(
        "package tier3\n"
        + json.dumps(
            {
                "delivery": {
                    "resources": {
                        "config": {
                            "apiVersion": "v1",
                            "kind": "ConfigMap",
                            "metadata": {"name": "companion"},
                            "data": {"value": "test"},
                        }
                    }
                }
            }
        )
    )
    result = cue(module, "cmd", "render", path)
    if explicit_namespace:
        assert result.returncode == 0, result.stderr
        resources = list(yaml.safe_load_all(result.stdout))
        assert {item["kind"] for item in resources} == {"ConfigMap", "FlinkDeployment"}
        assert {item["metadata"]["namespace"] for item in resources} == {"tier3-pubsub"}
    else:
        assert result.returncode != 0, result.stdout
        assert not result.stdout.strip()


def test_the_pubsub_reactive_kinds_are_the_verdict_table(module):
    """The CUE copy of `ADAPTIVE` and its constants, so a change to one fails here."""
    result = lifecycle_export(
        module,
        "[pubsubAdaptiveKinds, pubsubAdaptiveReplayDelayMillis, "
        "pubsubAdaptiveMaxOutstanding]",
    )
    assert result.returncode == 0, result.stderr
    kinds, delay, outstanding = json.loads(result.stdout)
    assert kinds == {
        name: {
            "scheduler": size["scheduler"],
            "initial": size["initial"],
            "recovery": size["recovery"],
            "holdMillis": size["hold_millis"],
        }
        for name, size in pubsub_verdict.ADAPTIVE.items()
    }
    assert delay == pubsub_verdict.ADAPTIVE_REPLAY_DELAY_MILLIS
    assert outstanding == pubsub_verdict.ADAPTIVE_MAX_OUTSTANDING


REACTIVE_KINDS = [
    name
    for name, size in pubsub_verdict.ADAPTIVE.items()
    if size["scheduler"] == "reactive"
]
ADAPTIVE_KINDS = [
    name
    for name, size in pubsub_verdict.ADAPTIVE.items()
    if size["scheduler"] == "adaptive"
]


@pytest.mark.parametrize("name", REACTIVE_KINDS)
def test_a_reactive_pubsub_kind_renders_standalone_reactive_manifests(module, name):
    """Both manifests run one reactive job; only the TaskManagers differ."""
    sizes = pubsub_verdict.ADAPTIVE[name]
    result = lifecycle_export(
        module,
        "[application, upgradeApplication]",
        "pubsub-recovery",
        run_id="proposal-2055",
        application_image=GAR + "pubsub-recovery@" + SYNTHETIC_DIGEST,
        pubsub_trial=name,
        pubsub_restore_deduplication="true",
    )
    assert result.returncode == 0, result.stderr
    initial, recovery = json.loads(result.stdout)
    for app, replicas in ((initial, sizes["initial"]), (recovery, sizes["recovery"])):
        spec = app["spec"]
        assert spec["mode"] == "standalone"
        assert spec["flinkConfiguration"]["scheduler-mode"] == "reactive"
        assert spec["taskManager"]["replicas"] == replicas
        assert spec["job"]["parallelism"] == sizes["initial"]
        # The Operator's standalone UserLibMountDecorator skips a JobManager that
        # already mounts under /opt/flink/usrlib; otherwise it hides the image's JAR.
        jm = spec["jobManager"]["podTemplate"]["spec"]
        assert [v["name"] for v in jm["volumes"]] == ["usrlib-guard"]
        (mount,) = jm["containers"][0]["volumeMounts"]
        assert mount["mountPath"].startswith("/opt/flink/usrlib/")
        assert "volumes" not in spec["podTemplate"]["spec"]
        assert "volumes" not in spec["taskManager"]["podTemplate"]["spec"]
        assert spec["job"]["args"][-4:] == [
            "--restore-deduplication=true",
            f"--replay-delay-millis={pubsub_verdict.ADAPTIVE_REPLAY_DELAY_MILLIS}",
            f"--max-outstanding-messages={pubsub_verdict.ADAPTIVE_MAX_OUTSTANDING}",
            f"--hold-after-checkpoint-millis={sizes['hold_millis']}",
        ]
    # The patch the exercise applies is all that separates them.
    recovery["spec"]["taskManager"]["replicas"] = sizes["initial"]
    assert recovery == initial


@pytest.mark.parametrize("name", ADAPTIVE_KINDS)
def test_an_adaptive_pubsub_kind_renders_native_adaptive_manifests(module, name):
    """Both manifests run one native job; only the parallelism override differs."""
    sizes = pubsub_verdict.ADAPTIVE[name]
    result = lifecycle_export(
        module,
        "[application, upgradeApplication]",
        "pubsub-recovery",
        run_id="proposal-2126",
        application_image=GAR + "pubsub-recovery@" + SYNTHETIC_DIGEST,
        pubsub_trial=name,
        pubsub_restore_deduplication="true",
    )
    assert result.returncode == 0, result.stderr
    initial, recovery = json.loads(result.stdout)
    for app in (initial, recovery):
        spec = app["spec"]
        config = spec["flinkConfiguration"]
        assert spec["mode"] == "native"
        assert config["jobmanager.scheduler"] == "adaptive"
        assert "scheduler-mode" not in config
        assert spec["taskManager"]["replicas"] == sizes["initial"]
        assert spec["job"]["parallelism"] == sizes["initial"]
        assert "volumes" not in spec["jobManager"]["podTemplate"]["spec"]
        assert spec["job"]["args"][-4:] == [
            "--restore-deduplication=true",
            f"--replay-delay-millis={pubsub_verdict.ADAPTIVE_REPLAY_DELAY_MILLIS}",
            f"--max-outstanding-messages={pubsub_verdict.ADAPTIVE_MAX_OUTSTANDING}",
            f"--hold-after-checkpoint-millis={sizes['hold_millis']}",
        ]
    assert OVERRIDES not in initial["spec"]["flinkConfiguration"]
    override = recovery["spec"]["flinkConfiguration"].pop(OVERRIDES)
    assert override == f"{relay_vertex_id()}:{sizes['recovery']}"
    # The patch the exercise applies is all that separates them.
    assert recovery == initial


OVERRIDES = "pipeline.jobvertex-parallelism-overrides"


def relay_vertex_id():
    """The vertex id the relay pins, which a relay test holds to its job graph."""
    source = (
        KUBERNETES
        / "apps/pubsub/src/main/java/io/github/flink/gcp/connector"
        / "tier3/pubsub/PubSubRecoveryJob.java"
    ).read_text()
    (value,) = re.findall(r'DATASTREAM_VERTEX_ID = "([0-9a-f]{32})";', source)
    return value


@pytest.mark.parametrize(
    "tag, value, field",
    [
        # With the setting off, the trial would show only the duplicate.
        ("pubsub_restore_deduplication", "false", "restoreDeduplication"),
        # Only the DataStream entry point chains the holding observer.
        ("pubsub_entry_point", "table", "entryPoint"),
    ],
)
@pytest.mark.parametrize("name", list(pubsub_verdict.ADAPTIVE))
def test_an_adaptive_pubsub_kind_refuses_what_cannot_show_its_case(
    module, name, tag, value, field
):
    """The delivery refuses it as well as the plan does."""
    overrides = {"pubsub_restore_deduplication": "true", tag: value}
    result = lifecycle_export(
        module,
        "[application, upgradeApplication]",
        "pubsub-recovery",
        run_id="proposal-2055",
        application_image=GAR + "pubsub-recovery@" + SYNTHETIC_DIGEST,
        pubsub_trial=name,
        **overrides,
    )
    assert result.returncode != 0
    assert field in result.stderr, result.stderr


def test_a_native_pubsub_kind_keeps_native_mode_and_the_default_flow_control(module):
    result = lifecycle_export(
        module,
        "[application, upgradeApplication]",
        "pubsub-recovery",
        application_image=GAR + "pubsub-recovery@" + SYNTHETIC_DIGEST,
        pubsub_trial="tm-replacement",
    )
    assert result.returncode == 0, result.stderr
    for app in json.loads(result.stdout):
        assert app["spec"]["mode"] == "native"
        assert "scheduler-mode" not in app["spec"]["flinkConfiguration"]
        assert "volumes" not in app["spec"]["jobManager"]["podTemplate"]["spec"]
        assert app["spec"]["job"]["args"][-4:] == [
            "--restore-deduplication=false",
            "--replay-delay-millis=0",
            "--max-outstanding-messages=1000",
            "--hold-after-checkpoint-millis=0",
        ]


@pytest.mark.parametrize("trial", ["rescale", "none", ""])
def test_pubsub_rejects_an_unknown_trial(module, trial):
    """Chosen by comparisons, an unknown trial rendered both applications at one."""
    result = lifecycle_export(
        module,
        "[application, upgradeApplication]",
        "pubsub-recovery",
        application_image=GAR + "pubsub-recovery@" + SYNTHETIC_DIGEST,
        pubsub_trial=trial,
    )
    assert result.returncode != 0
    assert not result.stdout.strip()
    # The kind table refuses it, as it refuses an unknown Cloud Tasks trial.
    assert re.search(rf'undefined field: "?{re.escape(trial)}"?:', result.stderr)


def test_pubsub_access_probe_pod_from_real_cue(module, monkeypatch):
    """The probe runs the packaged program, as the workload, outside Spot."""
    from flink_tier3.bundle import package_sources
    from flink_tier3.pubsub.access import probe_spec
    from flink_tier3.pubsub.resources import ResourcePlan

    root = module.parent
    module.rename(root / "kubernetes")
    monkeypatch.setattr(repository, "ROOT", root)
    monkeypatch.setenv("GOMAXPROCS", "2")
    bundle = pubsub_plan.prepare(
        run_id="proposal-1581",
        nonce="a" * 32,
        started_at="2026-09-21T00:00:00Z",
        expires_at="2026-09-21T01:00:00Z",
        active_seconds=pubsub_plan.ACTIVE_SECONDS,
        revision="b" * 40,
        application_image=GAR + "pubsub-recovery@" + SYNTHETIC_DIGEST,
        trial={
            "version": 6,
            "trial": "jm-replacement",
            "entry_point": "datastream",
            "records_per_subscription": 1000,
            "restore_deduplication": False,
        },
    )
    probe = bundle["delivery"]["probe"]
    assert probe["kind"] == "Pod" and probe["apiVersion"] == "v1"
    assert probe["metadata"]["name"] == "proposal-1581-access-probe"
    assert probe["metadata"]["namespace"] == "tier3-pubsub"
    assert probe["metadata"]["labels"]["flink-gcp.io/run-id"] == "proposal-1581"
    assert probe["metadata"]["annotations"]["flink-gcp.io/approval"] == "a" * 32
    spec = probe["spec"]
    assert spec["serviceAccountName"] == "pubsub"
    assert spec["restartPolicy"] == "Never"
    assert spec["activeDeadlineSeconds"] == 900
    assert not spec.get("nodeSelector", {}).get("cloud.google.com/gke-spot")
    (container,) = spec["containers"]
    assert container["image"] == bundle["proposal"]["images"]["supervisor"]
    assert container["command"] == [
        "python3",
        "-I",
        "-c",
        package_sources()["pubsub/probe.py"],
    ]
    plan = ResourcePlan("proposal-1581", "a" * 32)
    window = SimpleNamespace(
        started=timestamp("2026-09-21T00:00:00Z"),
        cleanup_at=timestamp("2026-09-21T00:45:00Z"),
    )
    tested = json.loads(container["args"][0])
    assert tested == probe_spec(plan, window)
    assert tested["deadline"] == timestamp("2026-09-21T00:15:00Z")
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert spec["securityContext"]["runAsNonRoot"] is True
    # Shaped and placed as the supervisor: its resources, and Spot excluded
    # by affinity in every term rather than merely not selected.
    verify_pod(
        probe,
        "supervisor",
        bundle["proposal"]["images"]["supervisor"],
        POD_RESOURCES["supervisor"],
        spot=False,
    )


@pytest.mark.parametrize(
    "trial,entry_point", [("rescale-out", "datastream"), ("rescale-in", "table")]
)
def test_pubsub_approval_binds_the_real_cue_delivery(
    module, monkeypatch, trial, entry_point
):
    """The dispatch's assembled approval binds the delivery it was built from."""
    from flink_tier3 import lifecycle

    root = module.parent
    module.rename(root / "kubernetes")
    monkeypatch.setattr(repository, "ROOT", root)
    monkeypatch.setenv("GOMAXPROCS", "2")
    pubsub_trial = {
        "version": 6,
        "trial": trial,
        "entry_point": entry_point,
        "records_per_subscription": 1000,
        "restore_deduplication": False,
    }
    application_image = GAR + "pubsub-recovery@" + SYNTHETIC_DIGEST
    bundle = pubsub_plan.prepare(
        run_id="proposal-1429",
        nonce="a" * 32,
        started_at="2026-09-21T00:00:00Z",
        expires_at="2026-09-21T01:00:00Z",
        active_seconds=pubsub_plan.ACTIVE_SECONDS,
        revision="b" * 40,
        application_image=application_image,
        trial=pubsub_trial,
    )
    approval = lifecycle.pubsub_approval(
        run_id="proposal-1429",
        nonce="a" * 32,
        sha="b" * 40,
        trial=pubsub_trial,
        proposal=bundle["proposal"],
        namespaces={
            namespace: {
                "uid": namespace + "-uid",
                "quota_uid": namespace + "-quota",
                "hard": {"pods": "0", "persistentvolumeclaims": "0"},
            }
            for namespace in ("tier3-pubsub", "tier3-system")
        },
        operator_uid="operator-uid",
        baseline=["operator-uid"],
        images={
            "operator": GAR + "operator@" + SYNTHETIC_DIGEST,
            **bundle["proposal"]["images"],
        },
        owner={
            "nonce": "a" * 32,
            "kind": "run",
            "run_id": "proposal-1429",
            "github_run_id": "123",
            "attempt": "1",
            "sha": "b" * 40,
        },
        actor="octocat",
    )
    validate_approval(approval)
    expected_delivery = json.loads(json.dumps(bundle["delivery"]))
    delivery = bind(
        Approval.from_dict(approval),
        bundle["delivery"],
        timestamp("2026-09-21T00:10:00Z"),
    )
    expected_delivery["config"]["data"]["approval.json"] = delivery["config"]["data"][
        "approval.json"
    ]
    expected_delivery["supervisor"]["spec"]["activeDeadlineSeconds"] = 2820
    assert delivery == expected_delivery
    data = delivery["config"]["data"]
    assert json.loads(data["approval.json"])["pubsub_trial"] == pubsub_trial
    assert json.loads(data["proposal.json"]) == bundle["proposal"]
    assert {
        name: data["flink_tier3_" + name.replace("/", ".")]
        for name in delivered_sources()
    } == delivered_sources()


@pytest.mark.parametrize(
    "trial,initial_parallelism,recovery_parallelism,phase",
    [
        ("jm-replacement", 2, 2, "initial"),
        ("tm-replacement", 2, 2, "initial"),
        ("rescale-out", 1, 2, "upgrade"),
        ("rescale-in", 2, 1, "upgrade"),
    ],
)
@pytest.mark.parametrize("records", [3, 1001, 10000])
@pytest.mark.parametrize("entry_point", ["datastream", "table"])
def test_pubsub_proposal_from_real_cue(
    module,
    monkeypatch,
    trial,
    initial_parallelism,
    recovery_parallelism,
    phase,
    records,
    entry_point,
):
    root = module.parent
    module.rename(root / "kubernetes")
    monkeypatch.setattr(repository, "ROOT", root)
    monkeypatch.setenv("GOMAXPROCS", "2")
    inputs = {
        "run_id": "proposal-1361",
        "nonce": "a" * 32,
        "started_at": "2026-09-21T00:00:00Z",
        "expires_at": "2026-09-21T01:00:00.000Z",
        "active_seconds": 3420,
        "revision": "b" * 40,
        "application_image": GAR + "pubsub-recovery@" + SYNTHETIC_DIGEST,
        "trial": {
            "version": 6,
            "trial": trial,
            "entry_point": entry_point,
            "records_per_subscription": records,
            "restore_deduplication": False,
        },
    }
    bundle = pubsub_plan.prepare(**inputs)
    proposal = bundle["proposal"]
    initial, recovery = bundle["application"], bundle["recovery_application"]
    assert proposal["approved"] is False
    assert proposal["cost"]["kind"] == "planning-estimate"
    assert proposal["cost"]["usd"] == "1.987"
    assert proposal["application_sha256"] == digest(initial)
    assert proposal["recovery_application_sha256"] == digest(recovery)
    assert proposal["supervisor_sha256"] == digest(bundle["delivery"]["supervisor"])
    assert initial["spec"]["job"]["parallelism"] == initial_parallelism
    assert recovery["spec"]["job"]["parallelism"] == recovery_parallelism
    assert f"--phase={phase}" in recovery["spec"]["job"]["args"]
    for app in (initial, recovery):
        # The entry point is fixed across phases: state from one variant cannot restore the other.
        assert app["spec"]["job"]["args"][5] == f"--entry-point={entry_point}"
    assert (initial == recovery) == trial.endswith("replacement")
    for app in (initial, recovery):
        assert app["metadata"]["namespace"] == "tier3-pubsub"
        assert app["metadata"]["annotations"]["flink-gcp.io/approval"] == "a" * 32
        assert_pod_policy(app["spec"])
        assert app["spec"]["serviceAccount"] == "pubsub"
    assert proposal["input"]["messages"] == 2 * records
    assert proposal["input"]["payload_bytes"] == sum(
        len(f"v1|proposal-1361|{i}|{n}".encode())
        for i in range(2)
        for n in range(records)
    )
    cohorts = proposal["input"]["cohorts_per_subscription"]
    assert list(cohorts) == ["before_checkpoint", "after_checkpoint", "after_recovery"]
    ranges = list(cohorts.values())
    assert ranges[0]["start"] == 0
    for previous, cohort in itertools.pairwise(ranges):
        assert previous["start"] + previous["count"] == cohort["start"]
    assert ranges[-1]["start"] + ranges[-1]["count"] == records
    assert min(cohort["count"] for cohort in ranges) > 0
    assert proposal["cleanup_at"] == "2026-09-21T00:45:00Z"
    assert proposal["limits"] == {"seconds": 3600, "cleanup_seconds": 900}
    data = bundle["delivery"]["config"]["data"]
    assert json.loads(data["proposal.json"]) == proposal
    assert json.loads(data["application.json"]) == initial
    assert json.loads(data["upgrade-application.json"]) == recovery
    assert json.loads(data["approval.json"]) == {}
    with pytest.raises(Failure):
        validate_approval(proposal)
    assert len(proposal["resources"]["topics"]) == 3
    assert len(proposal["resources"]["subscriptions"]) == 3
    assert len(proposal["resources"]["grants"]) == 6

    # Reuse actual rendered objects to probe drift, without a hand-written CUE fake.
    rendered = [initial, recovery, bundle["delivery"]]
    for path, value in (
        ((0, "metadata", "namespace"), "tier3-smoke"),
        ((1, "metadata", "annotations", "flink-gcp.io/approval"), "foreign"),
        ((0, "spec", "taskManager", "replicas"), 3),
        ((1, "spec", "job", "args"), []),
        ((0, "spec", "podTemplate", "spec", "initContainers"), [{"name": "extra"}]),
        (
            (
                2,
                "supervisor",
                "spec",
                "template",
                "spec",
                "containers",
                0,
                "resources",
                "requests",
                "cpu",
            ),
            "2",
        ),
        ((2, "config", "data", "approval.json"), '{"approved":true}'),
        ((2, "supervisor", "spec", "parallelism"), 2),
        ((2, "supervisor", "spec", "activeDeadlineSeconds"), 3600),
        ((2, "config", "data", "upgrade-application.json"), "{}"),
    ):
        changed = copy.deepcopy(rendered)
        target = changed
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value
        if path[0] in (0, 1):
            # Real CUE drift changes the standalone and embedded manifest together.
            key = "application.json" if path[0] == 0 else "upgrade-application.json"
            changed[2]["config"]["data"][key] = json.dumps(changed[path[0]])
        monkeypatch.setattr(
            pubsub_plan, "render", lambda *a, result=changed, **kw: result
        )
        with pytest.raises(Failure):
            pubsub_plan.prepare(**inputs)
