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


"""The BigQuery recovery delivery, rendered from disposable manifests."""

import json
import shutil

import pytest
import yaml
from flink_tier3 import repository
from flink_tier3.bigquery import plan as bigquery_plan
from flink_tier3.bundle import delivered_sources
from flink_tier3.common import timestamp
from flink_tier3.lifecycle import bind
from flink_tier3.model import Approval, validate_approval
from flink_tier3.policy import GAR
from test_manifests import (
    KUBERNETES,
    SYNTHETIC_DIGEST,
    cue,
    leaf,
    workload,
)
from test_manifests import module as module  # noqa: PLC0414


def bigquery_leaf(module, **changes):
    inputs = {
        "id": "bq-contract",
        "expiresAt": "2026-09-20T08:00:00Z",
        "namespace": "tier3-bigquery",
        "image": "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/bigquery-recovery@"
        + SYNTHETIC_DIGEST,
        "phase": "initial",
        "mode": "EO",
        "destinations": 10,
        "parallelism": 2,
        **changes,
    }
    path = leaf(module, "runs/bigquery", {"run": inputs})
    shutil.copyfile(
        KUBERNETES / "tests/fixtures/bigquery.cue", module / path / "delivery.cue"
    )
    return path


# The FILE_LOADS arguments at the package's defaults, the connector's except the uploader, which
# the package keeps off; the Storage Write modes take none.
FILE_LOADS_DEFAULT_ARGS = [
    "--staging-format",
    "AVRO",
    "--max-concurrent-checkpoint-finalizations",
    "1",
    "--max-concurrent-destinations",
    "8",
    "--max-staging-file-bytes",
    "16777216",
    "--max-open-destinations",
    "16",
    "--background-staging-upload-enabled",
    "false",
]
BIGQUERY_MODE_ARGS = {"ALO": [], "EO": [], "FILE_LOADS": FILE_LOADS_DEFAULT_ARGS}
BIGQUERY_MODE_INTERVALS = {"ALO": "30 s", "EO": "30 s", "FILE_LOADS": "120 s"}


@pytest.mark.parametrize(
    "mode,records", [("ALO", 28800), ("EO", 1843200), ("FILE_LOADS", 1843200)]
)
@pytest.mark.parametrize("destinations", [10, 50])
def test_bigquery_initial_and_upgrade_preserve_the_trial(
    module, mode, records, destinations
):
    from flink_tier3.bigquery.oracle import Trial

    applications = []
    for phase in ("initial", "upgrade"):
        path = bigquery_leaf(module, mode=mode, destinations=destinations, phase=phase)
        rendered = cue(module, "cmd", "render", path)
        assert rendered.returncode == 0, rendered.stderr
        [app] = list(yaml.safe_load_all(rendered.stdout))
        applications.append(app)
        assert app["metadata"]["name"] == "bq-contract"
        assert app["metadata"]["namespace"] == "tier3-bigquery"
        assert app["metadata"]["labels"]["flink-gcp.io/run-id"] == "bq-contract"
        assert (
            app["metadata"]["annotations"]["flink-gcp.io/expires-at"]
            == "2026-09-20T08:00:00Z"
        )
        spec = app["spec"]
        assert spec["flinkVersion"] == "v2_2"
        assert spec["serviceAccount"] == "bigquery"
        assert spec["mode"] == "native"
        assert spec["image"].endswith("bigquery-recovery@" + SYNTHETIC_DIGEST)
        assert spec["job"] == {
            "jarURI": "local:///opt/flink/usrlib/bigquery-recovery.jar",
            "entryClass": "io.github.flink.gcp.connector.tier3.bigquery.BigQueryRecoveryJob",
            "parallelism": 2,
            "state": "running",
            "upgradeMode": "savepoint",
            "allowNonRestoredState": False,
            "args": [
                "--run-id",
                "bq-contract",
                "--phase",
                phase,
                "--mode",
                mode,
                "--destinations",
                str(destinations),
                "--records",
                str(records),
                "--bytes-per-second",
                "1048576",
                "--parallelism",
                "2",
                "--require-restored",
                str(phase == "upgrade").lower(),
                *BIGQUERY_MODE_ARGS[mode],
            ],
        }
        args = dict(
            zip(spec["job"]["args"][::2], spec["job"]["args"][1::2], strict=True)
        )
        # The rig's oracle learns FILE_LOADS separately (#1551); the render is checked here.
        if mode != "FILE_LOADS":
            trial = Trial(
                args["--run-id"],
                args["--mode"],
                int(args["--destinations"]),
                int(args["--records"]),
            )
            assert sum(trial.expected(i) for i in range(destinations)) == records
        config = spec["flinkConfiguration"]
        assert config["taskmanager.numberOfTaskSlots"] == "1"
        assert config["job.autoscaler.enabled"] == "false"
        assert (
            config["kubernetes.operator.job.upgrade.last-state-fallback.enabled"]
            == "false"
        )
        assert config["kubernetes.operator.snapshot.resource.enabled"] == "false"
        assert (
            config["execution.checkpointing.interval"] == BIGQUERY_MODE_INTERVALS[mode]
        )
        assert config["execution.checkpointing.max-concurrent-checkpoints"] == "1"
        assert config["execution.checkpointing.timeout"] == "120 s"
        assert config["execution.checkpointing.num-retained"] == "2"
        assert config["execution.checkpointing.tolerable-failed-checkpoints"] == "2"
        assert config["state.backend.type"] == "hashmap"
        assert config["execution.checkpointing.storage"] == "filesystem"
        assert config["high-availability.type"] == "kubernetes"
        assert config["restart-strategy.type"] == "fixed-delay"
        assert config["restart-strategy.fixed-delay.attempts"] == "3"
        assert config["restart-strategy.fixed-delay.delay"] == "10 s"
        for option, suffix in [
            ("execution.checkpointing.dir", "checkpoints"),
            ("execution.checkpointing.savepoint-dir", "savepoints"),
            ("high-availability.storageDir", "ha"),
        ]:
            assert (
                config[option]
                == f"gs://flink-gcp-tier3-bigquery/runs/bq-contract/{suffix}"
            )
        for manager, replicas in (("jobManager", 1), ("taskManager", 2)):
            assert spec[manager]["replicas"] == replicas
            assert spec[manager]["resource"] == {"cpu": 1, "memory": "2Gi"}
            template = spec[manager]["podTemplate"]
            assert (
                template["metadata"]["labels"]["flink-gcp.io/run-id"] == "bq-contract"
            )
            assert "volumes" not in template["spec"]
            [container] = template["spec"]["containers"]
            assert container["name"] == "flink-main-container"
            assert (
                container["resources"]["requests"]
                == container["resources"]["limits"]
                == {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "1Gi"}
            )
    # Updating the same deployment changes only the phase and restored-state requirement.
    for app in applications:
        app["spec"]["job"].pop("args")
    assert applications[0] == applications[1]


@pytest.mark.parametrize(
    "mode,records",
    [
        ("ALO", 100),
        ("ALO", 32768),
        ("EO", 100),
        ("EO", 2097152),
        ("FILE_LOADS", 100),
        ("FILE_LOADS", 2097152),
    ],
)
def test_bigquery_record_boundaries_render(module, mode, records):
    path = bigquery_leaf(module, mode=mode, destinations=50, records=records)
    result = cue(module, "cmd", "render", path)
    assert result.returncode == 0, result.stderr
    [app] = list(yaml.safe_load_all(result.stdout))
    args = app["spec"]["job"]["args"]
    assert args[args.index("--records") + 1] == str(records)


@pytest.mark.parametrize(
    "changes",
    [
        {"mode": "UNKNOWN"},
        {"destinations": 11},
        {"phase": "resume"},
        {"mode": "ALO", "records": 32769},
        {"mode": "EO", "records": 2097153},
        {"mode": "FILE_LOADS", "records": 2097153},
        {"mode": "ALO", "fileLoads": {"stagingFormat": "AVRO"}},
        {"mode": "EO", "fileLoads": {}},
        {"mode": "FILE_LOADS", "fileLoads": {"stagingFormat": "ORC"}},
        {
            "mode": "FILE_LOADS",
            "fileLoads": {"maxConcurrentCheckpointFinalizations": 0},
        },
        {
            "mode": "FILE_LOADS",
            "fileLoads": {"maxConcurrentCheckpointFinalizations": 9},
        },
        {"mode": "FILE_LOADS", "fileLoads": {"maxConcurrentDestinations": 65}},
        {"mode": "FILE_LOADS", "fileLoads": {"maxStagingFileBytes": 0}},
        {"mode": "FILE_LOADS", "fileLoads": {"maxOpenDestinations": 0}},
        {"mode": "FILE_LOADS", "fileLoads": {"maxOpenDestinations": 10001}},
        {"mode": "FILE_LOADS", "fileLoads": {"maxStagingFileBytes": 2**63}},
        {"mode": "FILE_LOADS", "fileLoads": {"maxPendingFiles": 16}},
        {"mode": "FILE_LOADS", "fileLoads": {"backgroundStagingUploadEnabled": "true"}},
        {
            "mode": "FILE_LOADS",
            "fileLoads": {
                "backgroundStagingUploadEnabled": True,
                "maxInFlightUploadOperations": 0,
            },
        },
        {"mode": "FILE_LOADS", "fileLoads": {"maxInFlightUploadOperations": 2}},
        {"destinations": 50, "records": 99},
        {"records": 0},
        {"records": "100"},
        {"namespace": "tier3-smoke"},
        {"id": "../another"},
        {
            "image": "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/flink@"
            + SYNTHETIC_DIGEST
        },
        {
            "image": "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/bigquery-recovery:latest"
        },
    ],
)
def test_bigquery_rejects_invalid_trial_inputs(module, changes):
    path = bigquery_leaf(module, **changes)
    result = cue(module, "cmd", "render", path)
    assert result.returncode != 0
    assert not result.stdout.strip()


@pytest.mark.parametrize(
    "changes,cause",
    [
        ({"mode": "ALO", "fileLoads": {"stagingFormat": "AVRO"}}, "explicit error"),
        ({"mode": "EO", "fileLoads": {}}, "explicit error"),
        ({"mode": "FILE_LOADS", "fileLoads": {"maxPendingFiles": 16}}, "not allowed"),
        (
            {"mode": "FILE_LOADS", "fileLoads": {"maxOpenDestinations": 10001}},
            "out of bound <=10000",
        ),
        (
            # The connector refuses the bound with the uploader off.
            {"mode": "FILE_LOADS", "fileLoads": {"maxInFlightUploadOperations": 2}},
            "not allowed",
        ),
    ],
)
def test_bigquery_file_loads_refusals_name_their_cause(module, changes, cause):
    # The knobs reach the render only through the arguments, so a refusal surfaces there.
    path = bigquery_leaf(module, **changes)
    result = cue(module, "cmd", "render", path)
    assert result.returncode != 0
    assert "spec.job.args" in result.stderr
    assert cause in result.stderr


def test_bigquery_file_loads_partial_knobs_keep_the_other_defaults(module):
    path = bigquery_leaf(
        module, mode="FILE_LOADS", fileLoads={"stagingFormat": "PARQUET"}
    )
    result = cue(module, "cmd", "render", path)
    assert result.returncode == 0, result.stderr
    [app] = list(yaml.safe_load_all(result.stdout))
    assert app["spec"]["job"]["args"][16:] == [
        "--staging-format",
        "PARQUET",
        *FILE_LOADS_DEFAULT_ARGS[2:],
    ]


def test_bigquery_file_loads_arguments_render_in_order(module):
    path = bigquery_leaf(
        module,
        mode="FILE_LOADS",
        fileLoads={
            "stagingFormat": "PARQUET",
            "maxConcurrentCheckpointFinalizations": 8,
            "maxConcurrentDestinations": 64,
            "maxStagingFileBytes": 33554432,
            "maxOpenDestinations": 50,
            "backgroundStagingUploadEnabled": True,
            "maxInFlightUploadOperations": 3,
        },
    )
    result = cue(module, "cmd", "render", path)
    assert result.returncode == 0, result.stderr
    [app] = list(yaml.safe_load_all(result.stdout))
    assert app["spec"]["job"]["args"][16:] == [
        "--staging-format",
        "PARQUET",
        "--max-concurrent-checkpoint-finalizations",
        "8",
        "--max-concurrent-destinations",
        "64",
        "--max-staging-file-bytes",
        "33554432",
        "--max-open-destinations",
        "50",
        "--background-staging-upload-enabled",
        "true",
        "--max-in-flight-upload-operations",
        "3",
    ]


def test_bigquery_file_loads_uploader_renders_its_default_bound(module):
    path = bigquery_leaf(
        module, mode="FILE_LOADS", fileLoads={"backgroundStagingUploadEnabled": True}
    )
    result = cue(module, "cmd", "render", path)
    assert result.returncode == 0, result.stderr
    [app] = list(yaml.safe_load_all(result.stdout))
    assert app["spec"]["job"]["args"][16:] == [
        *FILE_LOADS_DEFAULT_ARGS[:-1],
        "true",
        "--max-in-flight-upload-operations",
        "2",
    ]


@pytest.mark.parametrize(
    "override",
    [
        {"flinkVersion": "v2_3"},
        {"serviceAccount": "smoke"},
        {"job": {"parallelism": 1}},
        {"job": {"allowNonRestoredState": True}},
        {"job": {"upgradeMode": "stateless"}},
        {"taskManager": {"replicas": 1}},
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
def test_bigquery_delivery_cannot_change_fixed_topology(module, override):
    path = bigquery_leaf(module)
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
        {"serviceAccount": "smoke"},
        {"job": {"parallelism": 3}},
        {
            "image": "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/smoke@"
            + SYNTHETIC_DIGEST
        },
    ],
)
def test_bigquery_namespace_policy_without_application_package(module, override):
    # A plain resource prevents package constraints from masking run policy.
    document = workload()
    document["run"]["namespace"] = "tier3-bigquery"
    spec = document["delivery"]["resources"]["job"]["spec"]
    spec.update(
        flinkVersion="v2_2",
        serviceAccount="bigquery",
        image="us-central1-docker.pkg.dev/flink-gcp/flink-tier3/bigquery-recovery@"
        + SYNTHETIC_DIGEST,
    )
    spec["job"]["parallelism"] = 2
    path = leaf(module, "runs/plain-bigquery", document)
    valid = cue(module, "cmd", "render", path)
    assert valid.returncode == 0, valid.stderr
    [app] = list(yaml.safe_load_all(valid.stdout))
    assert app["metadata"]["namespace"] == "tier3-bigquery"
    spec.update(override)
    leaf(module, "runs/plain-bigquery", document)
    invalid = cue(module, "cmd", "render", path)
    assert invalid.returncode != 0, invalid.stdout
    assert not invalid.stdout.strip()


def bigquery_proposal(module, *tags):
    return cue(
        module,
        "export",
        "./lifecycle",
        "--out",
        "json",
        "-e",
        "[application, upgradeApplication]",
        "-t",
        "scenario=bigquery-recovery",
        "-t",
        "run_id=proposal-1779",
        "-t",
        "nonce=" + "a" * 32,
        "-t",
        "expires_at=2026-09-21T01:30:00Z",
        "-t",
        "active_seconds=5220",
        "-t",
        "application_image=us-central1-docker.pkg.dev/flink-gcp/flink-tier3/bigquery-recovery@"
        + SYNTHETIC_DIGEST,
        *tags,
    )


@pytest.mark.parametrize(
    "fault,initial,upgrade",
    [
        (None, 2, 2),
        ("none", 2, 2),
        ("tm-replacement", 2, 2),
        ("rescale-out", 1, 2),
        ("rescale-in", 2, 1),
    ],
)
@pytest.mark.parametrize("mode", ["EO", "FILE_LOADS"])
def test_bigquery_fault_renders_each_phase_size(module, fault, initial, upgrade, mode):
    """A rescale moves the writers and TaskManagers between one and two."""
    tags = ["-t", "bigquery_mode=" + mode]
    if fault is not None:
        tags += ["-t", "bigquery_fault=" + fault]
    result = bigquery_proposal(module, *tags)
    assert result.returncode == 0, result.stderr
    first, second = json.loads(result.stdout)
    for app, size in ((first, initial), (second, upgrade)):
        assert app["spec"]["job"]["parallelism"] == size
        assert app["spec"]["taskManager"]["replicas"] == size
        assert app["spec"]["jobManager"]["replicas"] == 1
        args = app["spec"]["job"]["args"]
        assert args[args.index("--parallelism") + 1] == str(size)
    # Nothing else differs between the phases but the phase and restoration.
    expected = json.loads(json.dumps(first))
    expected["spec"]["job"].update(parallelism=upgrade)
    expected["spec"]["taskManager"]["replicas"] = upgrade
    args = expected["spec"]["job"]["args"]
    args[3] = "upgrade"
    args[args.index("--parallelism") + 1] = str(upgrade)
    args[args.index("--require-restored") + 1] = "true"
    assert second == expected


@pytest.mark.parametrize("fault", ["jm-replacement", "rescale", ""])
def test_bigquery_rejects_an_unknown_fault(module, fault):
    result = bigquery_proposal(module, "-t", "bigquery_fault=" + fault)
    assert result.returncode != 0
    assert not result.stdout.strip()


@pytest.mark.parametrize(
    "mode,records", [("ALO", 28800), ("EO", 1843200), ("FILE_LOADS", 1843200)]
)
@pytest.mark.parametrize("destinations", [10, 50])
def test_bigquery_proposal_delivery_binds_both_phases(
    module, mode, records, destinations
):
    result = cue(
        module,
        "export",
        "./lifecycle",
        "--out",
        "json",
        "-e",
        "[application, upgradeApplication, delivery.resources]",
        "-t",
        "scenario=bigquery-recovery",
        "-t",
        "run_id=proposal-1312",
        "-t",
        "nonce=" + "a" * 32,
        "-t",
        "expires_at=2026-09-21T01:30:00Z",
        "-t",
        "active_seconds=5220",
        "-t",
        "bigquery_mode=" + mode,
        "-t",
        "bigquery_destinations=" + str(destinations),
        "-t",
        "application_image=us-central1-docker.pkg.dev/flink-gcp/flink-tier3/bigquery-recovery@"
        + SYNTHETIC_DIGEST,
    )
    assert result.returncode == 0, result.stderr
    initial, upgrade, delivery = json.loads(result.stdout)
    expected = json.loads(json.dumps(initial))
    expected["spec"]["job"]["args"][3] = "upgrade"
    expected["spec"]["job"]["args"][15] = "true"
    assert upgrade == expected
    assert initial["metadata"]["namespace"] == "tier3-bigquery"
    assert initial["metadata"]["annotations"]["flink-gcp.io/approval"] == "a" * 32
    assert initial["spec"]["job"]["args"] == [
        "--run-id",
        "proposal-1312",
        "--phase",
        "initial",
        "--mode",
        mode,
        "--destinations",
        str(destinations),
        "--records",
        str(records),
        "--bytes-per-second",
        "1048576",
        "--parallelism",
        "2",
        "--require-restored",
        "false",
        *BIGQUERY_MODE_ARGS[mode],
    ]
    assert initial["spec"]["job"]["parallelism"] == 2
    assert (
        initial["spec"]["flinkConfiguration"]["execution.checkpointing.interval"]
        == BIGQUERY_MODE_INTERVALS[mode]
    )
    assert initial["spec"]["taskManager"]["replicas"] == 2
    config = delivery["config"]
    assert config["immutable"] is True
    assert json.loads(config["data"]["approval.json"]) == {}
    assert json.loads(config["data"]["application.json"]) == initial
    assert json.loads(config["data"]["upgrade-application.json"]) == upgrade
    supervisor = delivery["supervisor"]
    assert supervisor["metadata"]["namespace"] == "tier3-system"
    assert supervisor["spec"]["activeDeadlineSeconds"] == 5220
    projection = supervisor["spec"]["template"]["spec"]["volumes"][0]["configMap"][
        "items"
    ]
    assert json.loads(config["data"]["proposal.json"]) == {}
    mounted = {item["path"]: config["data"][item["key"]] for item in projection}
    assert len(mounted) == len(projection)
    assert mounted.keys() == {
        "approval.json",
        "application.json",
        "upgrade-application.json",
        "proposal.json",
    } | {"flink_tier3/" + name for name in delivered_sources()}
    assert {"key": "proposal.json", "path": "proposal.json"} in projection
    assert {
        "key": "upgrade-application.json",
        "path": "upgrade-application.json",
    } in projection


@pytest.mark.parametrize("name", list(bigquery_plan.TRIALS))
def test_bigquery_prepare_accepts_real_cue_delivery(module, monkeypatch, name):
    """Every dispatchable trial, so the CUE and the plan cannot disagree on sizes."""
    trial = bigquery_plan.trial(name)
    mode, destinations = trial["mode"], trial["destinations"]
    root = module.parent
    module.rename(root / "kubernetes")
    monkeypatch.setattr(repository, "ROOT", root)
    monkeypatch.setenv("GOMAXPROCS", "2")
    bundle = bigquery_plan.prepare(
        run_id="proposal-1312",
        nonce="a" * 32,
        started_at="2026-09-21T00:00:00Z",
        expires_at="2026-09-21T01:30:00.000Z",
        active_seconds=5220,
        revision="b" * 40,
        application_image="us-central1-docker.pkg.dev/flink-gcp/flink-tier3/bigquery-recovery@"
        + SYNTHETIC_DIGEST,
        trial=trial,
    )
    proposal = bundle["proposal"]
    assert proposal["resources"]["trial"]["fault"] == trial.get("fault")
    assert proposal["approved"] is False
    assert proposal["resources"]["trial"]["mode"] == mode
    assert proposal["resources"]["trial"]["destinations"] == destinations
    assert (
        proposal["expires_at"]
        == bundle["application"]["metadata"]["annotations"]["flink-gcp.io/expires-at"]
    )
    assert json.loads(bundle["delivery"]["config"]["data"]["proposal.json"]) == proposal

    approval = {
        "version": 6,
        "scenario": "bigquery-recovery",
        "run_id": "proposal-1312",
        "nonce": "a" * 32,
        "sha": "b" * 40,
        "started_at": proposal["started_at"],
        "expires_at": proposal["expires_at"],
        "cleanup_at": proposal["cleanup_at"],
        "namespaces": {
            namespace: {
                "uid": namespace + "-uid",
                "quota_uid": namespace + "-quota",
                "hard": {"pods": "0", "persistentvolumeclaims": "0"},
            }
            for namespace in ("tier3-bigquery", "tier3-system")
        },
        "operator_uid": "operator-uid",
        "baseline_uids": ["operator-uid"],
        "application_sha256": proposal["application_sha256"],
        "upgrade_application_sha256": proposal["upgrade_application_sha256"],
        "images": {
            **proposal["images"],
            "operator": GAR + "operator@" + SYNTHETIC_DIGEST,
        },
        "lock_owner": {
            "nonce": "a" * 32,
            "kind": "run",
            "run_id": "proposal-1312",
            "github_run_id": "123",
            "attempt": "1",
            "sha": "b" * 40,
        },
        "bigquery_trial": {
            "mode": mode,
            "destinations": destinations,
        },
    }
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
    expected_delivery["supervisor"]["spec"]["activeDeadlineSeconds"] = 4620
    assert delivery == expected_delivery
    data = delivery["config"]["data"]
    assert (
        json.loads(data["approval.json"])["bigquery_trial"]
        == approval["bigquery_trial"]
    )
    assert {
        name: data["flink_tier3_" + name.replace("/", ".")]
        for name in delivered_sources()
    } == delivered_sources()
