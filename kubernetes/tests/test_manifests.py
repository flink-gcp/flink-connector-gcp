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

"""Exercise the CUE hierarchy with disposable manifests; never contact a cluster."""

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from flink_tier3.bundle import delivered_sources
from flink_tier3.exercise import DEFAULT_FAULT, FAULTS
from flink_tier3.policy import GAR

KUBERNETES = Path(__file__).resolve().parents[1]
CUE = shutil.which("cue")
ENV = {**os.environ, "GOMAXPROCS": "2"}
SCENARIOS = ("smoke",)
CLOUDTASKS_IMAGE = (
    "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/cloudtasks-recovery"
)
SYNTHETIC_DIGEST = "sha256:" + "a" * 64


@pytest.fixture
def module(tmp_path):
    """A disposable copy of the CUE module, which a test may change freely.

    The per-connector test modules import it from here.
    """
    destination = tmp_path / "module"
    shutil.copytree(
        KUBERNETES, destination, ignore=shutil.ignore_patterns("tests", "target")
    )
    return destination


SAFE_TO_EVICT = "cluster-autoscaler.kubernetes.io/safe-to-evict"


def expected_pod_policy(manager=None):
    """The capacity class and eviction stance of one rendered pod template.

    Only Spot nodes carry `cloud.google.com/gke-spot`, so normal capacity is
    spelled by the label's absence rather than by `"false"`, which would match
    no node. Autopilot refuses an extended run time to a Spot Pod, so the
    annotation belongs only where it can take effect.
    """
    selector = {"kubernetes.io/arch": "amd64"}
    if manager == "taskManager":
        selector["cloud.google.com/gke-spot"] = "true"
    return selector, manager == "jobManager"


def assert_pod_policy(spec):
    """Every template the manifest declares, not only the ones it happens to.

    A comprehension that skips an absent manager asserts nothing when both are
    absent, which is how an annotation went unverified on the very templates
    it was added to.
    """
    present = [m for m in ("jobManager", "taskManager") if m in spec]
    assert present, "the manifest declares no manager template to check"
    run_id = spec["podTemplate"]["metadata"]["labels"]["flink-gcp.io/run-id"]
    for manager in [None, *present]:
        template = (spec if manager is None else spec[manager])["podTemplate"]
        selector, annotated = expected_pod_policy(manager)
        assert template["spec"]["nodeSelector"] == selector, manager
        assert template["metadata"]["labels"]["flink-gcp.io/run-id"] == run_id, manager
        annotations = template["metadata"].get("annotations", {})
        assert (annotations.get(SAFE_TO_EVICT) == "false") is annotated, manager


def cue(module, *arguments):
    assert CUE, (
        "Run this suite through just tier3-check to select the pinned CUE binary"
    )
    source = None
    if any(arg == "./lifecycle" or arg.startswith("./lifecycle/") for arg in arguments):
        arguments = (*arguments, "json:", "-")
        source = json.dumps({"packageSources": delivered_sources()})
    return subprocess.run(
        [CUE, "-C", str(module), *arguments],
        input=source,
        capture_output=True,
        text=True,
        env=ENV,
        timeout=60,
        check=False,
    )


def leaf(module, path, document):
    directory = module / path
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "fixture.cue").write_text("package tier3\n" + json.dumps(document))
    return "./" + path


def workload():
    return {
        "run": {"id": "smoke-001", "expiresAt": "2026-09-11T04:00:00Z"},
        "delivery": {
            "resources": {
                "job": {
                    "apiVersion": "flink.apache.org/v1beta1",
                    "kind": "FlinkDeployment",
                    "metadata": {"name": "smoke-001"},
                    "spec": {
                        "image": "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/smoke@sha256:"
                        + "a" * 64,
                        "serviceAccount": "smoke",
                        "job": {
                            "parallelism": 1,
                            "state": "running",
                            "upgradeMode": "savepoint",
                        },
                    },
                }
            }
        },
    }


def test_parent_metadata_and_run_policy_reach_the_leaf(module):
    path = leaf(module, "runs/first", workload())
    result = cue(module, "cmd", "render", path)
    assert result.returncode == 0, result.stderr
    [job] = list(yaml.safe_load_all(result.stdout))
    assert job["metadata"] == {
        "name": "smoke-001",
        "namespace": "tier3-smoke",
        "labels": {
            "app.kubernetes.io/part-of": "flink-tier3",
            "app.kubernetes.io/managed-by": "cue",
            "flink-gcp.io/run-id": "smoke-001",
        },
        "annotations": {"flink-gcp.io/expires-at": "2026-09-11T04:00:00Z"},
    }
    assert job["spec"]["flinkVersion"] == "v2_2"
    assert job["spec"]["job"]["allowNonRestoredState"] is False
    assert job["spec"]["podTemplate"]["spec"]["nodeSelector"] == {
        "kubernetes.io/arch": "amd64"
    }


@pytest.mark.parametrize(
    ("location", "value"),
    [
        (("run", "expiresAt"), "not-a-timestamp"),
        (("run", "id"), "../another-run"),
        (("run", "namespace"), "default"),
        (("delivery", "resources", "job", "metadata", "namespace"), "default"),
        (
            ("delivery", "resources", "job", "metadata", "labels"),
            {"app.kubernetes.io/managed-by": "helm"},
        ),
        (("delivery", "resources", "job", "spec", "image"), "flink:latest"),
        (("delivery", "resources", "job", "spec", "flinkVersion"), "v2_3"),
        (("delivery", "resources", "job", "spec", "flinkVersion"), "v2_1"),
        (("delivery", "resources", "job", "spec", "serviceAccount"), ""),
        (("delivery", "resources", "job", "spec", "job", "parallelism"), 0),
        (("delivery", "resources", "job", "spec", "job", "parallelism"), 3),
        (
            ("delivery", "resources", "job", "spec", "job", "allowNonRestoredState"),
            True,
        ),
        (("delivery", "resources", "job", "spec", "job", "upgradeMode"), "invalid"),
        (("delivery", "resources", "job", "spec", "job", "parallelism"), "1"),
        (
            ("delivery", "resources", "job", "spec", "taskManager"),
            {
                "podTemplate": {
                    "spec": {"nodeSelector": {"cloud.google.com/gke-spot": "false"}}
                }
            },
        ),
        (
            ("delivery", "resources", "job", "spec", "jobManager"),
            {
                "podTemplate": {
                    "spec": {"nodeSelector": {"cloud.google.com/gke-spot": "true"}}
                }
            },
        ),
    ],
)
def test_leaf_cannot_relax_ancestors_or_vendor_schema(module, location, value):
    document = workload()
    target = document
    for key in location[:-1]:
        target = target.setdefault(key, {})
    target[location[-1]] = value
    path = leaf(module, "runs/invalid", document)
    result = cue(module, "cmd", "render", path)
    assert result.returncode != 0, result.stdout
    assert not result.stdout.strip()


@pytest.mark.parametrize("key", ["serviceAccount", "job"])
def test_required_job_fields_cannot_be_omitted(module, key):
    document = workload()
    del document["delivery"]["resources"]["job"]["spec"][key]
    path = leaf(module, "runs/missing", document)
    assert cue(module, "cmd", "render", path).returncode != 0


@pytest.mark.parametrize("manager", ["jobManager", "taskManager"])
def test_manager_templates_cannot_override_common_pod_policy(module, manager):
    document = workload()
    document["delivery"]["resources"]["job"]["spec"][manager] = {
        "podTemplate": {"metadata": {"annotations": {"example.org/custom": "ok"}}}
    }
    valid = leaf(module, "runs/valid", document)
    result = cue(module, "cmd", "render", valid)
    assert result.returncode == 0, result.stderr
    template = document["delivery"]["resources"]["job"]["spec"][manager]["podTemplate"]
    other = "true" if manager == "jobManager" else "false"
    template["spec"] = {"nodeSelector": {"cloud.google.com/gke-spot": other}}
    invalid = leaf(module, "runs/invalid", document)
    assert cue(module, "cmd", "render", invalid).returncode != 0


def test_siblings_are_separate_instances(module):
    first = leaf(module, "runs/first", workload())
    other = workload()
    other["run"]["id"] = "second"
    other["delivery"]["resources"]["job"]["metadata"]["name"] = "second"
    second = leaf(module, "runs/second", other)
    for path, expected in [(first, "smoke-001"), (second, "second")]:
        result = cue(module, "cmd", "render", path)
        assert result.returncode == 0, result.stderr
        assert [
            x["metadata"]["name"] for x in list(yaml.safe_load_all(result.stdout))
        ] == [expected]


def test_native_kubernetes_schema_rejects_bad_type(module):
    document = {
        "delivery": {
            "resources": {
                "config": {
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": "config"},
                    "data": {"x": "valid"},
                }
            }
        }
    }
    valid = leaf(module, "bootstrap/valid", document)
    result = cue(module, "cmd", "render", valid)
    assert result.returncode == 0, result.stderr
    document["delivery"]["resources"]["config"]["data"]["x"] = 7
    path = leaf(
        module,
        "bootstrap/invalid",
        document,
    )
    assert cue(module, "cmd", "render", path).returncode != 0


def test_service_must_remain_cluster_ip(module):
    document = workload()
    document["delivery"]["resources"]["service"] = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "rest"},
        "spec": {"type": "ClusterIP", "ports": [{"port": 8081}]},
    }
    valid = leaf(module, "runs/valid", document)
    result = cue(module, "cmd", "render", valid)
    assert result.returncode == 0, result.stderr
    document["delivery"]["resources"]["service"]["spec"]["type"] = "LoadBalancer"
    path = leaf(module, "runs/invalid", document)
    assert cue(module, "cmd", "render", path).returncode != 0


@pytest.mark.parametrize(
    "api_version,kind,fields",
    [
        ("v1", "ConfigMap", {"data": {"example": "value"}}),
        (
            "apps/v1",
            "Deployment",
            {
                "spec": {
                    "selector": {"matchLabels": {"app": "example"}},
                    "template": {
                        "metadata": {"labels": {"app": "example"}},
                        "spec": {
                            "containers": [
                                {
                                    "name": "example",
                                    "image": "example.invalid/test:fixture",
                                }
                            ]
                        },
                    },
                }
            },
        ),
        (
            "batch/v1",
            "Job",
            {
                "spec": {
                    "template": {
                        "spec": {
                            "restartPolicy": "Never",
                            "containers": [
                                {
                                    "name": "example",
                                    "image": "example.invalid/test:fixture",
                                }
                            ],
                        }
                    }
                }
            },
        ),
    ],
)
def test_namespaced_kinds_are_accepted(module, api_version, kind, fields):
    obj = {
        "apiVersion": api_version,
        "kind": kind,
        "metadata": {"name": "example"},
        **fields,
    }
    path = leaf(
        module,
        "runs/example",
        {"run": workload()["run"], "delivery": {"resources": {"example": obj}}},
    )
    result = cue(module, "cmd", "render", path)
    assert result.returncode == 0, result.stderr
    [exported] = list(yaml.safe_load_all(result.stdout))
    assert exported["kind"] == kind
    assert exported["metadata"]["namespace"] == "tier3-smoke"
    assert exported["metadata"]["labels"]["flink-gcp.io/run-id"] == "smoke-001"
    for field, value in fields.items():
        assert exported[field] == value


@pytest.mark.parametrize(
    "obj",
    [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "example"}},
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": {"name": "example"},
            "rules": [],
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "metadata": {"name": "example"},
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "ClusterRole",
                "name": "example",
            },
            "subjects": [],
        },
        {
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "examples.example.org"},
            "spec": {
                "group": "example.org",
                "names": {"kind": "Example", "plural": "examples"},
                "scope": "Namespaced",
                "versions": [
                    {
                        "name": "v1",
                        "served": True,
                        "storage": True,
                        "schema": {"openAPIV3Schema": {"type": "object"}},
                    }
                ],
            },
        },
    ],
)
def test_cluster_scoped_objects_are_not_application_resources(module, obj):
    document = workload()
    document["delivery"]["resources"]["example"] = obj
    invalid = leaf(module, "runs/invalid", document)
    assert cue(module, "cmd", "render", invalid).returncode != 0


def test_delivery_renders_only_resources(module):
    document = workload()
    resources = document["delivery"]["resources"]
    resources["config"] = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "config"},
        "data": {"example": "value"},
    }
    document["delivery"]["description"] = "This is not a Kubernetes resource"
    path = leaf(module, "runs/example", document)
    first = cue(module, "cmd", "render", path)
    second = cue(module, "cmd", "render", path)
    assert first.returncode == second.returncode == 0, first.stderr + second.stderr
    assert first.stdout == second.stdout
    output = list(yaml.safe_load_all(first.stdout))
    assert sorted(obj["kind"] for obj in output) == ["ConfigMap", "FlinkDeployment"]
    assert all(
        set(obj) <= {"apiVersion", "kind", "metadata", "spec", "data"} for obj in output
    )


def application_leaf(module):
    document = {"run": workload()["run"]}
    document["run"]["image"] = workload()["delivery"]["resources"]["job"]["spec"][
        "image"
    ]
    path = leaf(module, "runs/example", document)
    shutil.copyfile(
        KUBERNETES / "tests/fixtures/application.cue",
        module / "runs/example/application.cue",
    )
    return path


def test_standard_application_defaults_and_environment_policy(module):
    path = application_leaf(module)
    result = cue(module, "cmd", "render", path)
    assert result.returncode == 0, result.stderr
    [app] = list(yaml.safe_load_all(result.stdout))
    assert app["apiVersion"] == "flink.apache.org/v1beta1"
    assert app["kind"] == "FlinkDeployment"
    assert app["metadata"]["name"] == "smoke-001"
    spec = app["spec"]
    assert spec["flinkVersion"] == "v2_2"
    assert spec["imagePullPolicy"] == "IfNotPresent"
    assert spec["job"] == {
        "jarURI": "local:///opt/flink/usrlib/example.jar",
        "parallelism": 1,
        "state": "running",
        "upgradeMode": "stateless",
        "allowNonRestoredState": False,
    }
    for manager in ("jobManager", "taskManager"):
        assert spec[manager]["resource"] == {"cpu": 1, "memory": "2Gi"}
    assert_pod_policy(spec)


def test_application_defaults_can_change_within_environment_policy(module):
    path = application_leaf(module)
    overrides = {
        "delivery": {
            "resources": {
                "app": {
                    "spec": {
                        "imagePullPolicy": "Always",
                        "jobManager": {"resource": {"cpu": 0.5, "memory": "1Gi"}},
                        "job": {
                            "parallelism": 2,
                            "state": "suspended",
                            "upgradeMode": "savepoint",
                        },
                    }
                }
            }
        }
    }
    target = module / "runs/example/overrides.cue"
    target.write_text("package tier3\n" + json.dumps(overrides))
    result = cue(module, "cmd", "render", path)
    assert result.returncode == 0, result.stderr
    [app] = list(yaml.safe_load_all(result.stdout))
    spec = app["spec"]
    assert spec["imagePullPolicy"] == "Always"
    assert spec["jobManager"]["resource"] == {"cpu": 0.5, "memory": "1Gi"}
    assert spec["job"]["parallelism"] == 2
    assert spec["job"]["state"] == "suspended"
    assert spec["job"]["upgradeMode"] == "savepoint"
    overrides["delivery"]["resources"]["app"]["spec"]["job"]["parallelism"] = 3
    target.write_text("package tier3\n" + json.dumps(overrides))
    invalid = cue(module, "cmd", "render", path)
    assert invalid.returncode != 0
    assert not invalid.stdout.strip()


def test_cue_sources_are_formatted():
    result = cue(KUBERNETES, "fmt", "--check", "./...")
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_lifecycle_job_embeds_reviewed_source_and_excludes_spot(
    module, tmp_path, scenario
):
    result = lifecycle_export(module, "delivery.resources", scenario)
    assert result.returncode == 0, result.stderr
    bundle = json.loads(result.stdout)
    config, job = bundle["config"], bundle["supervisor"]
    assert config["immutable"]
    assert config["data"]["flink_tier3_runtime.py"] == delivered_sources()["runtime.py"]
    # The delivery is a subset, and nothing else at this layer would notice if
    # it stopped being one: every other assertion compares it against itself.
    assert "flink_tier3_analyze.py" not in config["data"]
    assert job["metadata"]["namespace"] == "tier3-system"
    # No Pod is replaced: a disrupted supervisor ends the Job, and the runner
    # settles the run.
    assert job["spec"]["backoffLimit"] == 0
    assert "podFailurePolicy" not in job["spec"]
    assert [c["name"] for c in job["spec"]["template"]["spec"]["containers"]] == [
        "supervisor"
    ]
    assert job["spec"]["activeDeadlineSeconds"] == 3300
    pod = job["spec"]["template"]["spec"]
    mounted = {}
    for item in pod["volumes"][0]["configMap"]["items"]:
        assert "/" not in item["key"]
        if item["path"].startswith("flink_tier3/"):
            relative = item["path"].removeprefix("flink_tier3/")
            assert item["key"] == "flink_tier3_" + relative.replace("/", ".")
        path = tmp_path / item["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(config["data"][item["key"]])
        mounted[item["path"]] = path
    expected = {"approval.json", "application.json"} | {
        "flink_tier3/" + name for name in delivered_sources()
    }
    assert mounted.keys() == expected
    for name, path in mounted.items():
        if name not in (
            "approval.json",
            "application.json",
            "upgrade-application.json",
        ):
            assert (
                path.read_text()
                == delivered_sources()[name.removeprefix("flink_tier3/")]
            )
    # Run only the rendered payload, outside the checkout, with installed SDKs.
    result = subprocess.run(
        [sys.executable, "-m", "flink_tier3", "supervisor", "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert pod["serviceAccountName"] == "tier3-supervisor"
    assert pod["restartPolicy"] == "Never"
    assert pod["affinity"]["nodeAffinity"][
        "requiredDuringSchedulingIgnoredDuringExecution"
    ]["nodeSelectorTerms"] == [
        {
            "matchExpressions": [
                {
                    "key": "cloud.google.com/gke-spot",
                    "operator": "NotIn",
                    "values": ["true"],
                }
            ]
        }
    ]
    # Headroom, not appetite: a request the scheduler cannot fit into the
    # remains of a busy node, where a system Pod preempts the supervisor
    # within half a minute of it starting.
    assert pod["containers"][0]["resources"]["requests"] == {
        "cpu": "1",
        "memory": "2Gi",
        "ephemeral-storage": "128Mi",
    }
    # The namespace quota is derived from the policy shape, so the shape the
    # cluster actually gets has to be that one and not a second copy of it.
    from flink_tier3.policy import POD_RESOURCES

    for category in ("requests", "limits"):
        assert (
            pod["containers"][0]["resources"][category] == POD_RESOURCES["supervisor"]
        ), category
    app = json.loads(config["data"]["application.json"])
    assert app["metadata"]["namespace"] == "tier3-smoke"
    assert_pod_policy(app["spec"])


@pytest.mark.parametrize(
    "tag",
    [
        "active_seconds=3601",
        "run_id=../bad",
        "nonce=wrong",
        "expires_at=wrong",
        "scenario=unknown",
        # A near miss of cloudtasks-recovery.
        "scenario=cloudtasks",
    ],
)
def test_lifecycle_rejects_invalid_dispatch_inputs(module, tag):
    tags = {
        "run_id": "probe",
        "nonce": "a" * 32,
        "expires_at": "2026-09-20T00:00:00Z",
        "active_seconds": "3300",
    }
    key, value = tag.split("=", 1)
    tags[key] = value
    args = ["export", "./lifecycle", "-e", "delivery.resources", "--out", "json"]
    for key, value in tags.items():
        args.extend(["-t", key + "=" + value])
    result = cue(module, *args)
    assert result.returncode != 0


def lifecycle_tags(scenario="smoke", **overrides):
    """Dispatch tags for one scenario."""
    tags = {
        "run_id": "probe",
        "nonce": "a" * 32,
        "expires_at": "2026-09-20T00:00:00Z",
        "active_seconds": "3300",
        "scenario": scenario,
    }
    tags.update(overrides)
    return [arg for key, value in tags.items() for arg in ("-t", f"{key}={value}")]


def lifecycle_export(module, expression, scenario="smoke", **overrides):
    return cue(
        module,
        "export",
        "./lifecycle",
        "-e",
        expression,
        "--out",
        "json",
        *lifecycle_tags(scenario, **overrides),
    )


@pytest.mark.parametrize(
    "override",
    [
        {"metadata": {"namespace": "default"}},
        {"spec": {"image": "flink:latest"}},
        {"spec": {"job": {"parallelism": 3}}},
        {"spec": {"job": {"allowNonRestoredState": True}}},
        {
            "spec": {
                "taskManager": {
                    "podTemplate": {
                        "spec": {"nodeSelector": {"cloud.google.com/gke-spot": "false"}}
                    }
                }
            }
        },
    ],
)
def test_lifecycle_application_cannot_relax_shared_run_policy(module, override):
    # First prove this copied source is valid, then contradict one shared rule.
    args = [
        "export",
        "./lifecycle",
        "-e",
        "application",
        "--out",
        "json",
        *lifecycle_tags(),
    ]
    valid = cue(module, *args)
    assert valid.returncode == 0, valid.stderr
    leaf(module, "lifecycle", {"application": override})
    invalid = cue(module, *args)
    assert invalid.returncode != 0
    assert not invalid.stdout.strip()


@pytest.mark.parametrize(
    ("scenario", "active_seconds", "admitted"),
    [
        ("smoke", 3420, True),
        ("smoke", 3421, False),
        ("cloudtasks-recovery", 5220, True),
        ("cloudtasks-recovery", 5221, False),
    ],
)
def test_lifecycle_active_seconds_ceiling_depends_on_scenario(
    module, scenario, active_seconds, admitted
):
    images = {"cloudtasks-recovery": f"{CLOUDTASKS_IMAGE}@{SYNTHETIC_DIGEST}"}
    overrides = {"application_image": images[scenario]} if scenario in images else {}
    result = lifecycle_export(
        module,
        "delivery.resources",
        scenario,
        active_seconds=str(active_seconds),
        **overrides,
    )
    assert (result.returncode == 0) is admitted, result.stderr
    if admitted:
        job = json.loads(result.stdout)["supervisor"]
        assert job["spec"]["activeDeadlineSeconds"] == active_seconds
    else:
        assert not result.stdout.strip()
        assert f"invalid value {active_seconds} (out of bound" in result.stderr


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_lifecycle_configmap_keeps_data_headroom(module, scenario):
    """The delivered source shares one ConfigMap with the run's documents."""
    result = lifecycle_export(module, "delivery.resources", scenario)
    assert result.returncode == 0, result.stderr
    config = json.loads(result.stdout)["config"]
    # Kubernetes counts UTF-8 data values, not JSON quoting and escaping.
    # Reserve 256 KiB below its 1 MiB data ceiling; bound the wire form too.
    assert not config.get("binaryData")
    assert sum(len(value.encode("utf-8")) for value in config["data"].values()) < (
        768 * 1024
    )
    assert len(json.dumps(config).encode("utf-8")) < 1024 * 1024


def test_the_lifecycle_delivery_renders(module):
    result = lifecycle_export(module, "delivery.resources")
    assert result.returncode == 0, result.stderr
    documents = list(json.loads(result.stdout).values())
    assert [item["kind"] for item in documents] == ["ConfigMap", "Job"]
    app = json.loads(documents[0]["data"]["application.json"])
    assert app["metadata"]["labels"]["flink-gcp.io/run-id"] == "probe"
    assert (
        app["spec"]["podTemplate"]["metadata"]["labels"]["flink-gcp.io/run-id"]
        == "probe"
    )


def test_the_lifecycle_fault_kinds_are_the_exercise_table(module):
    """The CUE copy of `FAULTS`, so a kind added to or changed in one fails here."""
    result = lifecycle_export(module, "[faultKinds, defaultFault]")
    assert result.returncode == 0, result.stderr
    kinds, default = json.loads(result.stdout)
    assert list(kinds) == list(FAULTS)
    assert kinds == {
        name: {
            "rescale": fault.rescale,
            "initial": fault.initial,
            "upgrade": fault.upgrade,
        }
        for name, fault in FAULTS.items()
    }
    assert default == DEFAULT_FAULT


@pytest.mark.parametrize("name", list(FAULTS))
@pytest.mark.parametrize(
    "scenario,tag,image",
    [
        ("bigquery-recovery", "bigquery_fault", "bigquery-recovery"),
        ("cloudtasks-recovery", "cloudtasks_trial", "cloudtasks-recovery"),
        ("pubsub-recovery", "pubsub_trial", "pubsub-recovery"),
    ],
)
def test_every_scenario_renders_each_fault_kinds_sizes(
    module, name, scenario, tag, image
):
    """Each phase's job parallelism and TaskManagers are the kind's, in each scenario."""
    fault = FAULTS[name]
    # A BigQuery trial names no fault for the default kind.
    value = "none" if tag == "bigquery_fault" and name == DEFAULT_FAULT else name
    result = lifecycle_export(
        module,
        "[application, upgradeApplication]",
        scenario,
        run_id="proposal-1789",
        active_seconds="5220",
        application_image=GAR + image + "@" + SYNTHETIC_DIGEST,
        **{tag: value},
    )
    assert result.returncode == 0, result.stderr
    # A Pub/Sub replacement's recovery manifest stays in the initial phase.
    recovery = (
        "initial" if tag == "pubsub_trial" and fault.rescale is None else "upgrade"
    )
    for app, phase, rendered in zip(
        json.loads(result.stdout), ("initial", "upgrade"), ("initial", recovery)
    ):
        size = fault.parallelism(phase)
        assert app["spec"]["job"]["parallelism"] == size
        assert app["spec"]["taskManager"]["replicas"] == size
        flags = " ".join(app["spec"]["job"]["args"]).replace("=", " ")
        assert f"--phase {rendered} " in flags
        assert re.search(rf"--(sink-)?parallelism {size}( |$)", flags), flags
