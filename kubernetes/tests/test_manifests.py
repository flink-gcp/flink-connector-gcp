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

import copy
import json
import os
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
import yaml
from flink_tier3 import bigquery_bundle, bigquery_plan, pubsub_plan, workflow
from flink_tier3.bundle import delivered_sources
from flink_tier3.common import Failure, digest
from flink_tier3.model import validate_approval
from flink_tier3.policy import BIGQUERY_CEILINGS, GAR

KUBERNETES = Path(__file__).resolve().parents[1]
CUE = shutil.which("cue")
ENV = {**os.environ, "GOMAXPROCS": "2"}
SESSIONS = KUBERNETES / "lifecycle/sessions"
SCENARIOS = ("smoke", "generic-recovery", "cloudtasks")
CLOUDTASKS_IMAGE = (
    "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/cloudtasks-measurement"
)
SYNTHETIC_DIGEST = "sha256:" + "a" * 64


@pytest.fixture
def module(tmp_path):
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


def test_delivery_renders_only_resources_in_kind_then_key_order(module):
    document = workload()
    resources = document["delivery"]["resources"]
    resources["a-job"] = resources.pop("job")
    for key, name in (("z-config", "a-config"), ("m-config", "z-config")):
        resources[key] = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": name},
            "data": {"example": "value"},
        }
    document["delivery"]["description"] = "This is not a Kubernetes resource"
    path = leaf(module, "runs/example", document)
    first = cue(module, "cmd", "render", path)
    second = cue(module, "cmd", "render", path)
    assert first.returncode == second.returncode == 0, first.stderr + second.stderr
    assert first.stdout == second.stdout
    output = list(yaml.safe_load_all(first.stdout))
    assert [obj["kind"] for obj in output] == [
        "ConfigMap",
        "ConfigMap",
        "FlinkDeployment",
    ]
    assert [obj["metadata"]["name"] for obj in output] == [
        "z-config",
        "a-config",
        "smoke-001",
    ]
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


def render_deliveries(module):
    outputs = {}
    for tree in ("runs", "lifecycle"):
        for source in sorted((module / tree).rglob("delivery.cue")):
            for ancestor in source.parent.parents:
                if ancestor == module:
                    break
                assert not (ancestor / "delivery.cue").is_file(), (
                    f"Nested delivery {source.relative_to(module)} inside "
                    f"{ancestor.relative_to(module)}; use shared common.cue files "
                    "in ancestors and delivery.cue only at delivery leaves"
                )
            directory = "./" + source.parent.relative_to(module).as_posix()
            # Lifecycle inputs are dispatch-owned; only this synthetic render
            # supplies them, once per scenario, and the default scenario is the
            # directory's entry. Ordinary run deliveries retain their concrete
            # inputs.
            if tree == "lifecycle":
                for scenario in SCENARIOS:
                    result = cue(
                        module,
                        "export",
                        directory,
                        "-e",
                        "delivery.resources",
                        "--out",
                        "json",
                        *lifecycle_tags(scenario),
                    )
                    assert result.returncode == 0, (
                        f"{directory} ({scenario}): {result.stderr}"
                    )
                    outputs.setdefault(
                        directory,
                        yaml.safe_dump_all(json.loads(result.stdout).values()),
                    )
            else:
                result = cue(module, "cmd", "render", directory)
                assert result.returncode == 0, f"{directory}: {result.stderr}"
                outputs[directory] = result.stdout
    return outputs


def test_ci_renders_concrete_run_inputs(module):
    application_leaf(module)
    (module / "runs/example/application.cue").rename(
        module / "runs/example/delivery.cue"
    )
    outputs = render_deliveries(module)
    [app] = list(yaml.safe_load_all(outputs["./runs/example"]))
    assert app["metadata"]["name"] == "smoke-001"
    assert (
        app["metadata"]["annotations"]["flink-gcp.io/expires-at"]
        == workload()["run"]["expiresAt"]
    )
    assert (
        app["spec"]["image"]
        == workload()["delivery"]["resources"]["job"]["spec"]["image"]
    )


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


def test_committed_deliveries_render():
    if not render_deliveries(KUBERNETES):
        pytest.skip(
            "No committed application deliveries yet; synthetic fixtures cover rendering"
        )


@pytest.mark.parametrize("missing", ["id", "expiresAt", "image"])
def test_ci_rejects_missing_run_inputs(module, missing):
    application_leaf(module)
    directory = module / "runs/example"
    (directory / "application.cue").rename(directory / "delivery.cue")
    inputs = json.loads((directory / "fixture.cue").read_text().split("\n", 1)[1])
    del inputs["run"][missing]
    (directory / "fixture.cue").write_text("package tier3\n" + json.dumps(inputs))
    with pytest.raises(AssertionError, match="runs/example"):
        render_deliveries(module)


def test_ci_checks_added_delivery_directories_individually(module):
    for path, run_id in (("first", "first"), ("nested/second", "second")):
        inputs = {"run": {**workload()["run"], "id": run_id}}
        inputs["run"]["image"] = workload()["delivery"]["resources"]["job"]["spec"][
            "image"
        ]
        leaf(module, "runs/" + path, inputs)
        shutil.copyfile(
            KUBERNETES / "tests/fixtures/application.cue",
            module / "runs" / path / "delivery.cue",
        )
    outputs = render_deliveries(module)
    for path, run_id in (("./runs/first", "first"), ("./runs/nested/second", "second")):
        [app] = list(yaml.safe_load_all(outputs[path]))
        assert app["kind"] == "FlinkDeployment"
        assert app["metadata"]["name"] == run_id
    bad = module / "runs/nested/second/bad.cue"
    bad.write_text(
        "package tier3\ndelivery: resources: app: spec: job: parallelism: 3\n"
    )
    with pytest.raises(AssertionError, match="runs/nested/second"):
        render_deliveries(module)


def test_ci_rejects_a_delivery_inside_another_delivery(module):
    parent = leaf(
        module,
        "runs/parent",
        {
            "run": workload()["run"],
            "delivery": {
                "resources": {
                    "settings": {
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {"name": "settings"},
                    }
                }
            },
        },
    )
    directory = module / parent
    (directory / "fixture.cue").rename(directory / "delivery.cue")
    child = directory / "child"
    child.mkdir()
    shutil.copyfile(
        KUBERNETES / "tests/fixtures/application.cue", child / "delivery.cue"
    )
    with pytest.raises(AssertionError, match="Nested delivery"):
        render_deliveries(module)


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
    assert "flink_tier3_protocol_1246.toml" not in config["data"]
    assert job["metadata"]["namespace"] == "tier3-system"
    assert job["spec"]["backoffLimit"] == 0
    assert job["spec"]["activeDeadlineSeconds"] == 3300
    pod = job["spec"]["template"]["spec"]
    mounted = {}
    for item in pod["volumes"][0]["configMap"]["items"]:
        path = tmp_path / item["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(config["data"][item["key"]])
        mounted[item["path"]] = path
    expected = {"approval.json", "application.json"} | {
        "flink_tier3/" + name for name in delivered_sources()
    }
    if scenario == "generic-recovery":
        expected.add("upgrade-application.json")
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
    if scenario == "cloudtasks":
        assert_example_session_manifests(app)
        return
    if scenario == "generic-recovery":
        from flink_tier3.exercise import validate_manifests
        from flink_tier3.policy import RECOVERY

        validate_manifests(app, json.loads(config["data"]["upgrade-application.json"]))
        assert app["spec"]["job"]["args"][5] == str(RECOVERY["records"])
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


def session_cells(name="example-wiring"):
    return tomllib.loads((SESSIONS / f"{name}.toml").read_text())["cells"]


def lifecycle_tags(scenario="smoke", **overrides):
    """Dispatch tags for one scenario; cloudtasks carries the example session."""
    tags = {
        "run_id": "probe",
        "nonce": "a" * 32,
        "expires_at": "2026-09-20T00:00:00Z",
        "active_seconds": "3300",
        "scenario": scenario,
    }
    if scenario == "cloudtasks":
        tags.update(
            cells=json.dumps(session_cells()),
            flink_version="2.2.1",
            application_image=f"{CLOUDTASKS_IMAGE}@{SYNTHETIC_DIGEST}",
            target="https://ct1246.invalid/task",
        )
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


def assert_example_session_manifests(manifests):
    cells = session_cells()
    assert isinstance(manifests, list)
    assert [item["metadata"]["name"] for item in manifests] == [
        cell["id"] for cell in cells
    ]
    for manifest, cell in zip(manifests, cells, strict=True):
        assert manifest["kind"] == "FlinkDeployment"
        assert manifest["metadata"]["namespace"] == "tier3-cloudtasks"
        assert manifest["metadata"]["labels"]["flink-gcp.io/run-id"] == "probe"
        annotations = manifest["metadata"]["annotations"]
        assert annotations["flink-gcp.io/approval"] == "a" * 32
        assert annotations["flink-gcp.io/scenario"] == "cloudtasks"
        assert annotations["flink-gcp.io/cell"] == cell["id"]
        assert annotations["flink-gcp.io/expires-at"] == "2026-09-20T00:00:00Z"
        spec = manifest["spec"]
        assert spec["image"] == f"{CLOUDTASKS_IMAGE}@{SYNTHETIC_DIGEST}"
        assert spec["flinkVersion"] == "v2_2"
        assert spec["serviceAccount"] == "cloudtasks-benchmark"
        assert spec["mode"] == "native"
        assert spec["job"]["parallelism"] == cell["parallelism"]
        assert spec["job"]["upgradeMode"] == "stateless"
        assert spec["job"]["allowNonRestoredState"] is False
        assert spec["job"]["jarURI"] == (
            "local:///opt/flink/usrlib/cloudtasks-measurement.jar"
        )
        config = spec["flinkConfiguration"]
        assert config["taskmanager.numberOfTaskSlots"] == str(cell["parallelism"])
        assert config["execution.checkpointing.interval"] == (
            f"{cell['checkpoint_seconds']} s"
        )
        assert config["execution.checkpointing.timeout"] == "120 s"
        assert (
            config["execution.checkpointing.externalized-checkpoint-retention"]
            == "RETAIN_ON_CANCELLATION"
        )
        storage = (
            f"gs://flink-gcp-cloudtasks-benchmark/runs/probe/cells/{cell['id']}/state"
        )
        for option, suffix in [
            ("execution.checkpointing.dir", "checkpoints"),
            ("execution.checkpointing.savepoint-dir", "savepoints"),
            ("high-availability.storageDir", "ha"),
        ]:
            assert config[option] == f"{storage}/{suffix}"
        assert_pod_policy(spec)
        for manager in ("jobManager", "taskManager"):
            assert spec[manager]["replicas"] == 1
            pod = spec[manager]["podTemplate"]["spec"]
            [container] = pod["containers"]
            assert container["name"] == "flink-main-container"
            assert (
                container["resources"]["requests"] == container["resources"]["limits"]
            )
    [a, b] = manifests
    assert a["spec"]["job"]["args"] == [
        "--run-id",
        "probe",
        "--cell-id",
        "example-wiring-a",
        "--queue",
        "projects/flink-gcp/locations/us-central1/queues/ct1246-probe",
        "--target",
        "https://ct1246.invalid/task",
        "--arm",
        "UNNAMED",
        "--body-bytes",
        "1024",
        "--parallelism",
        "1",
        "--concurrency",
        "1",
        "--checkpoint-seconds",
        "1",
        "--channel-pool-size",
        "1",
        "--distribution",
        "even",
        "--offered-rate",
        "10",
        "--warmup-seconds",
        "60",
        "--observation-seconds",
        "180",
        "--record-limit",
        "2430",
        "--attempt-limit",
        "3645",
        "--control-delay-millis",
        "0",
        "--emit-attempts",
        "true",
    ]
    assert b["spec"]["job"]["args"][8:10] == ["--arm", "STAGED_HASH"]
    assert container_limits(a, "jobManager") == {
        "cpu": "1",
        "memory": "2Gi",
        "ephemeral-storage": "1Gi",
    }
    assert container_limits(a, "taskManager") == {
        "cpu": "1",
        "memory": "4Gi",
        "ephemeral-storage": "1Gi",
    }
    assert container_limits(b, "taskManager") == {
        "cpu": "2",
        "memory": "8Gi",
        "ephemeral-storage": "2Gi",
    }
    assert a["spec"]["jobManager"]["resource"] == {"cpu": 1, "memory": "2Gi"}
    assert a["spec"]["taskManager"]["resource"] == {"cpu": 1, "memory": "4Gi"}
    assert b["spec"]["taskManager"]["resource"] == {"cpu": 2, "memory": "8Gi"}


def container_limits(manifest, manager):
    [container] = manifest["spec"][manager]["podTemplate"]["spec"]["containers"]
    return container["resources"]["limits"]


def cloudtasks_workload(**spec):
    """A tier3-cloudtasks run leaf in the shape of workload()."""
    document = workload()
    document["run"]["namespace"] = "tier3-cloudtasks"
    job = document["delivery"]["resources"]["job"]
    job["spec"].update(
        {
            "image": f"{CLOUDTASKS_IMAGE}@{SYNTHETIC_DIGEST}",
            "serviceAccount": "cloudtasks-benchmark",
            "flinkVersion": "v1_20",
            "job": {"parallelism": 4, "upgradeMode": "stateless"},
        }
    )
    job["spec"].update(spec)
    return document


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
        ("generic-recovery", 3421, False),
        ("cloudtasks", 17820, True),
        ("cloudtasks", 17821, False),
    ],
)
def test_lifecycle_active_seconds_ceiling_depends_on_scenario(
    module, scenario, active_seconds, admitted
):
    result = lifecycle_export(
        module, "delivery.resources", scenario, active_seconds=str(active_seconds)
    )
    assert (result.returncode == 0) is admitted, result.stderr
    if admitted:
        job = json.loads(result.stdout)["supervisor"]
        assert job["spec"]["activeDeadlineSeconds"] == active_seconds
    else:
        assert not result.stdout.strip()


def test_cloudtasks_line_selects_image_package_and_flink_version(module):
    image = f"{CLOUDTASKS_IMAGE}-flink120@{SYNTHETIC_DIGEST}"
    result = lifecycle_export(
        module,
        "delivery.resources",
        "cloudtasks",
        flink_version="1.20.4",
        application_image=image,
    )
    assert result.returncode == 0, result.stderr
    manifests = json.loads(
        json.loads(result.stdout)["config"]["data"]["application.json"]
    )
    assert len(manifests) == 2
    for manifest in manifests:
        assert manifest["spec"]["flinkVersion"] == "v1_20"
        assert manifest["spec"]["image"] == image
        assert manifest["metadata"]["namespace"] == "tier3-cloudtasks"


@pytest.mark.parametrize(
    ("flink_version", "image"),
    [
        ("2.2.1", f"{CLOUDTASKS_IMAGE}-flink120@{SYNTHETIC_DIGEST}"),
        ("1.20.4", f"{CLOUDTASKS_IMAGE}@{SYNTHETIC_DIGEST}"),
        ("2.2.1", ""),
        ("2.2.1", f"{CLOUDTASKS_IMAGE}@sha256:{'a' * 63}"),
        ("2.2.1", f"{CLOUDTASKS_IMAGE}:latest"),
        (
            "2.2.1",
            f"us-central1-docker.pkg.dev/flink-gcp/flink-tier3/smoke@{SYNTHETIC_DIGEST}",
        ),
        ("2.1.0", f"{CLOUDTASKS_IMAGE}@{SYNTHETIC_DIGEST}"),
    ],
)
def test_cloudtasks_rejects_an_image_that_does_not_match_the_line(
    module, flink_version, image
):
    result = lifecycle_export(
        module,
        "delivery.resources",
        "cloudtasks",
        flink_version=flink_version,
        application_image=image,
    )
    assert result.returncode != 0
    assert not result.stdout.strip()


@pytest.mark.parametrize("target", ["", "http://ct1246.invalid/task", "ct1246.invalid"])
def test_cloudtasks_requires_an_https_target(module, target):
    result = lifecycle_export(module, "delivery.resources", "cloudtasks", target=target)
    assert result.returncode != 0
    assert not result.stdout.strip()


def mutate_cells(mutation):
    cells = session_cells()
    mutation(cells)
    return json.dumps(cells)


def _drop_second(cells):
    del cells[1]


def _duplicate_id(cells):
    cells[1]["id"] = cells[0]["id"]


def _duplicate_cell(cells):
    cells[1] = dict(cells[0])


def _unknown_key(cells):
    cells[0]["extra"] = 1


def _missing_key(cells):
    del cells[0]["emit_attempts"]


def _set(key, value):
    def apply(cells):
        cells[0][key] = value

    return apply


@pytest.mark.parametrize(
    "mutation",
    [
        lambda cells: cells.clear(),
        _duplicate_id,
        _duplicate_cell,
        _unknown_key,
        _missing_key,
        _set("id", "Example-Wiring-A"),
        _set("id", "a" * 41),
        _set("arm", "NAMED"),
        _set("body_bytes", 2048),
        _set("parallelism", 2),
        _set("concurrency", 8),
        _set("checkpoint_seconds", 5),
        _set("channel_pool_size", 2),
        _set("distribution", "uniform"),
        _set("offered_rate", 0),
        _set("offered_rate", 10001),
        _set("warmup_seconds", 121),
        _set("observation_seconds", 601),
        _set("record_limit", 10000001),
        _set("attempt_limit", 0),
        _set("control_delay_millis", 50),
        _set("emit_attempts", "true"),
        _set("parallelism", "1"),
    ],
    ids=lambda mutation: getattr(mutation, "__name__", "custom"),
)
def test_cloudtasks_rejects_a_session_outside_the_cell_vocabulary(module, mutation):
    valid = lifecycle_export(module, "delivery.resources", "cloudtasks")
    assert valid.returncode == 0, valid.stderr
    invalid = lifecycle_export(
        module, "delivery.resources", "cloudtasks", cells=mutate_cells(mutation)
    )
    assert invalid.returncode != 0
    assert not invalid.stdout.strip()


def test_cloudtasks_single_cell_session_renders_one_manifest(module):
    result = lifecycle_export(
        module, "delivery.resources", "cloudtasks", cells=mutate_cells(_drop_second)
    )
    assert result.returncode == 0, result.stderr
    manifests = json.loads(
        json.loads(result.stdout)["config"]["data"]["application.json"]
    )
    assert [item["metadata"]["name"] for item in manifests] == ["example-wiring-a"]


def test_cloudtasks_manifests_follow_session_order(module):
    cells = session_cells()
    cells.reverse()
    result = lifecycle_export(
        module, "cellManifests", "cloudtasks", cells=json.dumps(cells)
    )
    assert result.returncode == 0, result.stderr
    assert [item["metadata"]["name"] for item in json.loads(result.stdout)] == [
        "example-wiring-b",
        "example-wiring-a",
    ]


def test_cloudtasks_parallelism_sixteen_takes_the_largest_task_manager_shape(module):
    cell = {**session_cells()[0], "parallelism": 16, "concurrency": 16}
    result = lifecycle_export(
        module, "cellManifests", "cloudtasks", cells=json.dumps([cell])
    )
    assert result.returncode == 0, result.stderr
    [manifest] = json.loads(result.stdout)
    spec = manifest["spec"]
    assert spec["job"]["parallelism"] == 16
    assert spec["flinkConfiguration"]["taskmanager.numberOfTaskSlots"] == "16"
    assert spec["taskManager"]["resource"] == {"cpu": 4, "memory": "16Gi"}
    assert container_limits(manifest, "taskManager") == {
        "cpu": "4",
        "memory": "16Gi",
        "ephemeral-storage": "4Gi",
    }
    assert container_limits(manifest, "jobManager") == {
        "cpu": "1",
        "memory": "2Gi",
        "ephemeral-storage": "1Gi",
    }
    assert spec["job"]["args"][12:16] == ["--parallelism", "16", "--concurrency", "16"]


def test_cloudtasks_configmap_for_thirty_two_cells_keeps_data_headroom(module):
    cells = [{**session_cells()[0], "id": f"cell-{i:02d}"} for i in range(32)]
    result = lifecycle_export(
        module,
        "delivery.resources",
        "cloudtasks",
        cells=json.dumps(cells),
        active_seconds="17820",
    )
    assert result.returncode == 0, result.stderr
    config = json.loads(result.stdout)["config"]
    assert len(json.loads(config["data"]["application.json"])) == 32
    # Kubernetes counts UTF-8 data values, not JSON quoting and escaping.
    # Reserve 256 KiB below its 1 MiB data ceiling; bound the wire form too.
    assert not config.get("binaryData")
    assert sum(len(value.encode("utf-8")) for value in config["data"].values()) < (
        768 * 1024
    )
    assert len(json.dumps(config).encode("utf-8")) < 1024 * 1024


@pytest.mark.parametrize(
    "override",
    [
        {"metadata": {"namespace": "tier3-smoke"}},
        {"spec": {"job": {"parallelism": 2}}},
        {"spec": {"serviceAccount": "smoke"}},
        {"spec": {"flinkVersion": "v1_19"}},
        # Only the TaskManager pins a capacity class, so it is the only one a
        # cell can relax here; a JobManager that opted into Spot is refused by
        # the runtime audit instead, which is what sees the merged Pod.
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
def test_lifecycle_cell_manifests_cannot_relax_shared_run_policy(module, override):
    # First prove this copied source is valid, then contradict one shared rule
    # on the first cell only.
    valid = lifecycle_export(module, "cellManifests", "cloudtasks")
    assert valid.returncode == 0, valid.stderr
    assert len(json.loads(valid.stdout)) == 2
    leaf(module, "lifecycle", {"cellManifests": [override, {}]})
    invalid = lifecycle_export(module, "cellManifests", "cloudtasks")
    assert invalid.returncode != 0
    assert not invalid.stdout.strip()


@pytest.mark.parametrize(
    ("flink_version", "parallelism"),
    [("v1_20", 1), ("v2_2", 4), ("v1_20", 16)],
)
def test_cloudtasks_namespace_policy_admits_both_lines_and_all_classes(
    module, flink_version, parallelism
):
    document = cloudtasks_workload(flinkVersion=flink_version)
    document["delivery"]["resources"]["job"]["spec"]["job"]["parallelism"] = parallelism
    path = leaf(module, "runs/cloudtasks", document)
    result = cue(module, "cmd", "render", path)
    assert result.returncode == 0, result.stderr
    [job] = list(yaml.safe_load_all(result.stdout))
    assert job["metadata"]["namespace"] == "tier3-cloudtasks"
    assert job["spec"]["flinkVersion"] == flink_version
    assert job["spec"]["job"]["parallelism"] == parallelism
    assert job["spec"]["job"]["allowNonRestoredState"] is False
    assert job["spec"]["podTemplate"]["spec"]["nodeSelector"] == {
        "kubernetes.io/arch": "amd64"
    }


@pytest.mark.parametrize(
    ("location", "value"),
    [
        (("spec", "job", "parallelism"), 2),
        (("spec", "job", "parallelism"), 3),
        (("spec", "job", "parallelism"), 32),
        (("spec", "flinkVersion"), "v1_19"),
        (("spec", "flinkVersion"), "v2_1"),
        (("spec", "serviceAccount"), "smoke"),
        (("spec", "job", "allowNonRestoredState"), True),
        (("metadata", "namespace"), "tier3-smoke"),
        (
            ("spec", "taskManager"),
            {
                "podTemplate": {
                    "spec": {"nodeSelector": {"cloud.google.com/gke-spot": "false"}}
                }
            },
        ),
        (
            ("spec", "jobManager"),
            {
                "podTemplate": {
                    "spec": {"nodeSelector": {"cloud.google.com/gke-spot": "false"}}
                }
            },
        ),
    ],
)
def test_cloudtasks_namespace_policy_rejects_other_shapes(module, location, value):
    document = cloudtasks_workload()
    target = document["delivery"]["resources"]["job"]
    for key in location[:-1]:
        target = target.setdefault(key, {})
    target[location[-1]] = value
    path = leaf(module, "runs/invalid", document)
    result = cue(module, "cmd", "render", path)
    assert result.returncode != 0, result.stdout
    assert not result.stdout.strip()


@pytest.mark.parametrize(
    ("location", "value"),
    [
        (("spec", "job", "parallelism"), 3),
        (("spec", "job", "parallelism"), 4),
        (("spec", "flinkVersion"), "v1_20"),
        (("spec", "serviceAccount"), "cloudtasks-benchmark"),
    ],
)
def test_smoke_namespace_policy_is_unchanged_by_the_cloudtasks_namespace(
    module, location, value
):
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


def test_example_session_is_the_reviewed_wiring_shape():
    session = tomllib.loads((SESSIONS / "example-wiring.toml").read_text())
    assert session["campaign"] == "example"
    assert [cell["id"] for cell in session["cells"]] == [
        "example-wiring-a",
        "example-wiring-b",
    ]
    assert {cell["arm"] for cell in session["cells"]} == {"UNNAMED", "STAGED_HASH"}


def test_ci_discovers_and_renders_lifecycle(module):
    documents = list(yaml.safe_load_all(render_deliveries(module)["./lifecycle"]))
    assert [item["kind"] for item in documents] == ["ConfigMap", "Job"]
    app = json.loads(documents[0]["data"]["application.json"])
    assert app["metadata"]["labels"]["flink-gcp.io/run-id"] == "probe"
    assert (
        app["spec"]["podTemplate"]["metadata"]["labels"]["flink-gcp.io/run-id"]
        == "probe"
    )


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
        **changes,
    }
    path = leaf(module, "runs/bigquery", {"run": inputs})
    shutil.copyfile(
        KUBERNETES / "tests/fixtures/bigquery.cue", module / path / "delivery.cue"
    )
    return path


@pytest.mark.parametrize("mode,records", [("ALO", 28800), ("EO", 1843200)])
@pytest.mark.parametrize("destinations", [10, 50])
def test_bigquery_initial_and_upgrade_preserve_the_trial(
    module, mode, records, destinations
):
    from flink_tier3.bigquery import Trial

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
                "--require-restored",
                str(phase == "upgrade").lower(),
            ],
        }
        args = dict(
            zip(spec["job"]["args"][::2], spec["job"]["args"][1::2], strict=True)
        )
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
        assert config["execution.checkpointing.interval"] == "30 s"
        assert config["execution.checkpointing.max-concurrent-checkpoints"] == "1"
        assert config["execution.checkpointing.timeout"] == "120 s"
        assert config["execution.checkpointing.num-retained"] == "2"
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
    "mode,records", [("ALO", 100), ("ALO", 32768), ("EO", 100), ("EO", 2097152)]
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
        {"job": {"parallelism": 1}},
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


PUBSUB_IMAGE = "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/pubsub-recovery"


def pubsub_delivery(module, **inputs):
    run = {
        "id": "pubsub-probe",
        "expiresAt": "2026-09-20T12:00:00Z",
        "image": f"{PUBSUB_IMAGE}@{SYNTHETIC_DIGEST}",
        "phase": "initial",
        "recordsPerSubscription": 1000,
        "parallelism": 1,
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
def test_pubsub_recovery_deployment_contract(module, phase, parallelism, records):
    path = pubsub_delivery(
        module, phase=phase, parallelism=parallelism, recordsPerSubscription=records
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
    ]
    config = spec["flinkConfiguration"]
    assert config["taskmanager.numberOfTaskSlots"] == "1"
    assert config["execution.checkpointing.interval"] == "30 s"
    assert config["execution.checkpointing.max-concurrent-checkpoints"] == "1"
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
        {"parallelism": 3},
        {"parallelism": "1"},
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


@pytest.mark.parametrize("parallelism", [1, 2])
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
        {"job": {"parallelism": 3}},
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


@pytest.mark.parametrize("mode,records", [("ALO", 28800), ("EO", 1843200)])
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
    expected["spec"]["job"]["args"][-1] = "true"
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
        "--require-restored",
        "false",
    ]
    assert initial["spec"]["job"]["parallelism"] == 2
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


@pytest.mark.parametrize("mode", ["ALO", "EO"])
@pytest.mark.parametrize("destinations", [10, 50])
def test_bigquery_prepare_accepts_real_cue_delivery(
    module, monkeypatch, mode, destinations
):
    root = module.parent
    module.rename(root / "kubernetes")
    monkeypatch.setattr(workflow, "ROOT", root)
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
        trial={
            "version": 1,
            "mode": mode,
            "destinations": destinations,
            "repetition": 1,
            "query_slots": 12,
            "maximum_bytes_billed": 4 * 1024**3,
            "query_timeout_ms": 60000,
            "additional_cost_usd": "5.00",
        },
    )
    proposal = bundle["proposal"]
    assert proposal["approved"] is False
    assert proposal["resources"]["trial"]["mode"] == mode
    assert proposal["resources"]["trial"]["destinations"] == destinations
    assert (
        proposal["expires_at"]
        == bundle["application"]["metadata"]["annotations"]["flink-gcp.io/expires-at"]
    )
    assert json.loads(bundle["delivery"]["config"]["data"]["proposal.json"]) == proposal

    approval = {
        "version": 4,
        "scenario": "bigquery-recovery",
        "run_id": "proposal-1312",
        "nonce": "a" * 32,
        "sha": "b" * 40,
        "started_at": proposal["started_at"],
        "expires_at": proposal["expires_at"],
        "cleanup_at": proposal["cleanup_at"],
        "ceilings": {**BIGQUERY_CEILINGS, "additional_cost_usd": "5.00"},
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
        "runtime_sha256": proposal["runtime_sha256"],
        "delivery_sha256": proposal["delivery_sha256"],
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
            "version": 1,
            "mode": mode,
            "destinations": destinations,
            "repetition": 1,
            "query_slots": 12,
            "maximum_bytes_billed": 4 * 1024**3,
            "query_timeout_ms": 60000,
            "additional_cost_usd": "5.00",
        },
    }
    # The synthetic CUE module is not a Git checkout; revision checks have separate tests.
    monkeypatch.setattr(bigquery_bundle, "_check_revision", lambda revision: None)
    prepared = bigquery_bundle.prepare(approval, prepared_at="2026-09-21T00:10:00Z")
    assert bigquery_bundle.validate(prepared, approval) == prepared
    assert prepared["application"] == bundle["application"]
    assert prepared["upgrade_application"] == bundle["upgrade_application"]
    expected_delivery = json.loads(json.dumps(bundle["delivery"]))
    expected_delivery["config"]["data"]["approval.json"] = prepared["delivery"][
        "config"
    ]["data"]["approval.json"]
    expected_delivery["supervisor"]["spec"]["activeDeadlineSeconds"] = 4620
    assert prepared["delivery"] == expected_delivery
    data = prepared["delivery"]["config"]["data"]
    assert (
        json.loads(data["approval.json"])["bigquery_trial"]
        == approval["bigquery_trial"]
    )
    assert {
        name: data["flink_tier3_" + name] for name in delivered_sources()
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
@pytest.mark.parametrize("records", [2, 1001, 10000])
def test_pubsub_proposal_from_real_cue(
    module,
    monkeypatch,
    trial,
    initial_parallelism,
    recovery_parallelism,
    phase,
    records,
):
    root = module.parent
    module.rename(root / "kubernetes")
    monkeypatch.setattr(workflow, "ROOT", root)
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
            "version": 1,
            "trial": trial,
            "records_per_subscription": records,
            "traffic_limits": dict(pubsub_plan.COUNTER_CEILINGS),
            "total_request_limit": 100000,
            "additional_cost_usd": "10.00",
        },
    }
    bundle = pubsub_plan.prepare(**inputs)
    proposal = bundle["proposal"]
    initial, recovery = bundle["application"], bundle["recovery_application"]
    assert proposal["approved"] is False
    assert proposal["cost"] == {
        "kind": "unestimated-proposed-cap",
        "estimate_usd": None,
    }
    assert proposal["application_sha256"] == digest(initial)
    assert proposal["recovery_application_sha256"] == digest(recovery)
    assert proposal["supervisor_sha256"] == digest(bundle["delivery"]["supervisor"])
    assert initial["spec"]["job"]["parallelism"] == initial_parallelism
    assert recovery["spec"]["job"]["parallelism"] == recovery_parallelism
    assert f"--phase={phase}" in recovery["spec"]["job"]["args"]
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
    before, after = cohorts["before_recovery"], cohorts["after_recovery"]
    assert before["start"] == 0
    assert before["count"] == after["start"]
    assert after["start"] + after["count"] == records
    assert min(before["count"], after["count"]) > 0
    assert proposal["cleanup_at"] == "2026-09-21T00:45:00Z"
    assert proposal["limits"]["pods"] == 7
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
