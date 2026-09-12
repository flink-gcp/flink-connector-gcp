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
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

KUBERNETES = Path(__file__).resolve().parents[1]
CUE = shutil.which("cue")
ENV = {**os.environ, "GOMAXPROCS": "2"}


@pytest.fixture
def module(tmp_path):
    destination = tmp_path / "module"
    shutil.copytree(
        KUBERNETES, destination, ignore=shutil.ignore_patterns("tests", "target")
    )
    return destination


def cue(module, *arguments):
    assert CUE, (
        "Run this suite through just tier3-check to select the pinned CUE binary"
    )
    return subprocess.run(
        [CUE, "-C", str(module), *arguments],
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
        "kubernetes.io/arch": "amd64",
        "cloud.google.com/gke-spot": "true",
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
            ("delivery", "resources", "job", "spec", "podTemplate"),
            {"spec": {"nodeSelector": {"cloud.google.com/gke-spot": "false"}}},
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
    template["spec"] = {"nodeSelector": {"cloud.google.com/gke-spot": "false"}}
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
    assert (
        spec["podTemplate"]["spec"]["nodeSelector"]["cloud.google.com/gke-spot"]
        == "true"
    )


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
    for tree in ("runs",):
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
