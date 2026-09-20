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
import sys
import tomllib
from pathlib import Path

import pytest
import yaml
from flink_tier3.bundle import package_sources

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


def cue(module, *arguments):
    assert CUE, (
        "Run this suite through just tier3-check to select the pinned CUE binary"
    )
    source = None
    if any(arg == "./lifecycle" or arg.startswith("./lifecycle/") for arg in arguments):
        arguments = (*arguments, "json:", "-")
        source = json.dumps({"packageSources": package_sources()})
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
    pod = spec["podTemplate"]["spec"]
    assert pod["nodeSelector"]["cloud.google.com/gke-spot"] == "true"
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
    assert config["data"]["flink_tier3_runtime.py"] == package_sources()["runtime.py"]
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
        "flink_tier3/" + name for name in package_sources()
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
                path.read_text() == package_sources()[name.removeprefix("flink_tier3/")]
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
    assert pod["containers"][0]["resources"]["requests"] == {
        "cpu": "250m",
        "memory": "512Mi",
        "ephemeral-storage": "128Mi",
    }
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
    assert (
        app["spec"]["podTemplate"]["spec"]["nodeSelector"]["cloud.google.com/gke-spot"]
        == "true"
    )


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
        assert spec["podTemplate"]["spec"]["nodeSelector"] == {
            "kubernetes.io/arch": "amd64",
            "cloud.google.com/gke-spot": "true",
        }
        for manager in ("jobManager", "taskManager"):
            assert spec[manager]["replicas"] == 1
            pod = spec[manager]["podTemplate"]["spec"]
            assert pod["nodeSelector"]["cloud.google.com/gke-spot"] == "true"
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
                "podTemplate": {
                    "spec": {"nodeSelector": {"cloud.google.com/gke-spot": "false"}}
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


def test_cloudtasks_configmap_for_thirty_two_cells_stays_under_768_kib(module):
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
    assert len(json.dumps(config).encode()) < 768 * 1024


@pytest.mark.parametrize(
    "override",
    [
        {"metadata": {"namespace": "tier3-smoke"}},
        {"spec": {"job": {"parallelism": 2}}},
        {"spec": {"serviceAccount": "smoke"}},
        {"spec": {"flinkVersion": "v1_19"}},
        {
            "spec": {
                "podTemplate": {
                    "spec": {"nodeSelector": {"cloud.google.com/gke-spot": "false"}}
                }
            }
        },
        {
            "spec": {
                "jobManager": {
                    "podTemplate": {
                        "spec": {"nodeSelector": {"cloud.google.com/gke-spot": "false"}}
                    }
                }
            }
        },
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
        "kubernetes.io/arch": "amd64",
        "cloud.google.com/gke-spot": "true",
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
            ("spec", "podTemplate"),
            {"spec": {"nodeSelector": {"cloud.google.com/gke-spot": "false"}}},
        ),
        (
            ("spec", "jobManager"),
            {
                "podTemplate": {
                    "spec": {"nodeSelector": {"cloud.google.com/gke-spot": "false"}}
                }
            },
        ),
        (
            ("spec", "taskManager"),
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
            assert template["spec"]["nodeSelector"] == {
                "kubernetes.io/arch": "amd64",
                "cloud.google.com/gke-spot": "true",
            }
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
                    "spec": {"nodeSelector": {"cloud.google.com/gke-spot": "false"}}
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
