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

"""Exercise the idle Helm boundary with synthetic charts and Kubernetes responses."""

import copy
import hashlib
import importlib.util
import io
import json
import subprocess
import tarfile

import pytest
import yaml
from conftest import SCRIPTS

SPEC = importlib.util.spec_from_file_location(
    "tier3_operator", SCRIPTS / "tier3-bootstrap.py"
)
operator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(operator)


def result(value):
    return subprocess.CompletedProcess([], 0, json.dumps(value), "")


def documents():
    objects = []
    for namespace in ("tier3-system", "tier3-smoke"):
        objects.extend(
            [
                {
                    "apiVersion": "rbac.authorization.k8s.io/v1",
                    "kind": "Role",
                    "metadata": {"name": "flink-operator", "namespace": namespace},
                    "rules": [
                        {"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]}
                    ],
                },
                {
                    "apiVersion": "rbac.authorization.k8s.io/v1",
                    "kind": "RoleBinding",
                    "metadata": {
                        "name": "flink-operator-role-binding",
                        "namespace": namespace,
                    },
                    "roleRef": {
                        "apiGroup": "rbac.authorization.k8s.io",
                        "kind": "Role",
                        "name": "flink-operator",
                    },
                    "subjects": [
                        {
                            "kind": "ServiceAccount",
                            "name": "flink-operator",
                            "namespace": "tier3-system",
                        }
                    ],
                },
            ]
        )
    objects.extend(
        [
            {
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {"name": "flink-operator", "namespace": "tier3-system"},
            },
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "flink-operator-config",
                    "namespace": "tier3-system",
                },
                "data": {
                    "flink-conf.yaml": "kubernetes.operator.watched.namespaces: tier3-smoke\n"
                },
            },
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "flink-kubernetes-operator",
                    "namespace": "tier3-system",
                },
                "spec": {
                    "replicas": 0,
                    "template": {
                        "spec": {
                            "serviceAccountName": "flink-operator",
                            "containers": [
                                {
                                    "name": "operator",
                                    "image": "apache/flink-kubernetes-operator:1.15.0",
                                }
                            ],
                        }
                    },
                },
            },
        ]
    )
    return objects


def archive_bytes(version="1.15.0", app_version="1.15.0", link=False):
    payload = yaml.safe_dump(
        {
            "name": "flink-kubernetes-operator",
            "version": version,
            "appVersion": app_version,
        }
    ).encode()
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        entry = tarfile.TarInfo("flink-kubernetes-operator/Chart.yaml")
        if link:
            entry.type = tarfile.SYMTYPE
            entry.linkname = "other.yaml"
        else:
            entry.size = len(payload)
        archive.addfile(entry, io.BytesIO(payload))
    return output.getvalue()


def prepare_inputs(monkeypatch, tmp_path, data=None):
    if data is None:
        data = archive_bytes()
    pin = {
        "version": "1.15.0",
        "url": "https://example.invalid/operator.tgz",
        "sha512": hashlib.sha512(data).hexdigest(),
    }
    (tmp_path / "upstream.yaml").write_text(yaml.safe_dump(pin))
    (tmp_path / "values.yaml").write_text("replicas: 0\n")
    monkeypatch.setattr(
        operator.urllib.request, "urlopen", lambda *a, **k: io.BytesIO(data)
    )
    return pin


def test_chart_preparation_checks_exact_bytes_and_passes_root_values(
    monkeypatch, tmp_path
):
    data = archive_bytes()
    pin = prepare_inputs(monkeypatch, tmp_path, data)
    calls = []

    def run(arguments):
        calls.append(arguments)
        return subprocess.CompletedProcess([], 0, yaml.safe_dump_all(documents()), "")

    monkeypatch.setattr(operator, "run", run)
    actual_pin, actual_documents = operator.prepare_operator_chart(tmp_path)
    assert actual_pin == pin
    assert actual_documents == documents()
    assert calls == [
        [
            "helm",
            "template",
            "flink-kubernetes-operator",
            str(tmp_path / ".terraform/operator-chart.tgz"),
            "--namespace",
            "tier3-system",
            "--values",
            str(tmp_path / "values.yaml"),
        ]
    ]
    assert (tmp_path / ".terraform/operator-chart.tgz").read_bytes() == data
    assert (
        list(
            yaml.safe_load_all(
                (tmp_path / ".terraform/operator-rendered.yaml").read_text()
            )
        )
        == documents()
    )


@pytest.mark.parametrize("tampered", [False, True])
def test_cached_chart_is_reverified_without_network(monkeypatch, tmp_path, tampered):
    data = archive_bytes()
    pin = prepare_inputs(monkeypatch, tmp_path, data)
    cache = tmp_path / ".terraform"
    cache.mkdir()
    (cache / "operator-chart.tgz").write_bytes(b"corrupt" if tampered else data)
    monkeypatch.setattr(
        operator.urllib.request,
        "urlopen",
        lambda *a, **k: pytest.fail("Post-apply verification must not download"),
    )

    def render(arguments):
        assert not tampered, "A damaged archive must not reach Helm"
        return subprocess.CompletedProcess([], 0, yaml.safe_dump_all(documents()), "")

    monkeypatch.setattr(operator, "run", render)
    if tampered:
        with pytest.raises(ValueError, match="SHA-512"):
            operator.prepare_operator_chart(tmp_path, download=False)
    else:
        assert operator.prepare_operator_chart(tmp_path, download=False) == (
            pin,
            documents(),
        )


@pytest.mark.parametrize(
    "case", ["checksum", "oversized", "version", "app-version", "symlink"]
)
def test_bad_chart_never_reaches_helm(monkeypatch, tmp_path, case):
    data = archive_bytes(
        version="1.14.0" if case == "version" else "1.15.0",
        app_version="1.14.0" if case == "app-version" else "1.15.0",
        link=case == "symlink",
    )
    if case == "oversized":
        data = b"x" * (2 * 1024 * 1024 + 1)
    pin = prepare_inputs(monkeypatch, tmp_path, data)
    if case == "checksum":
        pin["sha512"] = "0" * 128
        (tmp_path / "upstream.yaml").write_text(yaml.safe_dump(pin))
    monkeypatch.setattr(operator, "run", lambda _: pytest.fail("Helm must not run"))
    message = {
        "checksum": "SHA-512",
        "oversized": "exceeds.*byte limit",
        "version": "version",
        "app-version": "version",
        "symlink": "ordinary",
    }[case]
    with pytest.raises(ValueError, match=message):
        operator.prepare_operator_chart(tmp_path)
    assert not (tmp_path / ".terraform").exists()


@pytest.mark.parametrize(
    "case",
    [
        "extra-pod",
        "extra-crd",
        "extra-namespace",
        "extra-job-identity",
        "missing-role",
        "duplicate-role",
        "hook",
        "replicas",
        "image",
        "sidecar",
        "init-container",
        "pvc",
        "watch-all",
        "watch-extra",
        "binding-subject",
        "binding-role",
    ],
)
def test_rendered_chart_rejects_ownership_or_idle_violations(case):
    objects = documents()
    deployment = objects[-1]["spec"]
    pod = deployment["template"]["spec"]
    if case.startswith("extra-"):
        kind = {
            "extra-pod": "Pod",
            "extra-crd": "CustomResourceDefinition",
            "extra-namespace": "Namespace",
            "extra-job-identity": "ServiceAccount",
        }[case]
        objects.append(
            {"kind": kind, "metadata": {"name": "unowned", "namespace": "tier3-smoke"}}
        )
    elif case == "missing-role":
        objects.pop(0)
    elif case == "duplicate-role":
        objects[2] = copy.deepcopy(objects[0])
    elif case == "hook":
        objects[0]["metadata"]["annotations"] = {"helm.sh/hook": "pre-install"}
    elif case == "replicas":
        deployment["replicas"] = 1
    elif case == "image":
        pod["containers"][0]["image"] = "operator:latest"
    elif case == "sidecar":
        pod["containers"].append({"image": "sidecar:latest"})
    elif case == "init-container":
        pod["initContainers"] = [{"image": "init:latest"}]
    elif case == "pvc":
        pod["volumes"] = [
            {"name": "data", "persistentVolumeClaim": {"claimName": "data"}}
        ]
    elif case.startswith("watch-"):
        watched = "" if case == "watch-all" else "tier3-smoke,default"
        objects[-2]["data"]["flink-conf.yaml"] = (
            "kubernetes.operator.watched.namespaces: " + watched
        )
    elif case == "binding-subject":
        objects[1]["subjects"][0]["namespace"] = "default"
    else:
        objects[1]["roleRef"]["kind"] = "ClusterRole"
    with pytest.raises(ValueError):
        operator.operator_documents(yaml.safe_dump_all(objects), "1.15.0")


@pytest.mark.parametrize(
    "case", ["established", "pending", "deleting", "missing", "wrong-binding"]
)
def test_operator_requires_completed_bootstrap(monkeypatch, tmp_path, case):
    cluster = operator.Cluster(tmp_path / "config", "operator")
    calls = []

    def kubectl(*arguments):
        calls.append(arguments)
        kind, name = arguments[1:3]
        item = {"metadata": {"name": name}}
        if case == "missing" and kind == "serviceaccount":
            raise RuntimeError("ServiceAccount not found")
        if kind == "crd":
            item["status"] = {
                "conditions": [
                    {
                        "type": "Established",
                        "status": "False" if case == "pending" else "True",
                    }
                ]
            }
            if case == "deleting":
                item["metadata"]["deletionTimestamp"] = "2026-09-12T00:00:00Z"
        elif kind == "rolebinding":
            item["roleRef"] = {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": "tier3-smoke-job",
            }
            item["subjects"] = [
                {
                    "kind": "ServiceAccount",
                    "name": "wrong" if case == "wrong-binding" else "smoke",
                    "namespace": "tier3-smoke",
                }
            ]
        return result(item)

    monkeypatch.setattr(cluster, "kubectl", kubectl)
    if case == "established":
        cluster.operator_prerequisites()
        assert len(calls) == 7
        assert {args[2] for args in calls[:4]} == set(operator.CRDS)
    else:
        with pytest.raises(RuntimeError):
            cluster.operator_prerequisites()


@pytest.mark.parametrize("root", ["bootstrap", "operator"])
def test_local_tofu_selects_only_known_root_and_preserves_exit_code(
    monkeypatch, tmp_path, root
):
    cluster = operator.Cluster(tmp_path / "config", root)
    calls = []
    monkeypatch.setattr(cluster, "preflight", lambda: calls.append("preflight"))
    monkeypatch.setattr(
        operator, "prepare_operator_chart", lambda path: calls.append(path)
    )
    monkeypatch.setenv("KUBECONFIG", "wrong")
    monkeypatch.setenv("HELM_KUBETOKEN", "wrong-token")

    def run(arguments, **kwargs):
        calls.append(arguments)
        assert "KUBECONFIG" not in kwargs["env"]
        assert "HELM_KUBETOKEN" not in kwargs["env"]
        assert kwargs["env"]["KUBE_CONFIG_PATH"] == str(cluster.kubeconfig)
        return subprocess.CompletedProcess(arguments, 2)

    monkeypatch.setattr(operator.subprocess, "run", run)
    assert cluster.tofu(["plan", "-detailed-exitcode"]) == 2
    assert calls[0] == "preflight"
    if root == "operator":
        assert calls[1] == cluster.root_path
    assert calls[-1] == [
        "tofu",
        "-chdir=" + str(cluster.root_path),
        "plan",
        "-detailed-exitcode",
    ]


def test_unknown_root_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="Unknown"):
        operator.Cluster(tmp_path / "config", "../../other")


@pytest.mark.parametrize(
    "case",
    [
        "idle",
        "failed-release",
        "wrong-version",
        "wrong-app-version",
        "wrong-chart",
        "status-output",
        "live-replicas",
        "live-config",
        "live-rbac",
    ],
)
def test_post_apply_verification_reads_release_and_live_resources(
    monkeypatch, tmp_path, case
):
    cluster = operator.Cluster(tmp_path / "config", "operator")
    calls = []
    monkeypatch.setattr(cluster, "preflight", lambda: calls.append("preflight"))

    def cached_chart(root, *, download):
        assert root == cluster.root_path
        assert download is False
        return {"version": "1.15.0"}, documents()

    monkeypatch.setattr(operator, "prepare_operator_chart", cached_chart)

    def run(arguments, **kwargs):
        calls.append(arguments)
        assert arguments[:3] == ["helm", "get", "metadata"]
        assert kwargs["env"]["HELM_DRIVER"] == "secret"
        assert arguments[arguments.index("--kubeconfig") + 1] == str(cluster.kubeconfig)
        assert arguments[arguments.index("--kube-context") + 1] == operator.CONTEXT
        # Helm 3.20.2 removes chart data from status JSON; get metadata is flat.
        if case == "status-output":
            return result({"info": {"status": "deployed"}, "version": 1})
        return result(
            {
                "status": "failed" if case == "failed-release" else "deployed",
                "chart": "other"
                if case == "wrong-chart"
                else "flink-kubernetes-operator",
                "version": "1.14.0" if case == "wrong-version" else "1.15.0",
                "appVersion": "1.14.0" if case == "wrong-app-version" else "1.15.0",
            }
        )

    def kubectl(*arguments):
        calls.append(arguments)
        actual = next(
            item
            for item in documents()
            if item["kind"] == arguments[1]
            and item["metadata"]["name"] == arguments[2]
            and item["metadata"]["namespace"] == arguments[4]
        )
        if case == "live-replicas" and actual["kind"] == "Deployment":
            actual["status"] = {"replicas": 1}
        if case == "live-config" and actual["kind"] == "ConfigMap":
            actual["data"] = {}
        if case == "live-rbac" and actual["kind"] == "Role":
            actual["rules"] = []
        return result(actual)

    monkeypatch.setattr(operator, "run", run)
    monkeypatch.setattr(cluster, "kubectl", kubectl)
    if case == "idle":
        cluster.verify_operator()
        assert len(calls) == 9
    else:
        with pytest.raises(RuntimeError):
            cluster.verify_operator()
    assert calls[0] == "preflight"
