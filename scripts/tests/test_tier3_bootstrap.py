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
"""Test the bootstrap boundary with synthetic resources and command results."""

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest
from conftest import SCRIPTS

SPEC = importlib.util.spec_from_file_location(
    "tier3_bootstrap", SCRIPTS / "tier3-bootstrap.py"
)
bootstrap = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bootstrap)


def result(stdout="", returncode=0, stderr=""):
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def test_default_kubeconfig_is_rejected():
    with pytest.raises(ValueError, match="dedicated"):
        bootstrap.Cluster(Path.home() / ".kube/config")


def test_every_kubectl_command_selects_dedicated_context(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        bootstrap, "run", lambda args, **kwargs: calls.append(args) or result()
    )
    path = tmp_path / "kubeconfig"
    bootstrap.Cluster(path).kubectl("get", "namespaces")
    assert calls == [
        [
            "kubectl",
            "--kubeconfig",
            str(path),
            "--context",
            bootstrap.CONTEXT,
            "--request-timeout=30s",
            "get",
            "namespaces",
        ]
    ]


@pytest.mark.parametrize(
    "status",
    [
        {},
        {"allowed": True},
        {"allowed": "false"},
        {"allowed": False, "evaluationError": "authorizer unavailable"},
    ],
)
def test_denial_requires_a_completed_authorization_decision(
    monkeypatch, tmp_path, status
):
    cluster = bootstrap.Cluster(tmp_path / "kubeconfig")
    monkeypatch.setattr(
        cluster, "kubectl", lambda *a, **k: result(json.dumps({"status": status}))
    )
    with pytest.raises(RuntimeError, match="authorization"):
        cluster.can_i(False, "create", "", "pods")


def test_reason_bearing_denial_is_accepted(monkeypatch, tmp_path):
    cluster = bootstrap.Cluster(tmp_path / "kubeconfig")
    monkeypatch.setattr(
        cluster,
        "kubectl",
        lambda *a, **k: result(
            json.dumps(
                {
                    "status": {
                        "allowed": False,
                        "denied": True,
                        "reason": "GKE policy denies this request",
                    }
                }
            )
        ),
    )
    cluster.can_i(False, "create", "", "pods")


def test_authorization_transport_failure_is_not_a_denial(monkeypatch, tmp_path):
    cluster = bootstrap.Cluster(tmp_path / "kubeconfig")

    def fail(*args, **kwargs):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(cluster, "kubectl", fail)
    with pytest.raises(RuntimeError, match="connection refused"):
        cluster.can_i(False, "create", "", "pods")


@pytest.mark.parametrize("mode", ["plan", "apply"])
def test_access_before_crds_exist_uses_explicit_authorization_attributes(
    monkeypatch, tmp_path, mode
):
    cluster = bootstrap.Cluster(tmp_path / "kubeconfig")
    monkeypatch.setattr(cluster, "validate_target", lambda: None)
    requests = []

    def kubectl(*args, **kwargs):
        if args == ("auth", "whoami", "-o", "json"):
            return result(
                json.dumps(
                    {"status": {"userInfo": {"username": bootstrap.PRINCIPALS[mode]}}}
                )
            )
        if args[:2] == ("get", "resourcequota"):
            hard = {"pods": "0", "persistentvolumeclaims": "0"}
            return result(
                json.dumps({"spec": {"hard": hard}, "status": {"hard": hard}})
            )
        if args[:2] == ("get", "namespace"):
            return result("namespace/" + args[2])
        if args[:2] == ("get", "crd"):
            return result("")
        if args[0] == "get" and args[1].startswith("pods,"):
            return result('{"items": []}')
        assert args[:3] == (
            "create",
            "--raw=/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            "-f",
        )
        attributes = json.loads(Path(args[3]).read_text())["spec"]["resourceAttributes"]
        requests.append(attributes)
        allowed = attributes["verb"] in ("get", "list") or (
            mode == "apply"
            and (
                attributes["verb"] == "create"
                and attributes["namespace"] in (*bootstrap.NAMESPACES, "")
                or attributes["verb"] == "patch"
                and attributes["resource"] == "resourcequotas"
                and attributes["namespace"] in bootstrap.NAMESPACES
            )
        )
        return result(
            json.dumps(
                {
                    "status": {
                        "allowed": allowed,
                        "reason": "synthetic authorization decision",
                    }
                }
            )
        )

    monkeypatch.setattr(cluster, "kubectl", kubectl)
    cluster.access(mode)
    assert [r for r in requests if r["group"] == "flink.apache.org"] == [
        {
            "verb": "list",
            "group": "flink.apache.org",
            "resource": "flinkdeployments",
            "namespace": ns,
        }
        for ns in bootstrap.NAMESPACES
    ]


def test_wrong_principal_stops_before_permission_or_inventory_checks(
    monkeypatch, tmp_path
):
    cluster = bootstrap.Cluster(tmp_path / "kubeconfig")
    monkeypatch.setattr(cluster, "validate_target", lambda: None)
    calls = []

    def kubectl(*args, **kwargs):
        calls.append(args)
        return result(json.dumps({"status": {"userInfo": {"username": "unexpected"}}}))

    monkeypatch.setattr(cluster, "kubectl", kubectl)
    with pytest.raises(RuntimeError, match="principal"):
        cluster.access("plan")
    assert calls == [("auth", "whoami", "-o", "json")]


def test_preflight_rejects_active_inventory(monkeypatch, tmp_path):
    cluster = bootstrap.Cluster(tmp_path / "kubeconfig")
    monkeypatch.setattr(cluster, "validate_target", lambda: None)
    monkeypatch.setattr(cluster, "quota", lambda: None)

    def kubectl(*args):
        if args[:2] == ("get", "namespace"):
            return result("namespace/tier3-system")
        return result(
            json.dumps({"items": [{"kind": "Pod", "metadata": {"name": "active"}}]})
        )

    monkeypatch.setattr(cluster, "kubectl", kubectl)
    with pytest.raises(RuntimeError, match="workload objects"):
        cluster.preflight()


def test_tofu_preserves_plan_exit_code_and_uses_dedicated_config(monkeypatch, tmp_path):
    cluster = bootstrap.Cluster(tmp_path / "kubeconfig")
    calls = []
    for key in (
        "KUBE_HOST",
        "KUBE_INSECURE",
        "KUBE_TOKEN",
        "KUBE_CTX_AUTH_INFO",
        "KUBE_CONFIG_PATHS",
    ):
        monkeypatch.setenv(key, "unrelated-provider-override")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "synthetic-adc-path")
    monkeypatch.setattr(cluster, "preflight", lambda: calls.append("preflight"))

    def run(arguments, **kwargs):
        calls.append(arguments)
        assert kwargs["env"]["KUBE_CONFIG_PATH"] == str(cluster.kubeconfig)
        assert {key for key in kwargs["env"] if key.startswith("KUBE_")} == {
            "KUBE_CONFIG_PATH"
        }
        assert kwargs["env"]["GOOGLE_APPLICATION_CREDENTIALS"] == "synthetic-adc-path"
        return result(returncode=2)

    monkeypatch.setattr(bootstrap.subprocess, "run", run)
    assert cluster.tofu(["plan", "-detailed-exitcode"]) == 2
    assert calls[0] == "preflight"
    assert calls[1][0] == "tofu"
    assert calls[1][1].endswith("/opentofu/tier3-bootstrap")
    assert calls[1][2:] == ["plan", "-detailed-exitcode"]


@pytest.mark.parametrize("mutation", ["endpoint", "tls", "token", "plugin"])
def test_target_validation_rejects_unsafe_kubeconfig(monkeypatch, tmp_path, mutation):
    cluster = bootstrap.Cluster(tmp_path / "kubeconfig")
    server = {"server": "https://expected.us-central1.gke.goog"}
    user = {"exec": {"command": "gke-gcloud-auth-plugin"}}
    if mutation == "endpoint":
        server["server"] = "https://other.us-central1.gke.goog"
    elif mutation == "tls":
        server["insecure-skip-tls-verify"] = True
    elif mutation == "token":
        user["token"] = "synthetic-token"
    else:
        user["exec"]["command"] = "other-plugin"
    config = {"clusters": [{"cluster": server}], "users": [{"user": user}]}
    calls = []

    def kubectl(*args, **kwargs):
        calls.append(args)
        return result(json.dumps(config))

    monkeypatch.setattr(cluster, "kubectl", kubectl)
    monkeypatch.setattr(
        cluster, "endpoint", lambda: "https://expected.us-central1.gke.goog"
    )
    with pytest.raises(ValueError):
        cluster.validate_target()
    assert calls == [("config", "view", "--minify", "-o", "json")]


def test_authentication_uses_dedicated_adc_config_without_changing_default(
    monkeypatch, tmp_path
):
    path = tmp_path / "tier3-kubeconfig"
    cluster = bootstrap.Cluster(path)
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "synthetic-adc-path")
    monkeypatch.setenv("KUBECONFIG", "unrelated-config")
    monkeypatch.setattr(cluster, "validate_target", lambda: None)
    calls = []
    monkeypatch.setattr(
        bootstrap,
        "run",
        lambda args, **kwargs: calls.append((args, kwargs)) or result(),
    )
    cluster.authenticate()
    [arguments] = calls
    assert "--dns-endpoint" in arguments[0]
    assert arguments[1]["env"]["KUBECONFIG"] == str(path)
    assert (
        arguments[1]["env"]["CLOUDSDK_CONTAINER_USE_APPLICATION_DEFAULT_CREDENTIALS"]
        == "true"
    )
    assert path.stat().st_mode & 0o777 == 0o600
    assert bootstrap.os.environ["KUBECONFIG"] == "unrelated-config"


@pytest.mark.parametrize("mode", ["plan", "apply"])
@pytest.mark.parametrize("root", ["bootstrap", "operator"])
def test_ci_wrapper_refreshes_provider_environment_and_preserves_exit_code(
    monkeypatch, tmp_path, mode, root
):
    path_file = tmp_path / "github-path"
    output = tmp_path / "result.json"
    executable = tmp_path / "real-tofu"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "from pathlib import Path\n"
        f"Path({str(output)!r}).write_text(json.dumps({{"
        "'args': sys.argv[1:], 'config': {key: value for key, value in os.environ.items() "
        "if key.startswith(('KUBE_', 'HELM_KUBE')) or key == 'KUBECONFIG'}, "
        "'driver': os.environ['HELM_DRIVER'], "
        "'adc': os.environ['GOOGLE_APPLICATION_CREDENTIALS']}))\n"
        "sys.exit(2)\n"
    )
    executable.chmod(0o700)
    monkeypatch.setenv("GITHUB_PATH", str(path_file))
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "synthetic-adc")
    monkeypatch.setenv("KUBE_HOST", "https://wrong-cluster")
    monkeypatch.setenv("KUBE_TOKEN", "synthetic-token")
    monkeypatch.setenv("KUBE_CONFIG_PATH", "wrong-config")
    monkeypatch.setenv("HELM_KUBETOKEN", "wrong-token")
    monkeypatch.setenv("KUBECONFIG", "wrong-default")
    monkeypatch.setenv("HELM_DRIVER", "configmap")
    monkeypatch.setattr(bootstrap.shutil, "which", lambda _: str(executable))
    cluster = bootstrap.Cluster(tmp_path / ("dedicated-" + mode), root)
    calls = []
    monkeypatch.setattr(cluster, "authenticate", lambda: calls.append("auth"))
    monkeypatch.setattr(cluster, "access", lambda value: calls.append(value))
    monkeypatch.setattr(
        bootstrap, "prepare_operator_chart", lambda path: calls.append(path)
    )
    cluster.ci(mode)
    assert calls == ["auth", mode] + ([cluster.root_path] if root == "operator" else [])
    wrapper = Path(path_file.read_text().strip()) / "tofu"
    assert wrapper.stat().st_mode & 0o777 == 0o700
    result = subprocess.run([str(wrapper), "plan", "-detailed-exitcode"], check=False)
    assert result.returncode == 2
    assert json.loads(output.read_text()) == {
        "args": ["plan", "-detailed-exitcode"],
        "config": {"KUBE_CONFIG_PATH": str(cluster.kubeconfig)},
        "driver": "secret",
        "adc": "synthetic-adc",
    }
    assert "synthetic-token" not in wrapper.read_text()
    assert "synthetic-adc" not in wrapper.read_text()


def test_ci_access_failure_does_not_publish_a_wrapper(monkeypatch, tmp_path):
    path_file = tmp_path / "github-path"
    monkeypatch.setenv("GITHUB_PATH", str(path_file))
    monkeypatch.setattr(bootstrap.shutil, "which", lambda _: "/usr/bin/tofu")
    cluster = bootstrap.Cluster(tmp_path / "config")
    monkeypatch.setattr(cluster, "authenticate", lambda: None)

    def fail(mode):
        raise RuntimeError("wrong permissions")

    monkeypatch.setattr(cluster, "access", fail)
    with pytest.raises(RuntimeError, match="wrong permissions"):
        cluster.ci("plan")
    assert not path_file.exists()
    assert not list(tmp_path.glob("tier3-tofu-*"))
