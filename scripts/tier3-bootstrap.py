#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml==6.0.3"]
# ///
#
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
"""Authenticate and manage the idle Tier-3 bootstrap and Operator roots."""

import argparse
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONTEXT = "gke_flink-gcp_us-central1_flink-tier3"
NAMESPACES = ("tier3-system", "tier3-smoke")
MAX_CHART_BYTES = 2 * 1024 * 1024
CRDS = tuple(
    name + ".flink.apache.org"
    for name in (
        "flinkdeployments",
        "flinksessionjobs",
        "flinkstatesnapshots",
        "flinkbluegreendeployments",
    )
)
PRINCIPALS = {
    "plan": "opentofu-plan@flink-gcp.iam.gserviceaccount.com",
    "apply": "opentofu@flink-gcp.iam.gserviceaccount.com",
}
OPERATOR_RESOURCES = {
    ("ServiceAccount", "tier3-system", "flink-operator"),
    ("ConfigMap", "tier3-system", "flink-operator-config"),
    ("Deployment", "tier3-system", "flink-kubernetes-operator"),
    ("Role", "tier3-system", "flink-operator"),
    ("Role", "tier3-smoke", "flink-operator"),
    ("RoleBinding", "tier3-system", "flink-operator-role-binding"),
    ("RoleBinding", "tier3-smoke", "flink-operator-role-binding"),
}


def provider_environment(kubeconfig):
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("KUBE_", "HELM_KUBE")) and key != "KUBECONFIG"
    }
    environment["KUBE_CONFIG_PATH"] = str(kubeconfig)
    environment["HELM_DRIVER"] = "secret"
    return environment


def operator_documents(source, version):
    """Check the actual chart output, including hooks, before it reaches Helm."""
    documents = [item for item in yaml.safe_load_all(source) if item is not None]
    identities = [
        (item["kind"], item["metadata"].get("namespace"), item["metadata"]["name"])
        for item in documents
    ]
    if (
        len(identities) != len(OPERATOR_RESOURCES)
        or set(identities) != OPERATOR_RESOURCES
    ):
        raise ValueError(
            "Operator chart must render exactly the seven owned idle resources"
        )
    for item in documents:
        expected_api = {
            "ServiceAccount": "v1",
            "ConfigMap": "v1",
            "Deployment": "apps/v1",
            "Role": "rbac.authorization.k8s.io/v1",
            "RoleBinding": "rbac.authorization.k8s.io/v1",
        }[item["kind"]]
        if item.get("apiVersion") != expected_api:
            raise ValueError(
                "Operator chart contains an unexpected API group or version"
            )
        if "helm.sh/hook" in item["metadata"].get("annotations", {}):
            raise ValueError("Operator release must not contain Helm hooks")
        if item["kind"] == "Deployment":
            spec = item["spec"]
            pod = spec["template"]["spec"]
            containers = pod["containers"]
            if (
                spec.get("replicas") != 0
                or pod.get("serviceAccountName") != "flink-operator"
                or pod.get("initContainers")
                or len(containers) != 1
                or containers[0].get("image")
                != "apache/flink-kubernetes-operator:" + version
                or any(
                    "persistentVolumeClaim" in volume
                    for volume in pod.get("volumes", [])
                )
            ):
                raise ValueError(
                    "Operator Deployment must retain its pinned idle configuration"
                )
        if item["kind"] == "ConfigMap":
            configurations = [
                yaml.safe_load(data)
                for name, data in item.get("data", {}).items()
                if name in ("config.yaml", "flink-conf.yaml")
            ]
            if not configurations or any(
                config.get("kubernetes.operator.watched.namespaces") != "tier3-smoke"
                for config in configurations
            ):
                raise ValueError("Operator configuration must watch only tier3-smoke")
        if item["kind"] == "RoleBinding" and (
            item["roleRef"]
            != {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": "flink-operator",
            }
            or item["subjects"]
            != [
                {
                    "kind": "ServiceAccount",
                    "name": "flink-operator",
                    "namespace": "tier3-system",
                }
            ]
        ):
            raise ValueError(
                "Operator RoleBindings must bind only the Operator ServiceAccount"
            )
    return documents


def prepare_operator_chart(root, *, download=True):
    """Use the same verified archive and values for rendering and the Helm provider."""
    pin = yaml.safe_load((root / "upstream.yaml").read_text())
    cache = root / ".terraform"
    archive = cache / "operator-chart.tgz"
    if download:
        with urllib.request.urlopen(pin["url"], timeout=60) as response:
            data = response.read(MAX_CHART_BYTES + 1)
    else:
        data = archive.read_bytes()
    if len(data) > MAX_CHART_BYTES:
        raise ValueError(f"Operator chart exceeds the {MAX_CHART_BYTES}-byte limit")
    if hashlib.sha512(data).hexdigest() != pin["sha512"]:
        raise ValueError("Operator chart SHA-512 does not match the root pin")
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as chart:
        entry = chart.getmember("flink-kubernetes-operator/Chart.yaml")
        if not entry.isfile():
            raise ValueError("Expected an ordinary Operator Chart.yaml file")
        metadata = yaml.safe_load(chart.extractfile(entry))
        if (
            metadata.get("name") != "flink-kubernetes-operator"
            or metadata.get("version") != pin["version"]
            or metadata.get("appVersion") != pin["version"]
        ):
            raise ValueError("Operator chart version does not match the root pin")
    cache.mkdir(exist_ok=True)
    if download:
        archive.write_bytes(data)
    rendered = run(
        [
            "helm",
            "template",
            "flink-kubernetes-operator",
            str(archive),
            "--namespace",
            "tier3-system",
            "--values",
            str(root / "values.yaml"),
        ]
    ).stdout
    documents = operator_documents(rendered, pin["version"])
    (cache / "operator-rendered.yaml").write_text(rendered)
    print("Verified Operator chart " + pin["version"] + " and seven idle resources")
    return pin, documents


def run(arguments, *, env=None):
    result = subprocess.run(
        arguments,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
        env=env,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "Command failed")
    return result


class Cluster:
    def __init__(self, kubeconfig, root="bootstrap"):
        if root not in ("bootstrap", "operator"):
            raise ValueError("Unknown Tier-3 root")
        self.root = root
        self.root_path = ROOT / ("opentofu/tier3-" + root)
        self.kubeconfig = Path(kubeconfig).absolute()
        if self.kubeconfig.resolve() == (Path.home() / ".kube/config").resolve():
            raise ValueError("Use a dedicated kubeconfig, not the default kubeconfig")

    def kubectl(self, *arguments):
        return run(
            [
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                "--context",
                CONTEXT,
                "--request-timeout=30s",
                *arguments,
            ],
        )

    def endpoint(self):
        endpoint = run(
            [
                "gcloud",
                "container",
                "clusters",
                "describe",
                "flink-tier3",
                "--project",
                "flink-gcp",
                "--region",
                "us-central1",
                "--format=value(controlPlaneEndpointsConfig.dnsEndpointConfig.endpoint)",
            ]
        ).stdout.strip()
        if not endpoint.endswith(".us-central1.gke.goog"):
            raise ValueError("GKE did not return the expected regional DNS endpoint")
        return "https://" + endpoint

    def authenticate(self):
        if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            raise ValueError(
                "GOOGLE_APPLICATION_CREDENTIALS must name local ADC or CI WIF credentials"
            )
        self.kubeconfig.parent.mkdir(parents=True, exist_ok=True)
        if self.kubeconfig.is_symlink():
            raise ValueError("The dedicated kubeconfig must not be a symlink")
        # Restrict permissions before gcloud writes even the credential-free config.
        descriptor = os.open(self.kubeconfig, os.O_CREAT | os.O_WRONLY, 0o600)
        os.close(descriptor)
        os.chmod(self.kubeconfig, 0o600)
        env = {
            **os.environ,
            "KUBECONFIG": str(self.kubeconfig),
            "CLOUDSDK_CONTAINER_USE_APPLICATION_DEFAULT_CREDENTIALS": "true",
        }
        run(
            [
                "gcloud",
                "container",
                "clusters",
                "get-credentials",
                "flink-tier3",
                "--project",
                "flink-gcp",
                "--region",
                "us-central1",
                "--dns-endpoint",
            ],
            env=env,
        )
        self.validate_target()
        print(
            "Dedicated DNS kubeconfig prepared; credentials are resolved at execution time"
        )

    def validate_target(self):
        config = json.loads(
            self.kubectl("config", "view", "--minify", "-o", "json").stdout
        )
        server = config["clusters"][0]["cluster"]
        user = config["users"][0]["user"]
        plugin = user.get("exec", {})
        if server.get("server") != self.endpoint() or server.get(
            "insecure-skip-tls-verify"
        ):
            raise ValueError(
                "Kubeconfig does not select the verified GKE DNS endpoint with TLS verification"
            )
        if plugin.get("command") != "gke-gcloud-auth-plugin" or any(
            key in user
            for key in (
                "token",
                "tokenFile",
                "client-key",
                "client-key-data",
                "auth-provider",
            )
        ):
            raise ValueError(
                "Kubeconfig must use the GKE credential plugin without static credentials"
            )
        if self.kubectl("get", "--raw=/readyz").stdout.strip() != "ok":
            raise RuntimeError("GKE is not ready")

    def idle(self):
        for namespace in NAMESPACES:
            exists = self.kubectl(
                "get", "namespace", namespace, "--ignore-not-found", "-o", "name"
            ).stdout.strip()
            if not exists:
                continue
            items = json.loads(
                self.kubectl(
                    "get",
                    "pods,persistentvolumeclaims,deployments,statefulsets,jobs,cronjobs",
                    "--namespace",
                    namespace,
                    "-o",
                    "json",
                ).stdout
            )["items"]
            for item in items:
                if (
                    item["kind"] == "Deployment"
                    and namespace == "tier3-system"
                    and item["metadata"]["name"] == "flink-kubernetes-operator"
                    and item.get("spec", {}).get("replicas") == 0
                ):
                    continue
                raise RuntimeError(
                    "Tier-3 has workload objects; finish lifecycle cleanup before bootstrap"
                )
        for crd in CRDS:
            if not self.kubectl(
                "get", "crd", crd, "--ignore-not-found", "-o", "name"
            ).stdout.strip():
                continue
            for namespace in NAMESPACES:
                if not self.kubectl(
                    "get", "namespace", namespace, "--ignore-not-found", "-o", "name"
                ).stdout.strip():
                    continue
                items = json.loads(
                    self.kubectl(
                        "get", crd, "--namespace", namespace, "-o", "json"
                    ).stdout
                )["items"]
                if items:
                    raise RuntimeError(
                        "Tier-3 has Flink run objects; finish lifecycle cleanup before bootstrap"
                    )

    def quota(self):
        for namespace in NAMESPACES:
            item = json.loads(
                self.kubectl(
                    "get",
                    "resourcequota",
                    "tier3-idle",
                    "--namespace",
                    namespace,
                    "-o",
                    "json",
                ).stdout
            )
            for field in ("pods", "persistentvolumeclaims"):
                if item["spec"]["hard"].get(field) != "0":
                    raise RuntimeError("The idle quota must forbid Pods and PVCs")
                if item.get("status", {}).get("hard", {}).get(field) != "0":
                    raise RuntimeError(
                        "The idle quota has not been observed by the quota controller"
                    )

    def can_i(self, expected, verb, group, resource, namespace="", name=""):
        attributes = {
            "verb": verb,
            "group": group,
            "resource": resource,
            "namespace": namespace,
        }
        if name:
            attributes["name"] = name
        review = {
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SelfSubjectAccessReview",
            "spec": {"resourceAttributes": attributes},
        }
        # Explicit attributes work before CRDs exist and avoid discovery fallback.
        with tempfile.TemporaryDirectory(prefix="tier3-access-") as directory:
            path = Path(directory) / "review.json"
            path.write_text(json.dumps(review))
            response = self.kubectl(
                "create",
                "--raw=/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
                "-f",
                str(path),
            )
        status = json.loads(response.stdout).get("status", {})
        if (
            status.get("allowed") is not expected
            or status.get("evaluationError")
            or (expected and status.get("denied"))
        ):
            raise RuntimeError("Unexpected authorization result for " + str(attributes))

    def access(self, mode):
        self.validate_target()
        identity = json.loads(self.kubectl("auth", "whoami", "-o", "json").stdout)
        if identity["status"]["userInfo"]["username"] != PRINCIPALS[mode]:
            raise RuntimeError(
                "Authenticated principal does not match the selected plan/apply identity"
            )
        self.quota()
        self.idle()
        for namespace in NAMESPACES:
            self.can_i(True, "list", "apps", "deployments", namespace)
            self.can_i(True, "list", "flink.apache.org", "flinkdeployments", namespace)
            self.can_i(mode == "apply", "create", "apps", "deployments", namespace)
            self.can_i(
                mode == "apply",
                "create",
                "rbac.authorization.k8s.io",
                "roles",
                namespace,
            )
            self.can_i(mode == "apply", "patch", "", "resourcequotas", namespace)
        self.can_i(True, "list", "", "secrets", "tier3-system")
        self.can_i(mode == "apply", "create", "", "secrets", "tier3-system")
        self.can_i(
            mode == "apply",
            "create",
            "apiextensions.k8s.io",
            "customresourcedefinitions",
        )
        self.can_i(
            mode == "apply",
            "create",
            "rbac.authorization.k8s.io",
            "clusterrolebindings",
        )
        self.can_i(True, "list", "apiextensions.k8s.io", "customresourcedefinitions")
        for resource in ("clusterroles", "clusterrolebindings"):
            for name in ("tier3-bootstrap-reader", "tier3-bootstrap-writer"):
                self.can_i(
                    True, "get", "rbac.authorization.k8s.io", resource, name=name
                )
        self.can_i(False, "patch", "", "namespaces", name="default")
        self.can_i(False, "escalate", "rbac.authorization.k8s.io", "clusterroles")
        self.can_i(False, "bind", "rbac.authorization.k8s.io", "clusterroles")
        self.can_i(False, "create", "apps", "deployments", "default")
        if self.root == "operator":
            self.operator_prerequisites()
        print(
            "Verified " + mode + " principal, Kubernetes permissions, and idle quotas"
        )

    def preflight(self):
        self.validate_target()
        self.quota()
        self.idle()
        if self.root == "operator":
            self.operator_prerequisites()
        print("Verified " + self.root + " target and idle quotas")

    def operator_prerequisites(self):
        for name in CRDS:
            item = json.loads(self.kubectl("get", "crd", name, "-o", "json").stdout)
            if item["metadata"].get("deletionTimestamp") or not any(
                condition.get("type") == "Established"
                and condition.get("status") == "True"
                for condition in item.get("status", {}).get("conditions", [])
            ):
                raise RuntimeError(
                    "Operator requires Established bootstrap CRDs: " + name
                )
        for kind, name in (
            ("serviceaccount", "smoke"),
            ("role", "tier3-smoke-job"),
            ("rolebinding", "tier3-smoke-job"),
        ):
            item = json.loads(
                self.kubectl(
                    "get", kind, name, "--namespace", "tier3-smoke", "-o", "json"
                ).stdout
            )
            if item["metadata"].get("deletionTimestamp"):
                raise RuntimeError("Operator requires the persistent smoke identity")
            if kind == "rolebinding" and (
                item["roleRef"]
                != {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "Role",
                    "name": "tier3-smoke-job",
                }
                or item["subjects"]
                != [
                    {
                        "kind": "ServiceAccount",
                        "name": "smoke",
                        "namespace": "tier3-smoke",
                    }
                ]
            ):
                raise RuntimeError(
                    "The smoke RoleBinding does not bind the bootstrap identity"
                )
        print("Verified four Established CRDs and the persistent smoke identity")

    def verify_operator(self):
        self.preflight()
        pin, documents = prepare_operator_chart(self.root_path, download=False)
        release = json.loads(
            run(
                [
                    "helm",
                    "get",
                    "metadata",
                    "flink-kubernetes-operator",
                    "--namespace",
                    "tier3-system",
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "--kube-context",
                    CONTEXT,
                    "--output",
                    "json",
                ],
                env=provider_environment(self.kubeconfig),
            ).stdout
        )
        if (
            release.get("status") != "deployed"
            or release.get("chart") != "flink-kubernetes-operator"
            or release.get("version") != pin["version"]
            or release.get("appVersion") != pin["version"]
        ):
            raise RuntimeError("Operator release is not deployed at the pinned version")
        for expected in documents:
            metadata = expected["metadata"]
            actual = json.loads(
                self.kubectl(
                    "get",
                    expected["kind"],
                    metadata["name"],
                    "--namespace",
                    metadata["namespace"],
                    "-o",
                    "json",
                ).stdout
            )
            if actual["metadata"].get("deletionTimestamp"):
                raise RuntimeError("Operator resource is being deleted")
            for field in ("data", "rules", "roleRef", "subjects"):
                if field in expected and actual.get(field) != expected[field]:
                    raise RuntimeError(
                        "Operator resource differs from the chart: " + metadata["name"]
                    )
            if expected["kind"] == "Deployment" and (
                actual["spec"].get("replicas") != 0
                or actual.get("status", {}).get("replicas", 0) != 0
                or actual["spec"]["template"]["spec"]["serviceAccountName"]
                != "flink-operator"
                or [
                    container["image"]
                    for container in actual["spec"]["template"]["spec"]["containers"]
                ]
                != ["apache/flink-kubernetes-operator:" + pin["version"]]
            ):
                raise RuntimeError(
                    "Live Operator Deployment is not idle at the pinned version"
                )
        print(
            "Verified deployed Operator release, seven owned resources, zero Pods/PVCs and zero replicas"
        )

    def ci(self, mode):
        # tfaction must resolve a fresh executable on each runner. Its saved plan
        # contains neither this path nor the plan runner's short-lived identity.
        github_path = os.environ.get("GITHUB_PATH")
        executable = shutil.which("tofu")
        if not github_path or not executable:
            raise ValueError("CI bootstrap requires GITHUB_PATH and an installed tofu")
        self.authenticate()
        self.access(mode)
        if self.root == "operator":
            prepare_operator_chart(self.root_path)
        directory = Path(
            tempfile.mkdtemp(prefix="tier3-tofu-", dir=self.kubeconfig.parent)
        )
        wrapper = directory / "tofu"
        wrapper.write_text(
            "#!/usr/bin/env python3\n"
            "import os, sys\n"
            "environment = {key: value for key, value in os.environ.items() "
            "if not key.startswith(('KUBE_', 'HELM_KUBE')) and key != 'KUBECONFIG'}\n"
            f"environment['KUBE_CONFIG_PATH'] = {str(self.kubeconfig)!r}\n"
            "environment['HELM_DRIVER'] = 'secret'\n"
            f"os.execve({executable!r}, [{executable!r}, *sys.argv[1:]], environment)\n"
        )
        wrapper.chmod(0o700)
        with Path(github_path).open("a") as output:
            output.write(str(directory) + "\n")
        print(
            "Prepared tfaction "
            + self.root
            + " execution with isolated provider settings"
        )

    def tofu(self, arguments):
        self.preflight()
        # Provider environment overrides must not bypass the verified kubeconfig.
        if self.root == "operator":
            prepare_operator_chart(self.root_path)
        return subprocess.run(
            ["tofu", "-chdir=" + str(self.root_path), *arguments],
            env=provider_environment(self.kubeconfig),
            check=False,
            timeout=900,
        ).returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument(
        "--root", choices=("bootstrap", "operator"), default="bootstrap"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("auth")
    access = commands.add_parser("access")
    access.add_argument("mode", choices=PRINCIPALS)
    ci = commands.add_parser("ci")
    ci.add_argument("mode", choices=PRINCIPALS)
    commands.add_parser("preflight")
    commands.add_parser("verify")
    tofu = commands.add_parser("tofu")
    tofu.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        cluster = Cluster(args.kubeconfig, args.root)
        if args.command == "auth":
            cluster.authenticate()
        elif args.command == "access":
            cluster.access(args.mode)
        elif args.command == "ci":
            cluster.ci(args.mode)
        elif args.command == "tofu":
            return cluster.tofu(args.arguments)
        elif args.command == "verify":
            if args.root != "operator":
                raise ValueError("The verify command requires --root operator")
            cluster.verify_operator()
        else:
            cluster.preflight()
    except (
        ValueError,
        RuntimeError,
        subprocess.TimeoutExpired,
        OSError,
        tarfile.TarError,
        yaml.YAMLError,
    ) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
