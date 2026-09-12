#!/usr/bin/env python3
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
"""Authenticate, inspect access, and bootstrap persistent Tier-3 objects without Pods."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

CONTEXT = "gke_flink-gcp_us-central1_flink-tier3"
NAMESPACES = ("tier3-system", "tier3-smoke")
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
    def __init__(self, kubeconfig):
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
        print(
            "Verified " + mode + " principal, Kubernetes permissions, and idle quotas"
        )

    def preflight(self):
        self.validate_target()
        self.quota()
        self.idle()
        print("Verified bootstrap target and idle quotas")

    def ci(self, mode):
        # tfaction must resolve a fresh executable on each runner. Its saved plan
        # contains neither this path nor the plan runner's short-lived identity.
        github_path = os.environ.get("GITHUB_PATH")
        executable = shutil.which("tofu")
        if not github_path or not executable:
            raise ValueError("CI bootstrap requires GITHUB_PATH and an installed tofu")
        self.authenticate()
        self.access(mode)
        directory = Path(
            tempfile.mkdtemp(prefix="tier3-tofu-", dir=self.kubeconfig.parent)
        )
        wrapper = directory / "tofu"
        wrapper.write_text(
            "#!/usr/bin/env python3\n"
            "import os, sys\n"
            "environment = {key: value for key, value in os.environ.items() "
            "if not key.startswith('KUBE_')}\n"
            f"environment['KUBE_CONFIG_PATH'] = {str(self.kubeconfig)!r}\n"
            f"os.execve({executable!r}, [{executable!r}, *sys.argv[1:]], environment)\n"
        )
        wrapper.chmod(0o700)
        with Path(github_path).open("a") as output:
            output.write(str(directory) + "\n")
        print("Prepared tfaction bootstrap execution with isolated provider settings")

    def tofu(self, arguments):
        self.preflight()
        # Provider environment overrides must not bypass the verified kubeconfig.
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("KUBE_")
        }
        environment["KUBE_CONFIG_PATH"] = str(self.kubeconfig)
        root = Path(__file__).resolve().parents[1] / "opentofu/tier3-bootstrap"
        return subprocess.run(
            ["tofu", "-chdir=" + str(root), *arguments],
            env=environment,
            check=False,
            timeout=900,
        ).returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("auth")
    access = commands.add_parser("access")
    access.add_argument("mode", choices=PRINCIPALS)
    ci = commands.add_parser("ci")
    ci.add_argument("mode", choices=PRINCIPALS)
    commands.add_parser("preflight")
    tofu = commands.add_parser("tofu")
    tofu.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        cluster = Cluster(args.kubeconfig)
        if args.command == "auth":
            cluster.authenticate()
        elif args.command == "access":
            cluster.access(args.mode)
        elif args.command == "ci":
            cluster.ci(args.mode)
        elif args.command == "tofu":
            return cluster.tofu(args.arguments)
        else:
            cluster.preflight()
    except (
        ValueError,
        RuntimeError,
        subprocess.TimeoutExpired,
        OSError,
    ) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
