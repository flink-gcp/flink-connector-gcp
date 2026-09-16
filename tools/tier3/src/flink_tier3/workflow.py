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
"""GitHub, CUE and OpenTofu integration for the external lifecycle CLI."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import yaml

import flink_tier3 as rt

from . import bootstrap
from .bundle import package_sources

ROOT = Path.cwd()
ROOTS = ["flink-gcp", "tier3-bootstrap", "tier3-operator"]


def github_run(run_id, attempt=None):
    if not str(run_id).isdigit():
        raise rt.Failure("Invalid GitHub execution ID")
    path = f"repos/{rt.REPOSITORY}/actions/runs/{run_id}"
    if attempt is not None:
        if not str(attempt).isdigit() or int(attempt) < 1:
            raise rt.Failure("Invalid GitHub execution attempt")
        path += f"/attempts/{attempt}"
    return json.loads(rt.command(["gh", "api", path]))


def verify_source(owner, completed=False):
    actual = github_run(owner["github_run_id"])
    allowed = {
        "run": (".github/workflows/tier3-run.yaml", "workflow_dispatch"),
        "plan": (".github/workflows/ci.yaml", "pull_request"),
        "apply": (".github/workflows/tofu-apply.yaml", "push"),
    }
    path, event = allowed[owner["kind"]]
    if (
        actual["repository"]["full_name"] != rt.REPOSITORY
        or actual["path"] != path
        or actual["event"] != event
        or actual["head_sha"] != owner["sha"]
        or int(actual["run_attempt"]) < int(owner["attempt"])
        or (not completed and int(actual["run_attempt"]) != int(owner["attempt"]))
    ):
        raise rt.Failure("Lock holder does not match its originating GitHub execution")
    if owner["kind"] != "plan" and actual["head_branch"] != "main":
        raise rt.Failure("Workload/apply execution must originate on main")
    if completed and actual["status"] != "completed":
        raise rt.Failure(
            "Originating execution is still active; recovery cannot race it"
        )
    return actual


def execution(kind, nonce):
    owner = {
        "kind": kind,
        "nonce": nonce,
        "github_run_id": os.environ["GITHUB_RUN_ID"],
        "attempt": os.environ["GITHUB_RUN_ATTEMPT"],
        "sha": os.environ["GITHUB_SHA"],
    }
    if os.environ.get("GITHUB_REPOSITORY") != rt.REPOSITORY or not rt.SHA.fullmatch(
        owner["sha"]
    ):
        raise rt.Failure("Execution must originate in the expected repository")
    # PR GITHUB_SHA is the merge commit; the API head_sha identifies its source.
    if kind == "plan":
        owner["sha"] = github_run(owner["github_run_id"])["head_sha"]
    verify_source(owner)
    return owner


def external(kubeconfig, idle=False):
    cluster = bootstrap.Cluster(kubeconfig)
    cluster.authenticate()
    if idle:
        cluster.preflight()
    else:
        cluster.validate_target()
    return rt.Kubernetes(
        cluster.endpoint(), rt.KubernetesTransport(cluster.endpoint(), rt.GoogleToken())
    )


def render(
    run_id,
    nonce,
    expiry,
    active_seconds,
    approval=None,
    expression="application",
    scenario="smoke",
):
    args = [
        "cue",
        "export",
        "./lifecycle",
        "json:",
        "-",
        "--out",
        "json",
        "-e",
        expression,
        "-t",
        "run_id=" + run_id,
        "-t",
        "nonce=" + nonce,
        "-t",
        "expires_at=" + expiry,
        "-t",
        "active_seconds=" + str(active_seconds),
    ]
    if approval:
        args.extend(["-t", "approval=" + rt.json_bytes(approval).decode()])
    if scenario != "smoke":
        args.extend(["-t", "scenario=" + scenario])
    result = subprocess.run(
        args,
        cwd=ROOT / "kubernetes",
        input=json.dumps({"packageSources": package_sources()}),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if result.returncode:
        raise rt.Failure(
            f"CUE rendering failed (exit {result.returncode}): {result.stderr[-2000:]}"
        )
    return json.loads(result.stdout)


def image_receipts(http, images, expiry):
    receipts = {}
    for role, image in images.items():
        if not image.startswith(rt.GAR) or "@sha256:" not in image:
            raise rt.Failure("Image is outside the approved registry")
        package, version = image.removeprefix(rt.GAR).split("@")
        path = f"projects/{rt.PROJECT}/locations/{rt.REGION}/repositories/flink-tier3/packages/{rt.encoded(package)}/versions/{rt.encoded(version)}"
        response = http.get(
            "https://artifactregistry.googleapis.com/v1/" + path,
            timeout=rt.HTTP_TIMEOUT,
            allow_redirects=False,
        )
        response.raise_for_status()
        if response.status_code != 200:
            raise rt.ApiError(response.status_code, "GET", path)
        data = response.json()
        created = rt.timestamp(data["createTime"])
        if created + 7 * 86400 <= rt.timestamp(expiry) + 86400:
            raise rt.Failure(
                "Image is missing a 24-hour margin before seven-day deletion eligibility"
            )
        receipts[role] = {
            "image": image,
            "created_at": data["createTime"],
            "deletion_eligible_at": rt.utc(created + 7 * 86400),
        }
    return receipts


def snapshot(kube):
    namespaces = {}
    for ns in (rt.SMOKE, rt.SYSTEM):
        quota = kube.get("ResourceQuota", ns, "tier3-idle")
        namespaces[ns] = {
            "uid": kube.namespace(ns)["metadata"]["uid"],
            "quota_uid": quota["metadata"]["uid"],
            "hard": quota["spec"]["hard"],
        }
    operator = kube.get("Deployment", rt.SYSTEM, rt.OPERATOR)
    if not operator or operator["spec"].get("replicas") != 0:
        raise rt.Failure("Operator must be installed at zero replicas")
    template = operator["spec"]["template"]
    image = template["spec"]["containers"][0]["image"]
    pinned = yaml.safe_load((ROOT / "opentofu/tier3-operator/values.yaml").read_text())[
        "image"
    ]
    expected_image = pinned["repository"] + "@" + pinned["digest"]
    if image != expected_image:
        raise rt.Failure("Installed Operator image differs from the reviewed pin")
    rt.verify_pod(template, "operator", expected_image)
    inventory = kube.inventory()
    for item in inventory:
        if item["kind"] == "ReplicaSet" and (
            item.get("spec", {}).get("replicas", 0) != 0
            or not any(
                r.get("uid") == operator["metadata"]["uid"]
                for r in item["metadata"].get("ownerReferences", [])
            )
        ):
            raise rt.Failure(
                "Idle ReplicaSet must be stopped and owned by the Operator"
            )
        if (
            item["kind"] == "Service"
            and item.get("spec", {}).get("type", "ClusterIP") != "ClusterIP"
        ):
            raise rt.Failure("Tier-3 must not contain an external service")
    return (
        namespaces,
        operator["metadata"]["uid"],
        image,
        sorted(obj["metadata"]["uid"] for obj in inventory),
    )


def save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(rt.json_bytes(data))


def plans(args, store):
    owner = json.loads((args.directory / "owner.json").read_text())
    rt.EnvironmentLock(store).assert_owner(owner)
    # This command runs only after re-authentication as opentofu-plan. Its
    # authority may borrow this exact run lock, never a different holder's.
    empty = True
    deadline = time.monotonic() + rt.Schedule.plan_budget_seconds
    for root in ROOTS:
        rt.EnvironmentLock(store).assert_owner(owner)
        if root != "flink-gcp":
            cluster = bootstrap.Cluster(args.kubeconfig, root.removeprefix("tier3-"))
            cluster.authenticate()
            cluster.preflight()
            if root == "tier3-operator":
                bootstrap.prepare_operator_chart(cluster.root_path)
        path = ROOT / "opentofu" / root
        for arguments in (
            ["init", "-input=false"],
            ["plan", "-input=false", "-detailed-exitcode", "-no-color"],
        ):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise rt.Failure("Shared plan budget exhausted; lock retained")
            try:
                result = subprocess.run(
                    ["tofu", "-chdir=" + str(path), *arguments],
                    env=bootstrap.provider_environment(args.kubeconfig),
                    capture_output=True,
                    text=True,
                    timeout=remaining,
                    check=False,
                )
            except subprocess.TimeoutExpired as error:
                output = "".join(
                    value.decode(errors="replace")
                    if isinstance(value, bytes)
                    else value
                    for value in (error.stdout or "", error.stderr or "")
                )
                (args.directory / (root + "-" + arguments[0] + ".log")).write_text(
                    output
                )
                print(
                    output.encode()[-65536:].decode(errors="ignore"),
                    file=sys.stderr,
                    flush=True,
                )
                raise rt.Failure(
                    f"{root} {arguments[0]} exhausted the {rt.Schedule.plan_budget_seconds}-second plan budget; lock retained, inspect the output above and retry recovery"
                ) from None
            (args.directory / (root + "-" + arguments[0] + ".log")).write_text(
                result.stdout + result.stderr
            )
            if (
                result.returncode == 2
                and arguments[0] == "plan"
                and owner["kind"] in ("plan", "apply")
            ):
                empty = False
            elif result.returncode:
                raise rt.Failure(
                    f"{root} {arguments[0]} did not prove an empty plan (exit {result.returncode})"
                )
    save(
        args.directory / "plans.json",
        {
            "nonce": owner["nonce"],
            "roots": ROOTS,
            "empty": empty,
            "at": rt.utc(time.time()),
        },
    )
