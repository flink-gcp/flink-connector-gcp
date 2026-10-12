#!/usr/bin/env python3
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
"""Actions frontend for the bounded Tier-3 lifecycle; never dispatches a run."""

from __future__ import annotations

import argparse
import contextlib
import copy
import functools
import json
import math
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import flink_tier3 as rt

from . import bootstrap, repository
from . import runner as runner_api
from . import workflow as wf
from .bigquery import actors as bigquery_actors
from .bigquery import plan as bigquery_plan
from .model import APPROVAL_VERSION, SCENARIOS
from .policy import BIGQUERY, DIGEST, PUBSUB
from .pubsub import actors as pubsub_actors
from .pubsub import plan as pubsub_plan

PUBSUB_TRIALS = Path("kubernetes/lifecycle/pubsub-trials")


def trial_inputs(args):
    """Resolve the named trial and the live application digest."""
    trial = bigquery_plan.trial(args.trial)
    if not DIGEST.fullmatch(args.application_digest or ""):
        raise rt.Failure("BigQuery trials need a sha256 --application-digest")
    return trial, rt.GAR + "bigquery-recovery@" + args.application_digest


def window(seconds):
    """The run's window: it starts at admission and lasts `seconds`.

    Starting at admission rather than at dispatch keeps the job's queueing out
    of the startup budget every deadline counts from. A service approval
    requires whole seconds.
    """
    started = int(time.time())
    return started, started + seconds


def bigquery_approval(
    *,
    run_id,
    nonce,
    sha,
    trial,
    proposal,
    namespaces,
    operator_uid,
    baseline,
    images,
    owner,
    actor,
):
    """Assemble the approval from a rendered, verified proposal.

    Every digest comes from the proposal, which rendered and checked the
    manifests itself; nothing here renders or hashes a second time.
    """
    return {
        "version": APPROVAL_VERSION,
        "scenario": "bigquery-recovery",
        "run_id": run_id,
        "nonce": nonce,
        "sha": sha,
        "started_at": proposal["started_at"],
        "expires_at": proposal["expires_at"],
        "cleanup_at": proposal["cleanup_at"],
        "bigquery_trial": copy.deepcopy(trial),
        "namespaces": namespaces,
        "operator_uid": operator_uid,
        "baseline_uids": baseline,
        "images": images,
        "lock_owner": owner,
        "application_sha256": proposal["application_sha256"],
        "upgrade_application_sha256": proposal["upgrade_application_sha256"],
        "actor": actor,
    }


def pubsub_inputs(args):
    """Resolve the reviewed trial file and the live application digest."""
    if not rt.RUN_ID.fullmatch(args.pubsub_trial or ""):
        raise rt.Failure("Pub/Sub trials need a reviewed --pubsub-trial name")
    if not DIGEST.fullmatch(args.application_digest or ""):
        raise rt.Failure("Pub/Sub trials need a sha256 --application-digest")
    trial = pubsub_plan.load_reviewed_trial(
        repository.ROOT / PUBSUB_TRIALS / (args.pubsub_trial + ".toml")
    )
    return trial, rt.GAR + "pubsub-recovery@" + args.application_digest


def pubsub_approval(
    *,
    run_id,
    nonce,
    sha,
    trial,
    proposal,
    namespaces,
    operator_uid,
    baseline,
    images,
    owner,
    actor,
):
    """Assemble the approval from a rendered, verified proposal.

    Every digest comes from the proposal, which rendered and checked the
    manifests itself. The proposal names the second manifest the recovery
    application; the approval keeps the name every service scenario shares.
    """
    return {
        "version": APPROVAL_VERSION,
        "scenario": "pubsub-recovery",
        "run_id": run_id,
        "nonce": nonce,
        "sha": sha,
        "started_at": proposal["started_at"],
        "expires_at": proposal["expires_at"],
        "cleanup_at": proposal["cleanup_at"],
        "pubsub_trial": copy.deepcopy(trial),
        "namespaces": namespaces,
        "operator_uid": operator_uid,
        "baseline_uids": baseline,
        "images": images,
        "lock_owner": owner,
        "application_sha256": proposal["application_sha256"],
        "upgrade_application_sha256": proposal["recovery_application_sha256"],
        "actor": actor,
    }


def start_pubsub(args, store):
    """Dispatch one Pub/Sub recovery trial from its reviewed file.

    Everything that can refuse without the lock does so first: the cluster's
    idle state, the image receipts, the rendering and the ConfigMap size.
    """
    trial, application_image = pubsub_inputs(args)
    sha = refuse_before_admission(args, store)
    started, end = window(pubsub_plan.WINDOW_SECONDS)
    nonce = uuid.uuid4().hex
    owner = rig_owner(wf.execution("run", nonce), args.run_id, sha)
    kube = external(args.kubeconfig, idle=True)
    cluster = bootstrap.Cluster(args.kubeconfig)
    cluster.can_i(True, "create", "flink.apache.org", "flinkdeployments", PUBSUB)
    # Admission creates the workload access probe Pod in the namespace.
    cluster.can_i(True, "create", "", "pods", PUBSUB)
    namespaces, operator_uid, operator_image, baseline = wf.snapshot(kube, PUBSUB)
    rendered = pubsub_plan.prepare(
        run_id=args.run_id,
        nonce=nonce,
        started_at=rt.utc(started),
        expires_at=rt.utc(end),
        active_seconds=pubsub_plan.ACTIVE_SECONDS,
        revision=sha,
        application_image=application_image,
        trial=trial,
    )
    proposal = rendered["proposal"]
    images = {
        "operator": operator_image,
        "supervisor": proposal["images"]["supervisor"],
        "application": application_image,
    }
    receipts = wf.image_receipts(
        rt.authorized_session(rt.GoogleToken()), images, proposal["expires_at"]
    )
    approval = pubsub_approval(
        run_id=args.run_id,
        nonce=nonce,
        sha=sha,
        trial=trial,
        proposal=proposal,
        namespaces=namespaces,
        operator_uid=operator_uid,
        baseline=baseline,
        images=images,
        owner=owner,
        actor=os.environ["GITHUB_ACTOR"],
    )
    rt.validate_approval(approval)
    delivery = bind(
        rt.Approval.from_dict(approval), rendered["delivery"], math.ceil(time.time())
    )
    run_service_trial(
        args,
        store,
        kube,
        foundation=(namespaces, operator_uid, operator_image, baseline),
        namespace=PUBSUB,
        owner=owner,
        approval=approval,
        documents={
            "application.json": rendered["application"],
            "images.json": receipts,
            "upgrade-application.json": rendered["recovery_application"],
        },
        actor=pubsub_actors.runner,
        admitted=(
            delivery["config"],
            delivery["supervisor"],
            rendered["application"],
            delivery["probe"],
        ),
    )


def bind(approved, delivery, when):
    """Embed the approval and the supervisor Job's deadline in a delivery.

    These two fields vary after approval; every other rendered field is the
    one the proposal checked and the approval pins.
    """
    delivery["config"]["data"]["approval.json"] = rt.json_bytes(
        approved.to_dict()
    ).decode()
    delivery["supervisor"]["spec"]["activeDeadlineSeconds"] = (
        approved.schedule.active_seconds(when)
    )
    data = delivery["config"]["data"]
    if (
        sum(len(key.encode()) + len(value.encode()) for key, value in data.items())
        > rt.MIB
    ):
        raise rt.Failure("Approved ConfigMap exceeds the Kubernetes data size limit")
    return delivery


def runner_for(approval, kube, store):
    """Build the external runner over the approval's environment."""
    return runner_api.Runner(rt.Environment(kube, store, approval))


def rig_owner(owner, run_id, sha):
    """The lock owner, naming the rig commit when it is not the workflow's."""
    owner = owner | {"run_id": run_id}
    if sha != owner["sha"]:
        owner["rig_sha"] = sha
    return owner


def refuse_before_admission(args, store):
    """Refuse what every dispatch refuses before it touches the cluster or the
    lock, and return the rig commit the run is pinned to.

    The workflow always runs from `main`; the rig it checks out is the
    dispatched `main` commit unless `--rig-sha` names another. The workflow,
    not this code, verifies that such a commit heads a branch of this
    repository, before it checks the rig out. The owner approves the run's
    estimate before dispatching it; dispatching is that approval.
    """
    if os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise rt.Failure("Dispatch must run from main")
    sha = getattr(args, "rig_sha", None) or os.environ.get("GITHUB_SHA")
    if not rt.SHA.fullmatch(sha or ""):
        raise rt.Failure("Dispatch must name the exact rig commit")
    if not rt.RUN_ID.fullmatch(args.run_id):
        raise rt.Failure("Invalid run ID")
    if store.objects(f"runs/{args.run_id}/"):
        raise rt.Failure("Run ID already has immutable evidence; choose a new ID")
    return sha


def start_bigquery(args, store):
    """Dispatch one BigQuery recovery trial.

    Everything that can refuse without the lock does so first, including the
    rendering and ConfigMap size. The runner's token is minted here and lives
    only in this process and in the binding it writes; the supervisor adopts
    it from that binding.
    """
    trial, application_image = trial_inputs(args)
    sha = refuse_before_admission(args, store)
    started, end = window(bigquery_plan.WINDOW_SECONDS)
    nonce = uuid.uuid4().hex
    owner = rig_owner(wf.execution("run", nonce), args.run_id, sha)
    kube = external(args.kubeconfig, idle=True)
    bootstrap.Cluster(args.kubeconfig).can_i(
        True, "create", "flink.apache.org", "flinkdeployments", BIGQUERY
    )
    namespaces, operator_uid, operator_image, baseline = wf.snapshot(kube, BIGQUERY)
    rendered = bigquery_plan.prepare(
        run_id=args.run_id,
        nonce=nonce,
        started_at=rt.utc(started),
        expires_at=rt.utc(end),
        active_seconds=bigquery_plan.ACTIVE_SECONDS,
        revision=sha,
        application_image=application_image,
        trial=trial,
    )
    proposal = rendered["proposal"]
    images = {
        "operator": operator_image,
        "supervisor": proposal["images"]["supervisor"],
        "application": application_image,
    }
    receipts = wf.image_receipts(
        rt.authorized_session(rt.GoogleToken()), images, proposal["expires_at"]
    )
    approval = bigquery_approval(
        run_id=args.run_id,
        nonce=nonce,
        sha=sha,
        trial=trial,
        proposal=proposal,
        namespaces=namespaces,
        operator_uid=operator_uid,
        baseline=baseline,
        images=images,
        owner=owner,
        actor=os.environ["GITHUB_ACTOR"],
    )
    rt.validate_approval(approval)
    # Bound before the lock so the ConfigMap size refuses without holding
    # anything. Its Job deadline is taken now; the seconds the lock and the
    # documents take extend it by that much.
    delivery = bind(
        rt.Approval.from_dict(approval), rendered["delivery"], math.ceil(time.time())
    )
    run_service_trial(
        args,
        store,
        kube,
        foundation=(namespaces, operator_uid, operator_image, baseline),
        namespace=BIGQUERY,
        owner=owner,
        approval=approval,
        documents={
            "application.json": rendered["application"],
            "images.json": receipts,
            "upgrade-application.json": rendered["upgrade_application"],
        },
        actor=functools.partial(
            bigquery_actors.runner, application=rendered["application"]
        ),
        admitted=(
            delivery["config"],
            delivery["supervisor"],
            rendered["application"],
        ),
    )


def run_service_trial(
    args,
    store,
    kube,
    *,
    foundation,
    namespace,
    owner,
    approval,
    documents,
    actor,
    admitted,
):
    """Lock the environment, record the run and admit it through its actor.

    Everything that can refuse without the lock has done so. The runner's
    token is minted here and lives only in this process and in the binding it
    writes; the supervisor adopts it from that binding.
    """
    wf.save(args.directory / "owner.json", owner)
    rt.EnvironmentLock(store).acquire(owner)
    # Recheck idle after acquiring exclusivity; the earlier snapshot cannot
    # authorize a change that raced an infrastructure apply.
    if wf.snapshot(kube, namespace) != foundation:
        raise rt.Failure("Foundation changed while acquiring the environment lock")
    wf.save(args.directory / "approval.json", approval)
    artifact = functools.partial(rt.write_artifact, store, args.run_id)
    artifact("approval.json", approval)
    for name, value in documents.items():
        artifact(name, value)
    store.write(
        f"_control/runs/{args.run_id}.json",
        {"nonce": approval["nonce"], "phase": "approved", "roots": {}, "observed": {}},
    )
    env = rt.Environment(kube, store, approval, actor="runner")
    with contextlib.ExitStack() as session:
        try:
            runner = session.enter_context(actor(env, runner_token=uuid.uuid4().hex))
        except BaseException:
            # Nothing is admitted and no service state exists yet, so a plain
            # runner can settle what the lock and the documents hold.
            try:
                runner_for(approval, kube, store).settle(request_stop=True)
            finally:
                idle_output()
            raise

        def stop(_number, _frame):
            runner.env.stopping = True

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        failed = True
        try:
            runner.start(*admitted)
            failed = False
        finally:
            runner.settle(request_stop=failed)
            idle_output()


def start(args, store):
    scenario = getattr(args, "scenario", "smoke")
    if scenario not in SCENARIOS:
        raise rt.Failure("Unknown smoke scenario")
    if scenario == "bigquery-recovery":
        return start_bigquery(args, store)
    if scenario == "pubsub-recovery":
        return start_pubsub(args, store)
    sha = refuse_before_admission(args, store)
    now, end = window(rt.WINDOWS["smoke"])
    expires_at = rt.utc(end)
    nonce = uuid.uuid4().hex
    owner = rig_owner(wf.execution("run", nonce), args.run_id, sha)
    kube = external(args.kubeconfig, idle=True)
    bootstrap.Cluster(args.kubeconfig).can_i(
        True, "create", "flink.apache.org", "flinkdeployments", rt.SMOKE
    )
    namespaces, operator_uid, operator_image, baseline = wf.snapshot(kube, rt.SMOKE)
    schedule = rt.Schedule.for_window(now, end)
    active = schedule.active_seconds(time.time())
    application = wf.render(args.run_id, nonce, expires_at, active)
    bundle = wf.render(
        args.run_id, nonce, expires_at, active, expression="delivery.resources"
    )
    images = {
        "operator": operator_image,
        "supervisor": bundle["supervisor"]["spec"]["template"]["spec"]["containers"][0][
            "image"
        ],
        "smoke": application["spec"]["image"],
    }
    receipts = wf.image_receipts(
        rt.authorized_session(rt.GoogleToken()), images, expires_at
    )
    approval = {
        "version": APPROVAL_VERSION,
        "run_id": args.run_id,
        "nonce": nonce,
        "sha": sha,
        "started_at": rt.utc(now),
        "expires_at": expires_at,
        "cleanup_at": rt.utc(schedule.cleanup_at),
        "namespaces": namespaces,
        "operator_uid": operator_uid,
        "baseline_uids": baseline,
        "images": images,
        "lock_owner": owner,
        "application_sha256": rt.digest(application),
        "actor": os.environ["GITHUB_ACTOR"],
    }
    rt.validate_approval(approval)
    wf.save(args.directory / "owner.json", owner)
    rt.EnvironmentLock(store).acquire(owner)
    # Recheck idle after acquiring exclusivity; the earlier snapshot cannot
    # authorize a change that raced an infrastructure apply.
    if wf.snapshot(kube, rt.SMOKE) != (
        namespaces,
        operator_uid,
        operator_image,
        baseline,
    ):
        raise rt.Failure("Foundation changed while acquiring the environment lock")
    wf.save(args.directory / "approval.json", approval)
    artifact = functools.partial(rt.write_artifact, store, args.run_id)
    artifact("approval.json", approval)
    artifact("application.json", application)
    artifact("images.json", receipts)
    store.write(
        f"_control/runs/{args.run_id}.json",
        {"nonce": nonce, "phase": "approved", "roots": {}, "observed": {}},
    )
    active = rt.Approval.from_dict(approval).schedule.active_seconds(time.time())
    # Only the Job deadline changes between renders; application bytes do not
    # depend on active_seconds or the approval embedded in the ConfigMap.
    bundle = wf.render(
        args.run_id,
        nonce,
        expires_at,
        active,
        approval,
        "delivery.resources",
    )
    runner = runner_for(approval, kube, store)

    def stop(_number, _frame):
        runner.env.stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    failed = True
    try:
        runner.start(bundle["config"], bundle["supervisor"], application)
        failed = False
    finally:
        runner.settle(request_stop=failed)
        idle_output()


def recover(args, store):
    owner, _ = store.read(rt.ENVIRONMENT)
    if owner is None:
        print("Environment has no retained lock")
        return
    if str(owner["github_run_id"]) != args.source_id:
        if os.environ.get("GITHUB_EVENT_NAME") == "workflow_run":
            print("Completed execution does not hold the environment lock")
            return
        raise rt.Failure("Recovery source does not hold the environment lock")
    # The workflow's per-source concurrency group keeps a second recovery of
    # this source from running beside this one.
    if wf.github_run(owner["github_run_id"])["status"] != "completed":
        raise rt.Failure(
            "Originating execution is still active; recovery cannot race it"
        )
    wf.save(args.directory / "owner.json", owner)
    approval = None
    if owner["kind"] == "run":
        approval, _ = store.read(f"runs/{owner['run_id']}/approval.json")
    kube = external(args.kubeconfig, idle=approval is None)
    if approval:
        if approval["lock_owner"] != owner:
            raise rt.Failure("Immutable approval does not match lock holder")
        wf.save(args.directory / "approval.json", approval)
        runner = runner_for(approval, kube, store)
        control, _ = store.read(runner.env.records.path)
        if control is None:
            # No admission occurs before this record exists, and finalization
            # deletes it only after writing the final receipt.
            bootstrap.Cluster(args.kubeconfig).preflight()
            runner.restore_control()
        runner.settle(request_stop=True)
    elif owner["kind"] == "run":
        wf.snapshot(kube)
    # Infrastructure recovery already passed the idle preflight. A previous
    # installed image may differ from main after an interrupted upgrade; the
    # refreshed plan reports that drift without blocking lock release.
    wf.save(args.directory / "recovered.json", {"nonce": owner["nonce"]})
    idle_output()


def idle_output():
    if os.environ.get("GITHUB_OUTPUT"):
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
            output.write("idle=true\n")


def finish(args, store):
    owner = json.loads((args.directory / "owner.json").read_text())
    plans_receipt = json.loads((args.directory / "plans.json").read_text())
    approval_path = args.directory / "approval.json"
    success = True
    if approval_path.exists():
        approval = json.loads(approval_path.read_text())
        runner = runner_for(approval, external(args.kubeconfig), store)
        runner.env.records.evidence(
            "empty-plans",
            {
                "receipt": plans_receipt,
                "logs": {
                    root: (args.directory / (root + "-plan.log")).read_text()
                    for root in wf.ROOTS
                },
            },
            "runner",
        )
        success = runner.finalize(plans_receipt)
    else:
        if (
            plans_receipt.get("nonce") != owner["nonce"]
            or plans_receipt.get("roots") != wf.ROOTS
            or (owner["kind"] == "run" and not plans_receipt.get("empty"))
        ):
            raise rt.Failure("Missing idle infrastructure proof")
        external(args.kubeconfig, idle=True)
        if owner["kind"] in ("plan", "apply"):
            store.write(
                f"runs/infrastructure-{owner['github_run_id']}-{os.environ['GITHUB_RUN_ID']}/recovery.json",
                {"owner": owner, "plans": plans_receipt, "idle": True},
            )
        rt.EnvironmentLock(store).release(owner)
        success = plans_receipt["empty"]
    if not success:
        raise rt.Failure(
            "Environment restored to idle, but the run failed; see immutable evidence"
        )


def lock(args, store):
    if args.operation == "acquire":
        owner = wf.execution(args.kind, uuid.uuid4().hex) | {"target": args.target}
        wf.save(args.file, owner)
        deadline = time.time() + 1200
        while True:
            try:
                rt.EnvironmentLock(store).acquire(owner, inspect_runs=False)
                return
            except rt.ApiError as error:
                if error.status not in (409, 412) or time.time() >= deadline:
                    raise
                time.sleep(15)
    else:
        if not args.file.exists():
            return
        owner = json.loads(args.file.read_text())
        actual, _ = store.read(rt.ENVIRONMENT)
        if actual == owner:
            rt.EnvironmentLock(store).release(owner)


# Only the checkout commands authenticate to the cluster or plan the roots, so
# these stay out of every module the supervisor's delivery reaches.
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


def plans(args, store):
    owner = json.loads((args.directory / "owner.json").read_text())
    rt.EnvironmentLock(store).assert_owner(owner)
    # This command runs only after re-authentication as opentofu-plan. Its
    # authority may borrow this exact run lock, never a different holder's.
    empty = True
    deadline = time.monotonic() + rt.Schedule.plan_budget_seconds
    for root in wf.ROOTS:
        rt.EnvironmentLock(store).assert_owner(owner)
        if root != "flink-gcp":
            cluster = bootstrap.Cluster(args.kubeconfig, root.removeprefix("tier3-"))
            cluster.authenticate()
            cluster.preflight()
            if root == "tier3-operator":
                bootstrap.prepare_operator_chart(cluster.root_path)
        path = repository.ROOT / "opentofu" / root
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
    wf.save(
        args.directory / "plans.json",
        {
            "nonce": owner["nonce"],
            "roots": wf.ROOTS,
            "empty": empty,
            "at": rt.utc(time.time()),
        },
    )


def main(argv=None):
    parser = argparse.ArgumentParser(prog="flink-tier3 lifecycle", description=__doc__)
    parser.add_argument("--kubeconfig", type=Path)
    parser.add_argument(
        "--directory",
        type=Path,
        default=Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "tier3-lifecycle",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("start")
    run.add_argument("--run-id", required=True)
    run.add_argument(
        "--rig-sha", help="rig commit checked out, when not the workflow's"
    )
    run.add_argument("--scenario", choices=SCENARIOS, default="smoke")
    run.add_argument("--application-digest")
    run.add_argument(
        "--trial", help="BigQuery trial: " + ", ".join(bigquery_plan.TRIALS)
    )
    run.add_argument(
        "--pubsub-trial",
        help=f"Pub/Sub trial: a file under {PUBSUB_TRIALS}/, without .toml",
    )
    recovery = sub.add_parser("recover")
    recovery.add_argument("--source-id", required=True)
    sub.add_parser("plans")
    sub.add_parser("finish")
    locking = sub.add_parser("lock")
    locking.add_argument("operation", choices=("acquire", "release"))
    locking.add_argument("--file", type=Path, required=True)
    locking.add_argument("--kind", choices=("plan", "apply"))
    locking.add_argument("--target")
    args = parser.parse_args(argv)
    if args.command != "lock" and args.kubeconfig is None:
        parser.error("--kubeconfig is required")
    if args.command == "lock" and args.operation == "acquire" and args.kind is None:
        parser.error("lock acquire requires --kind")
    store = rt.Storage()
    {
        "start": start,
        "recover": recover,
        "plans": plans,
        "finish": finish,
        "lock": lock,
    }[args.command](args, store)


if __name__ == "__main__":
    main()
