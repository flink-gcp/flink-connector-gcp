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
import time
import uuid
from pathlib import Path

import flink_tier3 as rt

from . import bigquery_actors, bigquery_bundle, bigquery_plan, bootstrap
from . import runner as runner_api
from . import workflow as wf
from .cloudtasks import QUEUE_POLL_MASK, Ledger, Queues, load_session
from .exercise import validate_manifests
from .model import queue_name, session_plan
from .policy import (
    BIGQUERY,
    BIGQUERY_CEILINGS,
    CLOUDTASKS_CEILINGS,
    DIGEST,
    FLINK_LINES,
    RECOVERY,
)

ROOT = Path.cwd()
APPROVAL = "APPROVE ONE SMOKE RUN: 5 PODS, 60 MINUTES"
CLOUDTASKS_APPROVAL = (
    "APPROVE ONE CLOUD TASKS SESSION: 5 PODS, 300 MINUTES, 0 DISPATCHES"
)
BIGQUERY_APPROVAL = (
    f"APPROVE ONE BIGQUERY TRIAL: {BIGQUERY_CEILINGS['pods']} PODS, "
    f"{BIGQUERY_CEILINGS['seconds'] // 60} MINUTES"
)
SCENARIOS = ("smoke", "generic-recovery", "cloudtasks", "bigquery-recovery")
# How far the operator's typed expiry may lie beyond the run's own end. The
# window starts at admission, so queueing before dispatch costs the run none of
# its startup budget; the typed expiry is the latest the run may end, and it
# must not overstate that by more than this.
BIGQUERY_EXPIRY_SLACK_SECONDS = 600


def trial_inputs(args):
    """Resolve the named trial and the live application digest."""
    trial = bigquery_plan.trial(args.trial)
    if not DIGEST.fullmatch(args.application_digest or ""):
        raise rt.Failure("BigQuery trials need a sha256 --application-digest")
    return trial, rt.GAR + "bigquery-recovery@" + args.application_digest


def bigquery_window(now, expires_at):
    """The approved window: it starts at admission and ends by the typed expiry.

    The approval requires exactly 90 minutes on whole seconds, and a bundle
    cannot be prepared before its start. Starting the window at admission
    rather than deriving it from the expiry keeps dispatch queueing out of the
    600-second startup budget that every deadline counts from.
    """
    started = int(now)
    end = started + BIGQUERY_CEILINGS["seconds"]
    if not end <= rt.timestamp(expires_at) <= end + BIGQUERY_EXPIRY_SLACK_SECONDS:
        raise rt.Failure(
            "BigQuery expiry must be 90 to 100 minutes after dispatch; the run "
            "ends 90 minutes after admission, never later than the expiry"
        )
    return started, end


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
    """Assemble the version 4 approval from a rendered, verified proposal.

    Every digest comes from the proposal, which rendered and checked the
    manifests itself; nothing here renders or hashes a second time.
    """
    return {
        "version": 4,
        "scenario": "bigquery-recovery",
        "run_id": run_id,
        "nonce": nonce,
        "sha": sha,
        "started_at": proposal["started_at"],
        "expires_at": proposal["expires_at"],
        "cleanup_at": proposal["cleanup_at"],
        "ceilings": dict(BIGQUERY_CEILINGS),
        "bigquery_trial": copy.deepcopy(trial),
        "namespaces": namespaces,
        "operator_uid": operator_uid,
        "baseline_uids": baseline,
        "images": images,
        "lock_owner": owner,
        "runtime_sha256": proposal["runtime_sha256"],
        "delivery_sha256": proposal["delivery_sha256"],
        "application_sha256": proposal["application_sha256"],
        "upgrade_application_sha256": proposal["upgrade_application_sha256"],
        "actor": actor,
    }


def runner_for(approval, kube, store, queues=None, ledger=None):
    """Build the external runner with the scenario's collaborators."""
    if approval.get("scenario") == "cloudtasks":
        queues = queues or Queues(
            rt.authorized_session(rt.GoogleToken()), approval["queue"]
        )
        ledger = ledger or Ledger(store, approval["campaign"])
    return runner_api.Runner(
        rt.Environment(kube, store, approval, queues=queues, ledger=ledger)
    )


def session_inputs(args):
    """Resolve the reviewed session file, Flink line and live application digest."""
    if not rt.RUN_ID.fullmatch(args.session or ""):
        raise rt.Failure("Cloud Tasks sessions need a reviewed --session name")
    if args.flink_version not in FLINK_LINES:
        raise rt.Failure("Cloud Tasks sessions need a supported --flink-version")
    if not DIGEST.fullmatch(args.application_digest or ""):
        raise rt.Failure("Cloud Tasks sessions need a sha256 --application-digest")
    session = load_session(
        ROOT / "kubernetes/lifecycle/sessions" / (args.session + ".toml")
    )
    image = rt.GAR + FLINK_LINES[args.flink_version][0] + "@" + args.application_digest
    return session, args.flink_version, image


def schedulable(node):
    """A node the supervisor could land on: ready, uncordoned, AMD64, not Spot."""
    conditions = node.get("status", {}).get("conditions", [])
    labels = node.get("metadata", {}).get("labels", {})
    ready = any(
        c.get("type") == "Ready" and c.get("status") == "True" for c in conditions
    )
    return (
        ready
        and not node.get("spec", {}).get("unschedulable")
        and labels.get("kubernetes.io/arch") == "amd64"
        and labels.get("cloud.google.com/gke-spot") != "true"
    )


def require_capacity(kube):
    """Refuse a dispatch onto exactly one schedulable node.

    On that node a system Pod scaling up with the cluster preempts the
    supervisor within seconds; from no nodes Autopilot provisions one for it,
    and with two the system Pods have room. A draining node does not count.
    """
    nodes = kube.nodes()
    ready = sum(1 for node in nodes if schedulable(node))
    if nodes and ready < 2:
        raise rt.Failure(
            f"Cluster has {ready} schedulable of {len(nodes)} nodes; dispatch when "
            "it has none or at least two"
        )


def rig_owner(owner, args):
    """The lock owner, naming the rig commit when it is not the workflow's."""
    owner = owner | {"run_id": args.run_id}
    if args.sha != owner["sha"]:
        owner["rig_sha"] = args.sha
    return owner


def refuse_before_admission(args, store, phrase):
    """What every dispatch refuses before it touches the cluster or the lock.

    The workflow always runs from `main`; the rig it checks out is the
    dispatched `main` commit unless `--rig-sha` names another, which must then
    be the approved commit. The workflow, not this code, verifies that such a
    commit heads a branch of this repository, before it checks the rig out.
    """
    rig = getattr(args, "rig_sha", None) or os.environ.get("GITHUB_SHA")
    if (
        args.approve != phrase
        or os.environ.get("GITHUB_REF") != "refs/heads/main"
        or args.sha != rig
    ):
        raise rt.Failure(
            "Dispatch must explicitly approve the ceilings and the exact rig commit"
        )
    if not rt.RUN_ID.fullmatch(args.run_id):
        raise rt.Failure("Invalid run ID")
    if store.objects(f"runs/{args.run_id}/"):
        raise rt.Failure("Run ID already has immutable evidence; choose a new ID")


def start_bigquery(args, store):
    """Dispatch one approved BigQuery recovery trial.

    Everything that can refuse without the lock does so first, including the
    bundle's revision check, rendering and ConfigMap size. The runner's token is
    minted here and lives only in this process and in the binding it writes;
    the supervisor adopts it from that binding.
    """
    trial, application_image = trial_inputs(args)
    refuse_before_admission(args, store, BIGQUERY_APPROVAL)
    started, end = bigquery_window(time.time(), args.expires_at)
    nonce = uuid.uuid4().hex
    owner = rig_owner(wf.execution("run", nonce), args)
    kube = wf.external(args.kubeconfig, idle=True)
    require_capacity(kube)
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
        revision=args.sha,
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
        sha=args.sha,
        trial=trial,
        proposal=proposal,
        namespaces=namespaces,
        operator_uid=operator_uid,
        baseline=baseline,
        images=images,
        owner=owner,
        actor=os.environ["GITHUB_ACTOR"],
    )
    rt.validate_approval(approval, time.time())
    # Prepared before the lock so its revision check, render and ConfigMap
    # size refuse without holding anything. Its Job deadline is taken now; the
    # seconds the lock and the documents take extend it by that much, as the
    # authentication and re-render inside the runner factory already did.
    bundle = bigquery_bundle.prepare(
        approval, prepared_at=rt.utc(math.ceil(time.time()))
    )
    wf.save(args.directory / "owner.json", owner)
    rt.EnvironmentLock(store).acquire(owner)
    # Recheck idle after acquiring exclusivity; the earlier snapshot cannot
    # authorize a change that raced an infrastructure apply.
    if wf.snapshot(kube, BIGQUERY) != (
        namespaces,
        operator_uid,
        operator_image,
        baseline,
    ):
        raise rt.Failure("Foundation changed while acquiring the environment lock")
    wf.save(args.directory / "approval.json", approval)
    artifact = functools.partial(
        rt.write_artifact, store, args.run_id, scenario="bigquery-recovery"
    )
    artifact("approval.json", approval)
    artifact("application.json", rendered["application"])
    artifact("images.json", receipts)
    artifact("upgrade-application.json", rendered["upgrade_application"])
    store.write(
        f"_control/runs/{args.run_id}.json",
        {"nonce": nonce, "phase": "approved", "roots": {}, "observed": {}},
    )
    env = rt.Environment(kube, store, approval, actor="runner")
    with contextlib.ExitStack() as session:
        try:
            runner = session.enter_context(
                bigquery_actors.runner(env, bundle, runner_token=uuid.uuid4().hex)
            )
        except BaseException:
            # Nothing is admitted and no BigQuery state exists yet, so a plain
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
            runner.start(
                bundle["delivery"]["config"],
                bundle["delivery"]["supervisor"],
                bundle["application"],
            )
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
    cloudtasks = scenario == "cloudtasks"
    refuse_before_admission(
        args, store, CLOUDTASKS_APPROVAL if cloudtasks else APPROVAL
    )
    now = time.time()
    end = rt.timestamp(args.expires_at)
    session = line = application_image = None
    if cloudtasks:
        session, line, application_image = session_inputs(args)
        plan = session_plan(session["cells"]) + CLOUDTASKS_CEILINGS["cleanup_seconds"]
        if (
            not plan
            <= end - now
            <= min(CLOUDTASKS_CEILINGS["session_seconds"], plan + 600)
        ):
            raise rt.Failure(
                "Dispatch expiry must cover the session plan plus cleanup, "
                "within ten minutes and the session ceiling"
            )
    elif not 3300 <= end - now <= 3600:
        raise rt.Failure("Dispatch expiry must be 55 to 60 minutes ahead")
    nonce = uuid.uuid4().hex
    owner = rig_owner(wf.execution("run", nonce), args)
    namespace = rt.CLOUDTASKS if cloudtasks else rt.SMOKE
    kube = wf.external(args.kubeconfig, idle=True)
    require_capacity(kube)
    bootstrap.Cluster(args.kubeconfig).can_i(
        True, "create", "flink.apache.org", "flinkdeployments", namespace
    )
    queues = ledger = None
    if cloudtasks:
        # Cheap refusals before any lock or cluster mutation.
        queues = Queues(
            rt.authorized_session(rt.GoogleToken()), queue_name(args.run_id)
        )
        if queues.get(QUEUE_POLL_MASK) is not None:
            raise rt.Failure("Queue already exists; this run does not own it")
        ledger = Ledger(store, session["campaign"])
        ledger.admit([cell["id"] for cell in session["cells"]])
    namespaces, operator_uid, operator_image, baseline = wf.snapshot(kube, namespace)
    schedule = rt.Schedule.for_window(now, end)
    active = schedule.active_seconds(time.time())
    render_options = {"scenario": scenario} if scenario != "smoke" else {}
    manifest_options = {}
    if cloudtasks:
        render_options.update(
            cells=json.dumps(session["cells"], separators=(",", ":")),
            flink_version=line,
            application_image=application_image,
            target=rt.CLOUDTASKS_POLICY["target"],
        )
        manifest_options = {"expression": "cellManifests"}
    application = wf.render(
        args.run_id,
        nonce,
        args.expires_at,
        active,
        **manifest_options,
        **render_options,
    )
    bundle = wf.render(
        args.run_id,
        nonce,
        args.expires_at,
        active,
        expression="delivery.resources",
        **render_options,
    )
    images = {
        "operator": operator_image,
        "supervisor": bundle["supervisor"]["spec"]["template"]["spec"]["containers"][0][
            "image"
        ],
    }
    if cloudtasks:
        if any(m["spec"]["image"] != application_image for m in application):
            raise rt.Failure(
                "Rendered cells do not use the dispatched application image"
            )
        images["application"] = application_image
    else:
        images["smoke"] = application["spec"]["image"]
    receipts = wf.image_receipts(
        rt.authorized_session(rt.GoogleToken()), images, args.expires_at
    )
    approval = {
        "version": 1,
        "run_id": args.run_id,
        "nonce": nonce,
        "sha": args.sha,
        "started_at": rt.utc(now),
        "expires_at": args.expires_at,
        "cleanup_at": rt.utc(schedule.cleanup_at),
        "ceilings": rt.CEILINGS,
        "namespaces": namespaces,
        "operator_uid": operator_uid,
        "baseline_uids": baseline,
        "images": images,
        "lock_owner": owner,
        "runtime_sha256": rt.source_digest(),
        "delivery_sha256": rt.delivery_digest(),
        "application_sha256": rt.digest(application),
        "actor": os.environ["GITHUB_ACTOR"],
    }
    upgrade = None
    if scenario == "generic-recovery":
        upgrade = wf.render(
            args.run_id,
            nonce,
            args.expires_at,
            active,
            expression="upgradeApplication",
            **render_options,
        )
        approval.update(
            version=2,
            scenario=scenario,
            recovery_policy=RECOVERY,
            upgrade_application_sha256=rt.digest(upgrade),
        )
        validate_manifests(application, upgrade)
    if cloudtasks:
        if len(application) != len(session["cells"]):
            raise rt.Failure("Rendered cell count differs from the session")
        approval.update(
            version=3,
            scenario=scenario,
            campaign=session["campaign"],
            flink_version=line,
            queue=queues.name,
            target=rt.CLOUDTASKS_POLICY["target"],
            cells=[
                {**cell, "manifest_sha256": rt.digest(manifest)}
                for cell, manifest in zip(session["cells"], application, strict=True)
            ],
            cloudtasks_ceilings=rt.CLOUDTASKS_CEILINGS,
            cloudtasks_pod_resources=rt.CLOUDTASKS_POD_RESOURCES,
        )
    rt.validate_approval(approval, time.time())
    wf.save(args.directory / "owner.json", owner)
    rt.EnvironmentLock(store).acquire(owner)
    # Recheck idle after acquiring exclusivity; the earlier snapshot cannot
    # authorize a change that raced an infrastructure apply.
    if wf.snapshot(kube, namespace) != (
        namespaces,
        operator_uid,
        operator_image,
        baseline,
    ):
        raise rt.Failure("Foundation changed while acquiring the environment lock")
    wf.save(args.directory / "approval.json", approval)
    artifact = functools.partial(
        rt.write_artifact,
        store,
        args.run_id,
        scenario=approval.get("scenario", "smoke"),
    )
    artifact("approval.json", approval)
    artifact("application.json", application)
    artifact("images.json", receipts)
    if upgrade is not None:
        artifact("upgrade-application.json", upgrade)
    if cloudtasks:
        artifact("session.json", session)
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
        args.expires_at,
        active,
        approval,
        "delivery.resources",
        **render_options,
    )
    runner = runner_for(approval, kube, store, queues, ledger)

    def stop(_number, _frame):
        runner.env.stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    failed = True
    try:
        runner.start(
            bundle["config"], bundle["supervisor"], None if cloudtasks else application
        )
        failed = False
    finally:
        runner.settle(request_stop=failed)
        idle_output()


def claim_recovery(store, owner):
    # A source-specific concurrency group covers dispatch and workflow_run.
    # Completed source + a control CAS also prevents a second recovery writer.
    recovery_path = "_control/recovery.json"
    previous, generation = store.read(recovery_path)
    current_id, current_attempt = (
        os.environ["GITHUB_RUN_ID"],
        os.environ["GITHUB_RUN_ATTEMPT"],
    )
    if previous:
        previous_run = wf.github_run(previous["github_run_id"])
        if previous["github_run_id"] == current_id and int(
            previous.get("attempt", "0")
        ) < int(current_attempt):
            if not previous.get("attempt"):
                raise rt.Failure(
                    "Recovery attempt was not recorded; investigate its execution before takeover"
                )
            previous_run = wf.github_run(current_id, previous["attempt"])
        if previous_run["status"] != "completed":
            raise rt.Failure("Another recovery execution is active")
    store.write(
        recovery_path,
        {
            "github_run_id": current_id,
            "attempt": current_attempt,
            "nonce": owner["nonce"],
        },
        generation,
    )


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
    wf.verify_source(owner, completed=True)
    wf.save(args.directory / "owner.json", owner)
    claim_recovery(store, owner)
    approval = None
    if owner["kind"] == "run":
        approval, _ = store.read(f"runs/{owner['run_id']}/approval.json")
    kube = wf.external(args.kubeconfig, idle=approval is None)
    if approval:
        if approval["lock_owner"] != owner:
            raise rt.Failure("Immutable approval does not match lock holder")
        wf.save(args.directory / "approval.json", approval)
        control, _ = store.read(f"_control/runs/{owner['run_id']}.json")
        if control is None:
            # No admission occurs before this record exists. A finalization
            # crash can also leave only the environment lock.
            bootstrap.Cluster(args.kubeconfig).preflight()
            store.write(
                f"_control/runs/{owner['run_id']}.json",
                {
                    "nonce": approval["nonce"],
                    "phase": "cleaned",
                    "idle": True,
                    "state_clean": False,
                    "success": False,
                },
            )
        runner_for(approval, kube, store).settle(request_stop=True)
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
        runner = runner_for(approval, wf.external(args.kubeconfig), store)
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
        wf.external(args.kubeconfig, idle=True)
        if owner["kind"] in ("plan", "apply"):
            store.write(
                f"runs/infrastructure-{owner['github_run_id']}-{os.environ['GITHUB_RUN_ID']}/recovery.json",
                {"owner": owner, "plans": plans_receipt, "idle": True},
            )
        rt.EnvironmentLock(store).release(owner)
        success = plans_receipt["empty"]
    recovery, generation = store.read("_control/recovery.json")
    if recovery and recovery.get("nonce") == owner["nonce"]:
        store.delete("_control/recovery.json", generation)
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
    run.add_argument("--sha", required=True)
    run.add_argument(
        "--rig-sha", help="rig commit checked out, when not the workflow's"
    )
    run.add_argument("--expires-at", required=True)
    run.add_argument("--approve", required=True)
    run.add_argument("--scenario", choices=SCENARIOS, default="smoke")
    run.add_argument("--session")
    run.add_argument("--flink-version", choices=tuple(FLINK_LINES))
    run.add_argument("--application-digest")
    run.add_argument(
        "--trial", help="BigQuery trial: " + ", ".join(bigquery_plan.TRIALS)
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
    if args.command == "start":
        # Each scenario's own inputs, and none of another's: the run workflow
        # passes exactly these, so a mismatch refuses before cloud access.
        inputs = {
            "cloudtasks": {"session", "flink_version", "application_digest"},
            "bigquery-recovery": {"trial", "application_digest"},
        }.get(args.scenario, set())
        # Supplied is not the same as non-empty: an empty foreign input is still
        # another scenario's, and an empty own input is still missing.
        supplied = {
            name
            for name in ("session", "flink_version", "application_digest", "trial")
            if getattr(args, name) is not None
        }
        if supplied != inputs or not all(getattr(args, name) for name in inputs):
            flags = sorted("--" + name.replace("_", "-") for name in inputs)
            parser.error(
                f"{args.scenario} requires "
                + (", ".join(flags) if flags else "no scenario inputs")
                + " and accepts no others"
            )
    store = rt.Storage()
    {
        "start": start,
        "recover": recover,
        "plans": wf.plans,
        "finish": finish,
        "lock": lock,
    }[args.command](args, store)


if __name__ == "__main__":
    main()
