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
import json
import os
import signal
import time
import uuid
from pathlib import Path

import flink_tier3 as rt

from . import bootstrap
from . import runner as runner_api
from . import workflow as wf
from .exercise import validate_manifests
from .policy import RECOVERY

ROOT = Path.cwd()
APPROVAL = "APPROVE ONE SMOKE RUN: 4 PODS, 60 MINUTES, USD 1"


def start(args, store):
    scenario = getattr(args, "scenario", "smoke")
    if scenario not in ("smoke", "generic-recovery"):
        raise rt.Failure("Unknown smoke scenario")
    if (
        args.approve != APPROVAL
        or os.environ.get("GITHUB_REF") != "refs/heads/main"
        or args.sha != os.environ.get("GITHUB_SHA")
    ):
        raise rt.Failure(
            "Dispatch must explicitly approve the ceilings and exact current main SHA"
        )
    if not rt.RUN_ID.fullmatch(args.run_id):
        raise rt.Failure("Invalid run ID")
    if store.objects(f"runs/{args.run_id}/"):
        raise rt.Failure("Run ID already has immutable evidence; choose a new ID")
    now = time.time()
    end = rt.timestamp(args.expires_at)
    if not 3300 <= end - now <= 3600:
        raise rt.Failure("Dispatch expiry must be 55 to 60 minutes ahead")
    nonce = uuid.uuid4().hex
    owner = wf.execution("run", nonce) | {"run_id": args.run_id}
    kube = wf.external(args.kubeconfig, idle=True)
    bootstrap.Cluster(args.kubeconfig).can_i(
        True, "create", "flink.apache.org", "flinkdeployments", rt.SMOKE
    )
    namespaces, operator_uid, operator_image, baseline = wf.snapshot(kube)
    schedule = rt.Schedule.for_window(now, end)
    active = schedule.active_seconds(time.time())
    render_options = {"scenario": scenario} if scenario != "smoke" else {}
    application = wf.render(
        args.run_id, nonce, args.expires_at, active, **render_options
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
        "smoke": application["spec"]["image"],
        "supervisor": bundle["supervisor"]["spec"]["template"]["spec"]["containers"][0][
            "image"
        ],
    }
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
    rt.validate_approval(approval, time.time())
    wf.save(args.directory / "owner.json", owner)
    rt.EnvironmentLock(store).acquire(owner)
    # Recheck idle after acquiring exclusivity; the earlier snapshot cannot
    # authorize a change that raced an infrastructure apply.
    if wf.snapshot(kube) != (namespaces, operator_uid, operator_image, baseline):
        raise rt.Failure("Foundation changed while acquiring the environment lock")
    wf.save(args.directory / "approval.json", approval)
    store.write(f"runs/{args.run_id}/approval.json", approval)
    store.write(f"runs/{args.run_id}/application.json", application)
    store.write(f"runs/{args.run_id}/images.json", receipts)
    if upgrade is not None:
        store.write(f"runs/{args.run_id}/upgrade-application.json", upgrade)
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
    runner = runner_api.Runner(rt.Environment(kube, store, approval))

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
        runner_api.Runner(rt.Environment(kube, store, approval)).settle(
            request_stop=True
        )
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
        runner = runner_api.Runner(
            rt.Environment(wf.external(args.kubeconfig), store, approval)
        )
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
    run.add_argument("--expires-at", required=True)
    run.add_argument("--approve", required=True)
    run.add_argument(
        "--scenario", choices=("smoke", "generic-recovery"), default="smoke"
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
        "plans": wf.plans,
        "finish": finish,
        "lock": lock,
    }[args.command](args, store)


if __name__ == "__main__":
    main()
