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
"""Tier-3 lifecycle entrypoint."""

from __future__ import annotations

import argparse
import json
import os
import signal
import time
from pathlib import Path

import flink_tier3.bigquery.actors as bigquery_actors
import flink_tier3.pubsub.actors as pubsub_actors

from .bundle import delivery_digest
from .cloudtasks.evidence import Collector
from .cloudtasks.observe import CellObserver
from .cloudtasks.session import Ledger, Queues, load_cells
from .common import Failure, digest
from .environment import Environment
from .exercise import validate_manifests
from .google import GoogleToken, Storage, authorized_session
from .kubernetes import Kubernetes, KubernetesTransport
from .model import Approval
from .policy import SYSTEM
from .supervisor import HookChain, Supervisor


def mounted(directory, name):
    """One delivered document, refused by name when the delivery lacks it."""
    try:
        return json.loads((directory / name).read_text())
    except FileNotFoundError as error:
        raise Failure("Supervisor delivery is missing " + name) from error


def verify_delivery(directory):
    """The mounted delivery, checked against its own approval before anything runs.

    Pure: it reads only the mount, so a test can prove a delivery is admitted
    as well as that a bad one is refused.
    """
    approval = mounted(directory, "approval.json")
    # The mount is a subset, so the complete package's digest is not
    # computable here; the runner verified that one at dispatch.
    if delivery_digest() != approval["delivery_sha256"]:
        raise Failure("Supervisor source differs from approval")
    application = mounted(directory, "application.json")
    if digest(application) != approval["application_sha256"]:
        raise Failure("Supervisor application differs from approval")
    approved = Approval.from_dict(approval)
    upgrade, cells = None, None
    if approved.scenario in (
        "generic-recovery",
        "bigquery-recovery",
        "pubsub-recovery",
    ):
        upgrade = mounted(directory, "upgrade-application.json")
        if digest(upgrade) != approved.upgrade_application_sha256:
            raise Failure("Supervisor upgrade differs from approval")
    if approved.scenario == "generic-recovery":
        # The generic payload's fixed arguments. A BigQuery application has
        # its own, which `bigquery_plan.prepare` checked when it rendered the
        # manifests these two digests pin; this check would refuse every one.
        validate_manifests(application, upgrade)
    elif approved.scenario == "cloudtasks":
        cells = load_cells(directory, approved)
    return approval, approved, application, upgrade, cells


def supervisor_main(directory):
    approval, approved, application, upgrade, cells = verify_delivery(directory)
    account = Path("/var/run/secrets/kubernetes.io/serviceaccount")
    if (account / "namespace").read_text().strip() != SYSTEM:
        raise Failure("Supervisor must run in tier3-system")
    kube = Kubernetes(
        "https://kubernetes.default.svc",
        KubernetesTransport(
            "https://kubernetes.default.svc",
            lambda: (account / "token").read_text().strip(),
            str(account / "ca.crt"),
        ),
    )
    identity = kube.request(
        "POST",
        "/apis/authentication.k8s.io/v1/selfsubjectreviews",
        {"apiVersion": "authentication.k8s.io/v1", "kind": "SelfSubjectReview"},
    )
    if (
        identity.get("status", {}).get("userInfo", {}).get("username")
        != "system:serviceaccount:tier3-system:tier3-supervisor"
    ):
        raise Failure("Unexpected supervisor Kubernetes identity")
    store = Storage()
    queues = ledger = None
    if cells is not None:
        queues = Queues(authorized_session(GoogleToken()), approved.queue)
        ledger = Ledger(store, approved.campaign)
    env = Environment(kube, store, approval, queues=queues, ledger=ledger)
    hooks = None
    if cells is not None:
        # Observe every poll, then collect and export each cell's evidence.
        hooks = HookChain(CellObserver(), Collector(env, store))

    def stop(_number, _frame):
        env.stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    env.wait(
        lambda: bool(env.refresh().roots.get("supervisor")),
        min(time.time() + 90, env.schedule.cleanup_at),
    )
    if approved.scenario == "bigquery-recovery":
        # The authenticated actor, the approval-bound handoff, the quiescence
        # barrier and the exercise are built together and held open for the
        # whole supervision: the session must outlive every query it serves.
        with bigquery_actors.supervisor(env, application, upgrade) as supervisor:
            supervisor.supervise(os.environ["POD_UID"])
        return
    if approved.scenario == "pubsub-recovery":
        # The factory checks both mounted manifests against the trial, builds
        # the barrier and authenticates; the session serves the whole run.
        with pubsub_actors.supervisor(env, application, upgrade) as supervisor:
            supervisor.supervise(os.environ["POD_UID"])
        return
    Supervisor(env, upgrade, cells, hooks).supervise(os.environ["POD_UID"])


def main(argv=None):
    parser = argparse.ArgumentParser(prog="flink-tier3 supervisor", description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path("/lifecycle"))
    args = parser.parse_args(argv)
    supervisor_main(args.directory)


if __name__ == "__main__":
    main()
