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

import flink_tier3.actors as resource_actors
import flink_tier3.bigquery.actors as bigquery_actors
import flink_tier3.pubsub.actors as pubsub_actors

from .common import Failure, digest
from .environment import Environment
from .google import Storage
from .kubernetes import Kubernetes, KubernetesTransport
from .model import Approval
from .policy import SYSTEM
from .supervisor import Supervisor


def mounted(directory, name):
    """One delivered document, refused by name when the delivery lacks it."""
    try:
        return json.loads((directory / name).read_text())
    except FileNotFoundError as error:
        raise Failure("Supervisor delivery is missing " + name) from error


def verify_delivery(directory):
    """The mounted manifests, checked against the mounted approval's pins.

    Pure: it reads only the mount, so a test can prove a delivery is admitted
    as well as that a bad one is refused.
    """
    approval = mounted(directory, "approval.json")
    application = mounted(directory, "application.json")
    if digest(application) != approval["application_sha256"]:
        raise Failure("Supervisor application differs from approval")
    approved = Approval.from_dict(approval)
    upgrade = None
    if approved.scenario != "smoke":
        upgrade = mounted(directory, "upgrade-application.json")
        if digest(upgrade) != approved.upgrade_application_sha256:
            raise Failure("Supervisor upgrade differs from approval")
    return approval, approved, application, upgrade


def supervisor_main(directory):
    approval, approved, application, upgrade = verify_delivery(directory)
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
    identity = kube.review(
        "/apis/authentication.k8s.io/v1/selfsubjectreviews",
        {"apiVersion": "authentication.k8s.io/v1", "kind": "SelfSubjectReview"},
    )
    if (
        identity.get("status", {}).get("userInfo", {}).get("username")
        != "system:serviceaccount:tier3-system:tier3-supervisor"
    ):
        raise Failure("Unexpected supervisor Kubernetes identity")
    env = Environment(kube, Storage(), approval)

    def stop(_number, _frame):
        env.stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    env.wait(
        lambda: bool(env.refresh().roots.get("supervisor")),
        min(time.time() + 90, env.schedule.cleanup_at),
    )
    if approved.scenario == "bigquery-recovery":
        # The actor, the approval-bound handoff, the quiescence
        # barrier and the exercise are built together and held open for the
        # whole supervision: the session must outlive every query it serves.
        with bigquery_actors.supervisor(env, application, upgrade) as supervisor:
            supervisor.supervise(os.environ["POD_UID"])
        return
    if approved.scenario == "pubsub-recovery":
        # The factory builds the barrier; the session serves the whole run.
        with pubsub_actors.supervisor(env, upgrade) as supervisor:
            supervisor.supervise(os.environ["POD_UID"])
        return
    if approved.scenario in resource_actors.CONTROLLERS:
        # The supervisor reads the run's resource back once the input finishes.
        with resource_actors.supervisor(env, application, upgrade) as supervisor:
            supervisor.supervise(os.environ["POD_UID"])
        return
    Supervisor(env, upgrade).supervise(os.environ["POD_UID"])


def main(argv=None):
    parser = argparse.ArgumentParser(prog="flink-tier3 supervisor", description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path("/lifecycle"))
    args = parser.parse_args(argv)
    supervisor_main(args.directory)


if __name__ == "__main__":
    main()
