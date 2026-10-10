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
"""Admit a Pub/Sub recovery run: the ordered preparation before its application."""

import json

from ..common import Failure, digest
from .access import PROBE_REQUESTS, evaluate_workload_log
from .guard import PubSubGuard, admission_deadline
from .messages import COHORTS
from .meter import require_metered
from .plan import require_probe, require_trial_jobs

# The probe prints one short line per attempt; a longer log is refused after
# it is read, which `Kubernetes.logs` requests with a 1 MiB `limitBytes`.
PROBE_LOG_BYTES = 64 * 1024


def require_admission(runner, config, application, probe):
    """Refuse admission inputs that are not exactly the approved ones.

    The handoff must be the production one, guarded and able to tell a
    settled failure, and every manifest the runner is about to submit must be
    the approved one: both job manifests by digest and by the trial's job,
    and the probe by the plan it tests and the program it runs.
    """
    approval = runner.env.approval
    handoff = runner.pubsub
    if (
        handoff is None
        or not isinstance(handoff.controller.before_operation, PubSubGuard)
        or handoff.settled is None
    ):
        raise Failure("Pub/Sub admission requires its authenticated, guarded handoff")
    require_metered(runner.env)
    recovery = config.get("data", {}).get("upgrade-application.json")
    try:
        recovery = json.loads(recovery)
    except (TypeError, ValueError) as error:
        raise Failure("Pub/Sub delivery lacks its recovery manifest") from error
    if (
        digest(application) != approval.application_sha256
        or digest(recovery) != approval.upgrade_application_sha256
    ):
        raise Failure("Application differs from the approved manifest")
    require_trial_jobs(approval.run_id, approval.pubsub_trial, application, recovery)
    require_probe(
        probe, approval.pubsub_plan, runner.env.schedule, approval.images["supervisor"]
    )


def run_probe(runner, probe, deadline):
    """Create the workload's probe Pod, read what it saw, then remove it.

    The probe's whole request allowance is reserved before its Pod exists,
    because its own requests are counted inside it, beyond this meter.
    """
    env = runner.env
    meter = require_metered(env)
    meter.charge_external("probe", PROBE_REQUESTS)
    runner.create_root("probe", probe)

    def ended():
        runner.admission_open()
        runner.supervisor_open()
        pod = env.root("probe")
        if not pod:
            raise Failure("Access probe Pod disappeared before it ended")
        return pod.get("status", {}).get("phase") in ("Succeeded", "Failed")

    env.wait(ended, deadline)
    pod = env.root("probe")
    if not pod:
        raise Failure("Access probe Pod disappeared before it ended")
    data = env.kube.logs(pod)
    if len(data) >= PROBE_LOG_BYTES:
        raise Failure("Access probe log exceeds its read ceiling")
    # The log API prefixes each line with its timestamp.
    text = "\n".join(
        line.partition(" ")[2] for line in data.decode(errors="replace").splitlines()
    )
    env.records.evidence(
        "pubsub-workload-probe",
        {
            "uid": pod["metadata"]["uid"],
            "phase": pod.get("status", {}).get("phase"),
            "text": text,
        },
        "runner",
    )
    if pod.get("status", {}).get("phase") != "Succeeded":
        raise Failure("Access probe Pod did not succeed")
    workload = evaluate_workload_log(env.approval.pubsub_plan, text)
    meter.report_external("probe", workload["requests"])
    runner.pubsub.record_workload(workload)
    env.kube.delete(pod)

    def gone():
        return env.root("probe") is None

    env.wait(gone, deadline)


def supervisor_participating(runner):
    """Whether the supervisor has joined and recorded its own access."""
    runner.admission_open()
    runner.supervisor_open()
    state = runner.env.refresh().pubsub or {}
    return (state.get("handoff") or {}).get("actors", {}).get(
        "supervisor"
    ) is not None and "supervisor" in (state.get("access") or {})


def admit(runner, probe):
    """The ordered preparation, after the supervisor and Operator are ready.

    Resources are created before their grants, because installing a policy
    reads the resource it belongs to; each identity's access is probed before
    any message exists, so the probes' pulls consume nothing; and the first
    cohort waits in its subscriptions before the application is created.
    """
    env, handoff = runner.env, runner.pubsub
    deadline = admission_deadline(env)
    handoff.initialize()
    runner.admission_open()
    handoff.prepare()
    runner.admission_open()
    runner.supervisor_open()
    handoff.access(deadline=deadline)
    runner.cleanup.quota(runner.namespace, "run")
    runner.admission_open()
    run_probe(runner, probe, deadline)
    env.wait(lambda: supervisor_participating(runner), deadline)
    handoff.verify()
    handoff.publish_cohort(COHORTS[0], deadline=deadline, check=runner.admission_open)
