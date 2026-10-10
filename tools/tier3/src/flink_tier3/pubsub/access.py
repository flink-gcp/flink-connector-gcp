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
"""What each identity must and must not be able to do on the run's resources."""

import json

from ..common import ApiError, Failure, utc
from ..policy import HTTP_TIMEOUT, PROJECT, PUBSUB_ADMISSION

PUBLISH = "pubsub.topics.publish"
CONSUME = "pubsub.subscriptions.consume"
# The service accounts the run's identities act as.
MEMBERS = {
    role: f"serviceAccount:tier3-{account}@{PROJECT}.iam.gserviceaccount.com"
    for role, account in (
        ("runner", "runner"),
        ("workload", "pubsub"),
        ("supervisor", "supervisor"),
    )
}
# Seconds between read-only attempts while grants take effect.
INTERVAL = 15
# The workload probe's request allowance: a permission test of each of the
# run's six resources per round and a pull of each of its two subscriptions,
# one round or pull retry per interval of the admission window, plus the
# metadata server's ping, project and token requests with their retries.
ROUNDS = PUBSUB_ADMISSION["startup_seconds"] // INTERVAL + 1
PROBE_REQUESTS = ROUNDS * (6 + 2) + 40


def admission_until(schedule):
    """When admission must have created the application: the run's deadline."""
    return min(
        schedule.started + PUBSUB_ADMISSION["startup_seconds"], schedule.cleanup_at
    )


def expectations(plan, role):
    """Per resource: the permission tested, and whether the identity must hold it.

    The runner's and the supervisor's project roles grant metadata, policy
    administration and deletion but neither publish nor consume, and the
    workload has no project role, so these two permissions are decided by the
    run's explicit bindings alone, which makes each one a positive or a
    negative control.
    """
    member = MEMBERS[role]
    return {
        name: {
            "permission": CONSUME if "/subscriptions/" in name else PUBLISH,
            "held": any(member in binding["members"] for binding in bindings),
        }
        for name, bindings in plan.grants().items()
    }


def pulls(plan, role):
    """The subscriptions the identity reads, each checked by an empty pull."""
    return sorted(
        name
        for name, want in expectations(plan, role).items()
        if want["held"] and want["permission"] == CONSUME
    )


def compare(expected, seen):
    """Missing grants, which may still be propagating, and unexpected ones."""
    missing = sorted(n for n, w in expected.items() if w["held"] and not seen[n])
    extra = sorted(n for n, w in expected.items() if not w["held"] and seen[n])
    return missing, extra


def _attempt_budget(expected):
    """Seconds one round of permission tests may take, sent one at a time."""
    return len(expected) * HTTP_TIMEOUT


def wait_for_access(env, resources, role, *, deadline):
    """Poll permission tests until every grant holds, then pull each subscription.

    A grant that is missing may still be propagating, so the round is retried
    while a whole further round still fits before the deadline. One that is
    present but must not be is refused at once: the policies were empty
    before installation, so no propagation removes it. A pull refused with
    403 is retried the same way, because the data plane can see a grant later
    than the permission test does. Nothing here changes a resource; a pull
    still counts as a write to the session's unsettled-write latch, because
    it could lease a message. A local stop ends the wait.
    """
    expected = expectations(env.approval.pubsub_plan, role)
    attempts, first = 0, utc(env.clock())

    def open_or_stop():
        if env.stopping or env.evidence_failed:
            raise Failure("Pub/Sub admission has stopped")

    while True:
        open_or_stop()
        attempts += 1
        seen = {
            name: resources.holds("access", name, want["permission"])
            for name, want in expected.items()
        }
        missing, extra = compare(expected, seen)
        if extra:
            raise Failure(
                f"Pub/Sub {role} holds access it must not have: " + ", ".join(extra)
            )
        if not missing:
            break
        if env.clock() + INTERVAL + _attempt_budget(expected) > deadline:
            raise Failure(
                f"Pub/Sub {role} access did not take effect before the deadline: "
                + ", ".join(missing)
            )
        env.sleep(INTERVAL)
    pulled, pull_attempts = [], 0
    for subscription in pulls(env.approval.pubsub_plan, role):
        while True:
            open_or_stop()
            pull_attempts += 1
            try:
                resources.peek("access", subscription)
            except ApiError as error:
                if error.status != 403 or env.clock() + INTERVAL + HTTP_TIMEOUT > (
                    deadline
                ):
                    raise
                env.sleep(INTERVAL)
                continue
            pulled.append(subscription)
            break
    return {
        "attempts": attempts,
        "pull_attempts": pull_attempts,
        "first": first,
        "passed": utc(env.clock()),
        "granted": sorted(n for n, held in seen.items() if held),
        "pulled": pulled,
    }


def probe_spec(plan, schedule):
    """What the workload probe Pod tests, as the one argument it receives."""
    return {
        "member": MEMBERS["workload"],
        "expected": expectations(plan, "workload"),
        "pulls": pulls(plan, "workload"),
        "deadline": admission_until(schedule),
        "interval": INTERVAL,
        "requests": PROBE_REQUESTS,
    }


def evaluate_workload_log(plan, text):
    """Re-derive the workload's access from the probe Pod's own observations.

    The Pod's exit status is not taken on trust: the log must name the
    workload identity, end with a pass that counts the requests the probe
    sent within its allowance, and show a last attempt that meets every
    expectation, and an empty pull of every subscription the workload reads.
    """
    try:
        events = [json.loads(line) for line in text.splitlines() if line.strip()]
    except ValueError as error:
        raise Failure("Pub/Sub workload probe log is malformed") from error
    if not all(isinstance(event, dict) for event in events):
        raise Failure("Pub/Sub workload probe log is malformed")
    expected = expectations(plan, "workload")
    identities = [e for e in events if e.get("event") == "identity"]
    attempts = [e for e in events if e.get("event") == "attempt"]
    if (
        len(identities) != 1
        or identities[0].get("email") != MEMBERS["workload"]
        or not attempts
        or not events
        or events[-1].get("event") != "passed"
        or any(e.get("event") == "refused" for e in events)
    ):
        raise Failure("Pub/Sub workload probe did not pass as the workload identity")
    seen = attempts[-1].get("seen")
    if not isinstance(seen, dict) or set(seen) != set(expected):
        raise Failure("Pub/Sub workload probe tested other resources")
    missing, extra = compare(expected, seen)
    pulled = [e.get("name") for e in events if e.get("event") == "pulled"]
    if missing or extra or pulled != pulls(plan, "workload"):
        raise Failure("Pub/Sub workload probe observations do not meet the plan")
    requests = events[-1].get("requests")
    if (
        not isinstance(requests, dict)
        or not set(requests) <= {"pubsub", "credential"}
        or any(type(n) is not int or n < 0 for n in requests.values())
        or sum(requests.values()) > PROBE_REQUESTS
    ):
        raise Failure("Pub/Sub workload probe did not account for its requests")
    return {
        "requests": dict(sorted(requests.items())),
        "attempts": len(attempts),
        "pull_attempts": sum(
            e.get("event") in ("pulled", "pull-refused") for e in events
        ),
        "granted": sorted(name for name, held in seen.items() if held),
        "pulled": pulled,
    }
