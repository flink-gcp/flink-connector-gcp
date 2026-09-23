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
"""Offline Pub/Sub DataStream trial proposals; never authorize execution."""

import copy
import json
import re
from dataclasses import asdict

from .bundle import delivery_digest, source_digest
from .common import Failure, digest, json_bytes, quantity, timestamp, utc
from .model import Schedule
from .policy import GAR, POD_RESOURCES, PUBSUB_CEILINGS, SHA
from .pubsub import ResourcePlan
from .pubsub_messages import MAX_BATCH
from .pubsub_traffic import COUNTER_CEILINGS, TrafficLimits
from .workflow import render

TRIALS = ("jm-replacement", "tm-replacement", "rescale-out", "rescale-in")
WINDOW_SECONDS = 3600
ACTIVE_SECONDS = Schedule.for_window(0, WINDOW_SECONDS).active_seconds(0)
FIELDS = {
    "version",
    "trial",
    "records_per_subscription",
    "traffic_limits",
    "total_request_limit",
}


def validate_trial(value):
    """Validate proposed caps, without asserting feasibility or bill enforcement."""
    if not isinstance(value, dict) or set(value) != FIELDS:
        raise Failure("Pub/Sub trial fields must match the version 2 schema")
    for key, low, high in (
        ("version", 2, 2),
        ("records_per_subscription", 2, 10000),
        ("total_request_limit", 1, 100000),
    ):
        if type(value[key]) is not int or not low <= value[key] <= high:
            raise Failure("Invalid Pub/Sub trial field: " + key)
    if value["trial"] not in TRIALS:
        raise Failure("Unknown Pub/Sub trial")
    limits = value["traffic_limits"]
    if not isinstance(limits, dict) or set(limits) != set(COUNTER_CEILINGS):
        raise Failure("Pub/Sub trial requires every traffic counter")
    TrafficLimits(**limits, admit_until=1)
    if value["total_request_limit"] < limits["pubsub_requests"]:
        raise Failure("Total request proposal is below the data request limit")


def load_trial(path):
    """Read at most 8 KiB, refusing unknown fields and duplicate object keys."""

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate trial field: " + key)
            result[key] = value
        return result

    try:
        with path.open("rb") as stream:
            data = stream.read(8193)
        if len(data) > 8192:
            raise ValueError("Trial input exceeds 8 KiB")
        result = json.loads(data, object_pairs_hook=pairs)
        validate_trial(result)
        return result
    except (OSError, UnicodeError, ValueError, TypeError, RecursionError) as error:
        raise Failure("Invalid Pub/Sub trial proposal: " + str(error)) from error


def _pod(pod, role):
    containers = pod.get("containers", [])
    if (
        len(containers) != 1
        or pod.get("initContainers")
        or pod.get("ephemeralContainers")
    ):
        raise Failure("Pub/Sub proposal requires one container per Pod")
    for category in ("requests", "limits"):
        resources = containers[0].get("resources", {}).get(category, {})
        expected = POD_RESOURCES[role]
        if set(resources) != set(expected) or any(
            quantity(resources[k]) != quantity(v) for k, v in expected.items()
        ):
            raise Failure("Rendered Pod differs from the Pub/Sub resource proposal")


def input_plan(run_id, trial):
    """Validate one feasible pass and derive its exact logical input domain."""
    validate_trial(trial)
    records = trial["records_per_subscription"]
    boundary = records // 2
    cohorts = {
        "before_recovery": {"start": 0, "count": boundary},
        "after_recovery": {"start": boundary, "count": records - boundary},
    }
    # The helper's payload has no attributes; this excludes service framing.
    input_bytes = sum(
        len(f"v1|{run_id}|{i}|{n}".encode()) for i in range(2) for n in range(records)
    )
    publish_calls = 2 * sum(
        (c["count"] + MAX_BATCH - 1) // MAX_BATCH for c in cohorts.values()
    )
    minimum_pulls = sum(
        (2 * c["count"] + MAX_BATCH - 1) // MAX_BATCH for c in cohorts.values()
    )
    limits = TrafficLimits(**trial["traffic_limits"], admit_until=1)
    if (
        limits.input_messages < 2 * records
        or limits.input_bytes < input_bytes
        or limits.publish_calls < publish_calls
        or limits.output_messages < 2 * records
        or limits.pull_calls < minimum_pulls
        or limits.pubsub_requests < publish_calls + 2 * minimum_pulls
    ):
        raise Failure(
            "Traffic proposal cannot cover even one complete input/output pass"
        )
    return {
        "subscriptions": 2,
        "records_per_subscription": records,
        "cohorts_per_subscription": cohorts,
        "batch_size": MAX_BATCH,
        "publish_calls": publish_calls,
        "messages": 2 * records,
        "payload_bytes": input_bytes,
    }


def prepare(
    *,
    run_id,
    nonce,
    started_at,
    expires_at,
    active_seconds,
    revision,
    application_image,
    trial,
):
    """Freeze one offline trial and its exact manifests for later review."""
    trial = copy.deepcopy(trial)
    validate_trial(trial)
    resources = ResourcePlan(run_id, nonce)
    if not isinstance(revision, str) or not SHA.fullmatch(revision):
        raise Failure("Pub/Sub proposal requires a full source revision")
    if not isinstance(application_image, str) or not re.fullmatch(
        re.escape(GAR) + r"pubsub-recovery@sha256:[0-9a-f]{64}", application_image
    ):
        raise Failure("Pub/Sub proposal requires its application image by digest")
    start, end = timestamp(started_at), timestamp(expires_at)
    if (
        start != int(start)
        or end - start != WINDOW_SECONDS
        or type(active_seconds) is not int
        or active_seconds != ACTIVE_SECONDS
    ):
        raise Failure(
            "Pub/Sub proposal requires a one-hour window and 3420 supervisor seconds"
        )
    schedule = Schedule.for_window(start, end)
    planned_input = input_plan(run_id, trial)
    records = trial["records_per_subscription"]
    limits = TrafficLimits(**trial["traffic_limits"], admit_until=schedule.cleanup_at)
    initial, recovery, delivery = render(
        run_id,
        nonce,
        utc(end),
        active_seconds,
        scenario="pubsub-recovery",
        expression="[application, upgradeApplication, delivery.resources]",
        application_image=application_image,
        pubsub_trial=trial["trial"],
        pubsub_records=records,
    )
    for application, parallelism, phase in (
        (initial, 1 if trial["trial"] == "rescale-out" else 2, "initial"),
        (
            recovery,
            1 if trial["trial"] == "rescale-in" else 2,
            "upgrade" if trial["trial"].startswith("rescale-") else "initial",
        ),
    ):
        metadata = application["metadata"]
        annotations = metadata.get("annotations", {})
        if (
            metadata.get("name") != run_id
            or metadata.get("namespace") != "tier3-pubsub"
            or annotations.get("flink-gcp.io/approval") != nonce
            or annotations.get("flink-gcp.io/expires-at") != utc(end)
        ):
            raise Failure("Rendered application differs from the Pub/Sub identity")
        spec = application["spec"]
        if (
            spec["image"] != application_image
            or spec["job"]["parallelism"] != parallelism
            or spec["job"]["args"]
            != [
                f"--run-id={run_id}",
                f"--phase={phase}",
                f"--records-per-subscription={records}",
                f"--parallelism={parallelism}",
                f"--require-restored={str(phase == 'upgrade').lower()}",
            ]
        ):
            raise Failure("Rendered application differs from the Pub/Sub trial")
        _pod(spec["podTemplate"]["spec"], "smoke")
        for manager, replicas in (("jobManager", 1), ("taskManager", parallelism)):
            if spec[manager]["replicas"] != replicas or spec[manager]["resource"] != {
                "cpu": 1,
                "memory": "2Gi",
            }:
                raise Failure(
                    "Rendered Flink managers differ from the Pub/Sub Pod budget"
                )
            _pod(spec[manager]["podTemplate"]["spec"], "smoke")
    supervisor = delivery["supervisor"]["spec"]
    if (
        supervisor["parallelism"] != 1
        or supervisor["completions"] != 1
        or supervisor["activeDeadlineSeconds"] != active_seconds
    ):
        raise Failure(
            "Rendered supervisor differs from the Pub/Sub schedule or Pod budget"
        )
    _pod(supervisor["template"]["spec"], "supervisor")
    data = delivery["config"]["data"]
    if (
        json.loads(data["approval.json"]) != {}
        or json.loads(data["application.json"]) != initial
        or json.loads(data["upgrade-application.json"]) != recovery
    ):
        raise Failure("Pub/Sub delivery must contain the unapproved trial manifests")
    proposal = {
        "kind": "pubsub-trial-proposal",
        "version": 1,
        "approved": False,
        "run_id": run_id,
        "nonce": nonce,
        "revision": revision,
        "runtime_sha256": source_digest(),
        "delivery_sha256": delivery_digest(),
        "started_at": utc(start),
        "cleanup_at": utc(schedule.cleanup_at),
        "expires_at": utc(end),
        "trial": trial,
        "resources": resources.manifest(),
        "application_sha256": digest(initial),
        "recovery_application_sha256": digest(recovery),
        "supervisor_sha256": digest(delivery["supervisor"]),
        "input": planned_input,
        "traffic_limits": asdict(limits),
        "limits": {
            "seconds": WINDOW_SECONDS,
            "cleanup_seconds": 900,
            "pods": PUBSUB_CEILINGS["pods"],
            "pvcs": 0,
            "total_requests": trial["total_request_limit"],
        },
        "cost": {"kind": "unestimated"},
    }
    data["proposal.json"] = json_bytes(proposal).decode()
    return {
        "proposal": proposal,
        "application": initial,
        "recovery_application": recovery,
        "delivery": delivery,
    }
