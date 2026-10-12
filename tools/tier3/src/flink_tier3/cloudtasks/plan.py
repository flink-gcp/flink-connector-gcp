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
"""Offline, unapproved Cloud Tasks recovery proposal and delivery bundle."""

import copy
import json
import re
from dataclasses import asdict
from decimal import Decimal

from ..common import Failure, digest, json_bytes, quantity, timestamp, utc
from ..model import Schedule, _hourly
from ..policy import (
    CLEANUP_SECONDS,
    CLOUDTASKS,
    CLOUDTASKS_OBSERVATIONS,
    GAR,
    POD_RESOURCES,
    RUN_ID,
    SHA,
    WINDOWS,
)
from ..workflow import render
from .queue import PAGE_SIZE
from .trial import Trial

WINDOW_SECONDS = WINDOWS["cloudtasks-recovery"]
ACTIVE_SECONDS = Schedule.for_window(0, WINDOW_SECONDS).active_seconds(0)
# The one dispatchable trial: 30 minutes of input at the rate the
# application's staged creation was measured keeping up with (#1246).
TRIAL = {"records": 180000, "records_per_second": 100}
# The application's checkpoint interval, which bounds what one recovery replays.
CHECKPOINT_SECONDS = 30
REVIEWED_AT = "2026-10-12T00:00:00Z"
# The published price beyond the monthly free tier, as #1241 and #1246 read it.
OPERATION_USD_PER_MILLION = Decimal("0.40")
RESERVE_USD = Decimal("1.00")


def validate_trial(value):
    # Serialized, so `180000.0` or `True` cannot pass as a count.
    if json_bytes(value) != json_bytes(TRIAL):
        raise ValueError(
            "Cloud Tasks trial must be 180,000 records at 100 records per second"
        )


def estimate():
    """Planning estimate for the trial; not a bound on bills or SDK retries.

    Compute for the window, every task creation and listing call priced as a
    billable operation without the monthly free tier, and a reserve for state,
    evidence and telemetry, as the BigQuery estimate reserves.
    """
    shapes = [
        POD_RESOURCES["operator"],
        POD_RESOURCES["supervisor"],
        *[POD_RESOURCES["smoke"]] * 3,
    ]
    compute = _hourly(shapes) * Decimal(WINDOW_SECONDS) / 3600
    tasks = Decimal(operations()) / 1_000_000 * OPERATION_USD_PER_MILLION
    return compute + tasks + RESERVE_USD


def operations():
    """The billable operations the trial plans: creates, listings and admin.

    Replays after a recovery re-create tasks the service refuses as existing,
    which are counted too, at most one checkpoint's records per recovery.
    """
    records = TRIAL["records"]
    replays = 2 * TRIAL["records_per_second"] * CHECKPOINT_SECONDS
    listings = 2 * -(-records // PAGE_SIZE)
    return records + replays + listings + 10


def _verify_resources(pod, role):
    containers = pod.get("containers", [])
    if (
        len(containers) != 1
        or pod.get("initContainers")
        or pod.get("ephemeralContainers")
    ):
        raise Failure("Rendered Pod differs from the proposed container budget")
    expected = POD_RESOURCES[role]
    for category in ("requests", "limits"):
        actual = containers[0].get("resources", {}).get(category, {})
        if set(actual) != set(expected) or any(
            quantity(actual[key]) != quantity(value) for key, value in expected.items()
        ):
            raise Failure("Rendered Pod differs from the proposed resource budget")


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
    """Render an immutable-input proposal, without authentication or admission."""
    validate_trial(trial)
    if not RUN_ID.fullmatch(run_id) or not SHA.fullmatch(revision):
        raise ValueError("Proposal requires a valid run ID and full source revision")
    if not re.fullmatch(
        re.escape(GAR + "cloudtasks-recovery@") + r"sha256:[0-9a-f]{64}",
        application_image,
    ):
        raise ValueError("Proposal requires a Cloud Tasks GAR image digest")
    start, end = timestamp(started_at), timestamp(expires_at)
    if (
        start != int(start)
        or end != int(end)
        or end - start != WINDOW_SECONDS
        or type(active_seconds) is not int
        or active_seconds != ACTIVE_SECONDS
    ):
        raise ValueError(
            "Proposal requires a 90-minute window and a 5220-second supervisor deadline"
        )
    identity = Trial(run_id, **trial)
    initial, upgrade, delivery = render(
        run_id,
        nonce,
        utc(end),
        active_seconds,
        scenario="cloudtasks-recovery",
        expression="[application, upgradeApplication, delivery.resources]",
        application_image=application_image,
        cloudtasks_records=identity.records,
        cloudtasks_records_per_second=identity.records_per_second,
    )
    metadata, spec = initial["metadata"], initial["spec"]
    if (
        metadata.get("name") != run_id
        or metadata.get("namespace") != CLOUDTASKS
        or metadata.get("annotations", {}).get("flink-gcp.io/approval") != nonce
        or metadata.get("annotations", {}).get("flink-gcp.io/expires-at") != utc(end)
        or spec.get("image") != application_image
    ):
        raise Failure("Rendered application differs from the proposed identity")
    shared_pod = spec.get("podTemplate", {}).get("spec", {})
    if any(
        shared_pod.get(key)
        for key in ("containers", "initContainers", "ephemeralContainers")
    ):
        raise Failure("Shared Pod template adds an unpriced container")
    for manager, replicas in (("jobManager", 1), ("taskManager", 2)):
        shape = spec.get(manager, {})
        if shape.get("replicas") != replicas or shape.get("resource") != {
            "cpu": 1,
            "memory": "2Gi",
        }:
            raise Failure("Rendered application differs from the proposed Pod budget")
        _verify_resources(shape.get("podTemplate", {}).get("spec", {}), "smoke")
    _verify_resources(delivery["supervisor"]["spec"]["template"]["spec"], "supervisor")
    if spec["job"]["args"] != identity.arguments("initial"):
        raise Failure("Rendered application differs from the proposed input")
    expected_upgrade = copy.deepcopy(initial)
    expected_upgrade["spec"]["job"]["args"] = identity.arguments("upgrade")
    if upgrade != expected_upgrade:
        raise Failure("Rendered upgrade changes more than phase and restoration")
    data = delivery["config"]["data"]
    if (
        json.loads(data["application.json"]) != initial
        or json.loads(data["upgrade-application.json"]) != upgrade
        or json.loads(data["approval.json"]) != {}
    ):
        raise Failure("Proposal delivery does not contain the unapproved manifests")
    proposal = {
        "kind": "cloudtasks-trial-proposal",
        "version": 1,
        "approved": False,
        "revision": revision,
        "started_at": utc(start),
        "cleanup_at": utc(end - CLEANUP_SECONDS),
        "expires_at": utc(end),
        "trial": asdict(identity),
        "queue": identity.queue_path,
        "application_sha256": digest(initial),
        "upgrade_application_sha256": digest(upgrade),
        "supervisor_sha256": digest(delivery["supervisor"]),
        "images": {
            "application": application_image,
            "supervisor": delivery["supervisor"]["spec"]["template"]["spec"][
                "containers"
            ][0]["image"],
        },
        "limits": {
            "seconds": WINDOW_SECONDS,
            "cleanup_seconds": CLEANUP_SECONDS,
            "records": identity.records,
        },
        "observations": dict(CLOUDTASKS_OBSERVATIONS),
        "cost": {
            "kind": "planning-estimate",
            "usd": str(estimate()),
            "reviewed_at": REVIEWED_AT,
            "operations": operations(),
            "operation_usd_per_million": str(OPERATION_USD_PER_MILLION),
            "other_reserve_usd": str(RESERVE_USD),
        },
    }
    delivery["config"]["data"]["proposal.json"] = json_bytes(proposal).decode()
    return {
        "proposal": proposal,
        "application": initial,
        "upgrade_application": upgrade,
        "delivery": delivery,
    }
