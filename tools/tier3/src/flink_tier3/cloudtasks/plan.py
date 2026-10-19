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

from dataclasses import asdict
from decimal import Decimal

from ..common import digest, json_bytes, utc
from ..model import Schedule, _hourly
from ..policy import (
    CLEANUP_SECONDS,
    CLOUDTASKS,
    CLOUDTASKS_OBSERVATIONS,
    POD_RESOURCES,
    WINDOWS,
)
from ..proposal import check_inputs, render_verified
from .queue import PAGE_SIZE
from .trial import KINDS, Trial

NAMESPACE = CLOUDTASKS
WINDOW_SECONDS = WINDOWS["cloudtasks-recovery"]
ACTIVE_SECONDS = Schedule.for_window(0, WINDOW_SECONDS).active_seconds(0)
# Every dispatchable trial's input: 30 minutes at the rate the application's
# staged creation was measured keeping up with (#1246).
INPUT = {"records": 180000, "records_per_second": 100}
# The application's checkpoint interval, which bounds what one recovery replays.
CHECKPOINT_SECONDS = 30
REVIEWED_AT = "2026-10-12T00:00:00Z"
# The published price beyond the monthly free tier, as #1241 and #1246 read it.
OPERATION_USD_PER_MILLION = Decimal("0.40")
RESERVE_USD = Decimal("1.00")


def trial(kind, started_at=None):
    """The approved trial of one kind: its fault and the fixed input.

    Dispatch passes every scenario its window's start; this trial needs none.
    """
    if kind not in KINDS:
        raise ValueError("Unknown Cloud Tasks trial: " + str(kind))
    return {"kind": kind, **INPUT}


def validate_trial(value):
    # Serialized, so `180000.0` or `True` cannot pass as a count.
    kind = value.get("kind") if isinstance(value, dict) else None
    if kind not in KINDS or json_bytes(value) != json_bytes(trial(kind)):
        raise ValueError(
            "Cloud Tasks trial must be one of "
            + ", ".join(KINDS)
            + " with 180,000 records at 100 records per second"
        )


def estimate():
    """Planning estimate for the trial; not a bound on bills or SDK retries.

    Compute for the window, every task creation and listing call priced as a
    billable operation without the monthly free tier, and a reserve for state,
    evidence and telemetry, as the BigQuery estimate reserves. It is the same
    for every trial: a rescale trial runs one TaskManager for part of the
    window, which the estimate does not discount.
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
    records = INPUT["records"]
    replays = 2 * INPUT["records_per_second"] * CHECKPOINT_SECONDS
    listings = 2 * -(-records // PAGE_SIZE)
    return records + replays + listings + 10


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
    start, end = check_inputs(
        run_id=run_id,
        revision=revision,
        application_image=application_image,
        package="cloudtasks-recovery",
        service="Cloud Tasks",
        started_at=started_at,
        expires_at=expires_at,
        window_seconds=WINDOW_SECONDS,
        active_seconds=active_seconds,
        expected_active_seconds=ACTIVE_SECONDS,
    )
    identity = Trial(run_id, **trial)
    # A rescale's upgrade also moves the job and its TaskManagers to the other
    # size, which the shared exercise patches beside the arguments.
    initial, upgrade, delivery = render_verified(
        scenario="cloudtasks-recovery",
        namespace=NAMESPACE,
        identity=identity,
        run_id=run_id,
        nonce=nonce,
        end=end,
        active_seconds=active_seconds,
        application_image=application_image,
        cloudtasks_records=identity.records,
        cloudtasks_records_per_second=identity.records_per_second,
        cloudtasks_trial=identity.kind,
    )
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
