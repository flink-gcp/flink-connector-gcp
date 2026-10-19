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
"""Offline, unapproved Bigtable recovery proposal and delivery bundle."""

from dataclasses import asdict
from decimal import Decimal

from ..common import digest, json_bytes, timestamp, utc
from ..model import Schedule, _hourly
from ..policy import (
    BIGTABLE,
    BIGTABLE_OBSERVATIONS,
    CLEANUP_SECONDS,
    POD_RESOURCES,
    WINDOWS,
)
from ..proposal import check_inputs, render_verified
from .trial import KINDS, Trial

NAMESPACE = BIGTABLE
WINDOW_SECONDS = WINDOWS["bigtable-recovery"]
ACTIVE_SECONDS = Schedule.for_window(0, WINDOW_SECONDS).active_seconds(0)
# Every dispatchable trial's input: 30 minutes at the rate of the other
# recovery trials in the same window.
INPUT = {"records": 180000, "records_per_second": 100}
# Where the change-stream source stops, from the window's start. The input
# starts within `input_seconds` and runs for 30 minutes, 3,000 seconds in all,
# and each recovery pauses it for a minute or two; the job finishes at the end,
# and the oracles' `visibility_seconds` then fit before the cleanup start at
# 4,500 seconds.
CHANGE_STREAM_END_SECONDS = 3900
REVIEWED_AT = "2026-10-18T00:00:00Z"
# A one-node SSD cluster in us-central1, billed by the hour: the 90-minute
# window, rounded up (#1316's scope).
NODE_USD_PER_HOUR = Decimal("0.65")
NODE_HOURS = 2
RESERVE_USD = Decimal("1.00")


def trial(kind, started_at):
    """The approved trial of one kind: its fault, the fixed input and its end."""
    if kind not in KINDS:
        raise ValueError("Unknown Bigtable trial: " + str(kind))
    end = utc(timestamp(started_at) + CHANGE_STREAM_END_SECONDS)
    return {"kind": kind, **INPUT, "change_stream_end": end}


def validate_trial(value, started_at):
    # Serialized, so `180000.0` or `True` cannot pass as a count.
    kind = value.get("kind") if isinstance(value, dict) else None
    if kind not in KINDS or json_bytes(value) != json_bytes(trial(kind, started_at)):
        raise ValueError(
            "Bigtable trial must be one of "
            + ", ".join(KINDS)
            + " with 180,000 records at 100 records per second, ending its change"
            " stream 3,900 seconds into the window"
        )


def estimate():
    """Planning estimate for the trial; not a bound on bills.

    Compute for the window, as the Cloud Tasks estimate prices it, the
    instance's node-hours and a reserve for storage, state, evidence and
    telemetry. It is the same for every trial: a rescale trial runs one
    TaskManager for part of the window, which the estimate does not discount.
    """
    shapes = [
        POD_RESOURCES["operator"],
        POD_RESOURCES["supervisor"],
        *[POD_RESOURCES["smoke"]] * 3,
    ]
    compute = _hourly(shapes) * Decimal(WINDOW_SECONDS) / 3600
    return compute + NODE_USD_PER_HOUR * NODE_HOURS + RESERVE_USD


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
    validate_trial(trial, started_at)
    start, end = check_inputs(
        run_id=run_id,
        revision=revision,
        application_image=application_image,
        package="bigtable-recovery",
        service="Bigtable",
        started_at=started_at,
        expires_at=expires_at,
        window_seconds=WINDOW_SECONDS,
        active_seconds=active_seconds,
        expected_active_seconds=ACTIVE_SECONDS,
    )
    identity = Trial(run_id, **trial)
    initial, upgrade, delivery = render_verified(
        scenario="bigtable-recovery",
        namespace=NAMESPACE,
        identity=identity,
        run_id=run_id,
        nonce=nonce,
        end=end,
        active_seconds=active_seconds,
        application_image=application_image,
        bigtable_records=identity.records,
        bigtable_records_per_second=identity.records_per_second,
        bigtable_change_stream_end=identity.change_stream_end,
        bigtable_trial=identity.kind,
    )
    proposal = {
        "kind": "bigtable-trial-proposal",
        "version": 1,
        "approved": False,
        "revision": revision,
        "started_at": utc(start),
        "cleanup_at": utc(end - CLEANUP_SECONDS),
        "expires_at": utc(end),
        "trial": asdict(identity),
        "instance": identity.instance_path,
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
        "observations": dict(BIGTABLE_OBSERVATIONS),
        "cost": {
            "kind": "planning-estimate",
            "usd": str(estimate()),
            "reviewed_at": REVIEWED_AT,
            "node_usd_per_hour": str(NODE_USD_PER_HOUR),
            "node_hours": NODE_HOURS,
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
