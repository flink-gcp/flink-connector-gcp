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
"""Offline, unapproved Spanner recovery proposal and delivery bundle."""

from dataclasses import asdict
from decimal import Decimal

from ..common import digest, json_bytes, timestamp, utc
from ..model import Schedule, _hourly
from ..policy import (
    CLEANUP_SECONDS,
    POD_RESOURCES,
    SPANNER,
    SPANNER_OBSERVATIONS,
    WINDOWS,
)
from ..proposal import check_inputs, render_verified
from .instance import PROCESSING_UNITS
from .trial import KINDS, Trial

NAMESPACE = SPANNER
WINDOW_SECONDS = WINDOWS["spanner-recovery"]
ACTIVE_SECONDS = Schedule.for_window(0, WINDOW_SECONDS).active_seconds(0)
# Every dispatchable trial's input: 30 minutes at the rate of the other
# recovery trials in the same window.
INPUT = {"records": 180000, "records_per_second": 100}
# The seed the runner writes and the copy's pace per reading subtask, the
# package's default: one subtask copies the 120,000 rows in 40 minutes, two in
# 20.
SEED = {"seed_rows": 120000, "seed_rows_per_second": 50}
# Where the change-stream source stops, from the window's start, as the
# Bigtable plan derives it: the oracles' `visibility_seconds` then end a minute
# before the cleanup start at 4,500 seconds, and an input that starts late and has
# two slow recoveries can finish past it.
CHANGE_STREAM_END_SECONDS = 3900
REVIEWED_AT = "2026-10-28T00:00:00Z"
# 100 processing units of a regional Standard instance in us-central1: the
# 90-minute window rounded up to hours, on the unmeasured assumption that the
# instance is billed for at least an hour.
INSTANCE_USD_PER_HOUR = Decimal("0.09")
INSTANCE_HOURS = 2
RESERVE_USD = Decimal("1.00")


def trial(kind, started_at):
    """The approved trial of one kind: its fault, the fixed input, seed and end."""
    if kind not in KINDS:
        raise ValueError("Unknown Spanner trial: " + str(kind))
    end = utc(timestamp(started_at) + CHANGE_STREAM_END_SECONDS)
    return {"kind": kind, **INPUT, **SEED, "change_stream_end": end}


def validate_trial(value, started_at):
    # Serialized, so `180000.0` or `True` cannot pass as a count.
    kind = value.get("kind") if isinstance(value, dict) else None
    if kind not in KINDS or json_bytes(value) != json_bytes(trial(kind, started_at)):
        raise ValueError(
            "Spanner trial must be one of "
            + ", ".join(KINDS)
            + " with 180,000 records at 100 records per second and 120,000 seed"
            " rows copied at 50 rows per second per subtask, ending its change"
            " stream 3,900 seconds into the window"
        )


def estimate():
    """Planning estimate for the trial; not a bound on bills.

    Compute for the window, as the Bigtable estimate prices it, the
    instance's hours and a reserve for storage, state, evidence and telemetry.
    """
    shapes = [
        POD_RESOURCES["operator"],
        POD_RESOURCES["supervisor"],
        *[POD_RESOURCES["smoke"]] * 3,
    ]
    compute = _hourly(shapes) * Decimal(WINDOW_SECONDS) / 3600
    return compute + INSTANCE_USD_PER_HOUR * INSTANCE_HOURS + RESERVE_USD


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
        package="spanner-recovery",
        service="Spanner",
        started_at=started_at,
        expires_at=expires_at,
        window_seconds=WINDOW_SECONDS,
        active_seconds=active_seconds,
        expected_active_seconds=ACTIVE_SECONDS,
    )
    identity = Trial(run_id, **trial)
    initial, upgrade, delivery = render_verified(
        scenario="spanner-recovery",
        namespace=NAMESPACE,
        identity=identity,
        run_id=run_id,
        nonce=nonce,
        end=end,
        active_seconds=active_seconds,
        application_image=application_image,
        spanner_records=identity.records,
        spanner_records_per_second=identity.records_per_second,
        spanner_change_stream_end=identity.change_stream_end,
        spanner_seed_rows_per_second=identity.seed_rows_per_second,
        spanner_trial=identity.kind,
    )
    proposal = {
        "kind": "spanner-trial-proposal",
        "version": 1,
        "approved": False,
        "revision": revision,
        "started_at": utc(start),
        "cleanup_at": utc(end - CLEANUP_SECONDS),
        "expires_at": utc(end),
        "trial": asdict(identity),
        "instance": identity.instance_path,
        "database": identity.database_path,
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
            "seed_rows": identity.seed_rows,
        },
        "observations": dict(SPANNER_OBSERVATIONS),
        "cost": {
            "kind": "planning-estimate",
            "usd": str(estimate()),
            "reviewed_at": REVIEWED_AT,
            "processing_units": PROCESSING_UNITS,
            "instance_usd_per_hour": str(INSTANCE_USD_PER_HOUR),
            "instance_hours": INSTANCE_HOURS,
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
