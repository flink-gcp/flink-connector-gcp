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
"""Offline, unapproved Firestore recovery proposal and delivery bundle."""

from dataclasses import asdict
from decimal import Decimal

from ..common import digest, json_bytes, utc
from ..model import Schedule, _hourly
from ..policy import (
    CLEANUP_SECONDS,
    FIRESTORE,
    FIRESTORE_OBSERVATIONS,
    POD_RESOURCES,
    WINDOWS,
)
from ..proposal import check_inputs, render_verified
from .trial import KINDS, LATE_RECORDS, MODES, Trial

NAMESPACE = FIRESTORE
WINDOW_SECONDS = WINDOWS["firestore-recovery"]
ACTIVE_SECONDS = Schedule.for_window(0, WINDOW_SECONDS).active_seconds(0)
# Every dispatchable trial's input: 30 minutes at the rate of the other
# recovery trials in the same window.
INPUT = {"records": 180000, "records_per_second": 100}
# The seed the runner writes to each database and each copy's pace per
# reading subtask, the package's default pace. Both reads read at one read
# time planned at the job's first start, and without point-in-time recovery
# the service refuses a read time older than about an hour, so the seed has a
# floor and a ceiling.
#
# The floor: the copies must still be reading through both recoveries in the
# fastest layout, two subtasks holding splits from start to end, which a
# replacement trial gives when the service plans two splits or more. The
# Spanner trials of #1878, whose exercise and timings this one shares, began
# the upgrade about 410 seconds into the input and deleted the failover's Pod
# about 830 seconds in; 108,000 records at 2 x 50 a second take 1,080
# seconds.
#
# The ceiling: the slowest layout, one subtask holding the only split
# throughout, which the service gives when it plans one split for a small
# seed, must finish within about 45 minutes of the first start. 108,000 at 50
# a second take 2,160 seconds; both recoveries stop the copy for at most 114
# and 101 seconds in #1878, and each restore reads again at most one
# 30-second checkpoint interval, so the read ends about 2,440 seconds, or 41
# minutes, after the first start, and the job then finishes.
#
# Spanner's 120,000 rows would end about 2,680 seconds after the first start
# in the slowest layout, at the 45 minutes rather than inside them.
SEED = {"seed_records": 108000, "seed_records_per_second": 50}
REVIEWED_AT = "2026-11-08T00:00:00Z"
# Planning assumptions, which matched the us-central1 prices on the pricing
# page on 2026-11-08 (the #2085 preregistration): Standard-edition
# document or entity writes and reads, without the free tier.
USD_PER_100K_WRITES = Decimal("0.09")
USD_PER_100K_READS = Decimal("0.03")
RESERVE_USD = Decimal("1.00")


def trial(kind, started_at=None):
    """The approved trial of one kind: its fault, the fixed input and seed.

    Dispatch passes every scenario its window's start; this trial needs none.
    """
    if kind not in KINDS:
        raise ValueError("Unknown Firestore trial: " + str(kind))
    return {"kind": kind, **INPUT, **SEED}


def validate_trial(value):
    # Serialized, so `180000.0` or `True` cannot pass as a count.
    kind = value.get("kind") if isinstance(value, dict) else None
    if kind not in KINDS or json_bytes(value) != json_bytes(trial(kind)):
        raise ValueError(
            "Firestore trial must be one of "
            + ", ".join(KINDS)
            + " with 180,000 records at 100 records per second and 108,000 seed"
            " records copied at 50 records per second per subtask"
        )


def operations():
    """The document or entity writes and reads the trial plans, in both databases.

    Each database takes the input, the seed and its late records, and one
    copy of each seed record; it serves the copy's read of the seed and the
    oracles' reads of `input`, `seed` and `batch_out`. Copies a restore emits
    again and the sinks' replays are not counted: at most a checkpoint
    interval of records each, a few thousand.
    """
    records, seed = INPUT["records"], SEED["seed_records"]
    writes = records + seed + LATE_RECORDS + seed
    reads = seed + records + seed + LATE_RECORDS + seed
    return {"writes": len(MODES) * writes, "reads": len(MODES) * reads}


def estimate():
    """Planning estimate for the trial; not a bound on bills.

    Compute for the window, as the Spanner estimate prices it, the planned
    operations and a reserve for storage, state, evidence and telemetry.
    """
    shapes = [
        POD_RESOURCES["operator"],
        POD_RESOURCES["supervisor"],
        *[POD_RESOURCES["smoke"]] * 3,
    ]
    compute = _hourly(shapes) * Decimal(WINDOW_SECONDS) / 3600
    counts = operations()
    usage = (
        Decimal(counts["writes"]) * USD_PER_100K_WRITES
        + Decimal(counts["reads"]) * USD_PER_100K_READS
    ) / 100_000
    return compute + usage + RESERVE_USD


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
        package="firestore-recovery",
        service="Firestore",
        started_at=started_at,
        expires_at=expires_at,
        window_seconds=WINDOW_SECONDS,
        active_seconds=active_seconds,
        expected_active_seconds=ACTIVE_SECONDS,
    )
    identity = Trial(run_id, **trial)
    initial, upgrade, delivery = render_verified(
        scenario="firestore-recovery",
        namespace=NAMESPACE,
        identity=identity,
        run_id=run_id,
        nonce=nonce,
        end=end,
        active_seconds=active_seconds,
        application_image=application_image,
        firestore_records=identity.records,
        firestore_records_per_second=identity.records_per_second,
        firestore_seed_records_per_second=identity.seed_records_per_second,
        firestore_trial=identity.kind,
    )
    proposal = {
        "kind": "firestore-trial-proposal",
        "version": 1,
        "approved": False,
        "revision": revision,
        "started_at": utc(start),
        "cleanup_at": utc(end - CLEANUP_SECONDS),
        "expires_at": utc(end),
        "trial": asdict(identity),
        "databases": [identity.database_path(mode) for mode in MODES],
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
            "seed_records": identity.seed_records,
        },
        "observations": dict(FIRESTORE_OBSERVATIONS),
        "cost": {
            "kind": "planning-estimate",
            "usd": str(estimate()),
            "reviewed_at": REVIEWED_AT,
            "operations": operations(),
            "usd_per_100k_writes": str(USD_PER_100K_WRITES),
            "usd_per_100k_reads": str(USD_PER_100K_READS),
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
