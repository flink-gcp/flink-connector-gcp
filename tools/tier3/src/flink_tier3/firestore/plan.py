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

from ..common import digest, json_bytes, utc
from ..model import Schedule
from ..policy import (
    CLEANUP_SECONDS,
    FIRESTORE,
    FIRESTORE_OBSERVATIONS,
    WINDOWS,
)
from ..proposal import check_inputs, render_verified
from .trial import KINDS, MODES, Trial

NAMESPACE = FIRESTORE
WINDOW_SECONDS = WINDOWS["firestore-recovery"]
ACTIVE_SECONDS = Schedule.for_window(0, WINDOW_SECONDS).active_seconds(0)
# The fault sequence's input and seed (ADR-0204): ten minutes of input at 10
# records a second, which outlasts the five faults, and a seed each reading
# subtask copies at 5 records a second, so the copy outlasts them too, ten
# minutes with two subtasks and twenty with one, well inside the hour the
# read time stays valid.
SEQUENCE_INPUT = {"records": 6000, "records_per_second": 10}
SEQUENCE_SEED = {"seed_records": 6000, "seed_records_per_second": 5}


def trial(kind, started_at=None):
    """The approved trial of one kind: the fixed input and seed.

    Dispatch passes every scenario its window's start; this trial needs none.
    """
    if kind not in KINDS:
        raise ValueError("Unknown Firestore trial: " + str(kind))
    return {"kind": kind, **SEQUENCE_INPUT, **SEQUENCE_SEED}


def validate_trial(value):
    # Serialized, so `6000.0` or `True` cannot pass as a count.
    kind = value.get("kind") if isinstance(value, dict) else None
    if kind not in KINDS or json_bytes(value) != json_bytes(trial(kind)):
        raise ValueError(
            "Firestore trial must be one of "
            + ", ".join(KINDS)
            + " with its fixed input and seed"
        )


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
    }
    delivery["config"]["data"]["proposal.json"] = json_bytes(proposal).decode()
    return {
        "proposal": proposal,
        "application": initial,
        "upgrade_application": upgrade,
        "delivery": delivery,
    }
