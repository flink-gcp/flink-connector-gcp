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

from ..common import digest, json_bytes, utc
from ..model import Schedule
from ..policy import (
    CLEANUP_SECONDS,
    CLOUDTASKS,
    CLOUDTASKS_OBSERVATIONS,
    WINDOWS,
)
from ..proposal import check_inputs, render_verified
from .trial import KINDS, Trial

NAMESPACE = CLOUDTASKS
WINDOW_SECONDS = WINDOWS["cloudtasks-recovery"]
ACTIVE_SECONDS = Schedule.for_window(0, WINDOW_SECONDS).active_seconds(0)
# The fault sequence's input (ADR-0204): six minutes, enough to outlast the
# faults, as Spanner's is; the job finishes with it. The runbook's "Fault
# sequence" section gives the timing behind it.
SEQUENCE_INPUT = {"records": 3600, "records_per_second": 10}


def trial(kind, started_at=None):
    """The approved trial of one kind: the fixed input.

    Dispatch passes every scenario its window's start; this trial needs none.
    """
    if kind not in KINDS:
        raise ValueError("Unknown Cloud Tasks trial: " + str(kind))
    return {"kind": kind, **SEQUENCE_INPUT}


def validate_trial(value):
    # Serialized, so `3600.0` or `True` cannot pass as a count.
    kind = value.get("kind") if isinstance(value, dict) else None
    if kind not in KINDS or json_bytes(value) != json_bytes(trial(kind)):
        raise ValueError(
            "Cloud Tasks trial must be one of "
            + ", ".join(KINDS)
            + " with its fixed input"
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
    }
    delivery["config"]["data"]["proposal.json"] = json_bytes(proposal).decode()
    return {
        "proposal": proposal,
        "application": initial,
        "upgrade_application": upgrade,
        "delivery": delivery,
    }
