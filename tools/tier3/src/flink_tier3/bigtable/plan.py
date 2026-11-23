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

from ..common import digest, json_bytes, timestamp, utc
from ..model import Schedule
from ..policy import (
    BIGTABLE,
    BIGTABLE_OBSERVATIONS,
    CLEANUP_SECONDS,
    WINDOWS,
)
from ..proposal import check_inputs, render_verified
from .trial import KINDS, Trial

NAMESPACE = BIGTABLE
WINDOW_SECONDS = WINDOWS["bigtable-recovery"]
ACTIVE_SECONDS = Schedule.for_window(0, WINDOW_SECONDS).active_seconds(0)
# The fault sequence's input and change-stream end (ADR-0204). The job cannot
# finish before the change stream's end, which the input must precede by the
# relay's commit margin, so the input is four minutes, enough to outlast the
# faults; the runbook's "Fault sequence" section gives the timing behind it.
SEQUENCE_INPUT = {"records": 2400, "records_per_second": 10}
SEQUENCE_CHANGE_STREAM_END_SECONDS = 1800


def trial(kind, started_at):
    """The approved trial of one kind: the fixed input and its end."""
    if kind not in KINDS:
        raise ValueError("Unknown Bigtable trial: " + str(kind))
    end = utc(timestamp(started_at) + SEQUENCE_CHANGE_STREAM_END_SECONDS)
    return {"kind": kind, **SEQUENCE_INPUT, "change_stream_end": end}


def validate_trial(value, started_at):
    # Serialized, so `2400.0` or `True` cannot pass as a count.
    kind = value.get("kind") if isinstance(value, dict) else None
    if kind not in KINDS or json_bytes(value) != json_bytes(trial(kind, started_at)):
        raise ValueError(
            "Bigtable trial must be one of "
            + ", ".join(KINDS)
            + " with its fixed input, ending its change stream at its fixed offset"
            " into the window"
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
    }
    delivery["config"]["data"]["proposal.json"] = json_bytes(proposal).decode()
    return {
        "proposal": proposal,
        "application": initial,
        "upgrade_application": upgrade,
        "delivery": delivery,
    }
