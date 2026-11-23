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

from ..common import digest, json_bytes, timestamp, utc
from ..model import Schedule
from ..policy import (
    CLEANUP_SECONDS,
    SPANNER,
    SPANNER_OBSERVATIONS,
    WINDOWS,
)
from ..proposal import check_inputs, render_verified
from .trial import KINDS, Trial

NAMESPACE = SPANNER
WINDOW_SECONDS = WINDOWS["spanner-recovery"]
ACTIVE_SECONDS = Schedule.for_window(0, WINDOW_SECONDS).active_seconds(0)
# The fault sequence's input, seed and change-stream end (ADR-0204). The job
# cannot finish before the change stream's end, which the input must precede
# by the relay's commit margin, so the input and the seed copy are six minutes
# each; the runbook's "Fault sequence" section gives the timing behind them.
SEQUENCE_INPUT = {"records": 3600, "records_per_second": 10}
SEQUENCE_SEED = {"seed_rows": 3600, "seed_rows_per_second": 5}
SEQUENCE_CHANGE_STREAM_END_SECONDS = 1800


def trial(kind, started_at):
    """The approved trial of one kind: the fixed input, seed and end."""
    if kind not in KINDS:
        raise ValueError("Unknown Spanner trial: " + str(kind))
    end = utc(timestamp(started_at) + SEQUENCE_CHANGE_STREAM_END_SECONDS)
    return {
        "kind": kind,
        **SEQUENCE_INPUT,
        **SEQUENCE_SEED,
        "change_stream_end": end,
    }


def validate_trial(value, started_at):
    # Serialized, so `3600.0` or `True` cannot pass as a count.
    kind = value.get("kind") if isinstance(value, dict) else None
    if kind not in KINDS or json_bytes(value) != json_bytes(trial(kind, started_at)):
        raise ValueError(
            "Spanner trial must be one of "
            + ", ".join(KINDS)
            + " with its fixed input and seed, ending its change stream at its"
            " fixed offset into the window"
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
    }
    delivery["config"]["data"]["proposal.json"] = json_bytes(proposal).decode()
    return {
        "proposal": proposal,
        "application": initial,
        "upgrade_application": upgrade,
        "delivery": delivery,
    }
