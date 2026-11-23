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
"""Offline, unapproved BigQuery characterization proposal and delivery bundle."""

import copy
import json
import re
from dataclasses import asdict

from ..common import Failure, digest, json_bytes, timestamp, utc
from ..exercise import SEQUENCE_KIND
from ..model import Schedule
from ..policy import (
    BIGQUERY_OBSERVATIONS,
    CLEANUP_SECONDS,
    GAR,
    RUN_ID,
    SHA,
    WINDOWS,
)
from ..proposal import verify_resources
from ..workflow import render
from .oracle import ROW_BYTES, Trial
from .resources import ResourcePlan

WINDOW_SECONDS = WINDOWS["bigquery-recovery"]
ACTIVE_SECONDS = Schedule.for_window(0, WINDOW_SECONDS).active_seconds(0)
# The run's one query, `final-0`, which reads every table once the job has
# finished; a repeated request reuses its slot rather than taking another.
QUERY_SLOTS = 1
BYTES_PER_QUERY = 4 * 1024**3
QUERY_TIMEOUT_MS = 60000
# The dispatchable trials, the fault sequence of ADR-0204 once per delivery
# method: the at-least-once path (#2150), and the exactly-once paths (#2148),
# FILE_LOADS with the background staging uploader on, as the connector runs it
# by default. Each method runs alone, since one job holding both exactly-once
# sinks would need TaskManagers the namespace's quota has no room for. The
# query budget above is every trial's. A trial is (mode, destinations,
# uploader).
TRIALS = {
    f"alo-10-{SEQUENCE_KIND}": ("ALO", 10, False),
    f"eo-10-{SEQUENCE_KIND}": ("EO", 10, False),
    f"fl-10-uploader-{SEQUENCE_KIND}": ("FILE_LOADS", 10, True),
}
# The fault sequence's input: six minutes at 1 MiB/s of its mode's rows, 16
# a second of 64 KiB or 1,024 of 1 KiB. The job finishes with its input, so
# the input only has to outlast the faults; the runbook's "Fault sequence"
# section gives the timing behind it.
SEQUENCE_RECORDS = {mode: 360 * 1024**2 // size for mode, size in ROW_BYTES.items()}


def trial(name):
    """The approved trial a dispatch names, such as `alo-10-sequence`."""
    if name not in TRIALS:
        raise Failure("BigQuery trial must be one of " + ", ".join(TRIALS))
    mode, destinations, uploader = TRIALS[name]
    value = {"mode": mode, "destinations": destinations}
    if uploader:
        value["uploader"] = True
    value["records"] = SEQUENCE_RECORDS[mode]
    return value


def validate_trial(value):
    # Serialized, so `10.0` or `True` cannot pass as a destination count.
    if json_bytes(value) not in {json_bytes(trial(name)) for name in TRIALS}:
        raise ValueError("Trial must be one of " + ", ".join(TRIALS))


def resource_plan(run_id, nonce, expires_at, trial):
    """Use one finite input and query budget for proposals and run approvals."""
    validate_trial(trial)
    return ResourcePlan(
        Trial(
            run_id,
            trial["mode"],
            trial["destinations"],
            trial["records"],
            trial.get("uploader", False),
        ),
        nonce,
        int(timestamp(expires_at) * 1000),
        QUERY_SLOTS,
        BYTES_PER_QUERY,
        QUERY_TIMEOUT_MS,
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
    if not RUN_ID.fullmatch(run_id) or not SHA.fullmatch(revision):
        raise ValueError("Proposal requires a valid run ID and full source revision")
    if not re.fullmatch(
        re.escape(GAR + "bigquery-recovery@") + r"sha256:[0-9a-f]{64}",
        application_image,
    ):
        raise ValueError("Proposal requires a BigQuery GAR image digest")
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
    resources = resource_plan(run_id, nonce, utc(end), trial)
    identity, records = resources.trial, resources.trial.records
    tags = {
        "application_image": application_image,
        "bigquery_mode": identity.mode,
        "bigquery_destinations": identity.destinations,
        "bigquery_records": identity.records,
        "bigquery_uploader": "true" if identity.uploader else "false",
    }
    initial, upgrade, delivery = render(
        run_id,
        nonce,
        utc(end),
        active_seconds,
        scenario="bigquery-recovery",
        expression="[application, upgradeApplication, delivery.resources]",
        **tags,
    )
    metadata, spec = initial["metadata"], initial["spec"]
    if (
        metadata.get("name") != run_id
        or metadata.get("namespace") != "tier3-bigquery"
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
    for manager, replicas in (
        ("jobManager", 1),
        ("taskManager", identity.parallelism("initial")),
    ):
        shape = spec.get(manager, {})
        if shape.get("replicas") != replicas or shape.get("resource") != {
            "cpu": 1,
            "memory": "2Gi",
        }:
            raise Failure("Rendered application differs from the proposed Pod budget")
        verify_resources(shape.get("podTemplate", {}).get("spec", {}), "smoke")
    verify_resources(delivery["supervisor"]["spec"]["template"]["spec"], "supervisor")
    if initial["spec"]["job"]["args"] != identity.arguments("initial") or initial[
        "spec"
    ]["job"].get("parallelism") != identity.parallelism("initial"):
        raise Failure("Rendered application differs from the proposed input")
    # Only a rescale's upgrade changes the sizes, and then only to the trial's.
    expected_upgrade = copy.deepcopy(initial)
    expected_upgrade["spec"]["job"]["args"] = identity.arguments("upgrade")
    expected_upgrade["spec"]["job"]["parallelism"] = identity.parallelism("upgrade")
    expected_upgrade["spec"]["taskManager"]["replicas"] = identity.parallelism(
        "upgrade"
    )
    if upgrade != expected_upgrade:
        raise Failure(
            "Rendered upgrade changes more than phase, restoration and the trial's sizes"
        )
    data = delivery["config"]["data"]
    if (
        json.loads(data["application.json"]) != initial
        or json.loads(data["upgrade-application.json"]) != upgrade
        or json.loads(data["approval.json"]) != {}
    ):
        raise Failure("Proposal delivery does not contain the unapproved manifests")
    proposal = {
        "kind": "bigquery-trial-proposal",
        "version": 1,
        "approved": False,
        "revision": revision,
        "started_at": utc(start),
        "cleanup_at": utc(end - CLEANUP_SECONDS),
        "expires_at": utc(end),
        "resources": asdict(resources),
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
            "input_bytes": records * ROW_BYTES[identity.mode],
            "query_bytes": QUERY_SLOTS * BYTES_PER_QUERY,
        },
        "observations": dict(BIGQUERY_OBSERVATIONS),
    }
    delivery["config"]["data"]["proposal.json"] = json_bytes(proposal).decode()
    return {
        "proposal": proposal,
        "application": initial,
        "upgrade_application": upgrade,
        "delivery": delivery,
    }
