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
from decimal import Decimal

from .bigquery import ROW_BYTES, Trial
from .bigquery_resources import ResourcePlan
from .bundle import delivery_digest, source_digest
from .common import Failure, digest, json_bytes, quantity, timestamp, utc
from .model import Schedule, _hourly
from .policy import (
    BIGQUERY_CEILINGS,
    BIGQUERY_OBSERVATIONS,
    GAR,
    POD_RESOURCES,
    RUN_ID,
    SHA,
)
from .workflow import render

WINDOW_SECONDS = BIGQUERY_CEILINGS["seconds"]
ACTIVE_SECONDS = Schedule.for_window(0, WINDOW_SECONDS).active_seconds(0)
QUERY_SLOTS = 12
BYTES_PER_QUERY = 4 * 1024**3
QUERY_TIMEOUT_MS = 60000
REVIEWED_AT = "2026-09-20T00:00:00Z"
QUERY_USD_PER_TIB = Decimal("6.25")
RESERVE_USD = Decimal("1.00")
# The four dispatchable trials; the query budget above is every trial's.
TRIALS = {
    f"{mode.lower()}-{destinations}": (mode, destinations)
    for destinations in (10, 50)
    for mode in ("ALO", "EO")
}


def trial(name):
    """The approved trial a dispatch names, such as `alo-10`."""
    if name not in TRIALS:
        raise Failure("BigQuery trial must be one of " + ", ".join(TRIALS))
    mode, destinations = TRIALS[name]
    return {"mode": mode, "destinations": destinations}


def validate_trial(value):
    # Serialized, so `10.0` or `True` cannot pass as a destination count.
    if json_bytes(value) not in {json_bytes(trial(name)) for name in TRIALS}:
        raise ValueError("Trial must select ALO/EO and 10/50 destinations")


def estimate():
    """Planning estimate for one trial; not a bound on bills or SDK retries."""
    shapes = [
        POD_RESOURCES["operator"],
        POD_RESOURCES["supervisor"],
        *[POD_RESOURCES["smoke"]] * 3,
    ]
    compute = _hourly(shapes) * Decimal(WINDOW_SECONDS) / 3600
    query = Decimal(QUERY_SLOTS * BYTES_PER_QUERY) / 1024**4 * QUERY_USD_PER_TIB
    return compute + query + RESERVE_USD


def resource_plan(run_id, nonce, expires_at, trial):
    """Use one finite input and query budget for proposals and run approvals."""
    validate_trial(trial)
    return ResourcePlan(
        Trial(
            run_id,
            trial["mode"],
            trial["destinations"],
            28800 if trial["mode"] == "ALO" else 1843200,
        ),
        nonce,
        int(timestamp(expires_at) * 1000),
        QUERY_SLOTS,
        BYTES_PER_QUERY,
        QUERY_TIMEOUT_MS,
    )


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
    for manager, replicas in (("jobManager", 1), ("taskManager", 2)):
        shape = spec.get(manager, {})
        if shape.get("replicas") != replicas or shape.get("resource") != {
            "cpu": 1,
            "memory": "2Gi",
        }:
            raise Failure("Rendered application differs from the proposed Pod budget")
        _verify_resources(shape.get("podTemplate", {}).get("spec", {}), "smoke")
    _verify_resources(delivery["supervisor"]["spec"]["template"]["spec"], "supervisor")
    expected_args = [
        "--run-id",
        run_id,
        "--phase",
        "initial",
        "--mode",
        identity.mode,
        "--destinations",
        str(identity.destinations),
        "--records",
        str(records),
        "--bytes-per-second",
        "1048576",
        "--require-restored",
        "false",
    ]
    if initial["spec"]["job"]["args"] != expected_args:
        raise Failure("Rendered application differs from the proposed input")
    expected_upgrade = copy.deepcopy(initial)
    expected_upgrade["spec"]["job"]["args"][3] = "upgrade"
    expected_upgrade["spec"]["job"]["args"][-1] = "true"
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
        "kind": "bigquery-trial-proposal",
        "version": 1,
        "approved": False,
        "revision": revision,
        "runtime_sha256": source_digest(),
        "delivery_sha256": delivery_digest(),
        "started_at": utc(start),
        "cleanup_at": utc(end - BIGQUERY_CEILINGS["cleanup_seconds"]),
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
            "cleanup_seconds": BIGQUERY_CEILINGS["cleanup_seconds"],
            "pods": BIGQUERY_CEILINGS["pods"],
            "pvcs": BIGQUERY_CEILINGS["pvcs"],
            "input_bytes": records * ROW_BYTES[identity.mode],
            "query_bytes": QUERY_SLOTS * BYTES_PER_QUERY,
        },
        "observations": dict(BIGQUERY_OBSERVATIONS),
        "cost": {
            "kind": "planning-estimate",
            "usd": str(estimate()),
            "reviewed_at": REVIEWED_AT,
            "query_usd_per_tib": str(QUERY_USD_PER_TIB),
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
