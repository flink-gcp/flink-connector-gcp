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
from .bundle import source_digest
from .common import Failure, digest, json_bytes, quantity, timestamp, utc
from .model import _hourly
from .policy import GAR, POD_RESOURCES, RUN_ID, SHA
from .workflow import render

WINDOW_SECONDS = 5400
ACTIVE_SECONDS = 5220
QUERY_SLOTS = 12
BYTES_PER_QUERY = 4 * 1024**3
QUERY_TIMEOUT_MS = 60000
REVIEWED_AT = "2026-09-20T00:00:00Z"
QUERY_USD_PER_TIB = Decimal("6.25")
RESERVE_USD = Decimal("1.00")
FIELDS = {
    "version",
    "mode",
    "destinations",
    "repetition",
    "query_slots",
    "maximum_bytes_billed",
    "query_timeout_ms",
    "additional_cost_usd",
}


def load_trial(path):
    """Read a small, exact input schema; reject duplicate JSON object keys."""

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate trial field: " + key)
            result[key] = value
        return result

    try:
        with path.open("rb") as stream:
            data = stream.read(8193)
        if len(data) > 8192:
            raise ValueError("Trial proposal input exceeds 8 KiB")
        result = json.loads(data, object_pairs_hook=pairs)
        validate_trial(result)
        return result
    except (OSError, UnicodeError, ValueError, TypeError) as error:
        raise Failure("Invalid BigQuery trial proposal: " + str(error)) from error


def validate_trial(value):
    if not isinstance(value, dict) or set(value) != FIELDS:
        raise ValueError("Trial proposal fields must match the version 1 schema")
    integers = {
        "version": (1, 1),
        "repetition": (1, 3),
        "query_slots": (1, QUERY_SLOTS),
        "maximum_bytes_billed": (1024**3, BYTES_PER_QUERY),
        "query_timeout_ms": (1, QUERY_TIMEOUT_MS),
    }
    for key, (low, high) in integers.items():
        if type(value[key]) is not int or not low <= value[key] <= high:
            raise ValueError("Invalid trial proposal field: " + key)
    if (
        value["mode"] not in ("ALO", "EO")
        or type(value["destinations"]) is not int
        or value["destinations"] not in (10, 50)
    ):
        raise ValueError("Trial must select ALO/EO and 10/50 destinations")
    cost = value["additional_cost_usd"]
    if (
        not isinstance(cost, str)
        or not re.fullmatch(r"(?:[1-9]|10)\.[0-9]{2}", cost)
        or Decimal(cost) > 10
    ):
        raise ValueError("Cost proposal must be a USD string between 1.00 and 10.00")
    if Decimal(cost) < estimate(value):
        raise ValueError("Proposed cost is below the planning estimate")


def estimate(value):
    """Planning estimate, not a hard bound on service bills or SDK retries."""
    shapes = [
        POD_RESOURCES["operator"],
        POD_RESOURCES["supervisor"],
        *[POD_RESOURCES["smoke"]] * 3,
    ]
    compute = _hourly(shapes) * Decimal(WINDOW_SECONDS) / 3600
    query = (
        Decimal(value["query_slots"] * value["maximum_bytes_billed"])
        / 1024**4
        * QUERY_USD_PER_TIB
    )
    return compute + query + RESERVE_USD


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
    if not timestamp(REVIEWED_AT) <= start <= timestamp(REVIEWED_AT) + 30 * 86400:
        raise ValueError("Proposal starts outside the pricing review's 30-day window")
    records = 28800 if trial["mode"] == "ALO" else 1843200
    identity = Trial(run_id, trial["mode"], trial["destinations"], records)
    resources = ResourcePlan(
        identity,
        nonce,
        int(end * 1000),
        trial["query_slots"],
        trial["maximum_bytes_billed"],
        trial["query_timeout_ms"],
    )
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
        "started_at": utc(start),
        "cleanup_at": utc(end - 900),
        "expires_at": utc(end),
        "repetition": trial["repetition"],
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
            "cleanup_seconds": 900,
            "pods": 5,
            "pvcs": 0,
            "input_bytes": records * ROW_BYTES[identity.mode],
            "query_bytes": trial["query_slots"] * trial["maximum_bytes_billed"],
            "additional_cost_usd": trial["additional_cost_usd"],
        },
        "observations": {
            "warmup_seconds": 180,
            "baseline_seconds": 600,
            "post_recovery_seconds": 600,
            "startup_seconds": 600,
            "recovery_seconds": 300,
            "visibility_seconds": 600,
        },
        "cost": {
            "kind": "planning-estimate",
            "usd": str(estimate(trial)),
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
