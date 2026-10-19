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
"""Render and check an offline trial proposal's manifests before any approval."""

import copy
import json
import re

from .common import Failure, quantity, timestamp, utc
from .policy import GAR, POD_RESOURCES, RUN_ID, SHA
from .workflow import render


def verify_resources(pod, role):
    """One container, with exactly the requests and limits the role is priced at."""
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


def verify_sizes(spec, parallelism):
    """One JobManager, and one single-slot TaskManager per subtask."""
    job = spec.get("job", {})
    # bool is an int subclass, and True == 1.
    if type(job.get("parallelism")) is not int or job["parallelism"] != parallelism:
        raise Failure("Rendered application differs from the proposed parallelism")
    for manager, replicas in (("jobManager", 1), ("taskManager", parallelism)):
        shape = spec.get(manager, {})
        if shape.get("replicas") != replicas or shape.get("resource") != {
            "cpu": 1,
            "memory": "2Gi",
        }:
            raise Failure("Rendered application differs from the proposed Pod budget")
        verify_resources(shape.get("podTemplate", {}).get("spec", {}), "smoke")


def check_inputs(
    *,
    run_id,
    revision,
    application_image,
    package,
    service,
    started_at,
    expires_at,
    window_seconds,
    active_seconds,
    expected_active_seconds,
):
    """The proposal's identity, image and window, or ValueError; returns the window."""
    if not RUN_ID.fullmatch(run_id) or not SHA.fullmatch(revision):
        raise ValueError("Proposal requires a valid run ID and full source revision")
    if not re.fullmatch(
        re.escape(GAR + package + "@") + r"sha256:[0-9a-f]{64}", application_image
    ):
        raise ValueError(f"Proposal requires a {service} GAR image digest")
    start, end = timestamp(started_at), timestamp(expires_at)
    if (
        start != int(start)
        or end != int(end)
        or end - start != window_seconds
        or type(active_seconds) is not int
        or active_seconds != expected_active_seconds
    ):
        raise ValueError(
            f"Proposal requires a {window_seconds // 60}-minute window and a "
            f"{expected_active_seconds}-second supervisor deadline"
        )
    return start, end


def render_verified(
    *,
    scenario,
    namespace,
    identity,
    run_id,
    nonce,
    end,
    active_seconds,
    application_image,
    **tags,
):
    """Both phases and the delivery, refused unless they are exactly the proposal.

    `identity` renders each phase's `arguments`, and its `parallelism(phase)`
    sizes the job and its TaskManagers. The upgrade may change only the arguments
    and, for a rescale, those sizes, which the shared exercise patches.
    """
    initial, upgrade, delivery = render(
        run_id,
        nonce,
        utc(end),
        active_seconds,
        scenario=scenario,
        expression="[application, upgradeApplication, delivery.resources]",
        application_image=application_image,
        **tags,
    )
    metadata, spec = initial["metadata"], initial["spec"]
    if (
        metadata.get("name") != run_id
        or metadata.get("namespace") != namespace
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
    for phase, manifest in (("initial", initial), ("upgrade", upgrade)):
        verify_sizes(manifest["spec"], identity.parallelism(phase))
    verify_resources(delivery["supervisor"]["spec"]["template"]["spec"], "supervisor")
    if spec["job"]["args"] != identity.arguments("initial"):
        raise Failure("Rendered application differs from the proposed input")
    expected_upgrade = copy.deepcopy(initial)
    expected_upgrade["spec"]["job"]["args"] = identity.arguments("upgrade")
    expected_upgrade["spec"]["job"]["parallelism"] = identity.parallelism("upgrade")
    expected_upgrade["spec"]["taskManager"]["replicas"] = identity.parallelism(
        "upgrade"
    )
    if upgrade != expected_upgrade:
        raise Failure("Rendered upgrade changes more than phase, restoration and size")
    data = delivery["config"]["data"]
    if (
        json.loads(data["application.json"]) != initial
        or json.loads(data["upgrade-application.json"]) != upgrade
        or json.loads(data["approval.json"]) != {}
    ):
        raise Failure("Proposal delivery does not contain the unapproved manifests")
    return initial, upgrade, delivery
