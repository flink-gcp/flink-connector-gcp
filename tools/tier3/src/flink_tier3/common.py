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
"""Tier-3 lifecycle common."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import urllib.parse
from datetime import UTC, datetime
from decimal import Decimal

from .policy import MIB, POD_RESOURCES, SMOKE


class Failure(RuntimeError):
    """A failed lifecycle invariant or a bounded external operation."""


class IdlePending(Failure):
    """A controller has not yet observed the restored idle specification."""


class ApiError(Failure):
    def __init__(self, status, method, path):
        self.status = status
        super().__init__(f"{method} {path}: HTTP {status}")


def encoded(value):
    return urllib.parse.quote(value, safe="")


def json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(json_bytes(value)).hexdigest()


def timestamp(value):
    if not isinstance(value, str) or not value.endswith("Z"):
        raise Failure("Use an absolute UTC deadline ending in Z")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise Failure("Invalid UTC deadline") from error
    return parsed.timestamp()


def utc(value):
    return datetime.fromtimestamp(value, UTC).isoformat().replace("+00:00", "Z")


def command(args, timeout=60):
    result = subprocess.run(
        args, capture_output=True, text=True, timeout=timeout, check=False
    )
    if result.returncode:
        raise Failure(f"{args[0]} failed (exit {result.returncode})")
    return result.stdout


def quantity(value):
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(m|Ki|Mi|Gi|Ti|k|M|G)?", str(value))
    if not match:
        raise Failure("Unsupported Kubernetes resource quantity")
    factors = {
        None: 1,
        "m": Decimal("0.001"),
        "Ki": 1024,
        "Mi": MIB,
        "Gi": 1024 * MIB,
        "Ti": 1024**4,
        "k": 1000,
        "M": 10**6,
        "G": 10**9,
    }
    return Decimal(match[1]) * factors[match[2]]


def reference(obj):
    meta = obj["metadata"]
    return {
        "kind": obj["kind"],
        "namespace": meta["namespace"],
        "name": meta["name"],
        "uid": meta["uid"],
    }


def ha_metadata(obj, run_id):
    # Flink 2.2.1 HA ConfigMaps deliberately have no ownerReference. These are
    # observations only: never adopt them into the UID deletion graph. The
    # Operator's normal finalizer must remove them; leftovers block release.
    meta = obj["metadata"]
    return (
        obj["kind"] == "ConfigMap"
        and meta["namespace"] == SMOKE
        and re.fullmatch(
            re.escape(run_id) + r"-(?:cluster|[0-9a-f]{32})-config-map", meta["name"]
        )
        and meta.get("labels", {}).get("app") == run_id
        and meta.get("labels", {}).get("type") == "flink-native-kubernetes"
    )


def ownership(items, roots):
    owned = set(roots)
    while True:
        discovered = {
            obj["metadata"]["uid"]
            for obj in items
            if any(
                ref.get("uid") in owned
                for ref in obj["metadata"].get("ownerReferences", [])
            )
        }
        expanded = owned | discovered
        if expanded == owned:
            return owned
        owned = expanded


def verify_pod(pod, role, image):
    spec = pod["spec"]
    if (
        spec.get("initContainers")
        or spec.get("ephemeralContainers")
        or len(spec.get("containers", [])) != 1
    ):
        raise Failure("Unexpected Pod container count")
    container = spec["containers"][0]
    if container["image"] != image:
        raise Failure("Pod image differs from approval")
    expected = POD_RESOURCES[role]
    for category in ("requests", "limits"):
        actual = container.get("resources", {}).get(category, {})
        if set(actual) != set(expected) or any(
            quantity(actual[k]) != quantity(v) for k, v in expected.items()
        ):
            raise Failure("Effective Pod resources exceed or differ from approval")
    if role == "smoke":
        if spec.get("nodeSelector", {}).get("cloud.google.com/gke-spot") != "true":
            raise Failure("Flink Pods must select Spot nodes")
    else:
        # Autopilot uses normal capacity unless a Pod opts in to Spot. Normal
        # nodes need not have a gke-spot=false label.
        selector = spec.get("nodeSelector", {})
        if (
            selector.get("cloud.google.com/gke-spot") == "true"
            or selector.get("cloud.google.com/gke-provisioning") == "spot"
        ):
            raise Failure("Control Pods must not opt in to Spot")
        if role == "supervisor":
            terms = (
                spec.get("affinity", {})
                .get("nodeAffinity", {})
                .get("requiredDuringSchedulingIgnoredDuringExecution", {})
                .get("nodeSelectorTerms", [])
            )
            exclusion = {
                "key": "cloud.google.com/gke-spot",
                "operator": "NotIn",
                "values": ["true"],
            }
            if not terms or not all(
                exclusion in term.get("matchExpressions", []) for term in terms
            ):
                raise Failure("Supervisor affinity must exclude Spot in every term")


def contains(actual, expected):
    """Compare submitted fields while allowing API defaulting."""
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            k in actual and contains(actual[k], v) for k, v in expected.items()
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(expected)
            and all(
                contains(observed, submitted)
                for observed, submitted in zip(actual, expected, strict=True)
            )
        )
    return actual == expected
