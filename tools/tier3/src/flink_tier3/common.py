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


class TransportError(Failure):
    """A Kubernetes transport failure, distinct from an invariant violation."""


# A measurement that can carry its claim, and one that cannot. Shared because
# the analyzer reads them offline and a scenario decides them in the Pod, and
# the analyzer itself is not part of what the supervisor mounts.
USABLE = "usable"
INCONCLUSIVE = "inconclusive"
# Evidence problems rather than measurements: excluded everywhere. One of these
# is not a result that came out badly, it is a result nobody may read. They are
# named rather than positional because a caller that reaches into the tuple
# re-labels every record when the order changes, with nothing to fail.
UNEXPORTED = "unexported"
TAMPERED = "tampered"
INCONSISTENT = "inconsistent"
EXCLUDED = (UNEXPORTED, TAMPERED, INCONSISTENT)


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


def ha_metadata(obj, names, namespace=SMOKE):
    # Flink HA ConfigMaps deliberately have no ownerReference. These are
    # observations only: never adopt them into the UID deletion graph. The
    # Operator's normal finalizer must remove them; leftovers block release.
    meta = obj["metadata"]
    names = [names] if isinstance(names, str) else list(names)
    return (
        obj["kind"] == "ConfigMap"
        and meta["namespace"] == namespace
        and any(
            re.fullmatch(
                re.escape(name) + r"-(?:cluster|[0-9a-f]{32})-config-map", meta["name"]
            )
            and meta.get("labels", {}).get("app") == name
            for name in names
        )
        and meta.get("labels", {}).get("type") == "flink-native-kubernetes"
    )


def canonical_quantity(value, kind):
    """Render a Decimal in the form the API server stores for a quota."""
    if kind == "cpu":
        return str(int(value)) if value == int(value) else f"{int(value * 1000)}m"
    for suffix, factor in (("Gi", 1024 * MIB), ("Mi", MIB), ("Ki", 1024)):
        if value % factor == 0:
            return f"{int(value // factor)}{suffix}"
    return str(int(value))


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


def verify_pod(pod, role, image, expected=None, *, spot=None):
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
    expected = expected or POD_RESOURCES[role]
    for category in ("requests", "limits"):
        actual = container.get("resources", {}).get(category, {})
        if set(actual) != set(expected) or any(
            quantity(actual[k]) != quantity(v) for k, v in expected.items()
        ):
            raise Failure("Effective Pod resources exceed or differ from approval")
    if spot is None:
        spot = role in ("smoke", "application")
    if spot:
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
