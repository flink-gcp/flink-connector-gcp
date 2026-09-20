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
"""Owned Pub/Sub resources for the later independently supervised recovery run."""

import re
from dataclasses import dataclass

import google.auth.exceptions
import requests

from .common import ApiError, Failure, TransportError, json_bytes
from .policy import HTTP_TIMEOUT, PROJECT, REGION, RUN_ID

BASE = "https://pubsub.googleapis.com/v1/"


@dataclass(frozen=True)
class ResourcePlan:
    """Exact relay names and settings bound to a caller-approved ownership nonce."""

    run_id: str
    nonce: str

    def __post_init__(self):
        if not isinstance(self.run_id, str) or not RUN_ID.fullmatch(self.run_id):
            raise Failure("Invalid Pub/Sub run ID")
        if not isinstance(self.nonce, str) or not re.fullmatch(
            r"[0-9a-f]{32}", self.nonce
        ):
            raise Failure("Pub/Sub ownership nonce must be 32 lowercase hex digits")

    @property
    def manifest_path(self):
        return f"_control/pubsub/{self.run_id}.json"

    @property
    def labels(self):
        return {"tier3-run": self.run_id, "tier3-nonce": self.nonce}

    def topics(self):
        return [
            {
                "name": f"projects/{PROJECT}/topics/t3-{self.run_id}-{suffix}",
                "labels": self.labels,
                "messageStoragePolicy": {
                    "allowedPersistenceRegions": [REGION],
                    "enforceInTransit": False,
                },
            }
            for suffix in ("in-0", "in-1", "out")
        ]

    def subscriptions(self):
        return [
            {
                "name": topic["name"].replace("/topics/", "/subscriptions/"),
                "topic": topic["name"],
                "labels": self.labels,
                "ackDeadlineSeconds": 30,
                "messageRetentionDuration": "86400s",
                "expirationPolicy": {"ttl": "86400s"},
                "retainAckedMessages": False,
                "enableMessageOrdering": False,
                "enableExactlyOnceDelivery": False,
            }
            for topic in self.topics()
        ]

    def manifest(self):
        return {
            "version": 1,
            "project": PROJECT,
            "run_id": self.run_id,
            "nonce": self.nonce,
            "topics": self.topics(),
            "subscriptions": self.subscriptions(),
        }


def _duration(value, seconds):
    # ProtoJSON permits a fractional zero suffix on an integral duration.
    return (
        isinstance(value, str)
        and re.fullmatch(str(seconds) + r"(?:\.0{1,9})?s", value) is not None
    )


def _identity(actual, expected, *, allow_deleted_topic=False):
    if (
        not isinstance(actual, dict)
        or actual.get("name") != expected["name"]
        or not isinstance(actual.get("labels"), dict)
        or any(actual["labels"].get(k) != v for k, v in expected["labels"].items())
    ):
        raise Failure("Pub/Sub resource ownership is missing or inconsistent")
    if (
        "topic" in expected
        and actual.get("topic") != expected["topic"]
        and not (allow_deleted_topic and actual.get("topic") == "_deleted-topic_")
    ):
        raise Failure("Pub/Sub subscription topic changed")


def validate_resource(actual, expected):
    """Validate owned service readback, including proto3's omitted false values."""
    if actual is None:
        raise Failure(f"Pub/Sub resource is absent: {expected['name']}")
    _identity(actual, expected)
    if actual.get("state") != "ACTIVE":
        raise Failure("Pub/Sub resource is not ACTIVE")
    if actual.get("messageTransforms"):
        raise Failure("Pub/Sub message transforms are outside the trial")
    if "topic" not in expected:
        policy = actual.get("messageStoragePolicy")
        if (
            not isinstance(policy, dict)
            or policy.get("allowedPersistenceRegions") != [REGION]
            or policy.get("enforceInTransit", False) is not False
            or actual.get("messageRetentionDuration")
            or actual.get("kmsKeyName")
            or actual.get("schemaSettings")
            or actual.get("ingestionDataSourceSettings")
        ):
            raise Failure("Pub/Sub topic settings differ from the plan")
        return
    if (
        type(actual.get("ackDeadlineSeconds")) is not int
        or actual["ackDeadlineSeconds"] != 30
        or not _duration(actual.get("messageRetentionDuration"), 86400)
        or not isinstance(actual.get("expirationPolicy"), dict)
        or not _duration(actual["expirationPolicy"].get("ttl"), 86400)
        or any(
            actual.get(key, False) is not False
            for key in (
                "retainAckedMessages",
                "enableMessageOrdering",
                "enableExactlyOnceDelivery",
                "detached",
            )
        )
        or any(
            actual.get(key)
            for key in (
                "pushConfig",
                "bigqueryConfig",
                "cloudStorageConfig",
                "bigtableConfig",
                "filter",
                "deadLetterPolicy",
                "topicMessageRetentionDuration",
            )
        )
        or "retryPolicy" in actual
    ):
        raise Failure("Pub/Sub subscription settings differ from the plan")


class Resources:
    """Prepare, inspect and clean a recorded plan; never admit a Flink workload.

    ``before_operation(phase, method, name)`` is mandatory. The caller must
    prove current approval, exclusive environment ownership and the appropriate
    admission/cleanup budget before each Pub/Sub request or logical storage call.
    Storage GET denotes one adapter read (up to ten GCS data requests on generation
    churn), and storage PUT denotes one create upload (HTTP POST). The guard must
    reserve the whole adapter call's request/time budget, not count callbacks as
    HTTP requests; credential refresh and guard I/O need their own caller budget.
    HTTP must be the shared authorized session (no automatic retries); store is
    the shared GCS adapter. Construction performs no authentication or network I/O.
    """

    def __init__(self, http, store, plan, before_operation):
        if not isinstance(plan, ResourcePlan) or not callable(before_operation):
            raise Failure("Pub/Sub operations require a plan and lifecycle guard")
        self.http, self.store, self.plan = http, store, plan
        self.before_operation = before_operation

    def _record(self, phase):
        self.before_operation(phase, "GET", self.plan.manifest_path)
        value, generation = self.store.read(self.plan.manifest_path)
        return value, generation

    def _owned(self, phase):
        value, generation = self._record(phase)
        if (
            value is None
            or not generation
            or str(generation) == "0"
            or json_bytes(value) != json_bytes(self.plan.manifest())
        ):
            raise Failure("Missing or replaced Pub/Sub ownership manifest")

    def _call(self, phase, method, expected):
        if method in ("PUT", "DELETE"):
            self._owned(phase)
        name = expected["name"]
        self.before_operation(phase, method, name)
        try:
            response = self.http.request(
                method,
                BASE + name,
                json=expected if method == "PUT" else None,
                timeout=HTTP_TIMEOUT,
                allow_redirects=False,
            )
        except (
            requests.exceptions.RequestException,
            google.auth.exceptions.GoogleAuthError,
        ) as error:
            raise TransportError(
                f"{method} Pub/Sub request failed: {type(error).__name__}"
            ) from error
        if response.status_code == 404 and method in ("GET", "DELETE"):
            return None
        if not 200 <= response.status_code < 300:
            raise ApiError(response.status_code, method, name)
        try:
            value = response.json() if response.content else {}
        except ValueError as error:
            raise Failure("Malformed Pub/Sub response") from error
        if not isinstance(value, dict):
            raise Failure("Pub/Sub response must be an object")
        return value

    def provision(self):
        """Refuse collisions; record all creation intent before the first PUT.

        An ambiguous create is never retried or adopted. The retained manifest
        allows guarded cleanup of any matching resources left by partial work.
        """
        existing, _ = self._record("provision")
        if existing is not None:
            raise Failure("Pub/Sub run already has an ownership manifest")
        expected = self.plan.topics() + self.plan.subscriptions()
        for resource in expected:
            if self._call("provision", "GET", resource) is not None:
                raise Failure("Pub/Sub resource already exists; refusing adoption")
        self.before_operation("provision", "PUT", self.plan.manifest_path)
        self.store.write(self.plan.manifest_path, self.plan.manifest(), generation="0")
        for resource in expected:
            created = self._call("provision", "PUT", resource)
            _identity(created, resource)
        return self.inspect()

    def inspect(self):
        """Read all six recorded resources and validate the frozen settings."""
        self._owned("inspect")
        observations = []
        for expected in self.plan.topics() + self.plan.subscriptions():
            actual = self._call("inspect", "GET", expected)
            validate_resource(actual, expected)
            observations.append(actual)
        return observations

    def cleanup(self):
        """Delete owned subscriptions before topics; retain the intent record.

        The service's deleted-topic marker permits owned subscription cleanup,
        but a different live topic binding does not. Settings drift does not erase
        ownership. Stop on uncertain ownership or
        failed operations, so a failed subscription cleanup cannot delete its
        topic. Pub/Sub deletes have no generation precondition: the caller's
        exclusive control must cover the read/delete gap as well as this call.
        """
        self._owned("cleanup")
        absent = []
        for expected in self.plan.subscriptions() + self.plan.topics():
            actual = self._call("cleanup", "GET", expected)
            if actual is not None:
                _identity(actual, expected, allow_deleted_topic=True)
                self._call("cleanup", "DELETE", expected)
                if self._call("cleanup", "GET", expected) is not None:
                    raise Failure("Pub/Sub resource remains after deletion")
            absent.append(expected["name"])
        return absent
