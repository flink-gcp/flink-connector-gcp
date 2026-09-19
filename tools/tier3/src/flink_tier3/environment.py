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
"""Tier-3 lifecycle environment."""

from __future__ import annotations

import json
import time
from functools import wraps

from .common import ApiError, Failure, reference
from .model import Approval, Phase, validate_approval
from .policy import POLL
from .records import EnvironmentLock, Records


class Environment:
    """Dependencies and observations shared by one lifecycle actor."""

    def __init__(
        self,
        kube,
        store,
        approval,
        clock=time.time,
        sleep=time.sleep,
        actor="supervisor",
        queues=None,
        ledger=None,
    ):
        self.kube, self.store = kube, store
        # Cloud Tasks session collaborators; None for the smoke scenarios.
        self.queues, self.ledger = queues, ledger
        self.approval = (
            approval if isinstance(approval, Approval) else Approval.from_dict(approval)
        )
        self.schedule = self.approval.schedule
        self.clock, self.sleep, self.actor = clock, sleep, actor
        self.records = Records(store, self.approval, clock)
        self.evidence_failed = False
        self.stopping = False

    @property
    def roots(self):
        return self.records.cache.roots

    @property
    def observed(self):
        return self.records.cache.observed

    def refresh(self):
        return self.records.read()[0]

    def namespaces(self):
        for ns, saved in self.approval.namespaces.items():
            actual = self.kube.namespace(ns)
            if actual["metadata"]["uid"] != saved["uid"] or actual["metadata"].get(
                "deletionTimestamp"
            ):
                raise Failure("Namespace identity changed; refusing mutation")

    def assert_owner(self):
        EnvironmentLock(self.store).assert_owner(self.approval.lock_owner)

    def admission_open(self):
        self.namespaces()
        validate_approval(self.approval.to_dict(), self.clock())
        control = self.refresh()
        if (
            self.stopping
            or self.evidence_failed
            or control.evidence_failed
            or control.stop_requested
            or control.phase not in (Phase.APPROVED, Phase.READY)
        ):
            raise Failure("Run admission has been stopped")
        self.assert_owner()

    def require_running(self, message):
        """Refuse a mutation unless this run is still admitted and owned."""
        self.namespaces()
        control = self.refresh()
        if (
            self.stopping
            or self.evidence_failed
            or control.evidence_failed
            or control.stop_requested
            or control.phase != Phase.RUNNING
        ):
            raise Failure(message)
        self.assert_owner()
        return control

    def emit(self, event, payload):
        try:
            self.records.evidence(event, payload, self.actor)
        except (Failure, ValueError, OSError) as error:
            self.evidence_failed = True
            print(
                json.dumps({"event": "evidence-failed", "cause": type(error).__name__}),
                flush=True,
            )

    def remember(self, key, obj):
        self.records.remember_root(key, reference(obj))
        return obj

    def root(self, key):
        ref = self.roots.get(key)
        if not ref:
            return None
        obj = self.kube.get(ref["kind"], ref["namespace"], ref["name"])
        if obj and obj["metadata"]["uid"] != ref["uid"]:
            raise Failure("Run root was replaced; refusing ownership transfer")
        return obj

    def wait(self, predicate, deadline):
        while self.clock() < deadline:
            if predicate():
                return
            remaining = deadline - self.clock()
            if remaining > 0:
                self.sleep(min(POLL, remaining))
        raise Failure("Bounded wait expired")


def retry_conflicts(operation):
    """Re-read identities and versions when another cleanup writes first."""

    @wraps(operation)
    def attempt(*args, **kwargs):
        for retry in range(5):
            try:
                return operation(*args, **kwargs)
            except ApiError as error:
                if error.status not in (409, 422) or retry == 4:
                    raise

    return attempt
