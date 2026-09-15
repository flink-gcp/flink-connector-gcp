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
"""Tier-3 lifecycle records."""

from __future__ import annotations

import copy
import time
import uuid

from .common import ApiError, Failure, json_bytes, utc
from .model import Phase, RunRecord
from .policy import ENVIRONMENT, MIB


class Records:
    def __init__(self, store, approval, clock=time.time):
        self.store, self.approval, self.clock = store, approval, clock
        self.path = "_control/runs/" + approval.run_id + ".json"
        self.cache = RunRecord(approval.nonce)

    def read(self):
        value, generation = self.store.read(self.path)
        if value is None or value.get("nonce") != self.approval.nonce:
            raise Failure("Missing or replaced run control record")
        self.cache = RunRecord.from_dict(value)
        return self.cache, generation

    def _change(self, edit):
        for _ in range(5):
            record, generation = self.read()
            edit(record)
            try:
                self.store.write(self.path, record.to_dict(), generation)
                self.cache = record
                return record
            except ApiError as error:
                if error.status not in (409, 412):
                    raise
        raise Failure("Concurrent run control updates did not settle")

    @staticmethod
    def _remember(current, additions):
        for name, ref in additions.items():
            if name in current and current[name] != ref:
                raise Failure("Recorded UID identity cannot be replaced")
            current[name] = copy.deepcopy(ref)

    def remember_root(self, key, ref):
        return self._change(lambda record: self._remember(record.roots, {key: ref}))

    def observe(self, refs):
        return self._change(lambda record: self._remember(record.observed, refs))

    def set_phase(
        self,
        phase,
        *,
        reason=None,
        success=None,
        state_clean=None,
        evidence_failed=False,
    ):
        def edit(record):
            previous = record.phase
            record.set_phase(phase)
            if reason is not None:
                record.reason = reason
            record.evidence_failed |= evidence_failed
            if success is not None:
                record.success = (
                    success
                    and not record.evidence_failed
                    and (previous != Phase.CLEANED or record.success)
                )
            if state_clean is not None:
                record.state_clean = state_clean

        return self._change(edit)

    def begin_cleanup(self, reason):
        def edit(record):
            if record.phase != Phase.CLEANED:
                record.set_phase(Phase.CLEANING)
                record.reason = reason

        return self._change(edit)

    def request_stop(self):
        return self._change(lambda record: setattr(record, "stop_requested", True))

    def heartbeat(self):
        return self._change(
            lambda record: setattr(record, "heartbeat", utc(self.clock()))
        )

    def intend(self, key, manifest=True):
        fields = {
            "application": "application_intent",
            "config": "config_intent",
            "supervisor": "supervisor_intent",
        }

        def edit(record):
            if (
                record.evidence_failed
                or record.stop_requested
                or record.phase in (Phase.CLEANING, Phase.CLEANED)
            ):
                raise Failure("Run admission has been stopped")
            setattr(record, fields[key], copy.deepcopy(manifest))

        return self._change(edit)

    def record_lineage(self, lineage):
        def edit(record):
            if record.lineage not in (None, lineage):
                raise Failure("Smoke restarted with an unexpected fresh lineage")
            record.lineage = lineage

        return self._change(edit)

    def checkpoint(self):
        return self._change(lambda record: setattr(record, "checkpoint_observed", True))

    def mark_evidence_failed(self):
        return self._change(lambda record: setattr(record, "evidence_failed", True))

    def final_log(self, success):
        def edit(record):
            record.final_log_attempted = True
            record.evidence_failed |= not success

        return self._change(edit)

    def settled(self, evidence_failed):
        def edit(record):
            record.idle = True
            record.evidence_failed |= evidence_failed

        return self._change(edit)

    def evidence(self, event, payload, actor="supervisor"):
        prefix = f"runs/{self.approval.run_id}/{actor}/"
        budget = (88 if actor == "supervisor" else 10) * MIB
        objects = self.store.objects(prefix)
        data = {"event": event, "at": utc(self.clock()), "payload": payload}
        if sum(int(obj["size"]) for obj in objects) + len(json_bytes(data)) > budget:
            raise Failure("Durable evidence ceiling reached")
        self.store.write(prefix + str(uuid.uuid4()) + ".json", data)


class EnvironmentLock:
    def __init__(self, store):
        self.store = store

    def acquire(self, owner, inspect_runs=True):
        if inspect_runs and self.store.objects("_control/runs/"):
            raise Failure(
                "Unfinished run records require recovery before lock acquisition"
            )
        # Immutable approvals without a final result remain blockers even if a
        # control writer removed the mutable records.
        names = (
            {obj["name"] for obj in self.store.objects("runs/", maximum=100000)}
            if inspect_runs
            else set()
        )
        for name in names:
            if (
                name.endswith("/approval.json")
                and name.removesuffix("approval.json") + "result.json" not in names
            ):
                raise Failure(
                    "An approval lacks a final idle receipt; recover it first"
                )
        return self.store.write(ENVIRONMENT, owner, "0")

    def assert_owner(self, owner):
        value, generation = self.store.read(ENVIRONMENT)
        if value != owner:
            raise Failure("Environment lock owner changed")
        return generation

    def release(self, owner):
        self.store.delete(ENVIRONMENT, self.assert_owner(owner))
