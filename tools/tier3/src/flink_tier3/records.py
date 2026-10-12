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
import random
import time
import uuid

from .bigquery.handoff import require_bigquery_clean
from .common import ApiError, Failure, utc
from .model import Phase, RunRecord
from .policy import (
    ENVIRONMENT,
    GENERATION_RACES,
    RACE_BACKOFF_CAP,
    RACE_BACKOFF_FIRST,
    RATE_LIMIT_BACKOFF,
)
from .pubsub.lifecycle import require_pubsub_clean


def conditional_update(
    store, path, read, edit, serialize=lambda value: value, sleep=None
):
    """Read, edit and write back one object, retrying a lost generation race.

    A write refused for the object's mutation rate, and a lost race, are
    retried after a randomized exponential backoff from a fresh read, so the
    edit and every deadline it checks run again before the write is sent.
    """
    sleep = sleep or time.sleep
    races = throttled = 0
    while True:
        value, generation = read()
        edit(value)
        try:
            store.write(path, serialize(value), generation)
            return value
        except ApiError as error:
            if error.status == 429 and throttled < len(RATE_LIMIT_BACKOFF):
                sleep(RATE_LIMIT_BACKOFF[throttled] + random.random())
                throttled += 1
                continue
            if error.status not in (409, 412):
                raise
            races += 1
            if races == GENERATION_RACES:
                raise Failure("Concurrent control updates did not settle") from error
            # Full jitter, so two writers that collided do not collide again.
            ceiling = min(RACE_BACKOFF_FIRST * 2 ** (races - 1), RACE_BACKOFF_CAP)
            sleep(random.uniform(0, ceiling))


# The documents a run retains directly under its prefix, beside its receipts.
RUN_ARTIFACTS = (
    "approval.json",
    "application.json",
    "images.json",
    "upgrade-application.json",
    "result.json",
)


def write_artifact(store, run_id, name, value):
    """Write one immutable run document."""
    if name not in RUN_ARTIFACTS:
        raise ValueError("Unknown run artifact: " + name)
    return store.write(f"runs/{run_id}/{name}", value)


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
        record = conditional_update(
            self.store, self.path, self.read, edit, lambda record: record.to_dict()
        )
        self.cache = record
        return record

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
        evidence_failure=None,
    ):
        def edit(record):
            if phase == Phase.CLEANED:
                require_bigquery_clean(record)
                require_pubsub_clean(record)
            previous = record.phase
            record.set_phase(phase)
            if reason is not None:
                record.reason = reason
            record.fail_evidence(evidence_failure)
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

    def recovery_step(self, expected, value):
        """Persist a single scenario transition before its Kubernetes operation."""

        def edit(record):
            current = record.recovery.get("stage") if record.recovery else None
            if (
                record.phase != Phase.RUNNING
                or record.stop_requested
                or record.evidence_failed
                or current != expected
            ):
                raise Failure("Recovery exercise has stopped or already advanced")
            record.recovery = copy.deepcopy(value)

        return self._change(edit)

    def heartbeat(self):
        """Report the supervisor alive."""
        return self._change(
            lambda record: setattr(record, "heartbeat", utc(self.clock()))
        )

    def intend(self, key, manifest=True):
        fields = {
            "application": "application_intent",
            "config": "config_intent",
            "supervisor": "supervisor_intent",
            "probe": "probe_intent",
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

    def mark_evidence_failed(self, failure):
        return self._change(lambda record: record.fail_evidence(failure))

    def final_log(self, failure):
        """Record the final log attempt; `failure` is its cause, or None."""

        def edit(record):
            record.final_log_attempted = True
            record.fail_evidence(failure)

        return self._change(edit)

    def settled(self, evidence_failure):
        def edit(record):
            require_bigquery_clean(record)
            require_pubsub_clean(record)
            record.idle = True
            record.fail_evidence(evidence_failure)

        return self._change(edit)

    def evidence(self, event, payload, actor="supervisor"):
        prefix = f"runs/{self.approval.run_id}/{actor}/"
        data = {"event": event, "at": utc(self.clock()), "payload": payload}
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
        # control writer removed the mutable records. A delimiter listing keeps
        # the scan proportional to the number of runs, not their evidence.
        for run in sorted(self.store.prefixes("runs/")) if inspect_runs else ():
            approval, _ = self.store.read(run + "approval.json")
            if approval is None:
                continue
            result, _ = self.store.read(run + "result.json")
            if result is None:
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
