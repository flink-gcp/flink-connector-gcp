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
"""Persist the run's queue intent, its paused readback and its cleanup."""

from __future__ import annotations

import copy
import uuid
from dataclasses import asdict

from ..common import ApiError, Failure, digest, transient
from ..model import Phase
from .queue import Unsent, configured

# A queue is not readable the instant it is created, and reports the state it
# held before the pause for a moment after it, so admission waits for the
# readback to settle rather than refusing on the first read.
SETTLE_SECONDS = 120
SETTLE_POLL = 5
# The only state a queue reports on the way to the pause it was just given.
SETTLING_STATE = "RUNNING"
# The readback fields the receipt keeps.
RECEIPT_FIELDS = ("name", "state", "tombstoneTtl", "rateLimits", "retryConfig")


def require_cloudtasks_clean(record):
    """Retain shared resources and the lock until queue cleanup is recorded."""
    state = record.cloudtasks
    if state is None:
        return
    if (
        not record.stop_requested
        or not isinstance(state, dict)
        or state.get("stopped") is not True
        or state.get("cleaned") is not True
        or state.get("released") is not True
        or state.get("inflight") is not None
    ):
        raise Failure("Cloud Tasks queue cleanup or runner release is incomplete")


class CloudTasksLifecycle:
    """The run's queue, from intent to deletion, in the run's control record.

    The runner creates the queue, pauses it and reads it back before the
    application exists, and releases its authority when it settles; the
    supervisor deletes only a queue whose creation intent the record holds,
    once the runner has released and no workload Pod remains. Only this rig's
    runner handoff writes the release: the recovery workflow's runner is built
    without one, so it cannot release on behalf of the process that created the
    queue.

    A runner call that may have created the queue and did not return leaves its
    marker in the record, and cleanup then refuses: the create could still land
    after the supervisor found the name absent. Recovering such a run is an
    operator's decision, as it is for a BigQuery table create.
    """

    def __init__(self, env, queue, application):
        self.env, self.api = env, queue
        trial = queue.trial
        approval = env.approval
        if (
            approval.scenario != "cloudtasks-recovery"
            or approval.cloudtasks_plan != trial
            or digest(application) != approval.application_sha256
            or application.get("metadata", {}).get("name") != trial.run_id
            or application.get("spec", {}).get("job", {}).get("args")
            != trial.arguments("initial")
        ):
            raise Failure("Cloud Tasks queue differs from the approved application")
        self.trial = trial
        self.intent = {
            "version": 1,
            "trial": asdict(trial),
            "queue": trial.queue_path,
            "application_sha256": approval.application_sha256,
        }

    def _role(self, role):
        if self.env.actor != role:
            raise Failure("Cloud Tasks queue operation requires the " + role)

    def _state(self, record):
        state = record.cloudtasks
        if not isinstance(state, dict) or state.get("intent") != self.intent:
            raise Failure("Missing or replaced Cloud Tasks queue intent")
        return state

    def _read(self):
        self.env.assert_owner()
        return self._state(self.env.refresh())

    def _change(self, edit, *, open_only=False):
        self.env.assert_owner()

        def change(record):
            state = self._state(record)
            if open_only and (
                state["stopped"]
                or state["released"]
                or record.stop_requested
                or record.evidence_failed
                or record.phase in (Phase.CLEANING, Phase.CLEANED)
            ):
                raise Failure("Cloud Tasks queue admission has stopped")
            edit(state)

        return self._state(self.env.records._change(change))

    def initialize(self):
        """Persist the intent before any queue call; never replace it."""
        self._role("runner")
        self.env.admission_open()

        def initialize(record):
            if record.cloudtasks is not None:
                self._state(record)
                return
            if (
                record.phase not in (Phase.APPROVED, Phase.READY)
                or record.stop_requested
                or record.evidence_failed
            ):
                raise Failure("Cloud Tasks queue admission has stopped")
            record.cloudtasks = {
                "intent": copy.deepcopy(self.intent),
                "stopped": False,
                "creation_intent": False,
                "inflight": None,
                "released": False,
                "queue": None,
                "cleaned": False,
            }

        self.env.assert_owner()
        self.env.records._change(initialize)

    def _settled(self, read, deadline):
        """Repeat a read until it answers, retrying what says nothing."""
        while True:
            self.env.admission_open()
            try:
                return read()
            except Failure as error:
                if not transient(error) or self.env.clock() >= deadline:
                    raise
            self.env.sleep(min(SETTLE_POLL, max(0, deadline - self.env.clock())))

    def provision(self, deadline):
        """Create the queue, pause it and keep its paused readback.

        The name must be free first: run IDs are never reused, so a queue
        already carrying it is not this run's to admit or delete. The creation
        intent and the call marker are one record write, just before the
        create; the marker is cleared once the outcome is known, which a
        create that did not return leaves unknown.
        """
        self._role("runner")
        api = self.api.with_deadline(deadline)
        if self._settled(api.get, deadline) is not None:
            raise Failure("Cloud Tasks queue already exists; this run does not own it")
        self.env.admission_open()
        marker = {"operation": "provision", "id": uuid.uuid4().hex}

        def begin(value):
            if value["inflight"] is not None or value["creation_intent"]:
                raise Failure("Cloud Tasks queue admission cannot be resumed")
            value.update(creation_intent=True, inflight=marker)

        def settle(value, receipt=None, created=True):
            # A retried write whose earlier attempt landed finds its own outcome.
            if (
                value["inflight"] is None
                and value["creation_intent"] is created
                and value["queue"] == receipt
            ):
                return
            if value["inflight"] != marker:
                raise Failure("Cloud Tasks runner call identity changed")
            value["inflight"] = None
            if not created:
                value["creation_intent"] = False
            if receipt is not None:
                value["queue"] = receipt

        self._change(begin, open_only=True)
        try:
            api.create()
        except Unsent:
            # Refused before sending, so no create of this call exists.
            self._settle(lambda value: settle(value, created=False))
            raise
        except ApiError as error:
            # The service answered, so no create of this call can still land.
            # A 409 after the name read free names a queue, or a deleted
            # name's tombstone, that is not this run's to admit or delete.
            if 400 <= error.status < 500 and error.status not in (408, 429):
                self._settle(lambda value: settle(value, created=False))
            raise
        # From here the queue exists, so a failure clears the marker: the
        # create can no longer land behind cleanup.
        try:
            api.pause()
            readback = self._paused(api, deadline)
        except BaseException:
            self._settle(settle)
            raise
        receipt = {key: readback.get(key) for key in RECEIPT_FIELDS}
        self._settle(lambda value: settle(value, receipt), strict=True)
        self.env.emit("cloudtasks-queue-admitted", {"readback": receipt})
        return receipt

    def _settle(self, edit, strict=False):
        """Clear the call marker, retrying the record write as BigQuery does.

        Only the write is retried, never the service call. A failure here is
        raised when `strict`, and otherwise left to the original failure the
        caller is about to raise.
        """
        for attempt in range(3):
            try:
                self._change(edit)
                return
            except (Failure, OSError, ValueError):
                if attempt == 2 and strict:
                    raise

    def _paused(self, api, deadline):
        """The paused readback that admits the queue, once the service settles.

        An absent, unreadable or still-running answer inside the window is the
        queue initializing. A readback that names another queue or carries
        another configuration is a deviation whenever it arrives.
        """
        readback, reads = None, 0
        while True:
            readback = self._settled(api.get, deadline)
            reads += 1
            if readback is not None:
                if (
                    readback.get("name") != self.trial.queue_path
                    or not configured(readback)
                    or readback.get("state") not in (SETTLING_STATE, "PAUSED")
                ):
                    break
                if readback.get("state") == "PAUSED":
                    if reads > 1:
                        self.env.emit("cloudtasks-queue-settling", {"reads": reads})
                    return readback
            if self.env.clock() >= deadline:
                break
            self.env.sleep(min(SETTLE_POLL, max(0, deadline - self.env.clock())))
        self.env.emit("cloudtasks-queue-deviation", {"readback": readback})
        raise Failure("Cloud Tasks queue readback is not the paused configuration")

    def listing(self, deadline):
        """The queue's reads for one listing by the supervisor's oracle.

        The run's state is read here, before any of them, so a failure reading
        it is never taken for a failure of a Cloud Tasks call; the returned
        queue's `get` and `tasks` make only those calls, bounded by `deadline`.
        """
        self._role("supervisor")
        self._read()
        return self.api.with_deadline(deadline)

    def stop(self):
        """Close admission; this does not acknowledge runner exit."""
        self._read()
        self.env.records.request_stop()
        self._change(lambda state: state.update(stopped=True))

    def release(self):
        """Permanently release runner authority after its calls have returned.

        The runner calls the service only while it admits the run, so it
        releases as soon as it settles, without stopping the run: closing this
        queue's admission is all the release needs, and the supervisor still
        has the exercise to run.
        """
        self._role("runner")

        def release(state):
            if state["inflight"] is not None:
                raise Failure("Cloud Tasks runner call is in flight or unresolved")
            state.update(stopped=True, released=True)

        self._change(release)

    def released(self):
        """Read the runner's durable release, without transferring it."""
        state = self._read()
        return state["released"] is True and state["inflight"] is None

    def cleanup(self, quiesce, *, deadline):
        """Delete the intended queue once the runner and the workload are gone.

        Returns False while the deletion is not yet confirmed, for the caller's
        next pass. Only a recorded creation intent authorizes deletion, which
        also covers a create whose response was lost.
        """
        self._role("supervisor")
        self.stop()
        if not self.released():
            raise Failure("Cloud Tasks runner has not released its authority")
        if quiesce() is not True:
            raise Failure("Cloud Tasks workload is not quiescent")
        state = self._read()
        if state["creation_intent"]:
            self.env.assert_owner()
            api = self.api.with_deadline(deadline)
            api.delete()
            if api.get() is not None:
                return False
        self._change(lambda value: value.update(cleaned=True))
        return True
