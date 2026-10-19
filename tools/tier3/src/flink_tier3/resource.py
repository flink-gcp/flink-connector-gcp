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
"""One run-owned service resource, from intent to deletion, in the control record."""

from __future__ import annotations

import copy
import uuid
from dataclasses import asdict
from typing import NamedTuple

from .common import ApiError, Failure, digest, transient
from .model import Phase
from .policy import BIGTABLE_OBSERVATIONS, CLOUDTASKS_OBSERVATIONS
from .rest import Unsent

# The wait between reads of a resource that is still settling.
SETTLE_POLL = 5


class ResourceScenario(NamedTuple):
    """A scenario whose runner creates one resource before the application.

    `field` is the record field and the prefix of the approval's trial and
    plan, `service` and `noun` name it in messages, the noun also keys its
    name in the intent and its receipt in the record, and `timing` bounds its
    startup.
    """

    field: str
    service: str
    noun: str
    timing: dict

    @property
    def subject(self):
        return f"{self.service} {self.noun}"


RESOURCE_SCENARIOS = {
    "cloudtasks-recovery": ResourceScenario(
        "cloudtasks", "Cloud Tasks", "queue", CLOUDTASKS_OBSERVATIONS
    ),
    "bigtable-recovery": ResourceScenario(
        "bigtable", "Bigtable", "instance", BIGTABLE_OBSERVATIONS
    ),
}


def require_resources_clean(record):
    """Retain shared resources and the lock until every resource's cleanup is recorded."""
    for scenario in RESOURCE_SCENARIOS.values():
        state = getattr(record, scenario.field)
        if state is not None and (
            not record.stop_requested
            or not isinstance(state, dict)
            or state.get("stopped") is not True
            or state.get("cleaned") is not True
            or state.get("released") is not True
            or state.get("inflight") is not None
        ):
            raise Failure(f"{scenario.subject} cleanup or runner release is incomplete")


class ResourceLifecycle:
    """The state a runner-created, supervisor-deleted resource keeps in the record.

    A subclass names its scenario, whose `RESOURCE_SCENARIOS` entry names the
    record field and the messages, and supplies `_populate` and `delete`. The
    runner creates the resource before the application exists and releases
    its authority when it settles; the supervisor deletes only a resource whose
    creation intent the record holds, once the runner has released and no
    workload Pod remains.

    A runner call that may have created the resource and did not return leaves
    its marker in the record, and cleanup then refuses: the create could still
    land after the supervisor found the name absent. Recovering such a run is
    an operator's decision, as it is for a BigQuery table create.
    """

    scenario = ""

    def __init__(self, env, api, application):
        resource = RESOURCE_SCENARIOS[self.scenario]
        trial = api.trial
        approval = env.approval
        if (
            approval.scenario != self.scenario
            or getattr(approval, resource.field + "_plan") != trial
            or digest(application) != approval.application_sha256
            or application.get("metadata", {}).get("name") != trial.run_id
            or application.get("spec", {}).get("job", {}).get("args")
            != trial.arguments("initial")
        ):
            raise Failure(resource.subject + " differs from the approved application")
        self.env, self.api, self.trial = env, api, trial
        self.field, self.service, self.receipt_key = (
            resource.field,
            resource.service,
            resource.noun,
        )
        self.subject = resource.subject
        self.intent = {
            "version": 1,
            "trial": asdict(trial),
            resource.noun: api.name,
            "application_sha256": approval.application_sha256,
        }

    def provision(self, deadline):
        """Create the resource, populate it and keep the readback that admits it.

        The name must be free first: run IDs are never reused, so a resource
        already carrying it is not this run's to admit or delete.
        """
        self._role("runner")
        api = self.api.with_deadline(deadline)
        if self._settled(api.get, deadline) is not None:
            raise Failure(self.subject + " already exists; this run does not own it")
        marker = self._begin_create()
        try:
            api.create()
        except Unsent:
            # Refused before sending, so no create of this call exists.
            self._end_create(marker, created=False)
            raise
        except ApiError as error:
            # The service answered, so no create of this call can still land.
            # A 409 after the name read free names a resource, or a deleted
            # name's tombstone, that is not this run's to admit or delete.
            if 400 <= error.status < 500 and error.status not in (408, 429):
                self._end_create(marker, created=False)
            raise
        # A failure before the create is seen to have landed keeps the marker,
        # because the create could still land behind cleanup; from then on
        # the resource exists, so a failure clears it.
        self._landed(api, deadline)
        try:
            receipt = self._populate(api, deadline)
        except BaseException:
            self._end_create(marker)
            raise
        self._end_create(marker, receipt, strict=True)
        self.env.emit(
            f"{self.field}-{self.receipt_key}-admitted", {"readback": receipt}
        )
        return receipt

    def _landed(self, api, deadline):
        """Prove that the accepted create took effect; by default its answer does."""

    def _populate(self, api, deadline):
        """Bring the created resource to its admitted state; returns the receipt."""
        raise NotImplementedError

    def _role(self, role):
        if self.env.actor != role:
            raise Failure(f"{self.subject} operation requires the {role}")

    def _state(self, record):
        state = getattr(record, self.field)
        if not isinstance(state, dict) or state.get("intent") != self.intent:
            raise Failure(f"Missing or replaced {self.subject} intent")
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
                raise Failure(f"{self.subject} admission has stopped")
            edit(state)

        return self._state(self.env.records._change(change))

    def initialize(self):
        """Persist the intent before any service call; never replace it."""
        self._role("runner")
        self.env.admission_open()

        def initialize(record):
            if getattr(record, self.field) is not None:
                self._state(record)
                return
            if (
                record.phase not in (Phase.APPROVED, Phase.READY)
                or record.stop_requested
                or record.evidence_failed
            ):
                raise Failure(f"{self.subject} admission has stopped")
            setattr(
                record,
                self.field,
                {
                    "intent": copy.deepcopy(self.intent),
                    "stopped": False,
                    "creation_intent": False,
                    "inflight": None,
                    "released": False,
                    self.receipt_key: None,
                    "cleaned": False,
                },
            )

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

    def _begin_create(self):
        """Record the creation intent and the call marker in one write.

        Returns the marker that `_end_create` clears once the outcome is known,
        which a create that did not return leaves unknown.
        """
        self.env.admission_open()
        marker = {"operation": "provision", "id": uuid.uuid4().hex}

        def begin(value):
            if value["inflight"] is not None or value["creation_intent"]:
                raise Failure(f"{self.subject} admission cannot be resumed")
            value.update(creation_intent=True, inflight=marker)

        self._change(begin, open_only=True)
        return marker

    def _end_create(self, marker, receipt=None, created=True, strict=False):
        """Clear the call marker, keeping the receipt or dropping the intent."""

        def settle(value):
            # A retried write whose earlier attempt landed finds its own outcome.
            if (
                value["inflight"] is None
                and value["creation_intent"] is created
                and value[self.receipt_key] == receipt
            ):
                return
            if value["inflight"] != marker:
                raise Failure(f"{self.service} runner call identity changed")
            value["inflight"] = None
            if not created:
                value["creation_intent"] = False
            if receipt is not None:
                value[self.receipt_key] = receipt

        self._settle(settle, strict)

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

    def listing(self, deadline):
        """The resource's reads for one pass of the supervisor's oracle.

        The run's state is read here, before any of them, so a failure reading
        it is never taken for a failure of a service call; the returned
        client makes only those reads, bounded by `deadline`.
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
        resource's admission is all the release needs, and the supervisor
        still has the exercise to run.
        """
        self._role("runner")

        def release(state):
            if state["inflight"] is not None:
                raise Failure(f"{self.service} runner call is in flight or unresolved")
            state.update(stopped=True, released=True)

        self._change(release)

    def released(self):
        """Read the runner's durable release, without transferring it."""
        state = self._read()
        return state["released"] is True and state["inflight"] is None

    def cleanup(self, quiesce, *, deadline):
        """Delete the intended resource once the runner and the workload are gone.

        Returns False while the deletion is not yet confirmed, for the caller's
        next pass. Only a recorded creation intent authorizes deletion, which
        also covers a create whose response was lost.
        """
        self._role("supervisor")
        self.stop()
        if not self.released():
            raise Failure(f"{self.service} runner has not released its authority")
        if quiesce() is not True:
            raise Failure(f"{self.service} workload is not quiescent")
        state = self._read()
        if state["creation_intent"]:
            self.env.assert_owner()
            if not self.delete(deadline):
                return False
        self._change(lambda value: value.update(cleaned=True))
        return True

    def delete(self, deadline):
        """Delete the resource; whether its absence is confirmed."""
        raise NotImplementedError
