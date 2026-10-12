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
"""Durable Pub/Sub preparation and cleanup for a future admitted scenario."""

import copy
from contextlib import nullcontext

from ..common import Failure, json_bytes
from ..model import Phase
from .access import wait_for_access
from .handoff import require_handoff_released
from .resources import ResourcePlan, Resources

MAX_CONTROL_BYTES = 256 * 1024


def require_pubsub_clean(record):
    """Keep shared settlement closed while recorded service cleanup is pending."""
    if record.pubsub is not None and (
        not isinstance(record.pubsub, dict)
        or record.pubsub.get("stage") != "cleaned"
        or not record.stop_requested
    ):
        raise Failure("Pub/Sub resource cleanup is incomplete")
    if record.pubsub is not None:
        require_handoff_released(record.pubsub)


class PubSubLifecycle:
    """Internal controller; not a runnable approval or a numeric budget policy.

    The caller supplies the runner/supervisor role, exclusive
    resource control and a mandatory deadline guard, which sees every
    resource request. Only this rig's own code writes the record, its two
    actors and the lifecycle's recovery, so it is read as written rather than
    re-validated. Cleanup
    needs an external barrier covering all creators, writers and their
    in-flight service calls; a persisted stop flag is not such a barrier.
    """

    def __init__(self, env, http, before_operation):
        if not callable(before_operation):
            raise Failure("Pub/Sub lifecycle requires an operation guard")
        self.env, self.http, self.before_operation = env, http, before_operation
        self.plan = ResourcePlan(env.approval.run_id, env.approval.nonce)
        # Written once at initialization as evidence for the receipt; nothing
        # compares it again, because only this rig's own code writes the record.
        self.intent = {
            "manifest": self.plan.manifest(),
            "application_sha256": env.approval.application_sha256,
        }

    def until(self, deadline):
        """Hold service requests to ``deadline`` when the guard keeps one.

        Internal fixtures pass a plain callback, which keeps no deadline.
        """
        until = getattr(self.before_operation, "until", None)
        return nullcontext() if until is None else until(deadline)

    def _actor(self, *, runner=False):
        allowed = ("runner",) if runner else ("runner", "supervisor")
        if self.env.actor not in allowed:
            raise Failure(
                "Pub/Sub operation requires the submitting runner"
                if runner
                else "Pub/Sub operation requires a lifecycle actor"
            )

    @staticmethod
    def _state(record):
        if not isinstance(record.pubsub, dict):
            raise Failure("Pub/Sub resources are not initialized")
        return record.pubsub

    def _read(self):
        record = self.env.refresh()
        return record, self._state(record)

    @staticmethod
    def _open(record):
        if (
            record.stop_requested
            or record.evidence_failed
            or record.phase not in (Phase.APPROVED, Phase.READY)
        ):
            raise Failure("Pub/Sub preparation admission has stopped")

    def _prepared(self):
        """Refuse unless admission is open on prepared resources."""
        record, state = self._read()
        self._open(record)
        if state["stage"] != "prepared":
            raise Failure("Pub/Sub resources are not prepared")

    def _change(self, edit):
        def change(record):
            edit(record)
            if len(json_bytes(record.pubsub)) > MAX_CONTROL_BYTES:
                raise Failure("Pub/Sub control record exceeds 256 KiB")

        return self.env.records._change(change)

    def initialize(self):
        """Bind active run control to the approved application before service IO."""
        self._actor(runner=True)

        self._change(self._initialize)

    def _initialize(self, record):
        """Initialize within the caller's control update, including actor binding."""
        self._open(record)
        if record.pubsub is not None:
            return
        record.pubsub = {
            "intent": copy.deepcopy(self.intent),
            "stage": "initialized",
            "creation_intent": False,
            "resources": None,
            "policies": None,
        }

    def _resources(self):
        return Resources(self.http, self.env.store, self.plan, self.before_operation)

    def _preparing(self):
        """Resources whose creation intent is recorded before their manifest."""

        def guard(phase, method, name):
            if method == "PUT" and name == self.plan.manifest_path:

                def intend(record):
                    self._open(record)
                    state = self._state(record)
                    if state["stage"] != "preparing" or state["creation_intent"]:
                        raise Failure("Pub/Sub creation intent is already claimed")
                    state["creation_intent"] = True

                # Before the manifest that names the resources, so cleanup
                # knows that service writes may exist.
                self._change(intend)
            self.before_operation(phase, method, name)

        return Resources(self.http, self.env.store, self.plan, guard)

    def prepare(self):
        """Claim one preparation attempt, retaining partial work for cleanup only."""
        self._actor(runner=True)

        def claim(record):
            self._open(record)
            state = self._state(record)
            if state["stage"] != "initialized":
                raise Failure("Pub/Sub preparation cannot be resumed")
            state["stage"] = "preparing"

        self._change(claim)
        resources = self._preparing()
        observed = resources.provision()
        self._remember("resources", observed)
        # A stop during creation keeps the grants from being installed.
        self._open(self.env.refresh())
        policies = resources.install_grants()
        self._remember("policies", policies)

        def prepared(record):
            self._open(record)
            state = self._state(record)
            if state["stage"] != "preparing":
                raise Failure("Pub/Sub preparation cannot be resumed")
            state["stage"] = "prepared"

        self._change(prepared)

    def verify_prepared(self):
        """Re-read the prepared resources and policies before admitting work.

        Either actor may ask, while admission is open. Settings, ownership or
        policy drift since preparation refuses; nothing is written, so a
        refusal leaves the run to stop and clean. Explicit policies still do
        not prove effective access.
        """
        self._actor()
        self._prepared()
        return self._resources().inspect_grants()

    def access(self, *, deadline):
        """Probe this actor's own effective access, then record what was seen.

        Either actor, while admission is open, which is checked before each
        round of tests and each pull: a permission test answers for the
        identity that sends it, so each actor probes only itself; the
        workload's is probed by a Pod the runner creates.
        """
        self._actor()
        with self.until(deadline):
            summary = wait_for_access(
                self.env,
                self._resources(),
                self.env.actor,
                deadline=deadline,
                check=self._prepared,
            )

        def record(record):
            self._open(record)
            state = self._state(record)
            if state["stage"] != "prepared":
                raise Failure("Pub/Sub resources are not prepared")
            state.setdefault("access", {})[self.env.actor] = copy.deepcopy(summary)

        self._change(record)
        return summary

    def _remember(self, key, value):
        def remember(record):
            state = self._state(record)
            # Preserve successful readback even if stop arrived after its last IO.
            state[key] = copy.deepcopy(value)

        self._change(remember)

    def cleanup(self, quiesce):
        """Stop admission, prove writer quiescence, then remove owned resources.

        A failed barrier or uncertain manifest/deletion retains active control.
        Before creation intent, there is no authorized service write to reclaim.
        After that intent, a missing manifest permits only an absence check;
        any remaining resource refuses cleanup without authorizing deletion.
        Repeated cleanup rechecks service absence without restarting preparation.
        Cleanup holds its requests to no deadline, because it must finish even
        after the admission deadline has passed and the approval has expired.
        """
        self._actor()
        if not callable(quiesce):
            raise Failure("Pub/Sub cleanup requires a quiescence barrier")

        def stop(record):
            state = self._state(record)
            record.stop_requested = True
            state["stage"] = "cleaning"

        self._change(stop)
        if quiesce() is not True:
            raise Failure("Pub/Sub creators and writers are not quiescent")
        _, state = self._read()
        require_handoff_released(state)
        if state["creation_intent"]:
            with self.until(None):
                self._resources().cleanup_or_confirm_absent()

        def cleaned(record):
            state = self._state(record)
            if not record.stop_requested or state["stage"] != "cleaning":
                raise Failure("Pub/Sub cleanup control changed")
            state["stage"] = "cleaned"

        self._change(cleaned)
        return True
