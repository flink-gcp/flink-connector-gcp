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

from ..common import Failure, digest, json_bytes
from ..model import Phase
from .access import wait_for_access
from .handoff import require_handoff_released
from .resources import ResourcePlan, Resources

MAX_CONTROL_BYTES = 256 * 1024
# Who records each identity's access: an actor probes itself, and the runner
# reads the workload's from the probe Pod it created.
ACCESS_RECORDERS = {
    "runner": "runner",
    "supervisor": "supervisor",
    "workload": "runner",
}


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

    The caller supplies authenticated runner/supervisor identity, exclusive
    resource control and a mandatory budget/deadline guard. Each logical control
    access is guarded as ``(control, GET|UPDATE, run_record_path)`` in addition to
    the resource helper callbacks. The caller must budget the lock/record reads,
    CAS retries, guard and credential IO behind these logical operations.
    Cleanup needs an external barrier covering all creators, writers and their
    in-flight service calls; a persisted stop flag is not such a barrier.
    """

    def __init__(self, env, http, application, before_operation):
        if not callable(before_operation):
            raise Failure("Pub/Sub lifecycle requires an operation guard")
        self.env, self.http, self.before_operation = env, http, before_operation
        approval = env.approval
        self.plan = (
            approval.pubsub_plan
            if getattr(approval, "version", None) == 5
            else ResourcePlan(approval.run_id, approval.nonce)
        )
        args = application.get("spec", {}).get("job", {}).get("args", [])
        if (
            approval.scenario != "pubsub-recovery"
            or digest(application) != approval.application_sha256
            or application.get("metadata", {}).get("name") != approval.run_id
            or application.get("metadata", {}).get("namespace") != "tier3-pubsub"
            or not isinstance(args, list)
            or [a for a in args if isinstance(a, str) and a.startswith("--run-id")]
            != [f"--run-id={approval.run_id}"]
        ):
            raise Failure("Pub/Sub resources differ from the approved application")
        self.intent = {
            "manifest": self.plan.manifest(),
            "application_sha256": approval.application_sha256,
        }

    def reserve(self, name, *, deadline=None):
        """Open the guard's reservation for one method, when it keeps them.

        Internal fixtures pass a plain callback, which reserves nothing; the
        production guard refuses any operation outside a reservation.
        """
        method = getattr(self.before_operation, "method", None)
        return nullcontext() if method is None else method(name, deadline=deadline)

    def _actor(self, *, runner=False):
        allowed = ("runner",) if runner else ("runner", "supervisor")
        if self.env.actor not in allowed:
            raise Failure(
                "Pub/Sub operation requires the submitting runner"
                if runner
                else "Pub/Sub operation requires a lifecycle actor"
            )

    def _access(self, method):
        self.before_operation("control", method, self.env.records.path)
        self.env.assert_owner()

    def _state(self, record):
        state = record.pubsub
        if (
            not isinstance(state, dict)
            or json_bytes(state.get("intent")) != json_bytes(self.intent)
            or state.get("stage")
            not in ("initialized", "preparing", "prepared", "cleaning", "cleaned")
            or type(state.get("creation_intent")) is not bool
        ):
            raise Failure("Missing or replaced Pub/Sub resource intent")
        return state

    def _read(self):
        self._access("GET")
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

    def _change(self, edit):
        self._access("UPDATE")

        def change(record):
            # The CAS adapter can retry the edit after another actor's write.
            self._access("UPDATE")
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
            self._state(record)
            return
        record.pubsub = {
            "intent": copy.deepcopy(self.intent),
            "stage": "initialized",
            "creation_intent": False,
            "resources": None,
            "policies": None,
        }

    def _resources(self, mode):
        def guard(phase, method, name):
            self._actor(runner=mode == "prepare")
            # The caller's guard runs last, after this control I/O, so its
            # deadline check is the one taken just before the operation.
            record, state = self._read()
            if mode in ("verify", "access"):
                self._open(record)
                if state["stage"] != "prepared":
                    raise Failure("Pub/Sub resources are not prepared")
            elif mode == "cleanup":
                if (
                    state["stage"] not in ("cleaning", "cleaned")
                    or not record.stop_requested
                ):
                    raise Failure("Pub/Sub cleanup has not stopped preparation")
            else:
                self._open(record)
                if state["stage"] != "preparing":
                    raise Failure("Pub/Sub preparation cannot be resumed")
                if method == "PUT" and name == self.plan.manifest_path:

                    def intend(record):
                        self._open(record)
                        state = self._state(record)
                        if state["stage"] != "preparing" or state["creation_intent"]:
                            raise Failure("Pub/Sub creation intent is already claimed")
                        state["creation_intent"] = True

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
        resources = self._resources("prepare")
        observed = resources.provision()
        self._remember("resources", observed)
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

        Either actor may ask, while admission is open; every read first
        rechecks that, so a stop during verification refuses the rest.
        Settings, ownership or policy drift since preparation refuses; nothing
        is written, so a refusal leaves the run to stop and clean. Explicit
        policies still do not prove effective access.
        """
        self._actor()
        return self._resources("verify").inspect_grants()

    def access(self, *, deadline):
        """Probe this actor's own effective access, then record what was seen.

        Either actor, while admission is open: a permission test answers for
        the identity that sends it, so each actor probes only itself; the
        runner records the workload's separately, from the probe Pod.
        """
        self._actor()
        summary = wait_for_access(
            self.env, self._resources("access"), self.env.actor, deadline=deadline
        )
        self.record_access(self.env.actor, summary)
        return summary

    def record_access(self, role, summary):
        """Keep one identity's access observations with the run's control."""
        if role not in ACCESS_RECORDERS or self.env.actor != ACCESS_RECORDERS[role]:
            raise Failure("Pub/Sub access for " + role + " is recorded by its prober")

        def record(record):
            self._open(record)
            state = self._state(record)
            if state["stage"] != "prepared":
                raise Failure("Pub/Sub resources are not prepared")
            state.setdefault("access", {})[role] = copy.deepcopy(summary)

        self._change(record)

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
            self._resources("cleanup").cleanup_or_confirm_absent()

        def cleaned(record):
            state = self._state(record)
            if not record.stop_requested or state["stage"] != "cleaning":
                raise Failure("Pub/Sub cleanup control changed")
            state["stage"] = "cleaned"

        self._change(cleaned)
        return True
