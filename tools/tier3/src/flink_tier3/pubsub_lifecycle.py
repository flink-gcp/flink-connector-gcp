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

from .common import Failure, digest, json_bytes
from .model import Phase
from .pubsub import ResourcePlan, Resources

MAX_CONTROL_BYTES = 256 * 1024


def require_pubsub_clean(record):
    """Keep shared settlement closed while recorded service cleanup is pending."""
    if record.pubsub is not None and (
        not isinstance(record.pubsub, dict)
        or record.pubsub.get("stage") != "cleaned"
        or not record.stop_requested
    ):
        raise Failure("Pub/Sub resource cleanup is incomplete")


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
        self.plan = ResourcePlan(approval.run_id, approval.nonce)
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

        def initialize(record):
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

        self._change(initialize)

    def _resources(self, *, cleanup=False):
        def guard(phase, method, name):
            self._actor(runner=not cleanup)
            self.before_operation(phase, method, name)
            record, state = self._read()
            if cleanup:
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
        resources = self._resources()
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
        if state["creation_intent"]:
            self._resources(cleanup=True).cleanup_or_confirm_absent()

        def cleaned(record):
            state = self._state(record)
            if not record.stop_requested or state["stage"] != "cleaning":
                raise Failure("Pub/Sub cleanup control changed")
            state["stage"] = "cleaned"

        self._change(cleaned)
        return True
