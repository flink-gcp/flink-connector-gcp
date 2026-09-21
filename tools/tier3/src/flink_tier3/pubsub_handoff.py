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
"""Process-owned Pub/Sub calls and explicit release before service cleanup."""

import copy
import re
import uuid

from .common import Failure, json_bytes
from .model import Phase
from .pubsub_messages import MAX_BATCH


def _token(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{32}", value)


def _marker(value, actor):
    return value is None or (
        isinstance(value, dict)
        and set(value) == {"id", "operation"}
        and _token(value["id"])
        and value["operation"]
        in (("prepare", "publish") if actor == "runner" else ("collect",))
    )


def _handoff(state):
    if "handoff" not in state:
        return None
    value = state["handoff"]
    actors = value.get("actors") if isinstance(value, dict) else None
    if (
        not isinstance(value, dict)
        or type(value.get("version")) is not int
        or value["version"] != 1
        or not isinstance(value.get("traffic_binding"), dict)
        or not isinstance(actors, dict)
        or set(actors) != {"runner", "supervisor"}
        or actors["runner"] is None
    ):
        raise Failure("Invalid Pub/Sub actor handoff")
    for role, actor in actors.items():
        if actor is None:
            continue
        if (
            not isinstance(actor, dict)
            or set(actor) != {"token", "released", "inflight", "fenced_call"}
            or not _token(actor["token"])
            or type(actor["released"]) is not bool
            or not _marker(actor["inflight"], role)
            or not _marker(actor["fenced_call"], role)
            or (actor["released"] and actor["inflight"] is not None)
            or (actor["fenced_call"] is not None and not actor["released"])
        ):
            raise Failure("Invalid Pub/Sub actor state")
    if (
        actors["supervisor"] is not None
        and actors["supervisor"]["token"] == actors["runner"]["token"]
    ):
        raise Failure("Pub/Sub actors require distinct process tokens")
    return value


def require_handoff_released(state):
    """A recorded actor protocol must be released before deletion or settlement."""
    value = _handoff(state)
    if value is not None and any(
        actor is not None and not actor["released"]
        for actor in value["actors"].values()
    ):
        raise Failure("Pub/Sub actors have not released their authority")


class _InvocationChanged(Failure):
    """A completion acknowledgement no longer names the active call."""


class PubSubHandoff:
    """Internal process ownership; tokens do not authenticate or fence processes.

    Each original process keeps its own token; never reconstruct another process
    from a recorded token or call the underlying helpers directly. A replacement
    supervisor can reclaim only through an external quiescence proof. Neither
    stop, timeout nor Pod disappearance supplies that proof. All control accesses
    still use the resource controller's caller-owned operation/deadline guard.
    """

    def __init__(self, traffic, *, actor_token):
        if not _token(actor_token):
            raise Failure(
                "Pub/Sub actor token must be 32 lowercase hexadecimal characters"
            )
        self.traffic, self.controller = traffic, traffic.controller
        self.env, self.token = traffic.env, actor_token

    @staticmethod
    def _new_actor(token):
        return {
            "token": token,
            "released": False,
            "inflight": None,
            "fenced_call": None,
        }

    def _role(self, role=None):
        if self.env.actor not in ("runner", "supervisor") or (
            role is not None and self.env.actor != role
        ):
            raise Failure(
                "Pub/Sub handoff requires the " + (role or "lifecycle") + " actor"
            )

    def _state(self, record):
        state = self.controller._state(record)
        value = _handoff(state)
        if value is None or json_bytes(value["traffic_binding"]) != json_bytes(
            self.traffic.binding
        ):
            raise Failure("Missing or replaced Pub/Sub handoff binding")
        return value

    def _owned(self, record):
        self._role()
        value = self._state(record)["actors"][self.env.actor]
        if value is None or value["token"] != self.token:
            raise Failure("Pub/Sub actor process identity changed")
        return value

    def initialize(self):
        """Bind the runner before resource creation or message admission."""
        self._role("runner")

        def initialize(record):
            # Publish resource intent and its runner authority in the same CAS.
            # A lost acknowledgement must never leave an unbound resource record.
            self.controller._initialize(record)
            state = self.controller._state(record)
            if (
                self.env.stopping
                or self.env.evidence_failed
                or self.env.clock() >= self.traffic.limits.admit_until
            ):
                raise Failure("Pub/Sub actor binding window expired or stopped")
            if "handoff" in state:
                if self._owned(record)["released"]:
                    raise Failure("Pub/Sub runner has released its authority")
                return
            if (
                state["stage"] != "initialized"
                or state["creation_intent"]
                or state.get("traffic") is not None
            ):
                raise Failure("Pub/Sub actor binding must precede preparation")
            state["handoff"] = {
                "version": 1,
                "traffic_binding": copy.deepcopy(self.traffic.binding),
                "actors": {"runner": self._new_actor(self.token), "supervisor": None},
            }

        self.controller._change(initialize)

    def join(self):
        """Claim the one supervisor process after preparation and before its calls."""
        self._role("supervisor")

        def join(record):
            state = self.controller._state(record)
            value = self._state(record)
            if (
                state["stage"] != "prepared"
                or record.phase not in (Phase.READY, Phase.RUNNING)
                or record.stop_requested
                or record.evidence_failed
                or self.env.stopping
                or self.env.evidence_failed
                or self.env.clock() >= self.traffic.limits.admit_until
            ):
                raise Failure("Pub/Sub supervisor admission has stopped")
            self.traffic._state(record)
            actor = value["actors"]["supervisor"]
            if actor is None:
                if self.token == value["actors"]["runner"]["token"]:
                    raise Failure("Pub/Sub actors require distinct process tokens")
                value["actors"]["supervisor"] = self._new_actor(self.token)
            elif self._owned(record)["released"]:
                raise Failure("Pub/Sub supervisor has released its authority")

        self.controller._change(join)

    def _call(self, operation, callback):
        marker = {"id": uuid.uuid4().hex, "operation": operation}

        def begin(record):
            actor = self._owned(record)
            if actor["released"] or actor["inflight"] is not None:
                raise Failure("Pub/Sub actor is released or has an unresolved call")
            if operation == "prepare":
                self.controller._open(record)
                if (
                    self.env.stopping
                    or self.env.evidence_failed
                    or self.env.clock() >= self.traffic.limits.admit_until
                ):
                    raise Failure("Pub/Sub preparation window expired or stopped")
            else:
                self.traffic._allow(record, self.env.actor, admit=True)
            actor["inflight"] = marker

        self.controller._change(begin)
        try:
            result = callback()
        except (Failure, ValueError, OSError):
            # Even a transport timeout can leave service work in flight. Keep
            # its marker; only an external barrier can reclaim that authority.
            self.env.stopping = True
            self.stop()
            raise

        def finish(record):
            actor = self._owned(record)
            if actor["inflight"] is None:
                return
            if actor["inflight"] != marker:
                raise _InvocationChanged(
                    "Pub/Sub call identity changed before completion"
                )
            actor["inflight"] = None

        # Retry the acknowledgement only, never the external operation. A fresh
        # call's invocation ID prevents a lost response clearing its marker.
        for attempt in range(3):
            try:
                self.controller._change(finish)
                return result
            except _InvocationChanged:
                raise
            except Failure:
                if attempt == 2:
                    raise

    def prepare(self):
        self._role("runner")

        def prepare():
            self.controller.prepare()
            self.traffic.initialize()

        return self._call("prepare", prepare)

    def publish(self, input_index, start, count):
        self._role("runner")
        return self._call(
            "publish", lambda: self.traffic.publish(input_index, start, count)
        )

    def collect(self, batch_id, *, max_messages=MAX_BATCH):
        self._role("supervisor")
        return self._call(
            "collect", lambda: self.traffic.collect(batch_id, max_messages=max_messages)
        )

    def stop(self):
        """Close admission without claiming that any process or request has exited."""
        self._role()

        def stop(record):
            self._state(record)
            record.stop_requested = True

        self.controller._change(stop)

    def release(self):
        """Cooperatively surrender only this process's resolved call authority."""
        self._role()
        self.stop()

        def release(record):
            # A supervisor stopped before join has no data authority to release.
            # The shared stop above prevents it from claiming that authority later.
            if self._state(record)["actors"][self.env.actor] is None:
                return
            actor = self._owned(record)
            if actor["inflight"] is not None:
                raise Failure("Pub/Sub actor call is in flight or unresolved")
            actor["released"] = True

        self.controller._change(release)

    def released(self):
        """Observe all bound releases; this is not an external quiescence proof."""
        self._role()
        record, _ = self.controller._read()
        return all(
            actor is None or actor["released"]
            for actor in self._state(record)["actors"].values()
        )

    def cleanup(self, quiesce):
        """Require cooperative actor releases and the external workload barrier."""
        self._role("supervisor")
        if not callable(quiesce):
            raise Failure("Pub/Sub cleanup requires an external quiescence barrier")

        def barrier():
            record, state = self.controller._read()
            self._state(record)
            require_handoff_released(state)
            return quiesce()

        return self.controller.cleanup(barrier)

    def reclaim(self, quiesce):
        """Reclaim abandoned actors only after an external proof for their snapshot.

        The callback receives a copy of the actor binding and invocation markers.
        It must prove every bound process, creator, workload writer and in-flight
        service operation quiescent, and keep them fenced through settlement.
        A True return is caller-supplied proof, not a measurement by this helper.
        """
        self._role("supervisor")
        if not callable(quiesce):
            raise Failure("Pub/Sub reclamation requires an external quiescence barrier")

        def barrier():
            record, _ = self.controller._read()
            snapshot = copy.deepcopy(self._state(record))
            if quiesce(copy.deepcopy(snapshot)) is not True:
                raise Failure("Pub/Sub actor quiescence is unproven")

            def fence(record):
                value = self._state(record)
                if not record.stop_requested or json_bytes(value) != json_bytes(
                    snapshot
                ):
                    raise Failure(
                        "Pub/Sub actor control changed during quiescence proof"
                    )
                for actor in value["actors"].values():
                    if actor is not None:
                        if actor["inflight"] is not None:
                            actor["fenced_call"] = actor["inflight"]
                        actor["inflight"] = None
                        actor["released"] = True

            self.controller._change(fence)
            return True

        return self.controller.cleanup(barrier)
