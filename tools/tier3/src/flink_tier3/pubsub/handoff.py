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
import math
import re
import uuid

from ..common import Failure, json_bytes
from ..model import Phase
from ..policy import HTTP_TIMEOUT
from .messages import COHORTS, MAX_BATCH, PUBLISH_BATCH, Messages, cohort_ranges


def _token(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{32}", value)


def _time(value):
    return type(value) in (int, float) and math.isfinite(value)


def require_handoff_released(state):
    """A recorded actor protocol must be released before deletion or settlement."""
    value = state.get("handoff")
    if value is not None and any(
        actor is not None and not actor["released"]
        for actor in value["actors"].values()
    ):
        raise Failure("Pub/Sub actors have not released their authority")


def runner_released(actors):
    """Whether the runner bound and has since released its authority."""
    return actors["runner"] is not None and actors["runner"]["released"]


class _InvocationChanged(Failure):
    """A completion acknowledgement no longer names the active call."""


class CohortUnstarted(Failure):
    """The run closed before a cohort's start was recorded; nothing was sent."""


class PubSubHandoff:
    """Internal process ownership; tokens do not authenticate or fence processes.

    Each original process keeps its own token; never reconstruct another process
    from a recorded token or call the underlying helpers directly. A replacement
    supervisor can reclaim only through an external quiescence proof. Neither
    stop, timeout nor Pod disappearance supplies that proof. Every service
    request still passes the resource controller's caller-owned deadline guard.

    A publication or a pull updates the control record twice on its normal
    path: its begin marker, which also checks that message traffic is still
    admitted, and its completion. Cloud Storage takes about one mutation a
    second on one object, and both actors write the same record.
    """

    def __init__(self, controller, *, actor_token, settled=None):
        if not _token(actor_token):
            raise Failure(
                "Pub/Sub actor token must be 32 lowercase hexadecimal characters"
            )
        if settled is not None and not callable(settled):
            raise Failure("Pub/Sub settled-write check must be callable")
        self.controller, self.env = controller, controller.env
        self.token = actor_token
        self.records_per_subscription = self.env.approval.pubsub_trial[
            "records_per_subscription"
        ]
        # Whether every service write this actor sent has a definite outcome.
        # Without it, every failed call keeps its marker.
        self.settled = settled

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

    def _actors(self, record):
        value = self.controller._state(record).get("handoff")
        if value is None:
            raise Failure("Pub/Sub actors are not bound")
        return value["actors"]

    def _owned(self, record):
        self._role()
        value = self._actors(record)[self.env.actor]
        if value is None or value["token"] != self.token:
            raise Failure("Pub/Sub actor process identity changed")
        return value

    def _closed(self):
        return (
            self.env.stopping
            or self.env.evidence_failed
            or self.env.clock() >= self.env.schedule.cleanup_at
        )

    def _admitted(self, record):
        """Refuse new message traffic once this actor's admission has closed.

        The runner publishes the first cohort while admission is still
        READY, after every access probe and before the application exists,
        so the run becomes RUNNING with its input already waiting. The
        supervisor collects only from a RUNNING application.
        """
        admitting = (
            (Phase.READY, Phase.RUNNING)
            if self.env.actor == "runner"
            else (Phase.RUNNING,)
        )
        if (
            self.controller._state(record)["stage"] != "prepared"
            or record.phase not in admitting
            or record.stop_requested
            or record.evidence_failed
            or self._closed()
        ):
            raise Failure("Pub/Sub traffic admission has stopped")

    def initialize(self):
        """Bind the runner before resource creation or message admission."""
        self._role("runner")

        def initialize(record):
            # Publish resource intent and its runner authority in the same CAS.
            # A lost acknowledgement must never leave an unbound resource record.
            self.controller._initialize(record)
            state = self.controller._state(record)
            if self._closed():
                raise Failure("Pub/Sub actor binding window expired or stopped")
            if "handoff" in state:
                if self._owned(record)["released"]:
                    raise Failure("Pub/Sub runner has released its authority")
                return
            if state["stage"] != "initialized" or state["creation_intent"]:
                raise Failure("Pub/Sub actor binding must precede preparation")
            state["handoff"] = {
                "actors": {"runner": self._new_actor(self.token), "supervisor": None},
            }

        self.controller._change(initialize)

    def join(self):
        """Claim the one supervisor process after preparation and before its calls."""
        self._role("supervisor")

        def join(record):
            state = self.controller._state(record)
            actors = self._actors(record)
            if (
                state["stage"] != "prepared"
                or record.phase not in (Phase.READY, Phase.RUNNING)
                or record.stop_requested
                or record.evidence_failed
                or self._closed()
            ):
                raise Failure("Pub/Sub supervisor admission has stopped")
            if actors["supervisor"] is None:
                actors["supervisor"] = self._new_actor(self.token)
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
                if self._closed():
                    raise Failure("Pub/Sub preparation window expired or stopped")
            else:
                self._admitted(record)
            actor["inflight"] = marker

        try:
            self.controller._change(begin)
        except (Failure, ValueError, OSError):
            self._abandon(marker)
            raise
        try:
            result = callback()
        except (Failure, ValueError, OSError):
            # Even a transport timeout can leave service work in flight, so
            # the marker stays unless the session saw every write answered;
            # only an external barrier can reclaim an unsettled call.
            self.env.stopping = True
            self.stop()
            if self.settled is not None and self.settled() is True:
                try:
                    self._finish(marker)
                except Failure:
                    pass  # the marker stays, which only keeps the lock
            raise
        self._finish(marker)
        return result

    def _abandon(self, marker):
        """Clear the marker of a call whose begin failed, if it landed anyway.

        A begin write can land with its response lost. Nothing was sent, so
        the marker names no request in flight, and keeping it would hold the
        lock for a reclamation proof the call does not need. A refusal
        recorded nothing, and this read finds no marker of its own.
        """
        try:
            _, state = self.controller._read()
            actor = (state.get("handoff") or {}).get("actors", {}).get(self.env.actor)
            if actor is not None and actor["inflight"] == marker:
                self._finish(marker)
        except (Failure, ValueError, OSError):
            pass  # the marker stays, which only keeps the lock

    def _finish(self, marker):
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
                return
            except _InvocationChanged:
                raise
            except Failure:
                if attempt == 2:
                    raise

    def _messages(self):
        """The message helper, marking the run when an evidence upload fails."""
        handoff = self

        class EvidenceStore:
            def write(self, name, value, generation):
                try:
                    return handoff.env.store.write(name, value, generation=generation)
                except (Failure, ValueError, OSError):
                    handoff._evidence_failed()
                    raise

        return Messages(
            self.controller.http,
            EvidenceStore(),
            self.controller.plan,
            actor=self.env.actor,
            records_per_subscription=self.records_per_subscription,
            before_operation=self.controller.before_operation,
        )

    def _evidence_failed(self):
        self.env.evidence_failed = True

        def failed(record):
            record.evidence_failed = True

        try:
            self.controller._change(failed)
        except (Failure, ValueError, OSError) as error:
            raise Failure(
                "Pub/Sub evidence failure could not be recorded; "
                "stop actors independently before restarting"
            ) from error

    def prepare(self):
        self._role("runner")
        return self._call("prepare", self.controller.prepare)

    def verify(self):
        """Re-read the prepared resources and policies; see the controller."""
        self._role()
        return self.controller.verify_prepared()

    def access(self, *, deadline):
        """Probe this bound actor's own effective access before admission ends.

        A released actor has stopped the run, which the controller refuses.
        """
        self._role()
        return self.controller.access(deadline=deadline)

    def publish(self, input_index, start, count, *, deadline=None):
        """Publish one batch, held to ``deadline`` or else to ``cleanup_at``."""
        self._role("runner")
        until = self.env.schedule.cleanup_at if deadline is None else deadline
        with self.controller.until(until):
            return self._call(
                "publish", lambda: self._messages().publish(input_index, start, count)
            )

    def collect(self, batch_id, *, max_messages=MAX_BATCH):
        self._role("supervisor")
        with self.controller.until(self.env.schedule.cleanup_at):
            return self._call(
                "collect",
                lambda: self._messages().collect(batch_id, max_messages=max_messages),
            )

    def request_cohort(self, name, *, deadline):
        """Ask the runner to publish a later cohort, once the one before it is out.

        Only the runner may publish input. Repeating a request with the same
        deadline changes nothing, even after that deadline or a stop, so a
        retry after a lost response confirms the recorded request; another
        deadline is refused.
        """
        self._role("supervisor")
        if name not in COHORTS[1:]:
            raise ValueError("Unknown requestable Pub/Sub cohort: " + str(name))
        if not _time(deadline):
            raise ValueError("Pub/Sub cohort deadline must be a finite time")

        def request(record):
            state = self.controller._state(record)
            cohorts = state.get("cohorts", {})
            if name in cohorts:
                if cohorts[name]["deadline"] != deadline:
                    raise Failure("Pub/Sub cohort deadline cannot be changed")
                return
            if not self.env.clock() < deadline <= self.env.schedule.cleanup_at:
                raise Failure("Pub/Sub cohort deadline is outside the run window")
            if self._owned(record)["released"]:
                raise Failure("Pub/Sub supervisor has released its authority")
            self._admitted(record)
            previous = cohorts.get(COHORTS[COHORTS.index(name) - 1])
            if previous is None or previous["published_at"] is None:
                raise Failure("Pub/Sub cohort requested before the previous one")
            state["cohorts"] = {
                **cohorts,
                name: {
                    "deadline": deadline,
                    "requested_at": self.env.clock(),
                    "started_at": None,
                    "published_at": None,
                },
            }

        self.controller._change(request)

    def cohorts(self):
        """The recorded cohort publications."""
        self._role()
        _, state = self.controller._read()
        return copy.deepcopy(state.get("cohorts", {}))

    def publish_cohort(self, name, *, deadline=None, check=None):
        """Publish one cohort to both input topics, once, before its deadline.

        Admission publishes the first cohort with its own ``deadline``; a later
        one is published only as requested, by its recorded deadline. The
        start is recorded before the first request, so it precedes every
        message of the cohort, and only while a request still fits before the
        deadline. ``check`` runs before each batch. A cohort that started is
        never started again: a failed batch stops the run.
        """
        self._role("runner")
        if name not in COHORTS:
            raise ValueError("Unknown Pub/Sub cohort: " + str(name))
        if (deadline is None) != (name != COHORTS[0]) or not (
            deadline is None or _time(deadline)
        ):
            raise ValueError("Only the first Pub/Sub cohort takes a deadline")
        cohort = cohort_ranges(self.records_per_subscription)[name]
        until = deadline

        def begin(record):
            nonlocal until
            # A stop can land between the caller's last read and this one;
            # refusing here sends nothing, which a failure would misreport.
            if self._owned(record)["released"]:
                raise CohortUnstarted("Pub/Sub runner has released its authority")
            try:
                self._admitted(record)
            except Failure as error:
                raise CohortUnstarted(str(error)) from error
            state = self.controller._state(record)
            cohorts = state.get("cohorts", {})
            current = cohorts.get(name)
            if name == COHORTS[0]:
                if current is not None:
                    raise Failure("Pub/Sub cohort has already started")
                current = {"deadline": deadline, "requested_at": None}
            elif current is None or current["started_at"] is not None:
                raise Failure("Pub/Sub cohort was not requested or already started")
            until = current["deadline"]
            now = self.env.clock()
            # The guard refuses a request that would start within its budget
            # of the deadline; a start recorded then could never publish.
            if now + HTTP_TIMEOUT > until:
                raise Failure("Pub/Sub cohort deadline expired before publication")
            state["cohorts"] = {
                **cohorts,
                name: {**current, "started_at": now, "published_at": None},
            }

        def finish(record):
            current = self.controller._state(record).get("cohorts", {}).get(name)
            if current is None or current["started_at"] is None:
                raise Failure("Pub/Sub cohort publication was not started")
            if current["published_at"] is None:
                current["published_at"] = self.env.clock()

        self.controller._change(begin)
        end = cohort["start"] + cohort["count"]
        for index in range(2):
            for start in range(cohort["start"], end, PUBLISH_BATCH):
                if check is not None:
                    check()
                self.publish(
                    index, start, min(PUBLISH_BATCH, end - start), deadline=until
                )
        self.controller._change(finish)

    def serve(self):
        """Publish a cohort the supervisor requested; return its name, or None."""
        self._role("runner")
        cohorts = self.cohorts()
        for name in COHORTS[1:]:
            cohort = cohorts.get(name)
            if cohort is not None and cohort["started_at"] is None:
                self.publish_cohort(name)
                return name
        return None

    def stop(self):
        """Close admission without claiming that any process or request has exited."""
        self._role()

        def stop(record):
            record.stop_requested = True

        self.controller._change(stop)

    def release(self):
        """Cooperatively surrender only this process's resolved call authority."""
        self._role()

        def release(record):
            # A supervisor stopped before join has no data authority to release.
            # The shared stop above prevents it from claiming that authority later.
            if self._actors(record)[self.env.actor] is None:
                return
            actor = self._owned(record)
            if actor["inflight"] is not None:
                raise Failure("Pub/Sub actor call is in flight or unresolved")
            actor["released"] = True

        self.stop()
        self.controller._change(release)

    def released(self):
        """Observe all bound releases; this is not an external quiescence proof."""
        self._role()
        record, _ = self.controller._read()
        return all(
            actor is None or actor["released"]
            for actor in self._actors(record).values()
        )

    def actors(self):
        """The recorded actor bindings."""
        self._role()
        record, _ = self.controller._read()
        return copy.deepcopy(self._actors(record))

    def supervisor_binding(self, actors=None):
        """Who holds the supervisor binding: nobody, this process, or another."""
        bound = (actors or self.actors())["supervisor"]
        if bound is None:
            return "none"
        return "mine" if bound["token"] == self.token else "other"

    def cleanup(self, quiesce):
        """Require cooperative actor releases and the external workload barrier."""
        self._role("supervisor")
        if not callable(quiesce):
            raise Failure("Pub/Sub cleanup requires an external quiescence barrier")
        return self.controller.cleanup(quiesce)

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
            _, state = self.controller._read()
            snapshot = copy.deepcopy(state["handoff"])
            if quiesce(copy.deepcopy(snapshot)) is not True:
                raise Failure("Pub/Sub actor quiescence is unproven")

            def fence(record):
                value = self.controller._state(record)["handoff"]
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
