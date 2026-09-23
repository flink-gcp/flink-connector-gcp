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
"""Durable query requests and runner release before BigQuery resource cleanup."""

from __future__ import annotations

import math
import re
import uuid

from .common import Failure, digest, json_bytes
from .policy import BIGQUERY_OBSERVATIONS


def require_bigquery_clean(record):
    """Retain shared resources and the lock until service cleanup is recorded."""
    state = record.bigquery
    if state is None:
        return
    handoff = state.get("handoff") if isinstance(state, dict) else None
    if (
        not record.stop_requested
        or not isinstance(state, dict)
        or state.get("stopped") is not True
        or state.get("cleaned") is not True
        or not isinstance(handoff, dict)
        or handoff.get("released") is not True
        or "inflight" not in handoff
        or handoff["inflight"] is not None
    ):
        raise Failure("BigQuery resource cleanup or runner release is incomplete")


class _CallIdentityChanged(Failure):
    """A completion acknowledgement encountered another invocation."""


_TOKEN = re.compile(r"[0-9a-f]{32}")


class BigQueryHandoff:
    """Internal actor protocol; callers still own authentication and workload fencing.

    One submitting process owns the runner token. Never transfer it to another
    process or call the resource controller directly after initializing this
    protocol.

    The supervisor is constructed without it. Its Pod starts before the runner
    writes the binding, and no delivery channel carries the token to it: the
    recovery workflow builds its actors from the approval and reads the
    ConfigMap, so a token in either would let it match the binding without
    meaning to, and act as the runner whose release lets cleanup delete tables.
    The token guards against that accidental match, not a deliberate one: the
    binding sits in the control record, readable by anything that can read the
    evidence bucket. The supervisor binds the fields its own approval fixes,
    and adopts the token the first time the binding is read.
    That is exactly the submitting runner's token, because `initialize` writes
    the binding once and refuses a different one thereafter; from then on a
    replaced binding is refused like any other.
    """

    def __init__(self, controller, *, runner_token, evidence_bytes, query_until):
        if runner_token is None:
            if controller.env.actor != "supervisor":
                raise ValueError("Only the supervisor binds without a runner token")
        elif not isinstance(runner_token, str) or not _TOKEN.fullmatch(runner_token):
            raise ValueError("Runner token must be 32 lowercase hexadecimal characters")
        if type(evidence_bytes) is not int or not 0 < evidence_bytes <= 100 * 1024**2:
            raise ValueError(
                "Explicit query evidence budget must be 1 byte through 100 MiB"
            )
        if (
            type(query_until) not in (int, float)
            or not math.isfinite(query_until)
            or not 0 < query_until <= controller.plan.expires_ms / 1000
        ):
            raise ValueError("Query deadline must be finite and within table expiry")
        self.controller, self.env = controller, controller.env
        self.binding = {
            "version": 1,
            "runner_token": runner_token,
            "evidence_bytes": evidence_bytes,
            "query_until": query_until,
        }
        self.per_query = evidence_bytes // controller.plan.query_slots
        if not self.per_query:
            raise ValueError("Evidence budget must reserve bytes for every query slot")

    def _role(self, role):
        if self.env.actor != role:
            raise Failure("BigQuery handoff requires the " + role + " actor")

    def _state(self, state):
        value = state.get("handoff")
        binding = value.get("binding") if isinstance(value, dict) else None
        if not isinstance(binding, dict):
            raise Failure("Missing or replaced BigQuery actor binding")
        if self.binding["runner_token"] is None:
            # Adopted, never inferred: the fields this actor's approval fixes
            # must match first, so a binding for another budget or window is
            # refused rather than learned.
            token = binding.get("runner_token")
            if {**binding, "runner_token": None} != self.binding or not (
                isinstance(token, str) and _TOKEN.fullmatch(token)
            ):
                raise Failure("Missing or replaced BigQuery actor binding")
            self.binding["runner_token"] = token
        if binding != self.binding:
            raise Failure("Missing or replaced BigQuery actor binding")
        return value

    def _read(self):
        state = self.controller._read()
        return state, self._state(state)

    def _change(self, edit, *, open_only=False):
        def change(state):
            value = self._state(state)
            if open_only and value["released"]:
                raise Failure("BigQuery runner has released its authority")
            edit(value)

        return self._state(self.controller._change(change, open_only=open_only))

    def initialize(self):
        """Claim an empty resource intent once, before provisioning any table."""
        self._role("runner")
        self.env.admission_open()
        if self.env.clock() >= self.binding["query_until"]:
            raise Failure("BigQuery query window expired")
        handoff = {
            "binding": dict(self.binding),
            "released": False,
            "inflight": None,
            "requests": {},
        }
        # One record change, so release always has a binding to clear. The
        # check below meets an intent an earlier call wrote, and never adopts
        # one written without a binding.
        self.controller.initialize(handoff)

        def initialize(state):
            if state.get("handoff") is None:
                raise Failure("BigQuery resource intent predates its actor binding")
            value = self._state(state)
            if value["released"]:
                raise Failure("BigQuery runner has released its authority")

        self.controller._change(initialize, open_only=True)

    def _call(self, operation, callback, *, deadline, creates=False):
        """Serialize calls through CAS; retain ambiguous creation outcomes."""
        self._role("runner")

        marker = {"operation": operation, "id": uuid.uuid4().hex}

        def begin(value):
            if self.env.clock() >= deadline:
                raise Failure("BigQuery observation deadline expired")
            if value["inflight"] is not None:
                raise Failure("BigQuery runner call is in flight or unresolved")
            value["inflight"] = marker

        self._change(begin, open_only=True)
        completed = False
        try:
            result = callback(self.controller.with_deadline(deadline))
            completed = True
            return result
        finally:
            # A failed creating call may still execute at the service. Its
            # marker survives this process and cannot be cleared by release.
            if completed or not creates:

                def finish(value):
                    if value["inflight"] is None:
                        return
                    if value["inflight"] != marker:
                        raise _CallIdentityChanged(
                            "BigQuery runner call identity changed"
                        )
                    value["inflight"] = None

                # Retry only the conditional acknowledgement, never the API
                # operation. An invocation ID prevents a lost response from
                # clearing a later call with the same operation name.
                for attempt in range(3):
                    try:
                        self._change(finish)
                        break
                    except _CallIdentityChanged:
                        raise
                    except Failure:
                        if attempt == 2:
                            raise

    def provision(self):
        """Provision through the submitting runner, retaining a failed-call marker."""
        return self._call(
            "provision",
            lambda controller: controller.provision(),
            creates=True,
            deadline=min(
                self.env.schedule.started + BIGQUERY_OBSERVATIONS["startup_seconds"],
                self.binding["query_until"],
            ),
        )

    def request(self, name, *, deadline):
        """Request one serial observation; another snapshot requires a new name."""
        self._role("supervisor")
        if not isinstance(name, str) or not re.fullmatch(
            r"[a-z0-9][a-z0-9-]{0,39}", name
        ):
            raise ValueError("Observation name must be a short lowercase label")
        if (
            type(deadline) not in (int, float)
            or not math.isfinite(deadline)
            or not self.env.clock() < deadline <= self.binding["query_until"]
        ):
            raise ValueError("Observation deadline is outside the query window")
        self.env.require_running("BigQuery observation admission has stopped")

        def request(state):
            if self.env.clock() >= deadline:
                raise Failure("BigQuery observation deadline expired")
            value = self._state(state)
            if value["released"]:
                raise Failure("BigQuery runner has released its authority")
            previous = value["requests"].get(name)
            if previous is not None:
                if previous != {"deadline": deadline}:
                    raise Failure("Observation deadline cannot be changed")
                return
            if len(value["requests"]) >= self.controller.plan.query_slots:
                raise Failure("BigQuery observation budget exhausted")
            if any(
                state["queries"].get(key, {}).get("stage") != "collected"
                for key in value["requests"]
            ):
                raise Failure("A BigQuery observation is still pending")
            value["requests"][name] = {"deadline": deadline}

        self.controller._change(request, open_only=True)

    def poll(self):
        """Service one requested job; return its name when evidence is durable."""
        self._role("runner")
        self.env.require_running("BigQuery query processing has stopped")
        state, value = self._read()
        if value["released"] or value["inflight"] is not None:
            raise Failure("BigQuery runner is released or has an unresolved call")
        for name, request in value["requests"].items():
            saved = state["queries"].get(name)
            if saved and saved["stage"] == "collected":
                continue
            if self.env.clock() >= request["deadline"]:
                raise Failure("BigQuery observation deadline expired")
            if saved is None:
                self.controller.reserve_query(name)
                state, _ = self._read()
                saved = state["queries"][name]
            if saved["stage"] in ("reserved", "submitting"):
                self._call(
                    "submit:" + name,
                    lambda controller, name=name: controller.submit_query(name),
                    creates=True,
                    deadline=request["deadline"],
                )
            job = self._call(
                "status:" + name,
                lambda controller, slot=saved["slot"]: controller.api.job(slot),
                deadline=request["deadline"],
            )
            if job is None:
                raise Failure("Submitted BigQuery query is not readable")
            if job["status"]["state"] != "DONE":
                return None
            if self.env.clock() >= request["deadline"]:
                raise Failure("BigQuery observation deadline expired")
            self._call(
                "collect:" + name,
                lambda controller, name=name: controller.collect_query(
                    name, max_bytes=self.per_query
                ),
                deadline=request["deadline"],
            )
            return name
        return None

    def result(self, name):
        """Read bound GCS evidence; the supervisor never reads private query results."""
        self._role("supervisor")
        state, value = self._read()
        if name not in value["requests"]:
            raise Failure("BigQuery observation was not requested")
        saved = state["queries"].get(name)
        if not saved or saved["stage"] != "collected":
            return None
        pointer = saved["evidence"]
        artifact, generation = self.env.store.read(
            self.controller.prefix + f"queries/{saved['slot']}.json"
        )
        if (
            not isinstance(artifact, dict)
            or not isinstance(pointer, dict)
            or generation != pointer.get("generation")
            or digest(artifact) != pointer.get("sha256")
            or artifact.get("intent_sha256") != digest(self.controller.intent)
            or artifact.get("observation") != name
            or artifact.get("slot") != saved["slot"]
            or "result" not in artifact
            or len(json_bytes(artifact)) > self.per_query
        ):
            raise Failure(
                "BigQuery observation evidence is missing, replaced or oversized"
            )
        return artifact["result"]

    def stop(self):
        """Close new requests and calls; this does not acknowledge runner exit."""
        if self.env.actor not in ("runner", "supervisor"):
            raise Failure("Unexpected BigQuery lifecycle actor")
        self._read()
        self.env.records.request_stop()
        self.controller.request_stop()

    def release(self):
        """Permanently release runner authority after all its calls have returned."""
        self._role("runner")
        self.stop()

        def release(value):
            if value["inflight"] is not None:
                raise Failure("BigQuery runner call is in flight or unresolved")
            value["released"] = True

        self._change(release)

    def released(self):
        """Read the bound runner's durable acknowledgement, without transferring it."""
        _, value = self._read()
        return value["released"] is True and value["inflight"] is None

    def cleanup(self, quiesce, *, deadline):
        """Require runner release plus the caller's external workload barrier."""
        self._role("supervisor")
        self.stop()

        def barrier():
            _, value = self._read()
            if not value["released"] or value["inflight"] is not None:
                raise Failure("BigQuery submitting runner has not released authority")
            return quiesce()

        return self.controller.with_deadline(deadline).cleanup(barrier)
