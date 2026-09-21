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
"""Shared, conservative reservations for prepared Pub/Sub message traffic."""

import copy
import math
import re
from dataclasses import asdict, dataclass

from .common import Failure, digest, json_bytes
from .model import Phase
from .pubsub_messages import MAX_BATCH, Messages

COUNTER_CEILINGS = {
    "publish_calls": 20000,
    "pull_calls": 2000,
    "input_messages": 20000,
    "input_bytes": 128 * 20000,
    "output_messages": 200000,
    "pubsub_requests": 30000,
    "evidence_bytes": 64 * 1024 * 1024,
}


@dataclass(frozen=True)
class TrafficLimits:
    """Explicit message-helper limits, not approval of a runnable GCP trial."""

    publish_calls: int
    pull_calls: int
    input_messages: int
    input_bytes: int
    output_messages: int
    pubsub_requests: int
    evidence_bytes: int
    admit_until: float

    def __post_init__(self):
        for key, maximum in COUNTER_CEILINGS.items():
            value = getattr(self, key)
            if type(value) is not int or not 0 < value <= maximum:
                raise Failure("Invalid Pub/Sub traffic limit: " + key)
        if (
            type(self.admit_until) not in (int, float)
            or not math.isfinite(self.admit_until)
            or self.admit_until <= 0
        ):
            raise Failure("Invalid Pub/Sub traffic admission deadline")


class PubSubTraffic:
    """Bind Messages to prepared control; callers still own full admission/fencing.

    Initialize before the first data call and use this wrapper for both actors;
    calling Messages directly bypasses these counters. The resource controller's
    guard still accounts for credentials, control/lock I/O and CAS retries, checks
    effective access and proves exclusive resource control. Actor strings are
    routing checks, not authentication. No CLI admits this protocol yet.
    """

    def __init__(self, controller, application, limits):
        self.controller, self.env = controller, controller.env
        if not isinstance(limits, TrafficLimits):
            raise Failure("Pub/Sub traffic requires explicit limits")
        if (
            digest(application) != controller.intent["application_sha256"]
            or not self.env.schedule.cleanup_at >= limits.admit_until
        ):
            raise Failure(
                "Pub/Sub traffic differs from application or cleanup deadline"
            )
        args = application.get("spec", {}).get("job", {}).get("args", [])
        values = [
            a
            for a in args
            if isinstance(a, str) and a.startswith("--records-per-subscription")
        ]
        if len(values) != 1 or not re.fullmatch(
            r"--records-per-subscription=(?:[1-9][0-9]{0,3}|10000)", values[0]
        ):
            raise Failure(
                "Pub/Sub traffic requires the application's exact input domain"
            )
        self.records = int(values[0].split("=", 1)[1])
        if getattr(self.env.approval, "version", None) == 5:
            approved = self.env.approval.pubsub_traffic_limits
            if (
                limits != approved
                or self.records
                != self.env.approval.pubsub_trial["records_per_subscription"]
            ):
                raise Failure("Pub/Sub traffic differs from the serialized trial")
        self.limits = limits
        self.binding = {
            "version": 1,
            "records_per_subscription": self.records,
            "limits": asdict(limits),
        }

    def _state(self, record):
        state = self.controller._state(record)
        traffic = state.get("traffic")
        if (
            not isinstance(traffic, dict)
            or json_bytes(traffic.get("binding")) != json_bytes(self.binding)
            or not isinstance(traffic.get("used"), dict)
            or set(traffic["used"]) != set(COUNTER_CEILINGS)
            or any(
                type(value) is not int or not 0 <= value <= getattr(self.limits, key)
                for key, value in traffic["used"].items()
            )
        ):
            raise Failure("Missing or replaced Pub/Sub traffic binding")
        return state, traffic["used"]

    def initialize(self):
        """Save immutable limits and zero counters before any message helper call."""
        self.controller._actor(runner=True)

        def initialize(record):
            self.controller._open(record)
            state = self.controller._state(record)
            if (
                state["stage"] != "prepared"
                or self.env.clock() >= self.limits.admit_until
            ):
                raise Failure(
                    "Pub/Sub traffic needs prepared resources and an open window"
                )
            if state.get("traffic") is not None:
                self._state(record)
                return
            state["traffic"] = {
                "binding": copy.deepcopy(self.binding),
                "used": dict.fromkeys(COUNTER_CEILINGS, 0),
            }

        self.controller._change(initialize)

    def _allow(self, record, actor, *, admit):
        state, used = self._state(record)
        if self.env.actor != actor:
            raise Failure("Pub/Sub traffic requires the " + actor + " actor")
        if (
            state["stage"] not in ("prepared", "cleaning")
            or record.phase not in (Phase.RUNNING, Phase.CLEANING)
            or self.env.clock() >= self.env.schedule.expires_at
        ):
            raise Failure("Pub/Sub traffic evidence window has closed")
        if admit and (
            state["stage"] != "prepared"
            or record.phase != Phase.RUNNING
            or record.stop_requested
            or record.evidence_failed
            or self.env.stopping
            or self.env.evidence_failed
            or self.env.clock() >= self.limits.admit_until
        ):
            raise Failure("Pub/Sub traffic admission has stopped")
        return used

    def _reserve(self, amounts, actor, *, admit):
        def reserve(record):
            used = self._allow(record, actor, admit=admit)
            for key, amount in amounts.items():
                if (
                    type(amount) is not int
                    or amount < 0
                    or used[key] + amount > getattr(self.limits, key)
                ):
                    raise Failure("Pub/Sub traffic budget exhausted: " + key)
            for key, amount in amounts.items():
                used[key] += amount

        # Never refund: a reservation or external write may have committed even
        # when its response was lost. CAS retries re-read the shared counters.
        self.controller._change(reserve)

    def _messages(self, actor, amounts):
        if self.env.actor != actor:
            raise Failure("Pub/Sub traffic requires the " + actor + " actor")
        traffic = self
        reserved = False

        def guard(phase, method, name):
            nonlocal reserved
            self.controller.before_operation(phase, method, name)
            if not reserved:
                # Messages validates its arguments before the first intent guard.
                self._reserve(amounts(), actor, admit=True)
                reserved = True
            if method == "POST":
                self._reserve({"pubsub_requests": 1}, actor, admit=True)

        class EvidenceStore:
            def write(self, name, value, generation):
                if generation != "0" or not reserved:
                    raise Failure(
                        "Pub/Sub traffic requires a reserved create-only upload"
                    )
                try:
                    saved = copy.deepcopy(value)
                    traffic._reserve(
                        {"evidence_bytes": len(json_bytes(saved))},
                        actor,
                        admit=name.endswith("/intent.json"),
                    )
                    return traffic.env.store.write(name, saved, generation="0")
                except (Failure, ValueError, OSError):
                    traffic.env.evidence_failed = True

                    def failed(record):
                        traffic.controller._state(record)
                        record.evidence_failed = True

                    try:
                        traffic.controller._change(failed)
                    except (Failure, ValueError, OSError) as error:
                        raise Failure(
                            "Pub/Sub evidence failure could not be recorded; "
                            "stop actors independently before restarting"
                        ) from error
                    raise

        return Messages(
            self.controller.http,
            EvidenceStore(),
            self.controller.plan,
            actor=actor,
            records_per_subscription=self.records,
            before_operation=guard,
        )

    def publish(self, input_index, start, count):
        """Reserve a logical input batch and payload bytes before its intent write."""

        def amounts():
            return {
                "publish_calls": 1,
                "input_messages": count,
                "input_bytes": sum(
                    len(
                        f"v1|{self.controller.plan.run_id}|{input_index}|{sequence}".encode()
                    )
                    for sequence in range(start, start + count)
                ),
            }

        return self._messages("runner", amounts).publish(input_index, start, count)

    def collect(self, batch_id, *, max_messages=MAX_BATCH):
        """Reserve all requested deliveries, including an empty or ambiguous pull."""
        return self._messages(
            "supervisor", lambda: {"pull_calls": 1, "output_messages": max_messages}
        ).collect(batch_id, max_messages=max_messages)
