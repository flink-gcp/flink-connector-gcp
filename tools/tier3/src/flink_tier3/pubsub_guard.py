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
"""Reserve each Pub/Sub lifecycle method's operation bound before it starts."""

from contextlib import contextmanager

from .common import Failure
from .model import validate_approval
from .policy import HTTP_TIMEOUT, PUBSUB_ADMISSION

# A conditional control update invokes the guard once before the record
# adapter and once per write attempt, and the adapter makes at most five.
CHANGE_CALLBACKS = 6
# A process-owned call adds its begin update and, at worst, either three
# completion attempts or a stop followed by three attempts to release the
# marker of a settled failure.
CALL_CHANGES = 5

# The helpers' guard callbacks by phase, as the application runbook documents
# them for each method, measured with the synthetic transport.
PROVISION = {"provision": 20, "inspect": 7}
INSTALL_GRANTS = {"inspect": 14, "grant": 42, "inspect-grants": 12}
INSPECT_GRANTS = {"inspect": 7, "inspect-grants": 12}
# With the manifest present; confirming absence without one takes seven.
CLEANUP = {"cleanup": 26}
# The intent upload, the one POST and the response upload.
PUBLISH = {"publish": 3}
# Intent, pull, response and observation uploads, then the acknowledgement
# and its record when the pull returned messages.
COLLECT = {"collect": 4, "acknowledge": 2}


def _bound(resources=(), messages=(), *, reads=0, changes=0):
    """Phase bounds; every resource callback also re-reads control once."""
    phases = {}
    for helper in (*resources, *messages):
        for phase, count in helper.items():
            phases[phase] = phases.get(phase, 0) + count
    rereads = sum(count for helper in resources for count in helper.values())
    return phases | {
        "control-GET": rereads + reads,
        "control-UPDATE": CHANGE_CALLBACKS * changes,
    }


# Changes: preparation claims its attempt, records creation intent, keeps the
# resource and policy readback, marks the stage and binds the traffic
# counters. Publication reserves the batch, its POST and two evidence uploads;
# collection reserves the batch, two POSTs and four uploads. Either may record
# an evidence failure once.
METHOD_BOUNDS = {
    "initialize": _bound(changes=1),
    "prepare": _bound((PROVISION, INSTALL_GRANTS), changes=6 + CALL_CHANGES),
    "verify": _bound((INSPECT_GRANTS,)),
    "join": _bound(changes=1),
    "publish": _bound(messages=(PUBLISH,), changes=5 + CALL_CHANGES),
    "collect": _bound(messages=(COLLECT,), changes=8 + CALL_CHANGES),
    "stop": _bound(changes=1),
    "release": _bound(changes=2),
    "released": _bound(reads=1),
    "cleanup": _bound((CLEANUP,), reads=2, changes=2),
    "reclaim": _bound((CLEANUP,), reads=2, changes=3),
}
# Methods that admit work. Their Pub/Sub requests stop at the deadline and
# once the approval no longer validates; the rest settle the run and must
# stay usable after both.
ADMISSION = ("initialize", "prepare", "verify", "join", "publish", "collect")
# Message traffic continues through the exercise, long after admission, so
# its default deadline is the traffic window's; a caller publishing during
# admission passes the admission deadline instead.
TRAFFIC = ("publish", "collect")
# Service requests name a project resource; storage and control operations
# name an object path.
SERVICE_PREFIX = "projects/"


def admission_deadline(env):
    """When admission must have created the application, or stop and clean."""
    return min(
        env.schedule.started + PUBSUB_ADMISSION["startup_seconds"],
        env.schedule.cleanup_at,
    )


class PubSubGuard:
    """The production ``before_operation`` for one Pub/Sub actor.

    A method reserves its whole bound before it starts, so a runaway loop or
    an unplanned retry is refused at the next operation rather than metered
    afterwards. A method entered inside another counts against the outer
    reservation: the nestings that exist, a stop inside a release or inside a
    failed call, are counted in the outer bound. Each operation is refused
    outside an open reservation, in a phase the method does not use, or
    beyond its bound.

    While a method admits work, a Pub/Sub request is also refused when it
    would start within the session's request budget of the deadline, or when
    the approval no longer validates. That budget ends at the response's
    headers; a streamed body is bounded only by the transport's per-read
    timeout and the helpers' 1 MiB response cap, not by the deadline. Control
    and storage operations are not: a stop, a released call marker or the
    evidence of a request already sent must still be written after either.
    Stop and evidence-failure flags are the controller's and traffic
    wrapper's to enforce, at the points where they admit new work.

    These are per-method bounds of logical operations. They are not the
    aggregate request ceiling across connector, credential and storage calls,
    and nothing here makes them durable across processes.
    """

    def __init__(self, env, *, deadline=None):
        self.env = env
        self.deadline = admission_deadline(env) if deadline is None else deadline
        self.open = None
        # The last top-level method's counts, for its caller's evidence.
        self.last = None

    @contextmanager
    def method(self, name, *, deadline=None):
        if name not in METHOD_BOUNDS:
            raise Failure("Unknown Pub/Sub lifecycle method: " + name)
        if self.open is not None:
            if deadline is not None:
                # The outer reservation's deadline would apply instead.
                raise Failure("A nested Pub/Sub method cannot set a deadline")
            yield
            return
        self.open = {
            "method": name,
            "bounds": METHOD_BOUNDS[name],
            "used": {},
            "deadline": self._deadline(name, deadline),
        }
        try:
            yield
        finally:
            done, self.open = self.open, None
            self.last = {
                "method": done["method"],
                "used": dict(sorted(done["used"].items())),
            }

    def _deadline(self, name, deadline):
        if name not in ADMISSION:
            return None
        if deadline is not None:
            return deadline
        return self.env.schedule.cleanup_at if name in TRAFFIC else self.deadline

    def __call__(self, phase, method, name):
        current = self.open
        if current is None:
            raise Failure("Pub/Sub operation outside a reserved method")
        key = f"control-{method}" if phase == "control" else phase
        used = current["used"].get(key, 0)
        if used >= current["bounds"].get(key, 0):
            raise Failure(
                f"Pub/Sub {current['method']} exceeded its reserved {key} operations"
            )
        if current["deadline"] is not None and name.startswith(SERVICE_PREFIX):
            now = self.env.clock()
            if now + HTTP_TIMEOUT > current["deadline"]:
                raise Failure("Pub/Sub request is too close to its deadline")
            validate_approval(self.env.approval.to_dict(), now)
        current["used"][key] = used + 1
