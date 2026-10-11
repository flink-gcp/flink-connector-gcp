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
"""Hold each Pub/Sub service request to its deadline and approval."""

from contextlib import contextmanager

from ..common import Failure
from ..model import validate_approval
from ..policy import HTTP_TIMEOUT
from .access import admission_until

# Service requests name a project resource; storage and control operations
# name an object path.
SERVICE_PREFIX = "projects/"


def admission_deadline(env):
    """When admission must have created the application, or stop and clean."""
    return admission_until(env.schedule)


class PubSubGuard:
    """The production ``before_operation`` for one Pub/Sub actor.

    A Pub/Sub request is refused when it would start within the session's
    request budget of the current deadline, or when the approval no longer
    validates. That budget ends at the response's headers; a streamed body is
    bounded only by the transport's per-read timeout and the helpers' 1 MiB
    response cap, not by the deadline. The deadline is the admission
    deadline unless a caller holds another with ``until``; message traffic
    belongs to the exercise and holds ``cleanup_at``, and cleanup holds none,
    because it must stay usable after both. Control and storage operations
    are not checked: a stop, a released call marker or the evidence of a
    request already sent must still be written after either. Stop and
    evidence-failure flags are the handoff's and the controller's to enforce,
    at the points where
    it admits new work.
    """

    def __init__(self, env):
        self.env = env
        self.deadline = admission_deadline(env)

    @contextmanager
    def until(self, deadline):
        """Hold service requests to ``deadline``; ``None`` holds them to none."""
        previous, self.deadline = self.deadline, deadline
        try:
            yield
        finally:
            self.deadline = previous

    def __call__(self, phase, method, name):
        if self.deadline is None or not name.startswith(SERVICE_PREFIX):
            return
        now = self.env.clock()
        if now + HTTP_TIMEOUT > self.deadline:
            raise Failure("Pub/Sub request is too close to its deadline")
        validate_approval(self.env.approval.to_dict(), now)
