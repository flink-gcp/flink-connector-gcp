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
"""Persist the run's queue intent, its paused readback and its cleanup."""

from __future__ import annotations

from ..common import Failure
from ..resource import ResourceLifecycle
from .queue import configured

# The only state a queue reports on the way to the pause it was just given.
SETTLING_STATE = "RUNNING"
# The readback fields the receipt keeps.
RECEIPT_FIELDS = ("name", "state", "tombstoneTtl", "rateLimits", "retryConfig")


class CloudTasksLifecycle(ResourceLifecycle):
    """The run's queue, from intent to deletion, in the run's control record.

    The runner creates the queue, pauses it and reads it back before the
    application exists, and releases its authority when it settles. Only this
    rig's runner handoff writes the release: the recovery workflow's runner is
    built without one, so it cannot release on behalf of the process that
    created the queue.
    """

    scenario = "cloudtasks-recovery"

    def _populate(self, api, deadline):
        """Pause the created queue and keep its paused readback."""
        api.pause()
        readback = self._paused(api, deadline)
        return {key: readback.get(key) for key in RECEIPT_FIELDS}

    def _paused(self, api, deadline):
        """The paused readback that admits the queue, once the service settles.

        An absent, unreadable or still-running answer inside the window is the
        queue initializing. A readback that names another queue or carries
        another configuration is a deviation whenever it arrives.
        """
        readback, reads = None, 0
        while True:
            readback = self._settled(api.get, deadline)
            reads += 1
            if readback is not None:
                if (
                    readback.get("name") != self.trial.queue_path
                    or not configured(readback)
                    or readback.get("state") not in (SETTLING_STATE, "PAUSED")
                ):
                    break
                if readback.get("state") == "PAUSED":
                    if reads > 1:
                        self.env.emit("cloudtasks-queue-settling", {"reads": reads})
                    return readback
            if not self._pause(deadline):
                break
        self.env.emit("cloudtasks-queue-deviation", {"readback": readback})
        raise Failure("Cloud Tasks queue readback is not the paused configuration")

    def delete(self, deadline):
        api = self.api.with_deadline(deadline)
        api.delete()
        return api.get() is None
