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
"""Cloud Tasks REST operations on one run's queue, without admission."""

from __future__ import annotations

from ..common import Failure, contains
from ..rest import TrialService
from .trial import Trial, parent

# v2beta3, because v2 has no `tombstoneTtl`, the retention the staged sink's
# committer reads back through this same version before its first create.
BASE = "https://cloudtasks.googleapis.com/v2beta3/"
# Paused queues dispatch nothing; the dispatch limits only keep an unexpected
# resume slow, and the one-hour tombstone is the retention the sink requires.
QUEUE_CONFIGURATION = {
    "rateLimits": {"maxDispatchesPerSecond": 1, "maxConcurrentDispatches": 1},
    "retryConfig": {"maxAttempts": 1},
    "tombstoneTtl": "3600s",
}
# A listing page: at most this many tasks, in the basic view, which carries
# each task's name and dispatch count and no body.
PAGE_SIZE = 1000
MAX_RESPONSE_BYTES = 4 * 1024 * 1024


class Queue(TrialService):
    """No automatic retries; the caller owns admission, intent and ownership.

    Inject a Google authorized session with retries disabled. The absolute UTC
    deadline bounds starting each call and reading its response.
    """

    label = "Cloud Tasks"
    base = BASE
    max_response_bytes = MAX_RESPONSE_BYTES
    trial_type = Trial

    @staticmethod
    def resource_name(trial):
        return trial.queue_path

    def get(self):
        """The queue, or None when it does not exist."""
        return self._call("GET", self.name, absent=True)

    def create(self):
        """Create the run's queue with the approved configuration."""
        created = self._call(
            "POST",
            parent() + "/queues",
            {"name": self.name, **QUEUE_CONFIGURATION},
        )
        if created.get("name") != self.name:
            raise Failure("Created queue name differs from the run's queue")
        return created

    def pause(self):
        return self._call("POST", self.name + ":pause", {})

    def delete(self):
        """Delete the queue; an absent one is already gone."""
        self._call("DELETE", self.name, absent=True)

    def tasks(self, page_token=None):
        """One page of the queue's tasks in the basic view, and the next token."""
        params = {"responseView": "BASIC", "pageSize": PAGE_SIZE}
        if page_token:
            params["pageToken"] = page_token
        page = self._call("GET", self.name + "/tasks", params=params)
        tasks = page.get("tasks", [])
        token = page.get("nextPageToken") or None
        if not isinstance(tasks, list) or not (token is None or isinstance(token, str)):
            raise Failure("Cloud Tasks listing page is malformed")
        return tasks, token


def configured(queue):
    """Whether a readback carries the approved configuration."""
    return isinstance(queue, dict) and contains(queue, QUEUE_CONFIGURATION)
