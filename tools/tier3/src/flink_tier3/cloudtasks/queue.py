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

import json
import math
import time

import google.auth.exceptions
import requests

from ..common import ApiError, Failure, TransportError, contains
from ..policy import HTTP_TIMEOUT
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


class Unsent(Failure):
    """A call refused before its request was sent, so it reached no service."""


class Queue:
    """No automatic retries; the caller owns admission, intent and ownership.

    Inject a Google authorized session with retries disabled. The absolute UTC
    deadline bounds starting each call and reading its response.
    """

    def __init__(self, http, trial, deadline, clock=time.time):
        if not isinstance(trial, Trial):
            raise TypeError("trial must be a validated Trial")
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("deadline must be a finite UTC timestamp")
        self.http, self.trial, self.deadline, self.clock = http, trial, deadline, clock
        self.name = trial.queue_path

    def with_deadline(self, deadline):
        """Share the transport with a deadline that cannot extend this one."""
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("deadline must be a finite UTC timestamp")
        return Queue(self.http, self.trial, min(self.deadline, deadline), self.clock)

    def _remaining(self, error=Failure):
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            raise error("Cloud Tasks operation deadline exceeded")
        return min(HTTP_TIMEOUT, remaining)

    def _call(self, method, path, body=None, params=None, absent=False):
        timeout = self._remaining(Unsent)
        try:
            with self.http.request(
                method,
                BASE + path,
                json=body,
                params=params,
                timeout=timeout,
                allow_redirects=False,
                stream=True,
            ) as response:
                self._remaining()
                if absent and response.status_code == 404:
                    return None
                if not 200 <= response.status_code < 300:
                    raise ApiError(response.status_code, method, path)
                data = bytearray()
                for chunk in response.iter_content(8192):
                    self._remaining()
                    data.extend(chunk)
                    if len(data) > MAX_RESPONSE_BYTES:
                        raise Failure("Cloud Tasks response exceeds 4 MiB")
                result = json.loads(data) if data else {}
                if not isinstance(result, dict):
                    raise Failure("Cloud Tasks response must be an object")
                return result
        except (
            requests.exceptions.RequestException,
            google.auth.exceptions.GoogleAuthError,
        ) as error:
            raise TransportError(
                f"{method} Cloud Tasks request failed: {type(error).__name__}"
            ) from error
        except (ValueError, UnicodeError) as error:
            raise Failure("Invalid Cloud Tasks JSON response") from error

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
