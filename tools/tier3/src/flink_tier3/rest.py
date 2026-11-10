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
"""Deadline-bounded JSON calls to a Google REST API, with no retries."""

import json
import math
import time

import google.auth.exceptions
import requests

from .common import ApiError, Failure, TransportError
from .policy import HTTP_TIMEOUT, MIB

# A refusal's body read for its message, and the part of the message kept.
ERROR_BODY_BYTES = 8192
ERROR_MESSAGE_CHARS = 500


class Unsent(Failure):
    """A call refused before its request was sent, so it reached no service."""


class JsonService:
    """One service's calls through an injected authorized session.

    A subclass names the service and its base URL and caps the response size;
    its instances hold `http`, an absolute UTC `deadline` and a `clock`. The
    deadline bounds starting each call and reading its response, and `Unsent`
    is raised when it has passed before a request was sent, so a caller can
    tell a call that reached no service from one that may have. With
    `refuse_late_body`, a response whose last bytes arrived after the deadline
    is refused too.
    """

    label = ""
    base = ""
    max_response_bytes = MIB
    refuse_late_body = False

    def _remaining(self, error=Failure):
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            raise error(f"{self.label} operation deadline exceeded")
        return min(HTTP_TIMEOUT, remaining)

    def _call(
        self,
        method,
        path,
        body=None,
        params=None,
        absent=False,
        shape=dict,
        headers=None,
    ):
        """The decoded response, or None for a 404 when `absent` allows one.

        `shape` is the JSON type the response must have; a streaming method
        such as Bigtable's ReadRows answers with an array. A refusal raises
        `ApiError` with the service's own message, when its body names one.
        """
        timeout = self._remaining(Unsent)
        try:
            with self.http.request(
                method,
                self.base + path,
                json=body,
                params=params,
                timeout=timeout,
                allow_redirects=False,
                stream=True,
                headers=headers,
            ) as response:
                self._remaining()
                if absent and response.status_code == 404:
                    return None
                if not 200 <= response.status_code < 300:
                    raise ApiError(
                        response.status_code,
                        method,
                        path,
                        message=self._error_message(response),
                    )
                data = bytearray()
                for chunk in response.iter_content(8192):
                    self._remaining()
                    data.extend(chunk)
                    if len(data) > self.max_response_bytes:
                        raise Failure(
                            f"{self.label} response exceeds "
                            f"{self.max_response_bytes // MIB} MiB"
                        )
                if self.refuse_late_body:
                    self._remaining()
                result = json.loads(data) if data else shape()
                if not isinstance(result, shape):
                    kind = "an object" if shape is dict else "an array"
                    raise Failure(f"{self.label} response must be {kind}")
                return result
        except (
            requests.exceptions.RequestException,
            google.auth.exceptions.GoogleAuthError,
        ) as error:
            raise TransportError(
                f"{method} {self.label} request failed: {type(error).__name__}"
            ) from error
        except (ValueError, UnicodeError) as error:
            raise Failure(f"Invalid {self.label} JSON response") from error

    def _error_message(self, response):
        """A Google API error body's `error.message`, shortened, or None.

        Best effort: the body is gathered across its chunks up to
        `ERROR_BODY_BYTES`, and the call's deadline is checked between them as
        for a successful response, so a slow body can overrun the deadline by
        one read at most. Only the first `ERROR_BODY_BYTES` are parsed, and
        what does not parse, cannot be read in time or names no message
        leaves the refusal its status alone.
        """
        data = bytearray()
        try:
            for chunk in response.iter_content(1024):
                self._remaining()
                data.extend(chunk)
                if len(data) >= ERROR_BODY_BYTES:
                    break
            body = json.loads(data[:ERROR_BODY_BYTES])
        except (
            Failure,
            ValueError,
            UnicodeError,
            requests.exceptions.RequestException,
        ):
            return None
        error = body.get("error") if isinstance(body, dict) else None
        message = error.get("message") if isinstance(error, dict) else None
        if not isinstance(message, str) or not message.strip():
            return None
        message = " ".join(message.split())
        if len(message) > ERROR_MESSAGE_CHARS:
            message = message[:ERROR_MESSAGE_CHARS] + "…"
        return message


class TrialService(JsonService):
    """One run's resource, named from its validated trial.

    A subclass names the trial type it takes and the resource's name in it.
    """

    trial_type = None

    def __init__(self, http, trial, deadline, clock=time.time):
        if not isinstance(trial, self.trial_type):
            raise TypeError("trial must be a validated Trial")
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("deadline must be a finite UTC timestamp")
        self.http, self.trial, self.deadline, self.clock = http, trial, deadline, clock
        self.name = self.resource_name(trial)

    def with_deadline(self, deadline):
        """Share the transport with a deadline that cannot extend this one."""
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("deadline must be a finite UTC timestamp")
        return type(self)(
            self.http, self.trial, min(self.deadline, deadline), self.clock
        )

    @staticmethod
    def resource_name(trial):
        raise NotImplementedError
