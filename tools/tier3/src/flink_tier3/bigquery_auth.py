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
"""OAuth identity checks and per-request budgets for BigQuery lifecycle actors."""

from __future__ import annotations

import json
import math
import time
from contextlib import AbstractContextManager

import google.auth
import requests
from google.auth.transport.requests import Request

from .bigquery_resources import BASE
from .common import Failure, TransportError
from .policy import HTTP_TIMEOUT, PROJECT

USERINFO = "https://www.googleapis.com/oauth2/v2/userinfo"
SCOPES = (
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/userinfo.email",
)
PRINCIPALS = {
    role: f"tier3-{role}@{PROJECT}.iam.gserviceaccount.com"
    for role in ("runner", "supervisor")
}


class BigQuerySession(AbstractContextManager):
    """One actor's OAuth session; no API retries or redirect following.

    Identity is verified with the exact bearer header used for the resource
    request, again when it changes. Budgets limit starting network calls and
    their timeout arguments, not synchronous credential code or blocked reads.
    """

    def __init__(self, role, credentials=None, *, monotonic=time.monotonic):
        if role not in PRINCIPALS:
            raise Failure("Unknown BigQuery lifecycle actor")
        self.principal = PRINCIPALS[role]
        self.credentials = credentials
        self.monotonic = monotonic
        self.http = requests.Session()
        self.http.trust_env = False
        self.http.max_redirects = 0
        self.auth_request = Request(self.http)
        self.verified = None
        self.closed = False

    def close(self):
        self.closed = True
        self.verified = None
        self.http.close()

    def __exit__(self, *_exc):
        self.close()

    def _budget(self, timeout):
        if self.closed:
            raise Failure("BigQuery authentication session is closed")
        if (
            type(timeout) not in (int, float)
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise Failure("BigQuery authentication requires a positive finite timeout")
        end = self.monotonic() + min(HTTP_TIMEOUT, timeout)

        def remaining():
            value = end - self.monotonic()
            if value <= 0:
                raise Failure("BigQuery authentication/request budget expired")
            return value

        return remaining

    def _headers(self, method, url, remaining):
        def refresh(url, method="GET", body=None, headers=None, timeout=None, **kwargs):
            # A credential exchange may make several requests. Each one shares
            # the resource call's budget; redirects cannot move credentials.
            budget = remaining()
            if timeout is not None:
                if (
                    type(timeout) not in (int, float)
                    or not math.isfinite(timeout)
                    or timeout <= 0
                ):
                    raise Failure("Invalid credential request timeout")
                budget = min(budget, timeout)
            return self.auth_request(
                url=url,
                method=method,
                body=body,
                headers=headers,
                timeout=budget,
                **dict(kwargs, allow_redirects=False),
            )

        if self.credentials is None:
            self.credentials = google.auth.default(scopes=SCOPES, request=refresh)[0]
        if getattr(self.credentials, "_use_non_blocking_refresh", False):
            raise Failure("BigQuery actor requires synchronous credential refresh")
        headers = {}
        self.credentials.before_request(refresh, method, url, headers)
        remaining()
        authorization = headers.get("authorization")
        if (
            not isinstance(authorization, str)
            or not authorization.startswith("Bearer ")
            or authorization == "Bearer "
        ):
            raise Failure("BigQuery actor requires an OAuth bearer token")
        if authorization != self.verified:
            with self.http.request(
                "GET",
                USERINFO,
                headers=headers,
                timeout=remaining(),
                allow_redirects=False,
                stream=True,
            ) as response:
                remaining()
                if response.status_code != 200:
                    raise Failure("BigQuery Google identity verification failed")
                data = bytearray()
                for chunk in response.iter_content(4096):
                    remaining()
                    data.extend(chunk)
                    if len(data) > 16384:
                        raise Failure("BigQuery identity response exceeds 16 KiB")
                remaining()
                try:
                    identity = json.loads(data)
                except (ValueError, UnicodeError) as error:
                    raise Failure("Invalid BigQuery identity response") from error
                if (
                    not isinstance(identity, dict)
                    or identity.get("email") != self.principal
                    or identity.get("verified_email") is not True
                ):
                    raise Failure("Google identity differs from the BigQuery actor")
            self.verified = authorization
        return headers

    def authenticate(self, *, timeout):
        """Verify the current credential before returning an executable actor."""
        try:
            self._headers("GET", USERINFO, self._budget(timeout))
        except (
            requests.exceptions.RequestException,
            google.auth.exceptions.GoogleAuthError,
        ) as error:
            raise TransportError(
                "BigQuery authentication failed: " + type(error).__name__
            ) from error

    def request(
        self, method, url, *, timeout, allow_redirects=False, stream=True, **kwargs
    ):
        if (
            method not in ("GET", "POST", "DELETE")
            or not url.startswith(BASE + "/")
            or allow_redirects is not False
            or stream is not True
            or set(kwargs) - {"json", "params"}
        ):
            raise Failure("Unexpected BigQuery authenticated request")
        try:
            remaining = self._budget(timeout)
            headers = self._headers(method, url, remaining)
            response = self.http.request(
                method,
                url,
                headers=headers,
                timeout=remaining(),
                allow_redirects=False,
                stream=True,
                **kwargs,
            )
            try:
                remaining()
            except Failure:
                response.close()
                raise
            return response
        except (
            requests.exceptions.RequestException,
            google.auth.exceptions.GoogleAuthError,
        ) as error:
            raise TransportError(
                "BigQuery authenticated request failed: " + type(error).__name__
            ) from error
