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
"""OAuth identity, per-request budgets and construction checks for service actors."""

from __future__ import annotations

import json
import math
import time
from contextlib import AbstractContextManager

import google.auth
import requests
from google.auth.transport.requests import Request

from .bundle import delivery_digest, source_digest
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


class ActorSession(AbstractContextManager):
    """One actor's OAuth session for one service; no retries or redirects.

    Identity is verified with the exact bearer header used for the resource
    request, again when it changes. Budgets limit starting network calls and
    their timeout arguments, not synchronous credential code or blocked reads.
    A subclass names the service, its URL prefix and the methods it may send.
    """

    SERVICE = None
    PREFIX = None
    METHODS = ()

    def __init__(self, role, credentials=None, *, monotonic=time.monotonic):
        if role not in PRINCIPALS:
            raise Failure(f"Unknown {self.SERVICE} lifecycle actor")
        self.principal = PRINCIPALS[role]
        self.credentials = credentials
        self.monotonic = monotonic
        self.http = requests.Session()
        self.http.trust_env = False
        self.http.max_redirects = 0
        # No response is a redirect. Even without following one, requests
        # reads a 3xx body whole to prepare `Response.next`, before any
        # caller's size cap; this way it is returned unread and refused as a
        # non-success status.
        self.http.get_redirect_target = lambda _response: None
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
            raise Failure(f"{self.SERVICE} authentication session is closed")
        if (
            type(timeout) not in (int, float)
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise Failure(
                f"{self.SERVICE} authentication requires a positive finite timeout"
            )
        end = self.monotonic() + min(HTTP_TIMEOUT, timeout)

        def remaining():
            value = end - self.monotonic()
            if value <= 0:
                raise Failure(f"{self.SERVICE} authentication/request budget expired")
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
            raise Failure(
                f"{self.SERVICE} actor requires synchronous credential refresh"
            )
        headers = {}
        self.credentials.before_request(refresh, method, url, headers)
        remaining()
        authorization = headers.get("authorization")
        if (
            not isinstance(authorization, str)
            or not authorization.startswith("Bearer ")
            or authorization == "Bearer "
        ):
            raise Failure(f"{self.SERVICE} actor requires an OAuth bearer token")
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
                    raise Failure(f"{self.SERVICE} Google identity verification failed")
                data = bytearray()
                for chunk in response.iter_content(4096):
                    remaining()
                    data.extend(chunk)
                    if len(data) > 16384:
                        raise Failure(
                            f"{self.SERVICE} identity response exceeds 16 KiB"
                        )
                remaining()
                try:
                    identity = json.loads(data)
                except (ValueError, UnicodeError) as error:
                    raise Failure(
                        f"Invalid {self.SERVICE} identity response"
                    ) from error
                if (
                    not isinstance(identity, dict)
                    or identity.get("email") != self.principal
                    or identity.get("verified_email") is not True
                ):
                    raise Failure(
                        f"Google identity differs from the {self.SERVICE} actor"
                    )
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
                f"{self.SERVICE} authentication failed: " + type(error).__name__
            ) from error

    def request(
        self, method, url, *, timeout, allow_redirects=False, stream=True, **kwargs
    ):
        if (
            method not in self.METHODS
            or not url.startswith(self.PREFIX)
            or allow_redirects is not False
            or stream is not True
            or set(kwargs) - {"json", "params"}
        ):
            raise Failure(f"Unexpected {self.SERVICE} authenticated request")
        try:
            remaining = self._budget(timeout)
            headers = self._headers(method, url, remaining)
            # The budget may expire before the request leaves; only a request
            # that leaves is sent.
            timeout = remaining()
            self._sending(method, url)
            response = self.http.request(
                method,
                url,
                headers=headers,
                timeout=timeout,
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
                f"{self.SERVICE} authenticated request failed: " + type(error).__name__
            ) from error

    def _sending(self, method, url):
        """Called once the request is about to leave, after identity checks."""


def require_installed_source(env, role, service):
    """Refuse an actor whose installed source is not the one approved for it.

    The runner runs from a complete installation; the supervisor runs from a
    mounted subset, whose digest is the pin the approval carries for it.
    """
    if role == "supervisor":
        installed, approved = delivery_digest(), env.approval.delivery_sha256
    else:
        installed, approved = source_digest(), env.approval.runtime_sha256
    if installed != approved:
        raise Failure(f"{service} actor source differs from approval")


def authenticate(env, http, deadline):
    """Verify the actor's identity before the deadline, still owning the run."""
    http.authenticate(timeout=min(HTTP_TIMEOUT, deadline - env.clock()))
    if env.clock() >= deadline:
        raise Failure(f"{http.SERVICE} actor construction deadline expired")
    env.assert_owner()
