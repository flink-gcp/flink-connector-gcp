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
"""Tier-3 lifecycle google."""

from __future__ import annotations

import json
import os
import random
import time
from contextlib import contextmanager
from datetime import UTC
from functools import partial

import google.auth
import requests
from google.api_core.exceptions import GoogleAPICallError
from google.auth.transport.requests import AuthorizedSession
from google.auth.transport.requests import Request as AuthRequest
from google.cloud import storage
from google.cloud.storage.exceptions import DataCorruption

from .common import (
    TRANSIENT_STATUSES,
    ApiError,
    Failure,
    StorageRequestFailed,
    json_bytes,
)
from .policy import (
    EVIDENCE,
    GENERATION_RACES,
    HTTP_TIMEOUT,
    PROJECT,
    RACE_BACKOFF_CAP,
    RACE_BACKOFF_FIRST,
    READ_RETRY_DELAYS,
)


class GoogleToken:
    def __init__(self, credentials=None):
        session = requests.Session()
        session.trust_env = False
        session.max_redirects = 0
        self.request = partial(AuthRequest(session), timeout=HTTP_TIMEOUT)
        self.credentials = (
            credentials
            or google.auth.default(
                scopes=[
                    "https://www.googleapis.com/auth/cloud-platform",
                    # GKE RBAC binds service-account emails, not numeric IDs.
                    "https://www.googleapis.com/auth/userinfo.email",
                ],
                request=self.request,
            )[0]
        )

    def __call__(self):
        try:
            self.credentials.before_request(self.request, "GET", "", {})
        except google.auth.exceptions.GoogleAuthError as error:
            raise Failure("Google credential refresh failed") from error
        return self.credentials.token


def authorized_session(token):
    session = AuthorizedSession(
        token.credentials,
        auth_request=token.request,
        max_refresh_attempts=0,
        refresh_timeout=HTTP_TIMEOUT,
    )
    session.trust_env = False
    session.max_redirects = 0
    return session


def actor_session(credentials=None):
    """A lifecycle actor's authorized session, which returns a redirect unread.

    Even without following one, requests reads a 3xx body whole to prepare
    `Response.next`, before any caller's size cap; seeing no target, it
    returns the response for the caller to refuse as a non-success status.
    """
    session = authorized_session(GoogleToken(credentials))
    session.get_redirect_target = lambda _response: None
    return session


# Failures that leave a request without an answer: a refused, reset or timed-out
# connection, including a body read that stalled, which requests reports as a
# ConnectionError, a body cut short, and a token refresh that failed in transport.
NO_ANSWER = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
    google.auth.exceptions.TransportError,
    ConnectionError,
)


def _no_answer(error):
    while error is not None:
        if isinstance(error, NO_ANSWER):
            return True
        error = error.__cause__
    return False


class Storage:
    def __init__(self, client=None, sleep=time.sleep, clock=time.monotonic):
        self.sleep, self.clock = sleep, clock
        # The SDK otherwise starts unbudgeted background bucket-metadata reads.
        os.environ["DISABLE_GCS_PYTHON_CLIENT_OTEL_BUCKET_METADATA"] = "true"
        if client is None:
            token = GoogleToken()
            client = storage.Client(
                project=PROJECT,
                credentials=token.credentials,
                _http=authorized_session(token),
                client_options={"api_endpoint": "https://storage.googleapis.com"},
            )
        self.client = client

    @contextmanager
    def operation(self, method, name):
        try:
            yield
        except GoogleAPICallError as error:
            raise ApiError(error.code, method, name) from error
        except (
            requests.exceptions.RequestException,
            google.auth.exceptions.GoogleAuthError,
            DataCorruption,
            ConnectionError,
        ) as error:
            failed = StorageRequestFailed if _no_answer(error) else Failure
            raise failed(
                f"{method} storage request failed: {type(error).__name__}"
            ) from error

    def _retried(self, name, call):
        """Run an idempotent read, retrying what a storage blip returns.

        A transient status, or a request that got no answer, is retried after
        each of the delays until the request has spent one timeout; the first
        failure is retried however long it took. An attempt that timed out has
        spent a timeout, so it is repeated at most once, where failing would end
        the run.
        """
        started = self.clock()
        for attempt, delay in enumerate(READ_RETRY_DELAYS):
            try:
                with self.operation("GET", name):
                    return call()
            except (ApiError, StorageRequestFailed) as error:
                if (
                    isinstance(error, ApiError)
                    and error.status not in TRANSIENT_STATUSES
                ):
                    raise
                if attempt and self.clock() - started >= HTTP_TIMEOUT:
                    raise
            self.sleep(delay)
        with self.operation("GET", name):
            return call()

    def read(self, name, bucket=EVIDENCE):
        for race in range(1, GENERATION_RACES + 1):
            blob = self.client.bucket(bucket).blob(name)
            try:
                self._retried(
                    name, partial(blob.reload, timeout=HTTP_TIMEOUT, retry=None)
                )
            except ApiError as error:
                if error.status == 404:
                    return None, "0"
                raise
            generation = blob.generation
            try:
                data = self._retried(
                    name,
                    partial(
                        blob.download_as_bytes,
                        if_generation_match=generation,
                        timeout=HTTP_TIMEOUT,
                        retry=None,
                    ),
                )
                return json.loads(data), str(generation)
            except ApiError as error:
                # Another writer can replace this generation after metadata was
                # read. Restart with a fresh blob; only metadata 404 means absent.
                if error.status not in (404, 412):
                    raise
            if race < GENERATION_RACES:
                # A busy record is rewritten about once a second; waiting lets
                # a read fall between two writes, as a conditional update does.
                ceiling = min(RACE_BACKOFF_FIRST * 2 ** (race - 1), RACE_BACKOFF_CAP)
                self.sleep(random.uniform(0, ceiling))
        raise Failure("Concurrent storage reads did not settle")

    def write(self, name, data, generation="0", bucket=EVIDENCE):
        with self.operation("POST", name):
            blob = self.client.bucket(bucket).blob(name)
            # Keep SDK corruption handling from issuing an unconditional delete.
            blob.upload_from_string(
                json_bytes(data),
                content_type="application/json",
                if_generation_match=int(generation),
                timeout=HTTP_TIMEOUT,
                retry=None,
                checksum=None,
            )
            return str(blob.generation)

    def delete(self, name, generation, bucket=EVIDENCE):
        if not generation or str(generation) == "0":
            raise Failure("Deletion requires an observed object generation")
        try:
            with self.operation("DELETE", name):
                self.client.bucket(bucket).blob(name).delete(
                    if_generation_match=int(generation),
                    timeout=HTTP_TIMEOUT,
                    retry=None,
                )
        except ApiError as error:
            if error.status != 404:
                raise

    def objects(self, prefix, bucket=EVIDENCE, maximum=20000):
        def call():
            result = []
            for blob in self.client.list_blobs(
                bucket,
                prefix=prefix,
                page_size=1000,
                max_results=maximum + 1,
                timeout=HTTP_TIMEOUT,
                retry=None,
            ):
                result.append(
                    {
                        "name": blob.name,
                        "generation": str(blob.generation),
                        "size": str(blob.size),
                        "created": _created(blob),
                    }
                )
                if len(result) > maximum:
                    raise Failure("Object inventory exceeds its count ceiling")
            return result

        return self._retried(prefix, call)

    def prefixes(self, prefix, bucket=EVIDENCE, maximum=20000):
        """Immediate child prefixes of ``prefix``, without listing their objects."""

        def call():
            iterator = self.client.list_blobs(
                bucket,
                prefix=prefix,
                delimiter="/",
                page_size=1000,
                max_results=maximum + 1,
                timeout=HTTP_TIMEOUT,
                retry=None,
            )
            # Prefixes arrive with the pages; only objects directly under the
            # prefix count against the item ceiling.
            for count, _ in enumerate(iterator, start=1):
                if count > maximum:
                    raise Failure("Object inventory exceeds its count ceiling")
            if len(iterator.prefixes) > maximum:
                raise Failure("Prefix inventory exceeds its count ceiling")
            return set(iterator.prefixes)

        return self._retried(prefix, call)


def _created(blob):
    """The RFC 3339 creation time at the API's millisecond precision."""
    created = blob.time_created
    if created is None:
        return None
    return (
        created.astimezone(UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
