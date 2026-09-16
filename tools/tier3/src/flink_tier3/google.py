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
from contextlib import contextmanager
from functools import partial

import google.auth
import requests
from google.api_core.exceptions import GoogleAPICallError
from google.auth.transport.requests import AuthorizedSession
from google.auth.transport.requests import Request as AuthRequest
from google.cloud import storage
from google.cloud.storage.exceptions import DataCorruption

from .common import ApiError, Failure, json_bytes
from .policy import EVIDENCE, HTTP_TIMEOUT, PROJECT


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


class Storage:
    def __init__(self, client=None):
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
        ) as error:
            raise Failure(
                f"{method} storage request failed: {type(error).__name__}"
            ) from error

    def read(self, name, bucket=EVIDENCE):
        for _ in range(5):
            try:
                with self.operation("GET", name):
                    blob = self.client.bucket(bucket).blob(name)
                    blob.reload(timeout=HTTP_TIMEOUT, retry=None)
            except ApiError as error:
                if error.status == 404:
                    return None, "0"
                raise
            generation = blob.generation
            try:
                with self.operation("GET", name):
                    data = blob.download_as_bytes(
                        if_generation_match=generation,
                        timeout=HTTP_TIMEOUT,
                        retry=None,
                    )
                return json.loads(data), str(generation)
            except ApiError as error:
                # Another writer can replace this generation after metadata was
                # read. Restart with a fresh blob; only metadata 404 means absent.
                if error.status not in (404, 412):
                    raise
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
        with self.operation("GET", prefix):
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
                    }
                )
                if len(result) > maximum:
                    raise Failure("Object inventory exceeds its count ceiling")
            return result
