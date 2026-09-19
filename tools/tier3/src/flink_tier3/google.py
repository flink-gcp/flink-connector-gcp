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
from datetime import UTC
from functools import partial

import google.auth
import requests
from google.api_core.exceptions import GoogleAPICallError
from google.auth.transport.requests import AuthorizedSession
from google.auth.transport.requests import Request as AuthRequest
from google.cloud import storage
from google.cloud.storage.exceptions import DataCorruption

from .common import ApiError, Failure, json_bytes
from .policy import EVIDENCE, HTTP_TIMEOUT, MIB, PROJECT


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
                        "created": _created(blob),
                    }
                )
                if len(result) > maximum:
                    raise Failure("Object inventory exceeds its count ceiling")
            return result

    def prefixes(self, prefix, bucket=EVIDENCE, maximum=20000):
        """Immediate child prefixes of ``prefix``, without listing their objects."""
        with self.operation("GET", prefix):
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

    def metadata(self, name, bucket=EVIDENCE):
        """Size, checksums, generation and creation time, or None when absent."""
        try:
            with self.operation("GET", name):
                blob = self.client.bucket(bucket).blob(name)
                blob.reload(timeout=HTTP_TIMEOUT, retry=None)
        except ApiError as error:
            if error.status == 404:
                return None
            raise
        return _metadata(blob)

    def open(self, name, generation, bucket=EVIDENCE, chunk_size=MIB):
        """A binary stream over one observed generation; a replacement fails it."""
        if not generation or str(generation) == "0":
            raise Failure("Streaming requires an observed object generation")
        with self.operation("GET", name):
            blob = self.client.bucket(bucket).blob(name)
            reader = blob.open(
                "rb",
                chunk_size=chunk_size,
                if_generation_match=int(generation),
                timeout=HTTP_TIMEOUT,
                retry=None,
            )
        return _TranslatedStream(self, name, reader)

    def rewrite(
        self,
        source_bucket,
        source_name,
        source_generation,
        destination_name,
        destination_bucket=EVIDENCE,
    ):
        """Server-side copy of one source generation into an absent destination.

        Returns the new destination generation as a string. When the
        destination already exists, returns its metadata dict instead so the
        caller can verify it; a 412 without a destination is the source's.
        """
        if not source_generation or str(source_generation) == "0":
            raise Failure("Rewrite requires an observed source generation")
        source = self.client.bucket(source_bucket).blob(
            source_name, generation=int(source_generation)
        )
        destination = self.client.bucket(destination_bucket).blob(destination_name)
        token = None
        try:
            with self.operation("POST", destination_name):
                while True:
                    token, _rewritten, _total = destination.rewrite(
                        source,
                        token=token,
                        if_generation_match=0,
                        if_source_generation_match=int(source_generation),
                        timeout=HTTP_TIMEOUT,
                        retry=None,
                    )
                    if token is None:
                        return str(destination.generation)
        except ApiError as error:
            if error.status != 412:
                raise
            existing = self.metadata(destination_name, destination_bucket)
            if existing is None:
                raise
            return existing


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


def _metadata(blob):
    return {
        "size": str(blob.size),
        "crc32c": blob.crc32c,
        "md5": blob.md5_hash,
        "generation": str(blob.generation),
        "created": _created(blob),
    }


class _TranslatedStream:
    """A read-only binary stream whose SDK errors become lifecycle failures."""

    def __init__(self, storage, name, reader):
        self.storage, self.name, self.reader = storage, name, reader

    def read(self, size=-1):
        with self.storage.operation("GET", self.name):
            return self.reader.read(size)

    def readable(self):
        return True

    def seekable(self):
        return False

    def close(self):
        self.reader.close()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()
