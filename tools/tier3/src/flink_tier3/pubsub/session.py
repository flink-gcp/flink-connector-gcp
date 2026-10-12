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
"""The Pub/Sub lifecycle actors' HTTP session and its record of unsettled writes."""

from contextlib import AbstractContextManager

import google.auth.exceptions


class PubSubSession(AbstractContextManager):
    """One Pub/Sub actor's authorized session, which remembers an unsettled write.

    Every request other than a GET or a permission test counts as a write. A
    write is unsettled when the transport raised once it was handed over, or
    the service answered 5xx: each may still complete at the service. A read,
    a write answered below 500, or one refused because its token could not be
    refreshed, which happens before anything is sent, is settled, whatever
    the caller then makes of the response. The record is the session's, not a call's, and never
    clears: nothing this session sees later proves the earlier write finished.
    Storage uploads go through another client and are not tracked here.
    """

    def __init__(self, http):
        self.http = http
        self.ambiguous = False

    def close(self):
        self.http.close()

    def __exit__(self, *_exc):
        self.close()

    def request(self, method, url, **kwargs):
        # testIamPermissions is a POST that changes nothing.
        write = method != "GET" and not url.endswith(":testIamPermissions")
        try:
            response = self.http.request(method, url, **kwargs)
        except google.auth.exceptions.GoogleAuthError:
            # Only the credential refresh raises this, before the request is
            # sent; the session refreshes no token after a response.
            raise
        except BaseException:
            if write:
                self.ambiguous = True
            raise
        if write and response.status_code >= 500:
            self.ambiguous = True
        return response

    def settled(self):
        """Whether every write this session sent has a definite outcome."""
        return not self.ambiguous
