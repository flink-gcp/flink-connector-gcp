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
"""The Pub/Sub lifecycle actors' OAuth session and its record of ambiguous writes."""

from .actor_auth import ActorSession
from .pubsub import BASE


class PubSubSession(ActorSession):
    """One Pub/Sub actor's session, which remembers an unsettled write.

    Every request other than a GET counts as a write. A write is unsettled
    when it left this process and no status came back, or the service
    answered 5xx, or its budget expired after it left: each may still
    complete at the service. A refusal before sending, a read, or a write
    answered below 500 within its budget is settled, whatever the caller then
    makes of the response. The latch is the session's, not a
    call's, and never clears: nothing this session sees later proves the
    earlier write finished. Storage uploads go through another client and are
    not tracked here.
    """

    SERVICE = "Pub/Sub"
    PREFIX = BASE
    METHODS = ("GET", "PUT", "POST", "DELETE")

    def __init__(self, role, credentials=None, **kwargs):
        super().__init__(role, credentials, **kwargs)
        self.ambiguous = False
        self._pending = False

    def _sending(self, method, _url):
        self._pending = method != "GET"

    def request(self, method, url, **kwargs):
        self._pending = False
        try:
            response = super().request(method, url, **kwargs)
        except BaseException:
            if self._pending:
                self.ambiguous = True
            raise
        if self._pending and response.status_code >= 500:
            self.ambiguous = True
        return response

    def settled(self):
        """Whether every write this session sent has a definite outcome."""
        return not self.ambiguous
