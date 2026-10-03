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
"""The Pub/Sub actor session: its identity checks and its unsettled-write latch."""

import io
import json

import pytest
import requests
from flink_tier3 import pubsub_auth as auth
from flink_tier3.actor_auth import PRINCIPALS, USERINFO
from flink_tier3.common import ApiError, Failure, TransportError
from flink_tier3.pubsub import BASE, ResourcePlan, Resources
from google.oauth2.credentials import Credentials
from test_bigquery_auth import Response, identity
from test_bigquery_auth import wire as wire  # noqa: PLC0414

TOPIC = BASE + "projects/p/topics/t"


def session(role="runner"):
    return auth.PubSubSession(role, Credentials("token"), monotonic=lambda: 0)


def test_the_session_verifies_the_actor_before_its_first_request(wire):
    wire.responses = [identity("supervisor"), {"name": "t"}]
    with session("supervisor") as http:
        assert http.request("GET", TOPIC, timeout=5).status_code == 200
    assert [call[1] for call in wire.calls] == [USERINFO, TOPIC]
    assert wire.calls[1][2]["headers"]["authorization"] == "Bearer token"
    assert http.principal == PRINCIPALS["supervisor"]


@pytest.mark.parametrize(
    "method, url",
    [
        ("PATCH", TOPIC),
        ("GET", "https://bigquery.googleapis.com/bigquery/v2/projects/p"),
        ("GET", "https://pubsub.googleapis.com/v1beta2/projects/p/topics/t"),
    ],
)
def test_the_session_sends_only_pubsub_methods_and_urls(wire, method, url):
    with session() as http, pytest.raises(Failure, match="Unexpected Pub/Sub"):
        http.request(method, url, timeout=5)
    assert wire.calls == []
    assert http.settled()


def test_a_different_identity_is_refused_before_the_write_leaves(wire):
    wire.responses = [identity("supervisor")]
    with session() as http, pytest.raises(Failure, match="differs from the Pub/Sub"):
        http.request("PUT", TOPIC, json={}, timeout=5)
    assert [call[1] for call in wire.calls] == [USERINFO]
    assert http.settled()


# Every request but a GET or a permission test counts as a write.
WRITES = [
    ("PUT", TOPIC),
    ("DELETE", TOPIC),
    ("POST", TOPIC + ":setIamPolicy"),
    ("POST", TOPIC + ":publish"),
    ("POST", TOPIC + ":pull"),
]
READS = [("GET", TOPIC), ("POST", TOPIC + ":testIamPermissions")]


@pytest.mark.parametrize(
    "outcome, unsettled",
    [
        (Response({}, 200), False),
        (Response({}, 404), False),
        (Response({}, 409), False),
        (Response({}, 429), False),
        (Response({}, 500), True),
        (Response({}, 503), True),
        (requests.exceptions.ReadTimeout(), True),
        (requests.exceptions.ConnectionError(), True),
    ],
)
@pytest.mark.parametrize("method, url", WRITES + READS)
def test_only_a_write_without_a_definite_answer_is_unsettled(
    wire, method, url, outcome, unsettled
):
    wire.responses = [identity(), outcome]
    with session() as http:
        if isinstance(outcome, Exception):
            with pytest.raises(TransportError):
                http.request(method, url, json={}, timeout=5)
        else:
            assert http.request(method, url, json={}, timeout=5) is outcome
        assert http.settled() is not (unsettled and (method, url) in WRITES)


def test_a_write_whose_budget_expires_after_sending_is_unsettled(wire):
    now = [0]
    wire.responses = [identity(), Response({})]
    wire.hook = lambda count: now.__setitem__(0, 10 if count == 2 else now[0])
    with (
        auth.PubSubSession(
            "runner", Credentials("token"), monotonic=lambda: now[0]
        ) as http,
        pytest.raises(Failure, match="budget expired"),
    ):
        http.request("PUT", TOPIC, json={}, timeout=5)
    assert not http.settled()


def test_the_latch_outlives_later_definite_answers(wire):
    wire.responses = [identity(), Response({}, 502), Response({}), Response({})]
    with session() as http:
        http.request("PUT", TOPIC, json={}, timeout=5)
        http.request("GET", TOPIC, timeout=5)
        http.request("DELETE", TOPIC, timeout=5)
        assert not http.settled()


def test_a_request_refused_after_a_settled_write_stays_settled(wire):
    wire.responses = [identity(), Response({})]
    with session() as http:
        http.request("PUT", TOPIC, json={}, timeout=5)
        with pytest.raises(Failure, match="Unexpected Pub/Sub"):
            http.request("PATCH", TOPIC, timeout=5)
        assert http.settled()


def test_a_write_whose_budget_expires_before_it_leaves_stays_settled(wire):
    """The timeout is computed before the request counts as sent."""
    now, calls = [0], [0]

    def monotonic():
        calls[0] += 1
        return now[0]

    wire.responses = [identity(), Response({})]
    with auth.PubSubSession(
        "runner", Credentials("token"), monotonic=monotonic
    ) as http:
        http.request("GET", TOPIC, timeout=5)
        # Budget, then the header check: the third reading is the send's.
        calls[0] = 0
        original = monotonic

        def expiring():
            value = original()
            return value + (10 if calls[0] >= 3 else 0)

        http.monotonic = expiring
        with pytest.raises(Failure, match="budget expired"):
            http.request("PUT", TOPIC, json={}, timeout=5)
        assert [call[0] for call in wire.calls] == ["GET", "GET"]
        assert http.settled()


class Raw(io.BytesIO):
    """A response body that records how much of it was read."""

    def __init__(self, data):
        super().__init__(data)
        self.taken = 0

    def read(self, size=-1, **kwargs):
        chunk = super().read(size)
        self.taken += len(chunk)
        return chunk

    def stream(self, size, decode_content=True):
        while chunk := self.read(size):
            yield chunk

    def release_conn(self):
        pass


class Adapter(requests.adapters.BaseAdapter):
    """Real requests sessions, answered locally: identity, then a redirect."""

    def __init__(self, body):
        super().__init__()
        self.body = body

    def send(self, request, **kwargs):
        response = requests.Response()
        response.request, response.url, response.connection = request, request.url, self
        if request.url == USERINFO:
            response.status_code = 200
            response.raw = Raw(json.dumps(identity()).encode())
        else:
            response.status_code = 302
            response.headers["Location"] = BASE + "projects/p/topics/elsewhere"
            response.raw = self.body
        return response

    def close(self):
        pass


def test_a_redirect_is_returned_unread_and_refused_as_a_status():
    """requests reads a redirect's body whole unless it sees no target."""
    body = Raw(b" " * (4 * 1024 * 1024))
    with session() as http:
        http.http.mount("https://", Adapter(body))
        resources = Resources(
            http, None, ResourcePlan("probe", "a" * 32), lambda *args: None
        )
        with pytest.raises(ApiError) as error:
            resources._request("inspect", "GET", "projects/p/topics/t")
    assert error.value.status == 302
    assert body.taken == 0
