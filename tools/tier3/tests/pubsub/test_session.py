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
"""The Pub/Sub actor session's record of unsettled writes."""

import io

import google.auth.exceptions
import pytest
import requests
from flink_tier3.google import actor_session
from flink_tier3.pubsub.resources import BASE
from flink_tier3.pubsub.session import PubSubSession
from google.oauth2.credentials import Credentials

TOPIC = BASE + "projects/flink-gcp/topics/t"


class Response:
    def __init__(self, status):
        self.status_code = status


class Http:
    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.closed = False
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        value = self.outcomes.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value

    def close(self):
        self.closed = True


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
        (Response(200), False),
        (Response(404), False),
        (Response(409), False),
        (Response(429), False),
        (Response(500), True),
        (Response(503), True),
        (requests.exceptions.ReadTimeout(), True),
        (requests.exceptions.ConnectionError(), True),
    ],
)
@pytest.mark.parametrize("method, url", WRITES + READS)
def test_only_a_write_without_a_definite_answer_is_unsettled(
    method, url, outcome, unsettled
):
    with PubSubSession(Http(outcome)) as http:
        if isinstance(outcome, Exception):
            with pytest.raises(type(outcome)):
                http.request(method, url, json={}, timeout=5)
        else:
            assert http.request(method, url, json={}, timeout=5) is outcome
        assert http.settled() is not (unsettled and (method, url) in WRITES)


def test_the_request_reaches_the_transport_whole():
    transport = Http(Response(200))
    with PubSubSession(transport) as http:
        http.request("POST", TOPIC + ":publish", json={"messages": []}, timeout=5)
    assert transport.calls == [
        ("POST", TOPIC + ":publish", {"json": {"messages": []}, "timeout": 5})
    ]


@pytest.mark.parametrize(
    "error",
    [
        google.auth.exceptions.RefreshError("refused"),
        google.auth.exceptions.TransportError("metadata server"),
    ],
)
def test_a_write_whose_token_could_not_be_refreshed_is_settled(error):
    """The refresh runs before the request is sent, so nothing reached the service."""
    with PubSubSession(Http(error)) as http:
        with pytest.raises(type(error)):
            http.request("PUT", TOPIC, json={}, timeout=5)
        assert http.settled()


def test_an_interrupted_write_is_unsettled():
    with PubSubSession(Http(KeyboardInterrupt())) as http:
        with pytest.raises(KeyboardInterrupt):
            http.request("PUT", TOPIC, json={}, timeout=5)
        assert not http.settled()


def test_the_record_outlives_later_definite_answers():
    with PubSubSession(Http(Response(502), Response(200), Response(200))) as http:
        http.request("PUT", TOPIC, json={}, timeout=5)
        http.request("GET", TOPIC, timeout=5)
        http.request("DELETE", TOPIC, timeout=5)
        assert not http.settled()


def test_closing_the_session_closes_its_transport():
    transport = Http()
    with PubSubSession(transport):
        assert not transport.closed
    assert transport.closed


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


class Redirecting(requests.adapters.BaseAdapter):
    """Answers every request locally with a redirect and a large body."""

    def __init__(self, body):
        super().__init__()
        self.body = body

    def send(self, request, **kwargs):
        response = requests.Response()
        response.request, response.url, response.connection = request, request.url, self
        response.status_code = 302
        response.headers["Location"] = TOPIC + "-elsewhere"
        response.raw = self.body
        return response

    def close(self):
        pass


def test_an_actor_session_returns_a_redirect_unread():
    """requests reads a redirect's body whole unless it sees no target."""
    body = Raw(b" " * (4 * 1024 * 1024))
    with PubSubSession(actor_session(Credentials("token"))) as http:
        http.http.mount("https://", Redirecting(body))
        response = http.request(
            "PUT", TOPIC, json={}, timeout=5, allow_redirects=False, stream=True
        )
        assert response.status_code == 302
        # A write answered below 500 has a definite outcome.
        assert http.settled()
    assert body.taken == 0
