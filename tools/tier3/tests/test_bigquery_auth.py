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
"""Exercise the real google-auth request adapter with synthetic OAuth/HTTP replies."""

import json

import pytest
import requests
from flink_tier3 import bigquery_auth as auth
from flink_tier3.bigquery_resources import BASE, BigQueryResources
from flink_tier3.common import ApiError, Failure, TransportError
from google.oauth2.credentials import Credentials
from test_bigquery_resources import NOW, table
from test_bigquery_resources import plan as plan  # noqa: PLC0414


class Response:
    def __init__(self, value, status=200):
        self.content = value if isinstance(value, bytes) else json.dumps(value).encode()
        self.status_code = status
        self.headers = {}
        self.closed = False

    def iter_content(self, size):
        for i in range(0, len(self.content), size):
            yield self.content[i : i + size]

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class Http:
    def __init__(self):
        self.responses = []
        self.calls = []
        self.closed = False
        self.hook = None

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if self.hook:
            self.hook(len(self.calls))
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return value if isinstance(value, Response) else Response(value)

    def close(self):
        self.closed = True


def identity(role="runner"):
    return {"email": auth.PRINCIPALS[role], "verified_email": True}


@pytest.fixture
def wire(monkeypatch):
    http = Http()
    monkeypatch.setattr(auth.requests, "Session", lambda: http)
    return http


@pytest.mark.parametrize("role", ["runner", "supervisor"])
def test_real_credentials_and_adapter_share_identity_and_resource_token(
    wire, plan, role
):
    now = [0]
    credentials = Credentials(
        None,
        refresh_token="test-refresh",
        client_id="test-client",
        client_secret="test-secret",
        token_uri="https://oauth2.googleapis.com/token",
    )
    wire.responses = [
        {"access_token": "test-access", "expires_in": 3600, "token_type": "Bearer"},
        identity(role),
        table(plan),
        table(plan),
    ]
    wire.hook = lambda count: now.__setitem__(0, now[0] + (1 if count <= 2 else 0))
    with auth.BigQuerySession(role, credentials, monotonic=lambda: now[0]) as http:
        api = BigQueryResources(http, plan, NOW + 5, clock=lambda: NOW + now[0])
        api.table(0)
        api.table(0)
        assert [call[2]["timeout"] for call in wire.calls] == [5, 4, 3, 3]
        assert wire.calls[1][1] == auth.USERINFO
        assert all(
            call[2]["headers"]["authorization"] == "Bearer test-access"
            for call in wire.calls[1:]
        )
        assert all(call[2]["allow_redirects"] is False for call in wire.calls)
        # Streaming is what lets the 16 KiB identity cap and the 1 MiB resource
        # cap refuse a body; a buffered read would already hold the whole one.
        assert all(call[2]["stream"] is True for call in wire.calls[1:])
        assert wire.trust_env is False and wire.max_redirects == 0
    assert wire.closed
    with pytest.raises(Failure, match="closed"):
        http.authenticate(timeout=1)


def test_token_change_requires_new_identity_before_next_resource(wire, plan):
    credentials = Credentials("first")
    wire.responses = [identity(), table(plan), identity("supervisor")]
    with auth.BigQuerySession("runner", credentials, monotonic=lambda: 0) as http:
        api = BigQueryResources(http, plan, NOW + 5, clock=lambda: NOW)
        api.table(0)
        credentials.token = "second"
        with pytest.raises(Failure, match="identity differs"):
            api.table(0)
    assert len(wire.calls) == 3
    assert wire.calls[-1][1] == auth.USERINFO
    assert wire.calls[-1][2]["headers"]["authorization"] == "Bearer second"


@pytest.mark.parametrize(
    "reply",
    [
        {"email": "other@example.com", "verified_email": True},
        {"email": auth.PRINCIPALS["runner"], "verified_email": "true"},
        {"email": auth.PRINCIPALS["runner"]},
        [],
        b"bad json",
        {
            "email": auth.PRINCIPALS["runner"],
            "verified_email": True,
            "extra": "x" * 16384,
        },
    ],
)
def test_bad_identity_never_reaches_bigquery(wire, plan, reply):
    response = Response(reply)
    wire.responses = [response]
    with (
        auth.BigQuerySession(
            "runner", Credentials("token"), monotonic=lambda: 0
        ) as http,
        pytest.raises(Failure),
    ):
        BigQueryResources(http, plan, NOW + 10, clock=lambda: NOW).table(0)
    assert len(wire.calls) == 1
    assert response.closed and wire.closed


@pytest.mark.parametrize("status", [301, 401, 403, 503])
def test_identity_errors_are_not_retried_or_redirected(wire, status):
    response = Response({}, status)
    wire.responses = [response]
    with (
        auth.BigQuerySession(
            "runner", Credentials("token"), monotonic=lambda: 0
        ) as http,
        pytest.raises(Failure, match="verification failed"),
    ):
        http.authenticate(timeout=2)
    assert len(wire.calls) == 1 and response.closed


def test_api_401_does_not_refresh_or_replay(wire, plan):
    response = Response({}, 401)
    wire.responses = [identity(), response]
    with auth.BigQuerySession(
        "runner", Credentials("token"), monotonic=lambda: 0
    ) as http:
        with pytest.raises(ApiError) as error:
            BigQueryResources(http, plan, NOW + 5, clock=lambda: NOW).table(0)
        assert error.value.status == 401
    assert len(wire.calls) == 2 and response.closed


@pytest.mark.parametrize("stage", ["refresh", "identity", "resource"])
def test_budget_exhaustion_stops_following_calls_and_closes_responses(
    wire, plan, stage
):
    now = [0]
    credentials = Credentials(
        None,
        refresh_token="test-refresh",
        client_id="test-client",
        client_secret="test-secret",
        token_uri="https://oauth2.googleapis.com/token",
    )
    replies = [
        Response({"access_token": "test-access", "expires_in": 3600}),
        Response(identity()),
        Response(table(plan)),
    ]
    wire.responses = replies.copy()
    last = {"refresh": 1, "identity": 2, "resource": 3}[stage]
    wire.hook = lambda count: now.__setitem__(0, 2 if count == last else 0)
    with (
        auth.BigQuerySession("runner", credentials, monotonic=lambda: now[0]) as http,
        pytest.raises(Failure, match="budget expired"),
    ):
        BigQueryResources(http, plan, NOW + 2, clock=lambda: NOW + now[0]).table(0)
    assert len(wire.calls) == last
    if stage != "refresh":
        assert replies[last - 1].closed


def test_default_credentials_receive_scopes_and_bounded_refresh_adapter(
    wire, monkeypatch
):
    called = []

    def default(*, scopes, request):
        called.append(scopes)
        request("https://oauth2.googleapis.com/token", timeout=1)
        request("https://oauth2.googleapis.com/token", timeout=3600)
        return Credentials("default-token"), "flink-gcp"

    monkeypatch.setattr(auth.google.auth, "default", default)
    wire.responses = [{}, {}, identity()]
    with auth.BigQuerySession("runner", monotonic=lambda: 0) as http:
        http.authenticate(timeout=3)
    assert called == [auth.SCOPES]
    # A shorter credential timeout is preserved; a longer one cannot buy time
    # the caller's budget does not have.
    assert [call[2]["timeout"] for call in wire.calls] == [1, 3, 3]


def test_asynchronous_refresh_is_refused_before_network(wire):
    credentials = Credentials("token")
    credentials.with_non_blocking_refresh()
    with (
        auth.BigQuerySession("runner", credentials, monotonic=lambda: 0) as http,
        pytest.raises(Failure, match="synchronous"),
    ):
        http.authenticate(timeout=3)
    assert not wire.calls


@pytest.mark.parametrize("timeout", [0, -1, True, None, float("nan"), float("inf")])
def test_invalid_budget_is_refused_before_credentials(wire, timeout):
    with (
        auth.BigQuerySession(
            "runner", Credentials("token"), monotonic=lambda: 0
        ) as http,
        pytest.raises(Failure, match="timeout"),
    ):
        http.authenticate(timeout=timeout)
    assert not wire.calls


@pytest.mark.parametrize(
    "change",
    [
        {"url": "https://example.com/"},
        {"method": "PUT"},
        {"allow_redirects": True},
        {"stream": False},
        {"verify": False},
        {"headers": {"authorization": "wrong"}},
    ],
)
def test_transport_refuses_unexpected_destinations_and_overrides(wire, change):
    args = {"method": "GET", "url": BASE + "/jobs/one", "timeout": 2}
    args.update(change)
    with (
        auth.BigQuerySession(
            "runner", Credentials("token"), monotonic=lambda: 0
        ) as http,
        pytest.raises(Failure, match="Unexpected"),
    ):
        http.request(**args)
    assert not wire.calls


def test_transport_error_does_not_expose_response_or_credential_text(wire):
    wire.responses = [requests.exceptions.ConnectionError("secret-value")]
    with (
        auth.BigQuerySession(
            "runner", Credentials("token"), monotonic=lambda: 0
        ) as http,
        pytest.raises(TransportError) as error,
    ):
        http.authenticate(timeout=1)
    assert "secret-value" not in str(error.value)
    assert len(wire.calls) == 1
