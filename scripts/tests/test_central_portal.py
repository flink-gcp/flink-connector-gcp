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
"""Synthetic subprocess logs and Portal responses; no publishing credentials required."""

import base64
import http.client
import io
import json
import sys
import urllib.error
from collections import deque

import pytest
from conftest import load_script

FIRST = "cbdbc784-e398-4996-8082-b9cbff37460c"
SECOND = "bb77b450-7129-495d-8ad2-fb09fc1bfae2"


@pytest.fixture
def helper(monkeypatch):
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    return load_script("central-portal.py")


def upload_line(deployment_id=FIRST):
    return (
        "[INFO] Uploaded bundle successfully, deployment name: "
        "flink-connector-gcp 1.0.0 (run 33323702321), "
        f"deploymentId: {deployment_id}. Deployment will require manual publishing"
    )


def command(lines, exit_code=0):
    return [sys.executable, "-c", f"print({lines!r}); raise SystemExit({exit_code})"]


def test_capture_id_and_preserve_console(helper, tmp_path, capsys):
    destination = tmp_path / "id"
    log = "\x1b[32m" + upload_line() + "\x1b[0m\n[INFO] BUILD SUCCESS"
    assert helper.capture(destination, command(log)) == 0
    assert destination.read_text() == FIRST + "\n"
    output = capsys.readouterr().out
    assert all(line in output for line in log.splitlines())


def test_id_is_saved_before_validation_finishes(helper, tmp_path):
    destination = tmp_path / "id"

    # Check the file between two output lines, without a sleep or elapsed-time
    # assertion. The real-child tests below separately hold process exit handling.
    class Process:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        @property
        def stdout(self):
            yield upload_line() + "\n"
            assert destination.read_text() == FIRST + "\n"
            yield "[ERROR] validation failed\n"

        def wait(self):
            return 7

    original = helper.subprocess.Popen
    try:
        helper.subprocess.Popen = lambda *args, **kwargs: Process()
        assert helper.capture(destination, ["fake-maven"]) == 7
    finally:
        helper.subprocess.Popen = original


@pytest.mark.parametrize("exit_code", [1, 9])
def test_real_child_failure_is_not_masked_by_capture(helper, tmp_path, exit_code):
    destination = tmp_path / "id"
    assert helper.capture(destination, command(upload_line(), exit_code)) == exit_code
    assert destination.read_text() == FIRST + "\n"


@pytest.mark.parametrize(
    "log",
    [
        "[INFO] BUILD SUCCESS",
        upload_line("not-a-uuid"),
        upload_line() + "\n" + upload_line(SECOND),
    ],
)
def test_missing_or_ambiguous_id_fails(helper, tmp_path, log):
    with pytest.raises(helper.PortalError, match="expected one staging deployment ID"):
        helper.capture(tmp_path / "id", command(log))


def test_repeated_same_id_is_one_upload(helper, tmp_path):
    destination = tmp_path / "id"
    assert (
        helper.capture(destination, command(upload_line() + "\n" + upload_line())) == 0
    )
    assert destination.read_text() == FIRST + "\n"


def test_capture_refuses_stale_file_without_running_command(helper, tmp_path):
    destination = tmp_path / "id"
    destination.write_text(FIRST + "\n")
    with pytest.raises(FileExistsError):
        helper.capture(destination, ["command-that-must-not-run"])


class FakePortal:
    def __init__(self, states, fail_request=None):
        self.states = {key: deque(value) for key, value in states.items()}
        self.calls = []
        self.fail_request = fail_request

    def status(self, deployment_id, **kwargs):
        self.calls.append(("status", deployment_id))
        sequence = self.states[deployment_id]
        value = sequence.popleft() if len(sequence) > 1 else sequence[0]
        if isinstance(value, Exception):
            raise value
        return value

    def request(self, method, path, **kwargs):
        self.calls.append((method, path))
        if (method, path) == self.fail_request:
            raise self.error("response lost")


@pytest.mark.parametrize("bad_state", ["FAILED", "PENDING", "VALIDATING", "DROPPED"])
@pytest.mark.parametrize("bad_id", [FIRST, SECOND])
def test_either_unready_deployment_prevents_every_publish(helper, bad_state, bad_id):
    states = {FIRST: ["VALIDATED"], SECOND: ["VALIDATED"]}
    states[bad_id] = [bad_state]
    portal = FakePortal(states)
    with pytest.raises(helper.PortalError, match="no Publish request was sent"):
        helper.publish(portal, [FIRST, SECOND])
    assert all(method == "status" for method, _ in portal.calls)


def test_failed_recovery_does_not_claim_neither_line_is_published(helper):
    portal = FakePortal({FIRST: ["PUBLISHED"], SECOND: ["FAILED"]})
    with pytest.raises(helper.PortalError, match="no Publish request was sent"):
        helper.publish(portal, [FIRST, SECOND])
    assert portal.calls == [("status", FIRST), ("status", SECOND)]


def test_checks_both_then_requests_both_then_waits(helper, monkeypatch):
    portal = FakePortal(
        {
            FIRST: ["VALIDATED", "PUBLISHING", "PUBLISHED"],
            SECOND: ["VALIDATED", "PUBLISHED"],
        }
    )
    sleeps = []
    monkeypatch.setattr(helper.time, "sleep", sleeps.append)
    helper.publish(portal, [FIRST, SECOND])
    assert portal.calls[:4] == [
        ("status", FIRST),
        ("status", SECOND),
        ("POST", f"deployment/{FIRST}"),
        ("POST", f"deployment/{SECOND}"),
    ]
    assert sleeps == [helper.POLL_INTERVAL]
    assert portal.calls.count(("status", FIRST)) == 3


@pytest.mark.parametrize("state", ["PUBLISHING", "PUBLISHED"])
def test_resume_does_not_republish_accepted_id(helper, state):
    portal = FakePortal(
        {FIRST: [state, "PUBLISHED"], SECOND: ["VALIDATED", "PUBLISHED"]}
    )
    helper.publish(portal, [FIRST, SECOND])
    assert [(method, path) for method, path in portal.calls if method == "POST"] == [
        ("POST", f"deployment/{SECOND}")
    ]


def test_lost_publish_response_is_not_blindly_retried(helper):
    portal = FakePortal(
        {FIRST: ["VALIDATED"], SECOND: ["VALIDATED"]}, ("POST", f"deployment/{FIRST}")
    )
    portal.error = helper.TransientPortalError
    with pytest.raises(helper.PortalError, match="response lost"):
        helper.publish(portal, [FIRST, SECOND])
    assert portal.calls.count(("POST", f"deployment/{FIRST}")) == 1
    assert ("POST", f"deployment/{SECOND}") not in portal.calls


def test_publication_failure_is_fatal(helper):
    portal = FakePortal(
        {FIRST: ["VALIDATED", "FAILED"], SECOND: ["VALIDATED", "PUBLISHED"]}
    )
    with pytest.raises(helper.PortalError, match="FAILED after Publish"):
        helper.publish(portal, [FIRST, SECOND])


def test_publication_wait_has_a_deadline(helper, monkeypatch):
    portal = FakePortal({FIRST: ["PUBLISHING"], SECOND: ["PUBLISHED"]})
    times = iter([0, helper.WAIT_TIMEOUT])
    monkeypatch.setattr(helper.time, "monotonic", lambda: next(times))
    with pytest.raises(helper.PortalError, match="timed out"):
        helper.publish(portal, [FIRST, SECOND])


def test_transient_poll_failure_retries_only_status(helper, monkeypatch):
    portal = FakePortal(
        {
            FIRST: ["VALIDATED", helper.TransientPortalError("offline"), "PUBLISHED"],
            SECOND: ["VALIDATED", "PUBLISHED"],
        }
    )
    sleeps = []
    monkeypatch.setattr(helper.time, "sleep", sleeps.append)
    helper.publish(portal, [FIRST, SECOND])
    assert sleeps == [helper.POLL_INTERVAL]
    assert portal.calls[-1] == ("status", FIRST)
    assert [call for call in portal.calls if call[0] == "POST"] == [
        ("POST", f"deployment/{FIRST}"),
        ("POST", f"deployment/{SECOND}"),
    ]


def test_transient_preflight_failure_still_prevents_every_publish(helper):
    portal = FakePortal(
        {FIRST: [helper.TransientPortalError("offline")], SECOND: ["VALIDATED"]}
    )
    with pytest.raises(helper.TransientPortalError):
        helper.publish(portal, [FIRST, SECOND])
    assert portal.calls == [("status", FIRST)]


def test_permanent_poll_failure_is_not_retried(helper):
    portal = FakePortal(
        {
            FIRST: ["VALIDATED", helper.PortalError("invalid response")],
            SECOND: ["VALIDATED", "PUBLISHED"],
        }
    )
    with pytest.raises(helper.PortalError, match="invalid response"):
        helper.publish(portal, [FIRST, SECOND])
    assert portal.calls.count(("status", FIRST)) == 2


def test_transient_poll_failures_still_reach_deadline(helper, monkeypatch):
    portal = FakePortal(
        {
            FIRST: ["PUBLISHING", helper.TransientPortalError("offline")],
            SECOND: ["PUBLISHED"],
        }
    )
    times = iter([0, helper.WAIT_TIMEOUT])
    monkeypatch.setattr(helper.time, "monotonic", lambda: next(times))
    with pytest.raises(helper.PortalError, match="timed out"):
        helper.publish(portal, [FIRST, SECOND])


def test_drop_attempts_both_even_when_first_fails(helper):
    portal = FakePortal(
        {FIRST: ["VALIDATED"], SECOND: ["FAILED"]}, ("DELETE", f"deployment/{FIRST}")
    )
    portal.error = helper.PortalError
    with pytest.raises(helper.PortalError, match="response lost"):
        helper.drop(portal, [FIRST, SECOND])
    assert ("DELETE", f"deployment/{SECOND}") in portal.calls


@pytest.mark.parametrize("state", ["PENDING", "VALIDATING", "PUBLISHING", "PUBLISHED"])
def test_drop_refuses_unsafe_states(helper, state):
    portal = FakePortal({FIRST: [state]})
    with pytest.raises(helper.PortalError, match="cannot Drop"):
        helper.drop(portal, [FIRST])
    assert portal.calls == [("status", FIRST)]


def test_drop_already_missing_is_success(helper):
    portal = FakePortal({FIRST: ["DROPPED"]})
    helper.drop(portal, [FIRST])
    assert portal.calls == [("status", FIRST)]


@pytest.fixture
def client(helper, monkeypatch):
    monkeypatch.setenv("CENTRAL_TOKEN_USERNAME", "synthetic-user")
    monkeypatch.setenv("CENTRAL_TOKEN_PASSWORD", "synthetic-secret")
    return helper.Portal()


class Response(io.BytesIO):
    def __init__(self, body, status=200):
        super().__init__(body)
        self.status = status


def test_request_auth_method_timeout_and_json(client, helper, monkeypatch):
    calls = []

    def open_request(request, timeout):
        calls.append((request, timeout))
        return Response(
            json.dumps({"deploymentId": FIRST, "deploymentState": "VALIDATED"}).encode()
        )

    monkeypatch.setattr(client.opener, "open", open_request)
    assert client.status(FIRST) == "VALIDATED"
    request, timeout = calls[0]
    assert request.full_url == f"{helper.BASE_URL}/status?id={FIRST}"
    assert request.method == "POST"
    assert timeout == 30
    expected = base64.b64encode(b"synthetic-user:synthetic-secret").decode()
    assert request.get_header("Authorization") == f"Bearer {expected}"


@pytest.mark.parametrize("method", ["POST", "DELETE"])
def test_mutation_requires_204(client, helper, monkeypatch, method):
    monkeypatch.setattr(client.opener, "open", lambda *a, **kw: Response(b"", 200))
    with pytest.raises(helper.PortalError, match="unexpected HTTP 200"):
        client.request(method, f"deployment/{FIRST}")
    monkeypatch.setattr(client.opener, "open", lambda *a, **kw: Response(b"", 204))
    assert client.request(method, f"deployment/{FIRST}") == b""


@pytest.mark.parametrize(
    ("status", "transient"),
    [
        (401, False),
        (403, False),
        (404, False),
        (429, True),
        (500, True),
        (503, True),
        (600, False),
    ],
)
def test_http_error_does_not_expose_response_or_auth(
    client, helper, monkeypatch, status, transient
):
    body = io.BytesIO(b"synthetic-secret")

    def fail(*args, **kwargs):
        raise urllib.error.HTTPError("url", status, "synthetic-secret", {}, body)

    monkeypatch.setattr(client.opener, "open", fail)
    with pytest.raises(helper.PortalError, match=f"HTTP {status}") as caught:
        client.status(FIRST)
    assert "synthetic-secret" not in str(caught.value)
    assert body.closed
    assert isinstance(caught.value, helper.TransientPortalError) == transient


def test_missing_only_accepted_for_drop(client, helper, monkeypatch):
    def fail(*args, **kwargs):
        raise urllib.error.HTTPError("url", 404, "missing", {}, io.BytesIO())

    monkeypatch.setattr(client.opener, "open", fail)
    assert client.status(FIRST, allow_missing=True) == "DROPPED"
    with pytest.raises(helper.PortalError, match="HTTP 404"):
        client.status(FIRST)


@pytest.mark.parametrize(
    "body",
    [
        b"not JSON",
        b"[]",
        b"{}",
        b"\xff",
        json.dumps({"deploymentId": SECOND, "deploymentState": "VALIDATED"}).encode(),
        json.dumps({"deploymentId": FIRST, "deploymentState": "UNKNOWN"}).encode(),
    ],
)
def test_bad_status_response_cannot_authorize_publish(
    client, helper, monkeypatch, body
):
    monkeypatch.setattr(client.opener, "open", lambda *a, **kw: Response(body))
    with pytest.raises(helper.PortalError, match="invalid Portal status response"):
        client.status(FIRST)


def test_connection_error_does_not_expose_exception(client, helper, monkeypatch):
    def fail(*args, **kwargs):
        raise urllib.error.URLError("synthetic-secret")

    monkeypatch.setattr(client.opener, "open", fail)
    with pytest.raises(
        helper.TransientPortalError, match="connection failed"
    ) as caught:
        client.status(FIRST)
    assert "synthetic-secret" not in str(caught.value)


def test_poll_summary_records_transitions_only(
    client, helper, monkeypatch, tmp_path, capsys
):
    destination = tmp_path / "summary"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(destination))
    states = {
        FIRST: deque(["VALIDATED", "PUBLISHING", "PUBLISHING", "PUBLISHED"]),
        SECOND: deque(["VALIDATED", "PUBLISHED"]),
    }

    def request(method, path, **kwargs):
        if path.startswith("status?"):
            deployment_id = path.removeprefix("status?id=")
            return json.dumps(
                {
                    "deploymentId": deployment_id,
                    "deploymentState": states[deployment_id].popleft(),
                }
            ).encode()
        return b""

    monkeypatch.setattr(client, "request", request)
    monkeypatch.setattr(helper.time, "sleep", lambda _: None)
    helper.publish(client, [FIRST, SECOND])
    message = f"Central deployment `{FIRST}`: PUBLISHING."
    assert destination.read_text().count(message) == 1
    assert capsys.readouterr().out.count(message) == 2


def test_redirect_does_not_forward_auth(helper):
    assert (
        helper.NoRedirect().redirect_request(None, None, 302, "", {}, "https://other")
        is None
    )


@pytest.mark.parametrize(
    "ids",
    [
        ["", SECOND],
        [FIRST, FIRST],
        [FIRST + "\n" + SECOND, SECOND],
        ["../other", SECOND],
    ],
)
def test_invalid_id_set_fails_before_credentials_or_network(helper, ids, capsys):
    assert helper.main(["publish", *ids]) == 1
    assert "CENTRAL_TOKEN" not in capsys.readouterr().out


def test_missing_credentials_is_actionable(helper, monkeypatch, capsys):
    monkeypatch.delenv("CENTRAL_TOKEN_USERNAME", raising=False)
    assert helper.main(["status", FIRST]) == 1
    assert (
        "set CENTRAL_TOKEN_USERNAME and CENTRAL_TOKEN_PASSWORD"
        in capsys.readouterr().out
    )


def test_cli_capture_summary_survives_child_failure(helper, tmp_path, monkeypatch):
    summary = tmp_path / "summary"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    assert (
        helper.main(["capture", str(tmp_path / "id"), *command(upload_line(), 1)]) == 1
    )
    assert FIRST in summary.read_text()
    assert "no publication authorized" in summary.read_text()


def test_cli_status_and_drop_return_nonzero_for_api_error(helper, monkeypatch):
    class UnavailablePortal:
        def status(self, *args, **kwargs):
            raise helper.PortalError("offline")

    monkeypatch.setattr(helper, "Portal", UnavailablePortal)
    assert helper.main(["status", FIRST]) == 1
    assert helper.main(["drop", FIRST, SECOND]) == 1


def test_truncated_status_still_attempts_the_second_drop(client, helper, monkeypatch):
    calls = []

    class TruncatedResponse(Response):
        def read(self):
            raise http.client.IncompleteRead(b"synthetic-secret", 100)

    def open_request(request, **kwargs):
        calls.append((request.method, request.full_url))
        if request.full_url.endswith(f"status?id={FIRST}"):
            return TruncatedResponse(b"")
        if request.full_url.endswith(f"status?id={SECOND}"):
            return Response(
                json.dumps(
                    {"deploymentId": SECOND, "deploymentState": "VALIDATED"}
                ).encode()
            )
        return Response(b"", 204)

    monkeypatch.setattr(client.opener, "open", open_request)
    with pytest.raises(helper.PortalError, match="connection failed") as caught:
        helper.drop(client, [FIRST, SECOND])
    assert "synthetic-secret" not in str(caught.value)
    assert ("DELETE", f"{helper.BASE_URL}/deployment/{SECOND}") in calls


@pytest.mark.parametrize(
    "ids", [[FIRST], [FIRST, SECOND, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]]
)
def test_publish_requires_exactly_two_ids_before_authentication(
    helper, monkeypatch, ids
):
    def unexpected_client():
        raise AssertionError("invalid publish arity must not reach authentication")

    monkeypatch.setattr(helper, "Portal", unexpected_client)
    with pytest.raises(SystemExit) as caught:
        helper.main(["publish", *ids])
    assert caught.value.code == 2
