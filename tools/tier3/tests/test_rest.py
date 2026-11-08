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
"""What a refused REST call keeps of the service's answer."""

import pytest
from flink_tier3 import rest
from flink_tier3.common import ApiError

from .bigquery.test_resources import NOW, Http, Response


class Service(rest.JsonService):
    label = "Example"
    base = "https://example.googleapis.com/"

    def __init__(self, *responses, clock=lambda: NOW):
        self.http = Http(*responses)
        self.deadline, self.clock = NOW + 60, clock


class Fragmented(Response):
    """A body the transport hands over in the pieces it arrived in."""

    def __init__(self, pieces, status):
        super().__init__(b"".join(pieces), status)
        self.pieces = pieces

    def iter_content(self, size):
        yield from self.pieces


def refusal(response, clock=lambda: NOW):
    with pytest.raises(ApiError) as raised:
        Service(response, clock=clock)._call("POST", "v1/thing:commit", {})
    return raised.value


def test_a_refusal_keeps_the_services_message():
    error = refusal(
        Response({"error": {"code": 400, "message": "Missing routing header."}}, 400)
    )
    assert error.status == 400
    assert str(error) == "POST v1/thing:commit: HTTP 400: Missing routing header."


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b"<html>Bad Request</html>",
        b'{"error": "flat"}',
        b"[1]",
        b'{"error": {}}',
        b'{"error": {"message": "  "}}',
    ],
)
def test_a_refusal_without_a_google_error_message_names_only_its_status(body):
    assert str(refusal(Response(body, 403))) == "POST v1/thing:commit: HTTP 403"


def test_a_long_message_is_shortened_and_kept_on_one_line():
    message = "first line\n" + "x" * 1000
    error = str(refusal(Response({"error": {"message": message}}, 400)))
    kept = error.split("HTTP 400: ", 1)[1]
    assert kept.startswith("first line x")
    assert "\n" not in kept
    assert len(kept) == rest.ERROR_MESSAGE_CHARS + 1


def test_a_message_of_exactly_the_limit_is_kept_whole():
    message = "x" * rest.ERROR_MESSAGE_CHARS
    error = str(refusal(Response({"error": {"message": message}}, 400)))
    assert error.endswith("HTTP 400: " + message)


def test_a_message_split_across_chunks_is_kept():
    pieces = [
        b'{"error":',
        b' {"code": 400, "message": "Missing',
        b' routing header."}}',
    ]
    error = refusal(Fragmented(pieces, 400))
    assert str(error).endswith("HTTP 400: Missing routing header.")


def test_a_body_still_arriving_at_the_deadline_leaves_the_status_alone():
    times = iter([NOW, NOW, NOW + 30, NOW + 61, NOW + 62])
    pieces = [b'{"error":', b' {"message": "late"}}']
    error = refusal(Fragmented(pieces, 503), clock=lambda: next(times))
    assert error.status == 503
    assert str(error) == "POST v1/thing:commit: HTTP 503"


def test_only_the_first_bytes_of_a_body_are_parsed():
    head = b'{"error": {"message": "denied"}}'
    padding = b" " * (rest.ERROR_BODY_BYTES - len(head))
    within = refusal(Fragmented([head, padding, b"   "], 400))
    assert str(within).endswith("HTTP 400: denied")
    beyond = b'{"error": {"message": "' + b"x" * rest.ERROR_BODY_BYTES + b'"}}'
    pieces = [beyond[i : i + 1000] for i in range(0, len(beyond), 1000)]
    assert str(refusal(Fragmented(pieces, 400))).endswith("HTTP 400")
