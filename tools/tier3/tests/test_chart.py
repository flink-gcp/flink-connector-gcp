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

"""Tests for the shared Operator chart download."""

import hashlib
import http.client
import io
import urllib.error

import pytest
from flink_tier3 import chart as CHART

ARCHIVE = (
    "https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.15.0/"
    "flink-kubernetes-operator-1.15.0-helm.tgz"
)
CDN = (
    "https://dlcdn.apache.org/flink/flink-kubernetes-operator-1.15.0/"
    "flink-kubernetes-operator-1.15.0-helm.tgz"
)
DATA = b"chart"
PIN = {"urls": [CDN, ARCHIVE], "sha512": hashlib.sha512(DATA).hexdigest()}


class Response(io.BytesIO):
    """A response that holds read() to the download's bound, as urllib's does.

    A body that fails while it is being read raises from read(), where urllib
    raises a read timeout or a truncated chunked transfer.
    """

    def __init__(self, body):
        self.failure = body if isinstance(body, BaseException) else None
        super().__init__(b"" if self.failure else body)

    def read(self, size=-1):
        assert size == CHART.MAX_CHART_BYTES + 1, "read must stay bounded"
        if self.failure:
            raise self.failure
        return super().read(size)


class WhileReading:
    """Serve a response whose body raises the given error from read()."""

    def __init__(self, error):
        self.error = error


def serve(monkeypatch, responses):
    """Answer each URL from responses: bytes are served, an exception is raised."""
    requested = []

    def urlopen(url, timeout):
        assert timeout == 60, "a hung host must not block the fallback"
        requested.append(url)
        response = responses[url]
        if isinstance(response, BaseException):
            raise response
        if isinstance(response, WhileReading):
            return Response(response.error)
        return Response(response)

    monkeypatch.setattr(CHART.urllib.request, "urlopen", urlopen)
    return requested


def test_pinned_urls_keep_their_order():
    assert CHART.pinned_urls({"urls": [ARCHIVE, CDN]}, "the pin") == [ARCHIVE, CDN]


@pytest.mark.parametrize(
    "pin",
    [
        {},
        {"url": ARCHIVE},
        {"urls": []},
        {"urls": ARCHIVE},
        {"urls": {CDN: None}},
        {"urls": [CDN, None]},
    ],
    ids=["missing", "single-url-key", "empty", "string", "mapping", "not-a-string"],
)
def test_malformed_url_list_is_refused(pin):
    with pytest.raises(ValueError, match="the pin must list.*`urls` array"):
        CHART.pinned_urls(pin, "the pin")


@pytest.mark.parametrize(
    "failure",
    [
        urllib.error.URLError(OSError(101, "Network is unreachable")),
        urllib.error.HTTPError(CDN, 404, "Not Found", {}, None),
        TimeoutError("timed out"),
        WhileReading(TimeoutError("read timed out")),
        WhileReading(http.client.IncompleteRead(b"chunk")),
        b"other bytes",
        b"x" * (2 * 1024 * 1024 + 1),
    ],
    ids=[
        "unreachable",
        "not-found",
        "connect-timeout",
        "read-timeout",
        "truncated",
        "altered",
        "oversized",
    ],
)
def test_failed_first_url_falls_back_to_the_next(monkeypatch, failure):
    requested = serve(monkeypatch, {CDN: failure, ARCHIVE: DATA})
    assert CHART.download(PIN["urls"], PIN["sha512"], "the pin") == DATA
    assert requested == [CDN, ARCHIVE]


def test_every_failure_is_reported_when_no_host_serves_the_chart(monkeypatch):
    serve(
        monkeypatch,
        {
            CDN: urllib.error.HTTPError(CDN, 404, "Not Found", {}, None),
            ARCHIVE: b"other bytes",
        },
    )
    with pytest.raises(ValueError) as error:
        CHART.download(PIN["urls"], PIN["sha512"], "pins/upstream.toml")
    message = str(error.value)
    assert f"{CDN}: HTTP Error 404" in message
    assert (
        f"{ARCHIVE}: Operator chart SHA-512 does not match pins/upstream.toml"
        in message
    )


@pytest.mark.parametrize("length", [7, 8, 9])
def test_size_limit_is_distinct_from_a_hash_error(monkeypatch, length):
    monkeypatch.setattr(CHART, "MAX_CHART_BYTES", 8)
    data = b"a" * length
    if length <= 8:
        CHART.check_bytes(data, hashlib.sha512(data).hexdigest(), "the pin")
    else:
        with pytest.raises(ValueError, match="exceeds the 8-byte limit"):
            CHART.check_bytes(data, hashlib.sha512(data).hexdigest(), "the pin")


def test_size_limit_is_reported_before_a_hash_mismatch(monkeypatch):
    monkeypatch.setattr(CHART, "MAX_CHART_BYTES", 8)
    with pytest.raises(ValueError, match="exceeds the 8-byte limit"):
        CHART.check_bytes(b"a" * 9, "0" * 128, "the pin")
