# Copyright 2026 laughingman7743
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

"""Tests for the dependency-free App Engine E2E handler."""

import importlib.util
import io
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
HANDLER_PATH = ROOT / "opentofu" / "flink-gcp" / "appengine-e2e" / "main.py"


def load_handler():
    spec = importlib.util.spec_from_file_location("appengine_e2e_handler", HANDLER_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HANDLER = load_handler()


@pytest.mark.parametrize(
    ("path", "expected_status", "expected_headers", "expected_body"),
    [
        ("/accepted", "204 No Content", [], b""),
        ("/redirect", "302 Found", [("Location", "/accepted")], b""),
        (
            "/too-many-requests",
            "429 Too Many Requests",
            [("Content-Type", "text/plain")],
            b"too many requests",
        ),
        (
            "/unavailable",
            "503 Service Unavailable",
            [("Content-Type", "text/plain")],
            b"unavailable",
        ),
    ],
)
def test_handler_response(path, expected_status, expected_headers, expected_body):
    request_body = io.BytesIO(b"request body")
    observed_response = []

    response_body = b"".join(
        HANDLER.app(
            {
                "CONTENT_LENGTH": str(len(request_body.getvalue())),
                "PATH_INFO": path,
                "wsgi.input": request_body,
            },
            lambda status, headers: observed_response.append((status, headers)),
        )
    )

    assert observed_response == [(expected_status, expected_headers)]
    assert response_body == expected_body
    assert request_body.read() == b""
