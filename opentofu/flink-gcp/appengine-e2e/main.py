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

"""Minimal App Engine handler for Cloud Tasks acceptance."""

import os
from http import HTTPStatus
from wsgiref.simple_server import make_server

_DEFAULT_RESPONSE = (HTTPStatus.NO_CONTENT, [], b"")
_RESPONSES = {
    "/redirect": (HTTPStatus.FOUND, [("Location", "/accepted")], b""),
    "/too-many-requests": (
        HTTPStatus.TOO_MANY_REQUESTS,
        [("Content-Type", "text/plain")],
        b"too many requests",
    ),
    "/unavailable": (
        HTTPStatus.SERVICE_UNAVAILABLE,
        [("Content-Type", "text/plain")],
        b"unavailable",
    ),
}


def app(environ, start_response):
    """Return the fixed response selected by the request path."""
    content_length = int(environ.get("CONTENT_LENGTH") or 0)
    environ["wsgi.input"].read(content_length)

    status, headers, body = _RESPONSES.get(
        environ.get("PATH_INFO", "/"), _DEFAULT_RESPONSE
    )
    start_response(f"{status.value} {status.phrase}", headers)
    return [body]


def main():
    """Serve the fixture on the port assigned by App Engine."""
    port = int(os.environ.get("PORT", "8080"))
    with make_server("", port, app) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
