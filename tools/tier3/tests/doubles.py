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
"""Test doubles that the rig tests of more than one connector share."""

import copy
import json

import pytest
import requests
from google.auth.transport.requests import AuthorizedSession

NOW = 1700000000


class Response:
    def __init__(self, value, status=200):
        self.status_code = status
        self.data = value if isinstance(value, bytes) else json.dumps(value).encode()
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def iter_content(self, size):
        for start in range(0, len(self.data), size):
            yield self.data[start : start + size]


class Http:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, copy.deepcopy(kwargs)))
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return value if isinstance(value, Response) else Response(value)


class Wire:
    """Every request and close an actor session would make, none of them sent."""

    def __init__(self):
        self.calls = []
        self.closed = []


@pytest.fixture
def wire(monkeypatch):
    value = Wire()

    def request(_session, method, url, *_args, **_kwargs):
        value.calls.append((method, url))
        raise AssertionError("an actor factory made a request")

    monkeypatch.setattr(requests.Session, "request", request)
    monkeypatch.setattr(
        requests.Session,
        "close",
        lambda session: (
            value.closed.append(type(session).__name__)
            if isinstance(session, AuthorizedSession)
            else None
        ),
    )
    return value


def mount(directory, environment, application, upgrade, **approval_changes):
    approval = environment.approval.to_dict()
    approval.update(approval_changes)
    (directory / "approval.json").write_text(json.dumps(approval))
    (directory / "application.json").write_text(json.dumps(application))
    if upgrade is not None:
        (directory / "upgrade-application.json").write_text(json.dumps(upgrade))
