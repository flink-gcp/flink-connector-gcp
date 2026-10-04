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
"""Probe the workload identity's access from inside its namespace.

The runner cannot act as the workload's service account, which only a Pod
running as the namespace's `pubsub` Kubernetes service account reaches. This
program is that Pod's whole command, passed inline because a Pod cannot mount
a ConfigMap from another namespace, so it imports nothing from this package.
It prints one JSON line per observation; the runner re-evaluates them rather
than trusting the exit status.
"""

import json
import logging
import sys
import time
import warnings

import google.auth
import requests
from google.auth.transport.requests import Request

BASE = "https://pubsub.googleapis.com/v1/"
TIMEOUT = 20
MAX_RESPONSE = 1024 * 1024


def emit(event, **values):
    print(json.dumps({"event": event, **values}, sort_keys=True), flush=True)


def post(session, path, body, headers):
    with session.post(
        BASE + path,
        json=body,
        headers=headers,
        timeout=TIMEOUT,
        allow_redirects=False,
        stream=True,
    ) as response:
        data = bytearray()
        for chunk in response.iter_content(chunk_size=8192):
            data.extend(chunk)
            if len(data) > MAX_RESPONSE:
                raise RuntimeError("response exceeds 1 MiB")
        return response.status_code, json.loads(data) if data else {}


class Late(Exception):
    """A request could no longer finish before the run's admission deadline."""


def run(spec, session, email, authorize, clock=time.time, sleep=time.sleep):
    def send(path, body):
        headers = {}
        # A token refresh happens here, before the check, and nothing replays
        # the request afterwards: every request, not only a retry, must start
        # early enough to finish in time.
        authorize(headers)
        if clock() + TIMEOUT > spec["deadline"]:
            raise Late
        return post(session, path, body, headers)

    try:
        return _run(spec, send, email, clock, sleep)
    except Late:
        emit("refused", reason="deadline")
        return 8


def _run(spec, send, email, clock, sleep):
    emit("identity", email=email)
    if email != spec["member"]:
        emit("refused", reason="identity")
        return 2
    attempts = 0
    while True:
        attempts += 1
        seen = {}
        for name, want in sorted(spec["expected"].items()):
            status, value = send(
                name + ":testIamPermissions",
                {"permissions": [want["permission"]]},
            )
            if status != 200:
                emit("refused", reason="status", name=name, status=status)
                return 3
            seen[name] = want["permission"] in value.get("permissions", [])
        emit("attempt", attempt=attempts, seen=seen)
        extra = [n for n, w in spec["expected"].items() if not w["held"] and seen[n]]
        missing = [n for n, w in spec["expected"].items() if w["held"] and not seen[n]]
        if extra:
            emit("refused", reason="extra", names=sorted(extra))
            return 4
        if not missing:
            break
        # A further round sends one request per resource, one at a time.
        budget = TIMEOUT * len(spec["expected"])
        if clock() + spec["interval"] + budget > spec["deadline"]:
            emit("refused", reason="missing", names=sorted(missing))
            return 5
        sleep(spec["interval"])
    for subscription in spec["pulls"]:
        while True:
            status, value = send(
                subscription + ":pull",
                {"maxMessages": 1, "returnImmediately": True},
            )
            if (
                status == 403
                and clock() + spec["interval"] + TIMEOUT <= spec["deadline"]
            ):
                emit("pull-refused", name=subscription)
                sleep(spec["interval"])
                continue
            if status != 200 or value.get("receivedMessages"):
                emit("refused", reason="pull", name=subscription, status=status)
                return 6
            emit("pulled", name=subscription)
            break
    emit("passed", attempts=attempts)
    return 0


def main(argv):
    # The runner reads every line as JSON; a library warning on stderr, such
    # as a retried metadata request, would make a passing probe unreadable.
    logging.disable(logging.CRITICAL)
    warnings.simplefilter("ignore")
    spec = json.loads(argv[0])
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    refresh = Request()
    credentials.refresh(refresh)

    def authorize(headers):
        # Refreshes the token from the metadata server only when it is due.
        credentials.before_request(refresh, "POST", BASE, headers)

    # A plain session: google-auth's authorized session would refresh and
    # replay a request answered 401, past the deadline check in run().
    session = requests.Session()
    session.trust_env = False
    session.max_redirects = 0
    session.get_redirect_target = lambda _response: None
    email = "serviceAccount:" + getattr(credentials, "service_account_email", "")
    try:
        return run(spec, session, email, authorize)
    except Exception as error:  # noqa: BLE001 - every failure is reported
        emit("refused", reason="error", error=type(error).__name__)
        return 7


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
