#!/usr/bin/env python3
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
"""Capture staging IDs and publish or drop Central Portal deployments (ADR-0147).

`capture ID_FILE COMMAND ...` streams the command's output and saves each upload
ID immediately, even when Maven subsequently fails validation. `publish ID ID`
checks both deployments before publishing either and waits for both to be PUBLISHED.
It also resumes the same IDs after an interrupted publish. `drop ID ...` attempts
every requested deletion; PUBLISHING and PUBLISHED deployments cannot be dropped.
"""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ID_PATTERN = r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
# central-publishing-maven-plugin 0.11.0; also observed in release run 33323702321.
UPLOAD = re.compile(
    r"\[INFO\] Uploaded bundle successfully, deployment name: .*?, deploymentId: "
    rf"({ID_PATTERN})\. Deployment will require manual publishing"
)
STATES = {"PENDING", "VALIDATING", "VALIDATED", "PUBLISHING", "PUBLISHED", "FAILED"}
BASE_URL = "https://central.sonatype.com/api/v1/publisher"
REQUEST_TIMEOUT = 30
WAIT_TIMEOUT = 1800
POLL_INTERVAL = 5


class PortalError(Exception):
    """An actionable failure that contains no credentials or HTTP response body."""


class TransientPortalError(PortalError):
    """A transport or service failure retryable only for read-only status polls."""


def summary(message: str) -> None:
    print(message, flush=True)
    if destination := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(destination).open("a", encoding="utf-8") as output:
            output.write(f"- {message}\n")


def capture(id_file: Path, command: list[str]) -> int:
    # Exclusive creation prevents a stale ID from authorizing another run.
    with id_file.open("x", encoding="utf-8") as output:
        ids = set()
        with subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        ) as process:
            for line in process.stdout:
                print(line, end="", flush=True)
                match = UPLOAD.fullmatch(ANSI.sub("", line).strip())
                if match and (deployment_id := match[1].lower()) not in ids:
                    ids.add(deployment_id)
                    output.write(f"{deployment_id}\n")
                    output.flush()
                    summary(f"Captured Central deployment `{deployment_id}`.")
            result = process.wait()
        if result:
            summary(
                f"Staging command failed (exit {result}); no publication authorized."
            )
            return result if result > 0 else 128 - result
        if len(ids) != 1:
            raise PortalError(
                f"expected one staging deployment ID, captured {len(ids)}"
            )
    return 0


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Portal:
    def __init__(self):
        username = os.environ.get("CENTRAL_TOKEN_USERNAME")
        password = os.environ.get("CENTRAL_TOKEN_PASSWORD")
        if not username or not password:
            raise PortalError("set CENTRAL_TOKEN_USERNAME and CENTRAL_TOKEN_PASSWORD")
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        self.authorization = f"Bearer {token}"
        self.opener = urllib.request.build_opener(NoRedirect)

    def request(self, method: str, path: str, *, allow_missing: bool = False):
        request = urllib.request.Request(
            f"{BASE_URL}/{path}",
            headers={"Authorization": self.authorization},
            method=method,
        )
        try:
            with self.opener.open(request, timeout=REQUEST_TIMEOUT) as response:
                expected = 200 if path.startswith("status?") else 204
                if response.status != expected:
                    raise PortalError(
                        f"{method} {path}: unexpected HTTP {response.status}"
                    )
                return response.read()
        except urllib.error.HTTPError as error:
            status = error.code
            error.close()
            if allow_missing and status == 404:
                return None
            failure = (
                TransientPortalError
                if status == 429 or 500 <= status < 600
                else PortalError
            )
            raise failure(
                f"{method} {path}: HTTP {status}; check the deployment ID"
            ) from None
        except (OSError, urllib.error.URLError, http.client.HTTPException):
            # A failed response does not prove that a Publish request was rejected.
            raise TransientPortalError(
                f"{method} {path}: connection failed; check state before retrying"
            ) from None

    def status(
        self,
        deployment_id: str,
        *,
        allow_missing: bool = False,
        previous_state: str | None = None,
    ) -> str:
        body = self.request(
            "POST", f"status?id={deployment_id}", allow_missing=allow_missing
        )
        if body is None:
            return "DROPPED"
        try:
            value = json.loads(body)
            if (
                not isinstance(value, dict)
                or value.get("deploymentId") != deployment_id
                or value.get("deploymentState") not in STATES
            ):
                raise ValueError
        except (ValueError, TypeError):
            raise PortalError(
                f"{deployment_id}: invalid Portal status response"
            ) from None
        state = value["deploymentState"]
        message = f"Central deployment `{deployment_id}`: {state}."
        if state == previous_state:
            print(message, flush=True)
        else:
            summary(message)
        return state


def publish(portal: Portal, ids: list[str]) -> None:
    # Check the entire set before the first irreversible request. Already publishing
    # or published IDs are accepted only to allow recovery with the original IDs.
    states = {deployment_id: portal.status(deployment_id) for deployment_id in ids}
    for deployment_id, state in states.items():
        if state not in {"VALIDATED", "PUBLISHING", "PUBLISHED"}:
            raise PortalError(f"{deployment_id}: {state}; no Publish request was sent")
    for deployment_id, state in states.items():
        if state == "VALIDATED":
            summary(f"Requesting Publish for Central deployment `{deployment_id}`.")
            portal.request("POST", f"deployment/{deployment_id}")

    deadline = time.monotonic() + WAIT_TIMEOUT
    remaining = set(ids)
    while remaining:
        for deployment_id in sorted(remaining):
            try:
                state = portal.status(
                    deployment_id, previous_state=states[deployment_id]
                )
            except TransientPortalError as error:
                print(
                    f"{error}; retrying status until the publication deadline",
                    flush=True,
                )
                continue
            states[deployment_id] = state
            if state == "PUBLISHED":
                remaining.remove(deployment_id)
            elif state not in {"VALIDATED", "PUBLISHING"}:
                raise PortalError(
                    f"{deployment_id}: {state} after Publish; retain both IDs"
                )
        if remaining:
            if time.monotonic() >= deadline:
                raise PortalError(
                    "publication wait timed out; resume with the same deployment IDs"
                )
            time.sleep(POLL_INTERVAL)
    summary("All requested Central deployments are PUBLISHED.")


def drop(portal: Portal, ids: list[str]) -> None:
    errors = []
    for deployment_id in ids:
        try:
            state = portal.status(deployment_id, allow_missing=True)
            if state == "DROPPED":
                summary(f"Central deployment `{deployment_id}` is already absent.")
                continue
            if state not in {"VALIDATED", "FAILED"}:
                raise PortalError(f"{deployment_id}: cannot Drop a {state} deployment")
            portal.request("DELETE", f"deployment/{deployment_id}", allow_missing=True)
            summary(f"Dropped Central deployment `{deployment_id}`.")
        except PortalError as error:
            errors.append(str(error))
    if errors:
        raise PortalError("; ".join(errors))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    staging = commands.add_parser(
        "capture", help="save an upload ID while running a command"
    )
    staging.add_argument("id_file", type=Path)
    staging.add_argument("command", nargs=argparse.REMAINDER)
    for action in ("status", "publish", "drop"):
        operation = commands.add_parser(action)
        operation.add_argument("ids", nargs=2 if action == "publish" else "+")
    args = parser.parse_args(argv)
    try:
        if args.action == "capture":
            if not args.command:
                parser.error("capture requires a command")
            return capture(args.id_file, args.command)
        if any(not re.fullmatch(ID_PATTERN, item) for item in args.ids):
            raise PortalError(
                "each deployment ID must be a UUID from the staging output"
            )
        ids = list(dict.fromkeys(item.lower() for item in args.ids))
        if len(ids) != len(args.ids):
            raise PortalError("deployment IDs must be distinct")
        portal = Portal()
        if args.action == "publish":
            publish(portal, ids)
        elif args.action == "drop":
            drop(portal, ids)
        else:
            for deployment_id in ids:
                portal.status(deployment_id)
    except (PortalError, OSError) as error:
        summary(f"central-portal: {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
