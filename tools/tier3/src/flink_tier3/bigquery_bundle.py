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
"""Offline approval-bound BigQuery delivery; this does not authenticate admission."""

import argparse
import json
import os
import subprocess
from pathlib import Path

from . import bigquery_plan, workflow
from .bundle import source_digest
from .common import Failure, digest, json_bytes, timestamp, utc
from .model import Approval
from .policy import MIB


def _check_revision(revision):
    """Require the approved checkout and unchanged CUE inputs."""

    def git(*args):
        try:
            result = subprocess.run(
                ["git", "--no-optional-locks", *args],
                cwd=workflow.ROOT,
                env={
                    key: value
                    for key, value in os.environ.items()
                    if not key.startswith("GIT_")
                },
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise Failure(
                "Cannot verify the approved repository revision: " + str(error)
            ) from error
        if result.returncode:
            raise Failure(
                "Cannot verify the approved repository revision: "
                + result.stderr[-1000:].strip()
            )
        return result.stdout

    if git("rev-parse", "HEAD").strip() != revision:
        raise Failure("Repository revision differs from approval")
    if git("diff", "--name-only", "HEAD", "--", "kubernetes"):
        raise Failure("Approved Kubernetes inputs have local changes")
    # Include ignored CUE files: Git's ordinary status hides them, but CUE may load them.
    others = git("ls-files", "--others", "-z", "--", "kubernetes").split("\0")
    if any(path.endswith(".cue") or "cue.mod" in Path(path).parts for path in others):
        raise Failure("Approved Kubernetes inputs contain untracked CUE files")


def prepare(approval, *, prepared_at):
    """Build from a separately supplied approval, without service access."""
    value = approval.to_dict() if isinstance(approval, Approval) else approval
    if not isinstance(value, dict):
        raise Failure("Approval must be a JSON object")
    when = timestamp(prepared_at)
    approved = Approval.from_dict(value, when)
    if approved.scenario != "bigquery-recovery":
        raise Failure("Execution bundles require a BigQuery approval")
    if when != int(when) or when < approved.schedule.started:
        raise Failure(
            "Bundle preparation requires a whole second within the run window"
        )
    if approved.runtime_sha256 != source_digest():
        raise Failure("Bundle source differs from approval")
    _check_revision(approved.sha)
    bundle = bigquery_plan.prepare(
        run_id=approved.run_id,
        nonce=approved.nonce,
        started_at=approved.started_at,
        expires_at=approved.expires_at,
        active_seconds=bigquery_plan.ACTIVE_SECONDS,
        revision=approved.sha,
        application_image=approved.images["application"],
        trial=approved.bigquery_trial,
    )
    if (
        digest(bundle["application"]) != approved.application_sha256
        or digest(bundle["upgrade_application"]) != approved.upgrade_application_sha256
        or bundle["proposal"]["images"]["supervisor"] != approved.images["supervisor"]
        or bundle["proposal"]["runtime_sha256"] != approved.runtime_sha256
    ):
        raise Failure(
            "Rendered bundle differs from the approved manifests, images or source"
        )
    _check_revision(approved.sha)
    # These two fields vary after approval; every other rendered field is retained.
    bundle["delivery"]["config"]["data"]["approval.json"] = json_bytes(
        approved.to_dict()
    ).decode()
    bundle["delivery"]["supervisor"]["spec"]["activeDeadlineSeconds"] = (
        approved.schedule.active_seconds(when)
    )
    data = bundle["delivery"]["config"]["data"]
    if (
        sum(len(key.encode()) + len(value.encode()) for key, value in data.items())
        > MIB
    ):
        raise Failure("Approved ConfigMap exceeds the Kubernetes data size limit")
    return {
        "kind": "bigquery-approval-bundle",
        "version": 1,
        "admission_enabled": False,
        "prepared_at": utc(when),
        "approval": approved.to_dict(),
        **bundle,
    }


def validate(bundle, approval):
    """Re-render against the caller's approval; embedded approval is not authority."""
    if not isinstance(bundle, dict) or not isinstance(bundle.get("prepared_at"), str):
        raise Failure("Bundle must contain its preparation time")
    expected = prepare(approval, prepared_at=bundle["prepared_at"])
    if json_bytes(bundle) != json_bytes(expected):
        raise Failure(
            "Bundle differs from the separately supplied approval and rendering"
        )
    return expected


def _read_json(path):
    def unique(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError("Duplicate JSON field: " + key)
            value[key] = item
        return value

    return json.loads(path.read_text(), object_pairs_hook=unique)


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="flink-tier3 bigquery-bundle", description=__doc__
    )
    commands = parser.add_subparsers(dest="operation", required=True)
    build = commands.add_parser(
        "prepare", help="Render an approval-bound bundle without admission"
    )
    build.add_argument("--approval-file", type=Path, required=True)
    build.add_argument(
        "--prepared-at", required=True, help="Planned whole-second preparation time"
    )
    check = commands.add_parser(
        "verify", help="Compare a bundle to a separately supplied approval"
    )
    check.add_argument("--approval-file", type=Path, required=True)
    check.add_argument("--bundle-file", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        approval = _read_json(args.approval_file)
        if args.operation == "prepare":
            result = prepare(approval, prepared_at=args.prepared_at)
        else:
            checked = validate(_read_json(args.bundle_file), approval)
            result = {
                "run_id": checked["approval"]["run_id"],
                "matches_approval": True,
                "admission_enabled": False,
            }
    except (OSError, ValueError, TypeError, Failure) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2))
