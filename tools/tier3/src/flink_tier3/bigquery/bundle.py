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
from pathlib import Path

from .. import approval_bundle
from ..common import Failure, digest
from . import plan as bigquery_plan


def prepare(approval, *, prepared_at):
    """Build from a separately supplied approval, without service access."""
    approved, when = approval_bundle.check_approval(
        approval, prepared_at, "bigquery-recovery", "BigQuery"
    )
    approval_bundle.check_revision(approved.sha)
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
        or bundle["proposal"]["delivery_sha256"] != approved.delivery_sha256
    ):
        raise Failure(
            "Rendered bundle differs from the approved manifests, images or source"
        )
    approval_bundle.check_revision(approved.sha)
    return approval_bundle.bind("bigquery-approval-bundle", approved, bundle, when)


def validate(bundle, approval):
    """Re-render against the caller's approval; embedded approval is not authority."""
    return approval_bundle.validate(prepare, bundle, approval)


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
