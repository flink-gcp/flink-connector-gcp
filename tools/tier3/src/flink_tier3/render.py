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
"""Render the lifecycle delivery with source from the installed package."""

import argparse
import json
from pathlib import Path

from . import pubsub_plan
from .bigquery_plan import load_trial, prepare
from .cloudtasks import load_session
from .common import Failure
from .policy import CLOUDTASKS_POLICY, FLINK_LINES
from .workflow import render


def main(argv=None):
    parser = argparse.ArgumentParser(prog="flink-tier3 render", description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--nonce", required=True)
    parser.add_argument("--expires-at", required=True)
    parser.add_argument("--active-seconds", type=int, required=True)
    parser.add_argument(
        "--scenario",
        choices=(
            "smoke",
            "generic-recovery",
            "cloudtasks",
            "bigquery-recovery",
            "pubsub-recovery",
        ),
        default="smoke",
    )
    parser.add_argument("--cells-file", type=Path, help="cloudtasks session TOML")
    parser.add_argument("--flink-version", choices=tuple(FLINK_LINES), default="2.2.1")
    parser.add_argument(
        "--application-image",
        help="application digest reference; an offline render may be synthetic",
    )
    parser.add_argument(
        "--trial-file", type=Path, help="unapproved BigQuery or Pub/Sub trial JSON"
    )
    parser.add_argument("--started-at", help="trial proposal's absolute start time")
    parser.add_argument(
        "--revision", help="trial proposal's intended full source revision"
    )
    parser.add_argument("--target", default=CLOUDTASKS_POLICY["target"])
    parser.add_argument("--expression", default="delivery.resources")
    args = parser.parse_args(argv)
    if args.scenario in ("bigquery-recovery", "pubsub-recovery"):
        if not all(
            (args.trial_file, args.started_at, args.revision, args.application_image)
        ):
            parser.error(
                args.scenario
                + " requires --trial-file, --started-at, --revision and --application-image"
            )
        if (
            args.cells_file
            or args.expression != "delivery.resources"
            or args.flink_version != "2.2.1"
            or args.target != CLOUDTASKS_POLICY["target"]
        ):
            parser.error("Trial proposals cannot select cells or a raw expression")
        proposal_prepare, proposal_load = (
            (pubsub_plan.prepare, pubsub_plan.load_trial)
            if args.scenario == "pubsub-recovery"
            else (prepare, load_trial)
        )
        try:
            bundle = proposal_prepare(
                run_id=args.run_id,
                nonce=args.nonce,
                started_at=args.started_at,
                expires_at=args.expires_at,
                active_seconds=args.active_seconds,
                revision=args.revision,
                application_image=args.application_image,
                trial=proposal_load(args.trial_file),
            )
        except (ValueError, Failure) as error:
            parser.error(str(error))
        print(json.dumps(bundle, indent=2))
        return
    if args.trial_file or args.started_at or args.revision:
        parser.error("trial inputs apply only to BigQuery or Pub/Sub proposals")
    tags = {}
    if args.scenario == "cloudtasks":
        if not args.cells_file or not args.application_image:
            parser.error("cloudtasks requires --cells-file and --application-image")
        session = load_session(args.cells_file)
        tags = {
            "cells": json.dumps(session["cells"], separators=(",", ":")),
            "flink_version": args.flink_version,
            "application_image": args.application_image,
            "target": args.target,
        }
    elif args.cells_file or args.application_image:
        parser.error("session inputs apply only to the cloudtasks scenario")
    print(
        json.dumps(
            render(
                args.run_id,
                args.nonce,
                args.expires_at,
                args.active_seconds,
                expression=args.expression,
                scenario=args.scenario,
                **tags,
            ),
            indent=2,
        )
    )
