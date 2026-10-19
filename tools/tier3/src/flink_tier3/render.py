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

from .bigquery.plan import TRIALS, prepare
from .bigquery.plan import trial as bigquery_trial
from .bigtable import plan as bigtable_plan
from .bigtable import trial as bigtable_trial
from .cloudtasks import plan as cloudtasks_plan
from .cloudtasks import trial as cloudtasks_trial
from .common import Failure
from .model import SCENARIOS
from .pubsub import plan as pubsub_plan
from .resource import RESOURCE_SCENARIOS
from .workflow import render

# The plan of each scenario whose runner creates one resource.
RESOURCE_PLANS = {
    "cloudtasks-recovery": cloudtasks_plan,
    "bigtable-recovery": bigtable_plan,
}


def main(argv=None):
    parser = argparse.ArgumentParser(prog="flink-tier3 render", description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--nonce", required=True)
    parser.add_argument("--expires-at", required=True)
    parser.add_argument("--active-seconds", type=int, required=True)
    parser.add_argument(
        "--scenario",
        choices=SCENARIOS,
        default="smoke",
    )
    parser.add_argument(
        "--application-image",
        help="application digest reference; an offline render may be synthetic",
    )
    parser.add_argument("--trial-file", type=Path, help="unapproved Pub/Sub trial JSON")
    parser.add_argument("--trial", choices=tuple(TRIALS), help="BigQuery trial")
    parser.add_argument(
        "--cloudtasks-trial", choices=cloudtasks_trial.KINDS, help="Cloud Tasks trial"
    )
    parser.add_argument(
        "--bigtable-trial", choices=bigtable_trial.KINDS, help="Bigtable trial"
    )
    parser.add_argument("--started-at", help="trial proposal's absolute start time")
    parser.add_argument(
        "--revision", help="trial proposal's intended full source revision"
    )
    parser.add_argument("--expression", default="delivery.resources")
    args = parser.parse_args(argv)
    for scenario, resource in RESOURCE_SCENARIOS.items():
        flag = f"--{resource.field}-trial"
        if getattr(args, resource.field + "_trial") and args.scenario != scenario:
            parser.error(f"{flag} applies only to {scenario}")
    if resource := RESOURCE_SCENARIOS.get(args.scenario):
        flag = f"--{resource.field}-trial"
        kind = getattr(args, resource.field + "_trial")
        if args.trial or args.trial_file:
            parser.error(f"{args.scenario} takes its trial as {flag}")
        if not all((kind, args.started_at, args.revision, args.application_image)):
            parser.error(
                f"{args.scenario} requires {flag}, --started-at, "
                "--revision and --application-image"
            )
        if args.expression != "delivery.resources":
            parser.error("Trial proposals cannot select a raw expression")
        plan = RESOURCE_PLANS[args.scenario]
        try:
            bundle = plan.prepare(
                run_id=args.run_id,
                nonce=args.nonce,
                started_at=args.started_at,
                expires_at=args.expires_at,
                active_seconds=args.active_seconds,
                revision=args.revision,
                application_image=args.application_image,
                trial=plan.trial(kind, args.started_at),
            )
        except (ValueError, Failure) as error:
            parser.error(str(error))
        print(json.dumps(bundle, indent=2))
        return
    if args.scenario in ("bigquery-recovery", "pubsub-recovery"):
        choice = args.trial_file if args.scenario == "pubsub-recovery" else args.trial
        wrong = args.trial if args.scenario == "pubsub-recovery" else args.trial_file
        if wrong or not all(
            (choice, args.started_at, args.revision, args.application_image)
        ):
            parser.error(
                args.scenario
                + " requires "
                + ("--trial-file" if args.scenario == "pubsub-recovery" else "--trial")
                + ", --started-at, --revision and --application-image"
            )
        if args.expression != "delivery.resources":
            parser.error("Trial proposals cannot select a raw expression")
        proposal_prepare, proposal_load = (
            (pubsub_plan.prepare, pubsub_plan.load_trial)
            if args.scenario == "pubsub-recovery"
            else (prepare, bigquery_trial)
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
                trial=proposal_load(choice),
            )
        except (ValueError, Failure) as error:
            parser.error(str(error))
        print(json.dumps(bundle, indent=2))
        return
    if args.trial_file or args.trial or args.started_at or args.revision:
        parser.error("trial inputs apply only to service trial proposals")
    if args.application_image:
        parser.error("--application-image applies only to service trial proposals")
    print(
        json.dumps(
            render(
                args.run_id,
                args.nonce,
                args.expires_at,
                args.active_seconds,
                expression=args.expression,
                scenario=args.scenario,
            ),
            indent=2,
        )
    )
