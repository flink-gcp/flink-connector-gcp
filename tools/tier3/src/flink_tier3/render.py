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

from .cloudtasks import load_session
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
        choices=("smoke", "generic-recovery", "cloudtasks"),
        default="smoke",
    )
    parser.add_argument("--cells-file", type=Path, help="cloudtasks session TOML")
    parser.add_argument("--flink-version", choices=tuple(FLINK_LINES), default="2.2.1")
    parser.add_argument(
        "--application-image",
        help="cloudtasks application digest reference; a local render may be synthetic",
    )
    parser.add_argument("--target", default=CLOUDTASKS_POLICY["target"])
    parser.add_argument("--expression", default="delivery.resources")
    args = parser.parse_args(argv)
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
