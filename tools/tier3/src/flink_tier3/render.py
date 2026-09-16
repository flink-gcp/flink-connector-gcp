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

from .workflow import render


def main(argv=None):
    parser = argparse.ArgumentParser(prog="flink-tier3 render", description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--nonce", required=True)
    parser.add_argument("--expires-at", required=True)
    parser.add_argument("--active-seconds", type=int, required=True)
    parser.add_argument(
        "--scenario", choices=("smoke", "generic-recovery"), default="smoke"
    )
    args = parser.parse_args(argv)
    print(
        json.dumps(
            render(
                args.run_id,
                args.nonce,
                args.expires_at,
                args.active_seconds,
                expression="delivery.resources",
                scenario=args.scenario,
            ),
            indent=2,
        )
    )
