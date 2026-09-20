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
"""Tier-3 commands for the reviewed repository and in-cluster supervisor."""

import argparse
from importlib import import_module
from pathlib import Path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repository",
        type=Path,
        default=Path.cwd(),
        help="Repository root containing kubernetes/ and opentofu/ (default: cwd)",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for command in (
        "lifecycle",
        "bootstrap",
        "schemas",
        "supervisor",
        "render",
        "analyze",
        "bigquery",
    ):
        commands.add_parser(command, add_help=False)
    args, remaining = parser.parse_known_args(argv)
    module = import_module(
        "." + {"supervisor": "runtime"}.get(args.command, args.command), __package__
    )
    # The supervisor runs in-cluster; offline analysis commands need no checkout.
    if args.command not in ("supervisor", "analyze", "bigquery"):
        from . import bootstrap, lifecycle, schemas, workflow

        root = args.repository.resolve()
        if not any(flag in remaining for flag in ("--help", "-h")) and not (
            (root / "kubernetes/cue.mod/module.cue").is_file()
            and (root / "opentofu").is_dir()
        ):
            parser.error("--repository must name the flink-connector-gcp checkout")
        bootstrap.ROOT = lifecycle.ROOT = schemas.ROOT = workflow.ROOT = root
    return module.main(remaining)
