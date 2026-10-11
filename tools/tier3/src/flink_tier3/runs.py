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
"""Downloaded run directories, as the deployed-run analyses read them.

A run directory is a local mirror of ``gs://flink-gcp-tier3-evidence/runs/RUN_ID/``:
its approval, its final receipt and the supervisor's and runner's evidence
events. Nothing here contacts a cluster or a bucket.
"""

from __future__ import annotations

import json
from pathlib import Path

from .common import Failure


class Run:
    """One downloaded run: its approval, result and evidence events."""

    def __init__(self, directory):
        self.directory = Path(directory)
        self.approval = read_json(self.directory / "approval.json")
        if not isinstance(self.approval, dict) or "run_id" not in self.approval:
            raise Failure("Not a run directory: " + str(directory))
        self.run_id = str(self.approval["run_id"])
        self.scenario = str(self.approval.get("scenario", ""))
        self.result = read_json(self.directory / "result.json")
        self.events = read_events(self.directory)

    def events_of(self, event):
        return [
            entry
            for entry in self.events
            if entry["event"] == event and isinstance(entry["payload"], dict)
        ]


def read_json(path):
    path = Path(path)
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise Failure("Unreadable JSON evidence: " + str(path)) from error


def read_events(run_dir):
    """Every ``{event, at, payload}`` object of the supervisor and runner."""
    entries = []
    for actor in ("supervisor", "runner"):
        base = Path(run_dir) / actor
        if not base.is_dir():
            continue
        for path in sorted(base.glob("*.json")):
            value = read_json(path)
            if not isinstance(value, dict) or not isinstance(value.get("event"), str):
                raise Failure("Malformed evidence event: " + str(path))
            entries.append(
                {
                    "event": value["event"],
                    "at": value.get("at"),
                    "payload": value.get("payload"),
                    "actor": actor,
                    "name": path.name,
                }
            )
    entries.sort(key=lambda e: (str(e["at"]), e["actor"], e["name"]))
    return entries


def discover_runs(path):
    """One run directory, or every immediate child that is one, sorted by id."""
    path = Path(path)
    if not path.is_dir():
        raise Failure("Evidence directory does not exist: " + str(path))
    if (path / "approval.json").is_file():
        return [Run(path)]
    runs = [
        Run(child)
        for child in sorted(path.iterdir())
        if child.is_dir() and (child / "approval.json").is_file()
    ]
    if not runs:
        raise Failure("No run directory with approval.json under " + str(path))
    return sorted(runs, key=lambda run: run.run_id)
