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
"""Offline analysis of downloaded deployed-run evidence.

The analyzer reads a local mirror of ``gs://flink-gcp-tier3-evidence/runs/``
(one run directory or a directory of them) and recomputes each deployed
BigQuery, Pub/Sub, Cloud Tasks, Bigtable and Spanner recovery run's assessment
from its exported evidence. It never contacts a cluster or a bucket, keeps
every ratio a ``Fraction`` until rendering, and writes byte-identical reports
for identical input. Single-host Cloud Tasks campaigns have their own analyzer,
``cloudtasks.vmanalyze``.
"""

from __future__ import annotations

import argparse
import json
from array import array
from fractions import Fraction
from pathlib import Path

from .bigquery import analyze as bigquery_analyze
from .bigtable import analyze as bigtable_analyze
from .cloudtasks import analyze as cloudtasks_analyze
from .common import Failure
from .pubsub import analyze as pubsub_analyze
from .recovery_analysis import counts as run_counts
from .runs import discover_runs
from .spanner import analyze as spanner_analyze


def plain(value):
    """JSON-ready copy: a Fraction becomes ``{fraction, float}``."""
    if isinstance(value, Fraction):
        return {
            "fraction": f"{value.numerator}/{value.denominator}",
            "float": float(value),
        }
    if isinstance(value, dict):
        return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [plain(v) for v in value]
    if isinstance(value, set | frozenset):
        return sorted(plain(v) for v in value)
    if isinstance(value, array):
        return list(value)
    return value


def render_markdown(report):
    return (
        "# Tier-3 evidence analysis\n\n"
        f"The analyzer read {len(report['runs'])} run directories.\n"
        + bigquery_analyze.render(report.get("bigquery") or [])
        + pubsub_analyze.render(report.get("pubsub") or [])
        + cloudtasks_analyze.render(report.get("cloudtasks") or [])
        + bigtable_analyze.render(report.get("bigtable") or [])
        + spanner_analyze.render(report.get("spanner") or [])
    )


def analyze(evidence):
    runs = discover_runs(evidence)
    return {
        "evidence": Path(evidence).name,
        "runs": [
            {
                "run_id": run.run_id,
                "scenario": run.scenario,
                "events": len(run.events),
                "result": None
                if not isinstance(run.result, dict)
                else {k: run.result.get(k) for k in ("success", "idle")},
            }
            for run in runs
        ],
        # Each is empty unless the directory holds a deployed run of its kind.
        "bigquery": bigquery_analyze.assess_runs(runs),
        "pubsub": pubsub_analyze.assess_runs(runs),
        "cloudtasks": cloudtasks_analyze.assess_runs(runs),
        "bigtable": bigtable_analyze.assess_runs(runs),
        "spanner": spanner_analyze.assess_runs(runs),
    }


def write_reports(report, out, markdown=render_markdown):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_bytes(
        (json.dumps(plain(report), sort_keys=True, indent=2) + "\n").encode()
    )
    (out / "report.md").write_bytes(markdown(report).encode())
    return out


def summary_line(report, out):
    return (
        f"analyzed {len(report['runs'])} runs"
        + (
            f"; bigquery runs: {bigquery}"
            if (bigquery := run_counts(report.get("bigquery") or []))
            else ""
        )
        + (
            f"; pubsub runs: {pubsub}"
            if (pubsub := run_counts(report.get("pubsub") or []))
            else ""
        )
        + (
            f"; cloudtasks runs: {cloudtasks}"
            if (cloudtasks := run_counts(report.get("cloudtasks") or []))
            else ""
        )
        + (
            f"; bigtable runs: {bigtable}"
            if (bigtable := run_counts(report.get("bigtable") or []))
            else ""
        )
        + (
            f"; spanner runs: {spanner}"
            if (spanner := run_counts(report.get("spanner") or []))
            else ""
        )
        + f"; reports in {out}"
    )


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="flink-tier3 analyze",
        description="Offline analysis of a downloaded evidence directory; "
        "never contacts a cluster or a bucket.",
    )
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args(argv)
    try:
        report = analyze(args.evidence)
        out = write_reports(report, args.out or args.evidence / "analysis")
    except Failure as error:
        print("analysis failed: " + str(error))
        return 1
    print(summary_line(report, out))
    return 0
