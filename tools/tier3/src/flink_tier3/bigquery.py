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
"""Generate BigQuery recovery SQL and check downloaded aggregate results offline."""

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path

PROJECT = "flink-gcp"
DATASET = "flink_gcp_tier3_bigquery"
MAX_RESULT_BYTES = 64 * 1024
MAX_INT64 = (1 << 63) - 1
ROW_BYTES = {"ALO": 64 * 1024, "EO": 1024}
FIELDS = {
    "run_id",
    "mode",
    "expected_records",
    "destinations",
    "destination",
    "total_rows",
    "valid_rows",
    "distinct_sequences",
}


@dataclass(frozen=True)
class Trial:
    """The input domain shared with the finite recovery application's options."""

    run_id: str
    mode: str
    destinations: int
    records: int

    def __post_init__(self):
        if not isinstance(self.run_id, str) or not re.fullmatch(
            r"[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?", self.run_id
        ):
            raise ValueError("run_id must match the Tier-3 run label grammar")
        if not isinstance(self.mode, str) or self.mode not in ROW_BYTES:
            raise ValueError("mode must be ALO or EO")
        if type(self.destinations) is not int or self.destinations not in (10, 50):
            raise ValueError("destinations must be 10 or 50")
        if type(self.records) is not int or not (
            2 * self.destinations <= self.records <= 2 * 1024**3 // ROW_BYTES[self.mode]
        ):
            raise ValueError("records must cover both writers and fit 2 GiB")

    def table(self, destination):
        return f"{PROJECT}.{DATASET}.bq_{self.run_id.replace('-', '_')}_d{destination}"

    def expected(self, destination):
        return (self.records + self.destinations - 1 - destination) // self.destinations


def query(trial):
    """One aggregate per physical table, including empty tables; no row filtering."""
    parts = []
    for destination in range(trial.destinations):
        valid = (
            f"run_id = '{trial.run_id}' AND sequence >= 0 "
            f"AND sequence < {trial.records} AND destination = {destination} "
            f"AND MOD(sequence, {trial.destinations}) = {destination}"
        )
        parts.append(
            f"SELECT '{trial.run_id}' AS run_id, '{trial.mode}' AS mode,\n"
            f"  {trial.records} AS expected_records, {trial.destinations} AS destinations,\n"
            f"  {destination} AS destination, COUNT(*) AS total_rows,\n"
            f"  COALESCE(SUM(CASE WHEN {valid} THEN 1 ELSE 0 END), 0) AS valid_rows,\n"
            f"  COUNT(DISTINCT CASE WHEN {valid} THEN sequence ELSE NULL END) "
            f"AS distinct_sequences\nFROM `{trial.table(destination)}`"
        )
    return "\nUNION ALL\n".join(parts) + "\nORDER BY destination\n"


def integer(value, field):
    # BigQuery JSON exports encode INT64 as strings; reject coercions and booleans.
    if isinstance(value, str) and re.fullmatch(r"0|[1-9][0-9]{0,18}", value):
        value = int(value)
    if type(value) is not int or not 0 <= value <= MAX_INT64:
        raise ValueError(f"{field} must be a nonnegative INT64")
    return value


def assess(trial, rows):
    """Assess a complete aggregate result, without asserting its provenance."""
    if not isinstance(rows, list) or len(rows) != trial.destinations:
        raise ValueError("Expected exactly one aggregate row per destination")
    destinations = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != FIELDS:
            raise ValueError("Aggregate row has missing or unknown fields")
        if row["run_id"] != trial.run_id or row["mode"] != trial.mode:
            raise ValueError("Aggregate identity differs from this trial")
        values = {key: integer(row[key], key) for key in FIELDS - {"run_id", "mode"}}
        if (
            values["expected_records"] != trial.records
            or values["destinations"] != trial.destinations
        ):
            raise ValueError("Aggregate input domain differs from this trial")
        destination = values["destination"]
        if destination >= trial.destinations or destination in destinations:
            raise ValueError("Aggregate destination is outside the trial or repeated")
        total, valid, distinct = (
            values[key] for key in ("total_rows", "valid_rows", "distinct_sequences")
        )
        expected = trial.expected(destination)
        if (
            not (distinct <= valid <= total)
            or distinct > expected
            or (valid > 0 and distinct == 0)
        ):
            raise ValueError("Aggregate counts are inconsistent with the input domain")
        destinations[destination] = {
            "destination": destination,
            "expected_sequences": expected,
            "total_rows": total,
            "valid_rows": valid,
            "distinct_sequences": distinct,
            "invalid_rows": total - valid,
            "missing_sequences": expected - distinct,
            "duplicate_rows": valid - distinct,
        }
    totals = {
        key: sum(row[key] for row in destinations.values())
        for key in (
            "total_rows",
            "valid_rows",
            "distinct_sequences",
            "invalid_rows",
            "missing_sequences",
            "duplicate_rows",
        )
    }
    passed = totals["invalid_rows"] == 0 and totals["missing_sequences"] == 0
    if trial.mode == "EO":
        passed = passed and totals["duplicate_rows"] == 0
    return {
        "version": 1,
        "scope": "query-result",
        "run_id": trial.run_id,
        "mode": trial.mode,
        "expected_records": trial.records,
        "destinations": trial.destinations,
        "verdict": "pass" if passed else "fail",
        **totals,
        "tables": [destinations[i] for i in range(trial.destinations)],
    }


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON field: {key}")
        result[key] = value
    return result


def read_result(path):
    """Read at most 64 KiB, even if the file grows while it is being read."""
    with path.open("rb") as stream:
        data = stream.read(MAX_RESULT_BYTES + 1)
    if len(data) > MAX_RESULT_BYTES:
        raise ValueError("Query result exceeds 64 KiB")
    try:
        return json.loads(data.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise ValueError("Query result must be bounded UTF-8 JSON") from error


def main(argv=None):
    parser = argparse.ArgumentParser(prog="flink-tier3 bigquery", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("query", "assess"):
        command = commands.add_parser(name)
        command.add_argument("--run-id", required=True)
        command.add_argument("--mode", choices=tuple(ROW_BYTES), required=True)
        command.add_argument("--destinations", type=int, required=True)
        command.add_argument("--records", type=int, required=True)
        if name == "assess":
            command.add_argument("--result", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        trial = Trial(args.run_id, args.mode, args.destinations, args.records)
        if args.command == "query":
            print(query(trial), end="")
            return 0
        report = assess(trial, read_result(args.result))
        print(json.dumps(report, sort_keys=True))
        return 0 if report["verdict"] == "pass" else 1
    except (ValueError, OSError) as error:
        parser.error(str(error))
