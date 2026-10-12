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
"""What the BigQuery and Pub/Sub sections share when they recompute a verdict.

Each section rebuilds what its own evidence can show and decides its own
verdict; these are the steps that read the same exported shapes and apply the
same rules to them, kept once so the two sections cannot drift.
"""


def measurements(run, measurement_event, complete_event):
    """Every sample the run emitted, and how many of its records are unreadable.

    A payload that is not an object, or a sample whose stage is not a string,
    is lost to the fold; it is counted here, because the rebuilt coverage would
    otherwise lower the verdict for what is only a truncated download.
    """
    readings = run.events_of(measurement_event)
    malformed = sum(
        1
        for entry in run.events
        if entry["event"] in (measurement_event, complete_event)
        and not isinstance(entry["payload"], dict)
    ) + sum(
        1 for entry in readings if not isinstance(entry["payload"].get("stage"), str)
    )
    return readings, malformed


def rebuild(readings, summarize):
    """Coverage folded from the readings themselves, in any order."""
    coverage = {}
    for entry in readings:
        stage = entry["payload"].get("stage")
        if isinstance(stage, str):
            coverage = summarize(coverage, stage, entry["payload"])
    return coverage


def missing_samples(claimed, rebuilt):
    """How many samples the record counted that the export does not hold.

    Each window's attempt count in the record is what the run had read by
    then; fewer readings in the export are a partial download, which would
    lower the rebuilt coverage. A count the record does not carry is not
    compared.
    """
    claimed = claimed if isinstance(claimed, dict) else {}
    missing = 0
    for window, entry in claimed.items():
        attempts = entry.get("attempts") if isinstance(entry, dict) else None
        if type(attempts) is int:
            against = rebuilt.get(window, {}).get("attempts", 0)
            missing += max(0, attempts - against)
    return missing


def unreadable(run, record, problems, samples):
    """A run whose evidence decides no verdict, and the verdict it stored."""
    return {
        "run_id": run.run_id,
        "verdict": None,
        "stored": record.get("verdict") if isinstance(record, dict) else None,
        "reasons": [],
        "problems": sorted(problems),
        "samples": samples,
    }


def verdict_problems(record, decided, complete):
    """Whether the verdict the run stored is the one its evidence recomputes to.

    A run that stored none and did not complete has nothing to compare; a
    completed record without one differs.
    """
    stored, reasons = record.get("verdict"), record.get("reasons")
    if (stored is not None or complete) and (
        stored != decided["verdict"]
        or (reasons if isinstance(reasons, list) else None) != decided["reasons"]
    ):
        return ["verdict-differs-from-the-record"]
    return []


def counts(assessments):
    """How many runs carry each verdict, for the analyzer's summary line.

    A run whose evidence decides no verdict is counted as `unreadable`.
    """
    table = {}
    for item in assessments:
        name = item["verdict"] or "unreadable"
        table[name] = table.get(name, 0) + 1
    return ", ".join(f"{name} {number}" for name, number in sorted(table.items()))
