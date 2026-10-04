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

from .common import INCONCLUSIVE, INCONSISTENT, TAMPERED, UNEXPORTED, USABLE


def measurements(run, measurement_event, complete_event):
    """Every sample the run emitted, and how many of its records are unreadable.

    A payload that is not an object, or a sample whose stage is not a string,
    is lost to the fold; it is counted here, because the rebuilt coverage would
    otherwise make a truncated download look like a record overstating itself.
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


def accounted(claimed, rebuilt, complete):
    """What the record's coverage claims beyond, or short of, its readings.

    Claiming an attempt or a family the readings do not support is the
    overstatement; a completed record accounting for fewer readings than the
    run emitted only disagrees with them. `True` is not a sample count.
    """
    claimed = claimed if isinstance(claimed, dict) else {}
    for window, entry in claimed.items():
        against = rebuilt.get(window) if isinstance(rebuilt.get(window), dict) else {}
        entry = entry if isinstance(entry, dict) else {}
        attempts, observed = entry.get("attempts"), entry.get("observed")
        if (
            type(attempts) is not int
            or attempts > against.get("attempts", 0)
            or not isinstance(observed, list)
            # Element types first: `set` of an unhashable element raises.
            or not all(isinstance(name, str) for name in observed)
            or not set(observed) <= set(against.get("observed", ()))
        ):
            return ["coverage-unsupported-by-its-readings"]
    if complete and claimed != rebuilt:
        return ["readings-the-record-does-not-account-for"]
    return []


def verdict_problems(record, decided, complete, disagrees):
    """The stored verdict against the one the evidence supports.

    Anything but the conservative outcome, or its absence, is the forgery,
    whatever its spelling; a stored `inconclusive`, or none, under-reports.
    """
    stored, reasons = record.get("verdict"), record.get("reasons")
    if (stored is not None or complete) and (
        stored != decided["verdict"]
        or (reasons if isinstance(reasons, list) else None) != decided["reasons"]
    ):
        return [
            disagrees
            if stored in (None, INCONCLUSIVE)
            else "verdict-unsupported-by-its-inputs"
        ]
    return []


def receipt_problems(result, record, complete, scenario, decided):
    """The receipt's claims against the record and the recomputed verdict.

    Success is checked in one direction: `false` beside a usable verdict is the
    runner's own further conditions, `true` beside anything else the forgery.
    """
    problems = []
    if complete and result.get("recovery") != record:
        problems.append("receipt-record-mismatch")
    if result.get("scenario") != scenario:
        problems.append("receipt-scenario-mismatch")
    if result.get("success") is True and decided["verdict"] != USABLE:
        problems.append("receipt-success-mismatch")
    return problems


def status(problems, overstatements, missing, verdict):
    """An overstatement outranks a missing object, which outranks a disagreement.

    A truncated export and a fabricated record look alike once the evidence is
    gone, and only one of the two is safe to report as benign.
    """
    if set(problems) & set(overstatements):
        return TAMPERED
    if missing:
        return UNEXPORTED
    if problems:
        return INCONSISTENT
    return verdict


def counts(assessments):
    """How many runs carry each status, for the analyzer's summary line."""
    table = {}
    for item in assessments:
        table[item["status"]] = table.get(item["status"], 0) + 1
    return ", ".join(f"{name} {number}" for name, number in sorted(table.items()))
