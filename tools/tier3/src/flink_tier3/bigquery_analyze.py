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
"""Recompute a deployed BigQuery run's verdict from its exported evidence.

The Pod decided the verdict and the receipt carries it, which is exactly why
this recomputes rather than reads it: a reader holding only the downloaded
evidence directory can then tell a run that earned its verdict from one whose
receipt says so. The vocabulary is the analysis vocabulary that already
exists — `usable`, `inconclusive`, and the excluded statuses for evidence that
cannot be read at all — rather than a second one invented for this scenario.

It takes a run the way ``analyze`` already reads one, and imports nothing from
it, because ``analyze`` is what calls this. Every event name and stage it looks
for comes from ``bigquery_verdict``, which the exercise writes from, so a
rename cannot reach only one side and leave this reporting missing evidence.
"""

from .bigquery_verdict import (
    COMPLETE_EVENT,
    COMPLETE_STAGE,
    MEASUREMENT_EVENT,
    SAMPLED_STAGES,
    summarize,
    verdict,
)
from .common import INCONCLUSIVE, INCONSISTENT, TAMPERED, UNEXPORTED, USABLE

SCENARIO = "bigquery-recovery"
# The problems that are a record claiming what its evidence denies. Every other
# problem is a disagreement: the record and its evidence do not match, but the
# record asserts nothing in its own favour, so it is inconsistent rather than
# forged. Under-reporting is not tampering.
OVERSTATEMENTS = (
    "coverage-unsupported-by-its-readings",
    "verdict-unsupported-by-its-inputs",
    # The receipt is a claim too, and its `success` is the one the run is read
    # for. A usable verdict is not the whole of what the runner requires, but
    # it is required, so `true` beside readings that support no such verdict is
    # always an edit. The converse is the runner's own business, which is why
    # the check below is one-directional.
    "receipt-success-mismatch",
)


def _measurements(run):
    """Every sample the run emitted, and how many of its records are unreadable.

    A payload that is not an object is dropped by `events_of`, so it is counted
    here instead. It cannot simply be ignored: the lost reading lowers the
    rebuilt coverage, and the record would then look like one overstating
    itself — a truncated download reported as forgery.
    """
    readings = run.events_of(MEASUREMENT_EVENT)
    malformed = sum(
        1
        for entry in run.events
        if entry["event"] in (MEASUREMENT_EVENT, COMPLETE_EVENT)
        and not isinstance(entry["payload"], dict)
    ) + sum(
        # A dict the fold cannot place is lost in exactly the same way.
        1
        for entry in readings
        if not isinstance(entry["payload"].get("stage"), str)
    )
    return readings, malformed


def _rebuild(readings):
    """Coverage folded from the readings themselves, not read from the record.

    This is the whole point of recomputing offline. Each sample was emitted as
    `{"stage": ..., **reading}`, so the same fold the Pod ran can be run again
    over the exported evidence — and a `coverage` claiming a family the
    readings beside it never returned is then a disagreement rather than an
    input. The fold is a per-stage union and a count, so the order the evidence
    happens to be listed in does not change the result.
    """
    coverage = {}
    for entry in readings:
        stage = entry["payload"].get("stage")
        if isinstance(stage, str):
            coverage = summarize(coverage, stage, entry["payload"])
    return coverage


def _accounted(claimed, rebuilt, complete):
    """What the record's coverage claims beyond, or short of, its readings.

    Two different accusations. A record claiming an attempt or a family the
    readings do not support is overstating itself, which is the forgery. A
    completed record that accounts for fewer readings than the run emitted is
    not overstating anything, but its evidence disagrees with it — an
    incomplete record is expected to, because it was written at the run's last
    transition and the remaining samples came after.
    """
    claimed = claimed if isinstance(claimed, dict) else {}
    problems = []
    for stage, window in claimed.items():
        against = rebuilt.get(stage) if isinstance(rebuilt.get(stage), dict) else {}
        window = window if isinstance(window, dict) else {}
        attempts = window.get("attempts")
        observed = window.get("observed")
        if (
            not isinstance(attempts, int)
            or attempts > against.get("attempts", 0)
            or not isinstance(observed, list)
            # Element types too, and before the subset test: `set` of an
            # unhashable element raises, and `analyze` catches only `Failure`,
            # so one edited record would abort the whole directory's analysis.
            or not all(isinstance(name, str) for name in observed)
            or not set(observed) <= set(against.get("observed", ()))
        ):
            problems.append("coverage-unsupported-by-its-readings")
            break
    if complete and claimed != rebuilt and not problems:
        problems.append("readings-the-record-does-not-account-for")
    return problems


def _excluded(run, problems):
    readings, _ = _measurements(run)
    return {
        "run_id": run.run_id,
        "status": UNEXPORTED,
        "verdict": None,
        "reasons": [],
        "problems": problems,
        "samples": len(readings),
    }


def assess(run):
    """One BigQuery run's status, its recomputed verdict and its problems.

    `unexported` is an evidence set that does not hold the run's own account
    of itself; `tampered` is a verdict its inputs do not support; and
    `inconsistent` is an evidence set that disagrees with itself. None of the
    three is a measurement, and each is excluded for the same reason the Cloud
    Tasks analysis excludes them: what a run measured cannot be asked of
    evidence that cannot be trusted to be that run's.

    A run that never completed is none of those three. It exported its account
    of itself and that account says it did not finish, which is the ordinary
    `inconclusive` outcome — the archetypal case being a run that proved both
    recoveries and then exhausted its approved query slots.
    """
    readings, malformed = _measurements(run)
    result = run.result if isinstance(run.result, dict) else None
    if result is None:
        return _excluded(run, ["missing-result.json"])
    # An unreadable record is an incomplete evidence set, not a dishonest one.
    # The rebuilt coverage is missing whatever that record held, so neither
    # accusation below can be made from it: a partial `gcloud storage cp` would
    # otherwise be reported as a forgery. Re-export and read it again.
    if malformed:
        return _excluded(run, [f"malformed-evidence:{malformed}"])
    completions = run.events_of(COMPLETE_EVENT)
    complete = bool(completions)
    record = completions[-1]["payload"] if complete else result.get("recovery")
    if not isinstance(record, dict):
        return _excluded(run, ["missing-" + COMPLETE_EVENT])
    # Decide over the coverage the readings support, never the coverage the
    # record claims: recomputing from a field the same hand could edit would
    # only restate the record to itself.
    rebuilt = _rebuild(readings)
    decided = verdict({**record, "coverage": rebuilt})
    problems = []
    if len(completions) > 1:
        problems.append(f"repeated-{COMPLETE_EVENT}:{len(completions)}")
    # A record that says it completed, with no completion evidence beside it,
    # is an export missing a file: an absent object is named only by the record
    # it should have accompanied. This is a problem and not a short circuit,
    # because the same shape is what a wholly fabricated receipt looks like —
    # and the checks below are the only thing that tells the two apart.
    missing = not complete and record.get("stage") == COMPLETE_STAGE
    if missing:
        problems.append("missing-" + COMPLETE_EVENT)
    unsupported = _accounted(record.get("coverage"), rebuilt, complete)
    problems.extend(unsupported)
    # The verdict the Pod wrote, against the decision the evidence supports.
    # This is the check the receipt cannot do itself. An incomplete record
    # carries no verdict to compare, and cannot recompute to `usable`, because
    # its stage is not the completed one.
    stored, reasons = record.get("verdict"), record.get("reasons")
    if (stored is not None or complete) and (
        stored != decided["verdict"]
        or (reasons if isinstance(reasons, list) else None) != decided["reasons"]
    ):
        # Anything that is not the conservative outcome, or its absence, is
        # the forgery — whatever its type. Testing for the safe value rather
        # than for the literal `usable` is deliberate twice over: a record
        # edited to `USABLE` makes the same claim and must not escape on a
        # spelling, and a field holding something no writer produces at all
        # cannot be read as a modest claim, so it takes the severe label. A
        # stored `inconclusive`, or none, under-reports instead, and nobody
        # forges a run into being unusable.
        problems.append(
            "verdict-disagrees-with-its-readings"
            if stored in (None, INCONCLUSIVE)
            else OVERSTATEMENTS[1]
        )
    if complete and result.get("recovery") != record:
        problems.append("receipt-record-mismatch")
    if result.get("scenario") != SCENARIO:
        problems.append("receipt-scenario-mismatch")
    # One-directional on purpose. The receipt's `success` is a conjunction that
    # also requires the run's own evidence to have been written, so `false`
    # beside a usable verdict is the runner obeying its rules. `true` beside a
    # verdict that is not usable is the forgery.
    if result.get("success") is True and decided["verdict"] != USABLE:
        problems.append(OVERSTATEMENTS[2])
    # Computed once every claim has been examined, the receipt's included: a
    # forgery can be made in the receipt as readily as in the record.
    tampered = bool(set(problems) & set(OVERSTATEMENTS))
    # An overstatement outranks the missing object, because a truncated export
    # and a fabricated record look alike once the readings are gone and only
    # one of the two is safe to report as benign. The operator clears a
    # re-fetchable case by fetching the evidence again and asking once more.
    status = (
        TAMPERED
        if tampered
        else UNEXPORTED
        if missing
        else INCONSISTENT
        if problems
        else decided["verdict"]
    )
    return {
        "run_id": run.run_id,
        "status": status,
        # An unexported run has no evidence to decide from, so it publishes no
        # verdict. A tampered or inconsistent one does: what its readings
        # actually support is what makes the exclusion legible.
        "verdict": None if status == UNEXPORTED else decided["verdict"],
        "reasons": decided["reasons"],
        "problems": sorted(problems),
        "samples": len(readings),
    }


def assess_runs(runs):
    """Every BigQuery run in a discovered evidence directory, in the order given."""
    return [assess(run) for run in runs if run.scenario == SCENARIO]


def counts(assessments):
    """How many runs carry each status, for the analyzer's own summary line."""
    table = {}
    for item in assessments:
        table[item["status"]] = table.get(item["status"], 0) + 1
    return ", ".join(f"{status} {number}" for status, number in sorted(table.items()))


def render(assessments):
    """The report's BigQuery section, or nothing when it holds no such run."""
    if not assessments:
        return ""
    lines = [
        "",
        "## Deployed BigQuery runs",
        "",
        "A verdict recomputed from the exported evidence, not read from the receipt.",
        "",
        "| run | status | verdict | samples | reasons | problems |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for item in assessments:
        lines.append(
            "| {run} | {status} | {verdict} | {samples} | {reasons} | {problems} |".format(
                run=item["run_id"],
                status=item["status"],
                verdict=item["verdict"] or "-",
                samples=item["samples"],
                reasons=", ".join(item["reasons"]) or "-",
                problems=", ".join(item["problems"]) or "-",
            )
        )
    lines.append("")
    lines.append(
        "Each family is required in the "
        + " and ".join(f"`{stage}`" for stage in SAMPLED_STAGES)
        + " windows, and counts as observed only where a reading returned it."
    )
    lines.append(
        f"A run whose stage is not `{COMPLETE_STAGE}` did not finish, which is "
        "inconclusive rather than an evidence problem."
    )
    return "\n".join(lines) + "\n"
