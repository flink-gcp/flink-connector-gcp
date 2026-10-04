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
for comes from ``flink_tier3.bigquery.verdict``, which the exercise writes
from, so a rename cannot reach only one side and leave this reporting missing
evidence.
"""

from ..common import UNEXPORTED
from ..recovery_analysis import (
    accounted,
    measurements,
    rebuild,
    receipt_problems,
    verdict_problems,
)
from ..recovery_analysis import status as assess_status
from .verdict import (
    COMPLETE_EVENT,
    COMPLETE_STAGE,
    MEASUREMENT_EVENT,
    SAMPLED_STAGES,
    summarize,
    verdict,
)

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
    return measurements(run, MEASUREMENT_EVENT, COMPLETE_EVENT)


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
    rebuilt = rebuild(readings, summarize)
    # The trial the receipt names decides which families are required.
    trial = result.get("bigquery_trial")
    decided = verdict(
        {**record, "coverage": rebuilt},
        trial.get("mode") if isinstance(trial, dict) else None,
    )
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
    unsupported = accounted(record.get("coverage"), rebuilt, complete)
    problems.extend(unsupported)
    # The verdict the Pod wrote, against the decision the evidence supports.
    # This is the check the receipt cannot do itself. An incomplete record
    # carries no verdict to compare, and cannot recompute to `usable`, because
    # its stage is not the completed one.
    # The verdict the Pod wrote, against the decision the evidence supports.
    # This is the check the receipt cannot do itself. An incomplete record
    # carries no verdict to compare, and cannot recompute to `usable`, because
    # its stage is not the completed one.
    problems.extend(
        verdict_problems(
            record, decided, complete, "verdict-disagrees-with-its-readings"
        )
    )
    problems.extend(receipt_problems(result, record, complete, SCENARIO, decided))
    status = assess_status(problems, OVERSTATEMENTS, missing, decided["verdict"])
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
