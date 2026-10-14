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

The Pod decided the verdict and the receipt carries it; this decides it again
from the exported readings, so a reader holding only the downloaded evidence
directory can check the stored verdict against them. The vocabulary is the
analysis vocabulary that already exists, `usable` and `inconclusive`, and a
run whose evidence cannot be read has no verdict.

It takes a run the way ``analyze`` already reads one, and imports nothing from
it, because ``analyze`` is what calls this. Every event name and stage it looks
for comes from ``flink_tier3.bigquery.verdict``, which the exercise writes
from, so a rename cannot reach only one side and leave this reporting missing
evidence.
"""

from ..recovery_analysis import (
    measurements,
    missing_samples,
    rebuild,
    recovery_record,
    recovery_timings,
    render_timings,
    unreadable,
    verdict_problems,
)
from .verdict import (
    COMPLETE_EVENT,
    COMPLETE_STAGE,
    MEASUREMENT_EVENT,
    SAMPLED_STAGES,
    summarize,
    verdict,
)

SCENARIO = "bigquery-recovery"


def assess(run):
    """One BigQuery run's verdict, beside the timing of its recoveries.

    The timing is read from whatever recovery record the run exported, so a
    run whose verdict cannot be recomputed still reports the recoveries it
    proved.
    """
    record = recovery_record(run, COMPLETE_EVENT)
    return {**_verdict(run), "recoveries": recovery_timings(run, record)}


def _verdict(run):
    """One BigQuery run's recomputed verdict, its stored one and its problems.

    A run whose evidence lacks what the verdict is decided from, or holds a
    record nothing can read, has no verdict; re-export it and read it again. A
    run that never completed exported its account of itself, which says so,
    and is `inconclusive` — the archetypal case being a run that proved both
    recoveries and then exhausted its approved query slots.
    """
    readings, malformed = measurements(run, MEASUREMENT_EVENT, COMPLETE_EVENT)
    result = run.result if isinstance(run.result, dict) else None
    if result is None:
        return unreadable(run, None, ["missing-result.json"], len(readings))
    complete = bool(run.events_of(COMPLETE_EVENT))
    record = recovery_record(run, COMPLETE_EVENT)
    # A reading the fold cannot place would lower the rebuilt coverage, and
    # the verdict with it, for what is only a truncated download.
    if malformed:
        return unreadable(
            run, record, [f"malformed-evidence:{malformed}"], len(readings)
        )
    if not isinstance(record, dict):
        return unreadable(run, None, ["missing-" + COMPLETE_EVENT], len(readings))
    # A record that says it completed, with no completion evidence beside it,
    # is an export missing a file.
    if not complete and record.get("stage") == COMPLETE_STAGE:
        return unreadable(run, record, ["missing-" + COMPLETE_EVENT], len(readings))
    # Decide over the coverage the readings support, never the coverage the
    # record claims: recomputing from the record's own field would only restate
    # it.
    coverage = rebuild(readings, summarize)
    if missing := missing_samples(record.get("coverage"), coverage):
        return unreadable(run, record, [f"missing-samples:{missing}"], len(readings))
    trial = result.get("bigquery_trial")
    decided = verdict(
        {**record, "coverage": coverage},
        trial.get("mode") if isinstance(trial, dict) else None,
    )
    return {
        "run_id": run.run_id,
        "verdict": decided["verdict"],
        "stored": record.get("verdict"),
        "reasons": decided["reasons"],
        "problems": verdict_problems(record, decided, complete),
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
        "A verdict recomputed from the exported evidence, beside the one the run stored.",
        "",
        "| run | verdict | stored | samples | reasons | problems |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for item in assessments:
        lines.append(
            "| {run} | {verdict} | {stored} | {samples} | {reasons} | {problems} |".format(
                run=item["run_id"],
                verdict=item["verdict"] or "-",
                stored=item["stored"] or "-",
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
    lines.extend(render_timings(assessments))
    return "\n".join(lines) + "\n"
