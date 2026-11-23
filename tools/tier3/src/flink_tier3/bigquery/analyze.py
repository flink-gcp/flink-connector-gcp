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

from ..exercise import SEQUENCE_KIND
from ..recovery_analysis import (
    BIGQUERY_SINGLE_FAULTS,
    SINGLE_FAULT,
    SINGLE_FAULT_NOTE,
    malformed_records,
    recovery_record,
    single_fault,
    unreadable,
    verdict_problems,
)
from .verdict import (
    COMPLETE_EVENT,
    COMPLETE_STAGE,
    sequence_verdict,
)

SCENARIO = "bigquery-recovery"


def assess(run):
    """One BigQuery run's verdict, beside the faults it injected (ADR-0204)."""
    record = recovery_record(run, COMPLETE_EVENT)
    outcomes = record.get("outcomes") if isinstance(record, dict) else None
    faults = outcomes.get("faults") if isinstance(outcomes, dict) else None
    return {**_verdict(run), "faults": faults if isinstance(faults, list) else []}


def _trial(run):
    result = run.result if isinstance(run.result, dict) else None
    trial = result.get("bigquery_trial") if result else None
    return trial if isinstance(trial, dict) else None


def _trial_problem(run):
    """Why the receipt's trial is not the fault sequence, or None when it is.

    A receipt names a single-fault trial only in the shape those trials were
    written in; a receipt without a trial, or with any other, is malformed.
    """
    trial = _trial(run)
    if trial is not None and trial.get("fault") == SEQUENCE_KIND:
        return None
    if trial is not None and _single_fault_trial(trial):
        return SINGLE_FAULT
    return "receipt-without-bigquery-trial"


def _single_fault_trial(trial):
    """Whether `trial` reads as one the plans wrote for a single-fault trial.

    Each named its delivery method and destination count, and no fault for
    the upgrade and JobManager replacement every early trial ran, or one of
    the faults later ones injected instead; the earliest also carried fields
    the plans later dropped, so other fields are not checked. Only the
    sequence named its record count, so a trial that names one is a sequence
    that lost its fault, whose stored verdict must not be shown as an
    archived run's.
    """
    return (
        trial.get("mode") in ("ALO", "EO", "FILE_LOADS")
        and "destinations" in trial
        and "records" not in trial
        and ("fault" not in trial or trial["fault"] in BIGQUERY_SINGLE_FAULTS)
    )


def _verdict(run):
    """One BigQuery run's recomputed verdict, its stored one and its problems.

    A run whose evidence lacks what the verdict is decided from, or holds a
    record nothing can read, has no verdict; re-export it and read it again. A
    run that never completed exported its account of itself, which says so,
    and is `inconclusive`. A run of a single-fault kind has no verdict here.
    """
    malformed = malformed_records(run, COMPLETE_EVENT)
    result = run.result if isinstance(run.result, dict) else None
    if result is None:
        return unreadable(run, None, ["missing-result.json"])
    complete = bool(run.events_of(COMPLETE_EVENT))
    record = recovery_record(run, COMPLETE_EVENT)
    problem = _trial_problem(run)
    if problem == SINGLE_FAULT:
        return single_fault(run, record)
    if problem is not None:
        return unreadable(run, None, [problem])
    # A truncated download.
    if malformed:
        return unreadable(run, record, [f"malformed-evidence:{malformed}"])
    if not isinstance(record, dict):
        return unreadable(run, None, ["missing-" + COMPLETE_EVENT])
    # A record that says it completed, with no completion evidence beside it,
    # is an export missing a file.
    if not complete and record.get("stage") == COMPLETE_STAGE:
        return unreadable(run, record, ["missing-" + COMPLETE_EVENT])
    # Decided by the data alone.
    decided = sequence_verdict(record)
    return {
        "run_id": run.run_id,
        "verdict": decided["verdict"],
        "stored": record.get("verdict"),
        "reasons": decided["reasons"],
        "problems": verdict_problems(record, decided, complete),
    }


def assess_runs(runs):
    """Every BigQuery run in a discovered evidence directory, in the order given."""
    return [assess(run) for run in runs if run.scenario == SCENARIO]


def _faults(item):
    """A sequence run's injected faults in order, or `-` for a single-fault run."""
    kinds = [
        fault.get("kind")
        for fault in item.get("faults") or []
        if isinstance(fault, dict) and isinstance(fault.get("kind"), str)
    ]
    return ", ".join(kinds) or "-"


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
        "| run | verdict | stored | faults | reasons | problems |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for item in assessments:
        lines.append(
            "| {run} | {verdict} | {stored} | {faults} | {reasons} "
            "| {problems} |".format(
                run=item["run_id"],
                verdict=item["verdict"] or "-",
                stored=item["stored"] or "-",
                faults=_faults(item),
                reasons=", ".join(item["reasons"]) or "-",
                problems=", ".join(item["problems"]) or "-",
            )
        )
    lines.append("")
    lines.append(
        "A `sequence` run (ADR-0204) is judged by its query oracle alone, "
        "through the faults listed."
    )
    lines.append(SINGLE_FAULT_NOTE)
    lines.append(
        f"A run whose stage is not `{COMPLETE_STAGE}` did not finish, which is "
        "inconclusive rather than an evidence problem."
    )
    return "\n".join(lines) + "\n"
