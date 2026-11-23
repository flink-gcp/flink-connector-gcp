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
it, because ``analyze`` is what calls this.
"""

from ..exercise import SEQUENCE_KIND
from ..recovery_analysis import (
    BIGQUERY_SINGLE_FAULTS,
    INCOMPLETE_NOTE,
    SINGLE_FAULT_NOTE,
    assess_sequence,
    by_record,
    fault_column,
)
from .verdict import data_reasons

SCENARIO = "bigquery-recovery"


def assess(run):
    """One BigQuery run's verdict, beside the faults it injected (ADR-0204).

    The trial is the receipt's, so a run without a receipt is reported as
    such before its trial is read. A run of a single-fault kind has no verdict
    here.
    """
    return assess_sequence(
        run,
        by_record(data_reasons),
        archived=_archived,
        trial_problem=_trial_problem,
        malformed_stored=True,
    )


def _trial(run):
    result = run.result if isinstance(run.result, dict) else None
    trial = result.get("bigquery_trial") if result else None
    return trial if isinstance(trial, dict) else None


def _trial_problem(run):
    """Why the receipt's trial is not the fault sequence, or None when it is.

    A run without a receipt is left to the receipt's own check. A current
    trial names its record count, a positive whole number as dispatch writes
    it, and no fault or the `sequence` an earlier revision of the plans
    wrote; a receipt without a trial, or with any other, is malformed.
    """
    trial = _trial(run)
    if not isinstance(run.result, dict) or (
        trial is not None
        and _records(trial.get("records"))
        and trial.get("fault", SEQUENCE_KIND) == SEQUENCE_KIND
    ):
        return None
    return "receipt-without-bigquery-trial"


def _records(value):
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _archived(run):
    """Whether the receipt's trial is one a single-fault run wrote."""
    trial = _trial(run)
    return trial is not None and _single_fault_trial(trial)


def _single_fault_trial(trial):
    """Whether `trial` reads as one the plans wrote for a single-fault trial.

    Each named its delivery method and destination count, and no fault for
    the upgrade and JobManager replacement every early trial ran, or one of
    the faults later ones injected instead; the earliest also carried fields
    the plans later dropped, so other fields are not checked. Only the
    sequence named its record count, so a trial that names one is a
    sequence, whose stored verdict must not be shown as an archived run's.
    """
    return (
        trial.get("mode") in ("ALO", "EO", "FILE_LOADS")
        and "destinations" in trial
        and "records" not in trial
        and ("fault" not in trial or trial["fault"] in BIGQUERY_SINGLE_FAULTS)
    )


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
                faults=fault_column(item),
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
    lines.append(INCOMPLETE_NOTE)
    return "\n".join(lines) + "\n"
