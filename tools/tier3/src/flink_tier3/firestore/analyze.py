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
"""Recompute a deployed Firestore run's verdict from its exported evidence.

The four oracle reports are read from the run's record, so a reader holding
only the downloaded evidence directory can check the stored verdict against
them. The oracle reports are not rebuilt: the supervisor read the
collections, which the databases' deletion removed, and exported only the
reports.
"""

from ..common import UNPLANNED_EVENT
from ..recovery_analysis import (
    INCOMPLETE_NOTE,
    SINGLE_FAULT_NOTE,
    approval_checks,
    assess_sequence,
    by_record,
    fault_column,
    oracle_figure,
    recorded_oracles,
)
from .trial import Trial
from .verdict import COMPONENTS, data_reasons

SCENARIO = "firestore-recovery"
FIELD = "firestore_trial"


def assess(run):
    """One Firestore run's verdict, beside the faults it injected (ADR-0204).

    The verdict is decided by the data alone.
    """
    return assess_sequence(
        run,
        by_record(data_reasons, _details),
        **approval_checks(FIELD, Trial),
    )


def _details(run, record):
    """The oracle reports, and the causes the run carried on through."""
    return {
        **recorded_oracles(run, record),
        "unplanned": _unplanned(run, record.get("outcomes")),
    }


def _unplanned(run, outcomes):
    """The causes a run carried on through, from its record and its events.

    The record a run last wrote can predate a cause, if it stopped while it
    waited one out, and an export can lack event files, so both are read;
    only text causes count.
    """
    recorded = outcomes.get("unplanned") if isinstance(outcomes, dict) else None
    seen = [entry["payload"].get("cause") for entry in run.events_of(UNPLANNED_EVENT)]
    causes = []
    for cause in [*(recorded if isinstance(recorded, list) else []), *seen]:
        if isinstance(cause, str) and cause not in causes:
            causes.append(cause)
    return causes


def assess_runs(runs):
    """Every Firestore run in a discovered evidence directory, in the order given."""
    return [assess(run) for run in runs if run.scenario == SCENARIO]


def render(assessments):
    """The report's Firestore section, or nothing when it holds no such run."""
    if not assessments:
        return ""
    lines = [
        "",
        "## Deployed Firestore runs",
        "",
        (
            "A verdict recomputed from the exported evidence: the oracle reports "
            "are read from the record."
        ),
        "",
        (
            "| run | verdict | stored | native sink | datastore sink | "
            "native batch | native extra | native read s | datastore batch | "
            "datastore extra | datastore read s | unplanned | faults | reasons | "
            "problems |"
        ),
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in assessments:
        oracles = item.get("oracles")
        verdicts = {c: oracle_figure(oracles, c, "verdict") for c in COMPONENTS}
        batches = " | ".join(
            f"{verdicts[mode + '_batch']} | "
            f"{oracle_figure(oracles, mode + '_batch', 'extra_deliveries')} | "
            f"{oracle_figure(oracles, mode + '_batch', 'read_seconds')}"
            for mode in ("native", "datastore")
        )
        lines.append(
            f"| {item['run_id']} | {item['verdict'] or '-'} | {item['stored'] or '-'} "
            f"| {verdicts['native_sink']} | "
            f"{verdicts['datastore_sink']} | {batches} | "
            f"{', '.join(item.get('unplanned') or []) or '-'} | {fault_column(item)} | "
            f"{', '.join(item['reasons']) or '-'} | {', '.join(item['problems']) or '-'} |"
        )
    lines.extend(
        [
            "",
            (
                "A usable `sequence` verdict (ADR-0204) establishes that through "
                "the faults listed, in each database, every input record's key "
                "holds its sequence in `input` and every seed record was copied "
                "with its value and no late record; extra copies are reported, not "
                "failed, and the sinks' executions are not observable. The run is "
                "judged by its data alone."
            ),
            (
                "`unplanned` names each interruption the run recorded and carried "
                "on through, which decides nothing."
            ),
            SINGLE_FAULT_NOTE,
            INCOMPLETE_NOTE,
        ]
    )
    return "\n".join(lines) + "\n"
