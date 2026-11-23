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
"""Recompute a deployed Spanner run's verdict from its exported evidence.

The three oracle reports are read from the run's record, so a reader holding
only the downloaded evidence directory can check the stored verdict against
them. The oracle reports are not rebuilt: the
supervisor read the tables, which the instance's deletion removed, and
exported only the reports.

Every event name and stage comes from ``flink_tier3.spanner.verdict``,
which the exercise writes from, so a rename cannot reach only one side.
"""

from ..recovery_analysis import (
    SINGLE_FAULT_NOTE,
    malformed_records,
    names_single_fault,
    recovery_record,
    single_fault,
    unreadable,
    verdict_problems,
)
from .trial import Trial
from .verdict import (
    COMPLETE_EVENT,
    COMPLETE_STAGE,
    COMPONENTS,
    sequence_verdict,
)

SCENARIO = "spanner-recovery"
FIELD = "spanner_trial"


def _trial(run):
    approval = run.approval if isinstance(run.approval, dict) else {}
    value = approval.get(FIELD)
    if not isinstance(value, dict):
        return None
    try:
        return Trial(run.run_id, **value)
    except (TypeError, ValueError):
        return None


def assess(run):
    """One Spanner run's verdict, beside the faults it injected (ADR-0204)."""
    record = recovery_record(run, COMPLETE_EVENT)
    outcomes = record.get("outcomes") if isinstance(record, dict) else None
    faults = outcomes.get("faults") if isinstance(outcomes, dict) else None
    return {**_verdict(run), "faults": faults if isinstance(faults, list) else []}


def _verdict(run):
    malformed = malformed_records(run, COMPLETE_EVENT)
    if names_single_fault(run, FIELD):
        return single_fault(run, recovery_record(run, COMPLETE_EVENT))
    if _trial(run) is None:
        return unreadable(run, None, ["approval-without-spanner-trial"])
    if not isinstance(run.result, dict):
        return unreadable(run, None, ["missing-result.json"])
    if malformed:
        return unreadable(run, None, [f"malformed-evidence:{malformed}"])
    complete = bool(run.events_of(COMPLETE_EVENT))
    record = recovery_record(run, COMPLETE_EVENT)
    if not isinstance(record, dict):
        return unreadable(run, None, ["missing-" + COMPLETE_EVENT])
    if not complete and record.get("stage") == COMPLETE_STAGE:
        return unreadable(run, record, ["missing-" + COMPLETE_EVENT])
    # Decided by the data alone.
    decided = sequence_verdict(record)
    outcomes = record.get("outcomes")
    oracles = outcomes.get("oracles") if isinstance(outcomes, dict) else None
    return {
        "run_id": run.run_id,
        "verdict": decided["verdict"],
        "stored": record.get("verdict"),
        "reasons": decided["reasons"],
        "problems": sorted(verdict_problems(record, decided, complete)),
        "oracles": oracles if isinstance(oracles, dict) else None,
    }


def assess_runs(runs):
    """Every Spanner run in a discovered evidence directory, in the order given."""
    return [assess(run) for run in runs if run.scenario == SCENARIO]


def _figure(oracles, component, *keys):
    value = oracles.get(component) if isinstance(oracles, dict) else None
    for key in keys:
        value = value.get(key) if isinstance(value, dict) else None
    return "-" if value is None or isinstance(value, (dict, list)) else value


def _faults(item):
    """A sequence run's injected faults in order, or `-` for a single-fault run."""
    kinds = [
        fault.get("kind")
        for fault in item.get("faults") or []
        if isinstance(fault, dict) and isinstance(fault.get("kind"), str)
    ]
    return ", ".join(kinds) or "-"


def render(assessments):
    """The report's Spanner section, or nothing when it holds no such run."""
    if not assessments:
        return ""
    lines = [
        "",
        "## Deployed Spanner runs",
        "",
        (
            "A verdict recomputed from the exported evidence: the oracle reports "
            "are read from the record."
        ),
        "",
        (
            "| run | verdict | stored | sink | sink extra executions | "
            "change stream | replays | batch | batch extra | partitions | faults | "
            "reasons | problems |"
        ),
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in assessments:
        oracles = item.get("oracles")
        verdicts = {c: _figure(oracles, c, "verdict") for c in COMPONENTS}
        lines.append(
            f"| {item['run_id']} | {item['verdict'] or '-'} | {item['stored'] or '-'} "
            f"| {verdicts['sink']} | "
            f"{_figure(oracles, 'sink', 'executions', 'extra_executions')} | "
            f"{verdicts['change_stream']} | {_figure(oracles, 'change_stream', 'replays')} | "
            f"{verdicts['batch']} | {_figure(oracles, 'batch', 'extra_deliveries')} | "
            f"{_figure(oracles, 'batch', 'plan', 'partitions')} | {_faults(item)} | "
            f"{', '.join(item['reasons']) or '-'} | {', '.join(item['problems']) or '-'} |"
        )
    lines.extend(
        [
            "",
            (
                "A usable `sequence` verdict (ADR-0204) establishes that through "
                "the faults listed every input record's key holds its sequence in "
                "`input`, every input key was relayed from the change stream at "
                "least once with no commit after its end, and every seed row was "
                "copied with its value and no late row; extra executions, replays "
                "and extra copies are reported, not failed, and the executions are "
                "a lower bound. The run is judged by its data alone."
            ),
            SINGLE_FAULT_NOTE,
        ]
    )
    return "\n".join(lines) + "\n"
