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
"""

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

SCENARIO = "spanner-recovery"
FIELD = "spanner_trial"


def assess(run):
    """One Spanner run's verdict, beside the faults it injected (ADR-0204).

    The verdict is decided by the data alone.
    """
    return assess_sequence(
        run,
        by_record(data_reasons, recorded_oracles),
        **approval_checks(FIELD, Trial),
    )


def assess_runs(runs):
    """Every Spanner run in a discovered evidence directory, in the order given."""
    return [assess(run) for run in runs if run.scenario == SCENARIO]


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
        verdicts = {c: oracle_figure(oracles, c, "verdict") for c in COMPONENTS}
        lines.append(
            f"| {item['run_id']} | {item['verdict'] or '-'} | {item['stored'] or '-'} "
            f"| {verdicts['sink']} | "
            f"{oracle_figure(oracles, 'sink', 'executions', 'extra_executions')} | "
            f"{verdicts['change_stream']} | "
            f"{oracle_figure(oracles, 'change_stream', 'replays')} | "
            f"{verdicts['batch']} | "
            f"{oracle_figure(oracles, 'batch', 'extra_deliveries')} | "
            f"{oracle_figure(oracles, 'batch', 'plan', 'partitions')} | "
            f"{fault_column(item)} | "
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
            INCOMPLETE_NOTE,
        ]
    )
    return "\n".join(lines) + "\n"
