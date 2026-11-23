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
"""Recompute a deployed Bigtable run's verdict from its exported evidence.

The three oracle reports are read from the run's record, so a reader holding
only the downloaded evidence directory can check the stored verdict against
them. The oracle reports are not rebuilt: the supervisor read the
tables, which the instance's deletion removed, and exported only the reports.
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

SCENARIO = "bigtable-recovery"
FIELD = "bigtable_trial"


def assess(run):
    """One Bigtable run's verdict, beside the faults it injected (ADR-0204).

    The verdict is decided by the data alone.
    """
    return assess_sequence(
        run,
        by_record(data_reasons, recorded_oracles),
        **approval_checks(FIELD, Trial),
    )


def assess_runs(runs):
    """Every Bigtable run in a discovered evidence directory, in the order given."""
    return [assess(run) for run in runs if run.scenario == SCENARIO]


def render(assessments):
    """The report's Bigtable section, or nothing when it holds no such run."""
    if not assessments:
        return ""
    lines = [
        "",
        "## Deployed Bigtable runs",
        "",
        (
            "A verdict recomputed from the exported evidence: the oracle reports "
            "are read from the record."
        ),
        "",
        (
            "| run | verdict | stored | eo | alo | alo extra | relay | "
            "relay replays | faults | reasons | problems |"
        ),
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in assessments:
        oracles = item.get("oracles")
        verdicts = {c: oracle_figure(oracles, c, "verdict") for c in COMPONENTS}
        lines.append(
            f"| {item['run_id']} | {item['verdict'] or '-'} | {item['stored'] or '-'} "
            f"| {verdicts['eo']} | {verdicts['alo']} | "
            f"{oracle_figure(oracles, 'alo', 'extra_contributions')} | "
            f"{verdicts['relay']} | "
            f"{oracle_figure(oracles, 'relay', 'extra_deliveries')} | "
            f"{fault_column(item)} | "
            f"{', '.join(item['reasons']) or '-'} | {', '.join(item['problems']) or '-'} |"
        )
    lines.extend(
        [
            "",
            (
                "A usable `sequence` verdict (ADR-0204) establishes that through "
                "the faults listed every input record reached `eo` exactly once "
                "with one marker, reached `alo` at least once, and was relayed "
                "from `eo`'s change stream at least once; extra `alo` "
                "contributions and relay deliveries are reported, not failed. The "
                "run is judged by its data alone."
            ),
            SINGLE_FAULT_NOTE,
            INCOMPLETE_NOTE,
        ]
    )
    return "\n".join(lines) + "\n"
