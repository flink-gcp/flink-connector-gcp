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

The coverage, with each window's greatest watermark, partitions_reread and
partitions_discovered readings, is rebuilt from the measurements the run
exported, and the recoveries and the three oracle reports are read from its
record, so a reader holding only the downloaded evidence directory can check
the stored verdict against them. The oracle reports are not rebuilt: the
supervisor read the tables, which the instance's deletion removed, and
exported only the reports.

Every event name and stage comes from ``flink_tier3.spanner.verdict``,
which the exercise writes from, so a rename cannot reach only one side.
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
from .observe import COMPONENTS
from .trial import Trial
from .verdict import (
    COMPLETE_EVENT,
    COMPLETE_STAGE,
    MEASUREMENT_EVENT,
    SAMPLED_STAGES,
    summarize,
    verdict,
    watermark_advanced,
)

SCENARIO = "spanner-recovery"


def _trial(run):
    approval = run.approval if isinstance(run.approval, dict) else {}
    value = approval.get("spanner_trial")
    if not isinstance(value, dict):
        return None
    try:
        return Trial(run.run_id, **value)
    except (TypeError, ValueError):
        return None


def assess(run):
    """One Spanner run's verdict, beside its recoveries' timing."""
    record = recovery_record(run, COMPLETE_EVENT)
    return {**_verdict(run), "recoveries": recovery_timings(run, record)}


def _verdict(run):
    readings, malformed = measurements(run, MEASUREMENT_EVENT, COMPLETE_EVENT)
    samples = len(readings)
    if _trial(run) is None:
        return unreadable(run, None, ["approval-without-spanner-trial"], samples)
    if not isinstance(run.result, dict):
        return unreadable(run, None, ["missing-result.json"], samples)
    if malformed:
        return unreadable(run, None, [f"malformed-evidence:{malformed}"], samples)
    complete = bool(run.events_of(COMPLETE_EVENT))
    record = recovery_record(run, COMPLETE_EVENT)
    if not isinstance(record, dict):
        return unreadable(run, None, ["missing-" + COMPLETE_EVENT], samples)
    if not complete and record.get("stage") == COMPLETE_STAGE:
        return unreadable(run, record, ["missing-" + COMPLETE_EVENT], samples)
    # Decide over the coverage the readings support, never the coverage the
    # record claims.
    coverage = rebuild(readings, summarize)
    if missing := missing_samples(record.get("coverage"), coverage):
        return unreadable(run, record, [f"missing-samples:{missing}"], samples)
    decided = verdict({**record, "coverage": coverage})
    outcomes = record.get("outcomes")
    oracles = outcomes.get("oracles") if isinstance(outcomes, dict) else None
    return {
        "run_id": run.run_id,
        "verdict": decided["verdict"],
        "stored": record.get("verdict"),
        "reasons": decided["reasons"],
        "problems": sorted(verdict_problems(record, decided, complete)),
        "oracles": oracles if isinstance(oracles, dict) else None,
        "watermark_advanced": watermark_advanced(coverage),
        "samples": samples,
    }


def assess_runs(runs):
    """Every Spanner run in a discovered evidence directory, in the order given."""
    return [assess(run) for run in runs if run.scenario == SCENARIO]


def _figure(oracles, component, *keys):
    value = oracles.get(component) if isinstance(oracles, dict) else None
    for key in keys:
        value = value.get(key) if isinstance(value, dict) else None
    return "-" if value is None or isinstance(value, (dict, list)) else value


def render(assessments):
    """The report's Spanner section, or nothing when it holds no such run."""
    if not assessments:
        return ""
    lines = [
        "",
        "## Deployed Spanner runs",
        "",
        (
            "A verdict recomputed from the exported evidence: the coverage and "
            "the watermark's advance are rebuilt from the run's measurements; "
            "the recoveries and the oracle reports are read from the record."
        ),
        "",
        (
            "| run | verdict | stored | samples | sink | sink extra executions | "
            "change stream | replays | watermark advanced | batch | batch extra | "
            "partitions | reasons | problems |"
        ),
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in assessments:
        oracles = item.get("oracles")
        verdicts = {c: _figure(oracles, c, "verdict") for c in COMPONENTS}
        advanced = item.get("watermark_advanced")
        lines.append(
            f"| {item['run_id']} | {item['verdict'] or '-'} | {item['stored'] or '-'} "
            f"| {item['samples']} | {verdicts['sink']} | "
            f"{_figure(oracles, 'sink', 'executions', 'extra_executions')} | "
            f"{verdicts['change_stream']} | {_figure(oracles, 'change_stream', 'replays')} | "
            f"{'-' if advanced is None else str(advanced).lower()} | "
            f"{verdicts['batch']} | {_figure(oracles, 'batch', 'extra_deliveries')} | "
            f"{_figure(oracles, 'batch', 'plan', 'partitions')} | "
            f"{', '.join(item['reasons']) or '-'} | {', '.join(item['problems']) or '-'} |"
        )
    lines.extend(
        [
            "",
            (
                "A usable verdict establishes that through both recoveries every "
                "input record's key holds its sequence in `input`, every input key "
                "was relayed from the change stream at least once with no commit "
                "after its end while the source's watermark advanced after each "
                "recovery, and every seed row was copied with its value and no "
                "late row; extra executions, replays and extra copies are "
                "reported, not failed, and the executions are a lower bound."
            ),
            (
                "Each family is required in the "
                + ", ".join(f"`{stage}`" for stage in SAMPLED_STAGES[:-1])
                + f" and `{SAMPLED_STAGES[-1]}` windows, and counts as observed "
                + "only where a reading returned it."
            ),
        ]
    )
    lines.extend(render_timings(assessments))
    return "\n".join(lines) + "\n"
