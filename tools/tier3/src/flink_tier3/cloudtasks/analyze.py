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
"""Recompute a deployed Cloud Tasks run's verdict from its exported evidence.

The Pod decided the verdict from the listing pages and measurements it
exported; this decides it again from those and the record, so a reader
holding only the downloaded evidence directory can check the stored verdict
against them. The
vocabulary is the analysis vocabulary the other sections use, `usable` and
`inconclusive`, and a run whose evidence cannot be read has no verdict.

Every event name and stage comes from ``flink_tier3.cloudtasks.verdict``,
which the exercise writes from, so a rename cannot reach only one side.
"""

from ..metrics import CHECKPOINTS_EVENT
from ..recovery_analysis import (
    RECOVERIES,
    measurements,
    milliseconds,
    missing_samples,
    rebuild,
    recovery_record,
    recovery_timings,
    render_timings,
    unreadable,
    verdict_problems,
)
from .oracle import assess as assess_pages
from .trial import Trial
from .verdict import (
    COMPLETE_EVENT,
    COMPLETE_STAGE,
    MEASUREMENT_EVENT,
    PAGE_EVENT,
    SAMPLED_STAGES,
    summarize,
    verdict,
)

SCENARIO = "cloudtasks-recovery"
PAGE_KEYS = ("ids", "foreign", "dispatched")


def _trial(run):
    approval = run.approval if isinstance(run.approval, dict) else {}
    value = approval.get("cloudtasks_trial")
    if not isinstance(value, dict):
        return None
    try:
        return Trial(run.run_id, **value)
    except (TypeError, ValueError):
        return None


def _pages(run, attempt):
    """The listing pages one attempt exported, by index, and how many are malformed."""
    pages, malformed = {}, 0
    for entry in run.events_of(PAGE_EVENT):
        payload = entry["payload"]
        if payload.get("attempt") != attempt:
            continue
        index = payload.get("page")
        if (
            type(index) is not int
            or index in pages
            or not all(
                isinstance(payload.get(key), list)
                and all(isinstance(item, str) for item in payload[key])
                for key in PAGE_KEYS
            )
        ):
            malformed += 1
            continue
        pages[index] = {key: payload[key] for key in PAGE_KEYS}
    return pages, malformed


def _result(run, record, decided, problems, report, samples):
    return {
        "run_id": run.run_id,
        "verdict": decided["verdict"],
        "stored": record.get("verdict"),
        "reasons": decided["reasons"],
        "problems": sorted(problems),
        "report": report,
        "samples": samples,
    }


def _unreadable(run, record, problems, samples):
    return {**unreadable(run, record, problems, samples), "report": None}


def _snapshots(run):
    """Every completed checkpoint and savepoint the exported polls saw, by id.

    Each poll exports the job's statistics, whose history holds the latest
    checkpoints and savepoints; the restored job's own history starts again,
    so the restored snapshot is found in the polls of the job that took it.
    """
    seen = {}
    for entry in run.events_of(CHECKPOINTS_EVENT):
        payload = entry["payload"]
        latest = payload.get("latest")
        latest = latest if isinstance(latest, dict) else {}
        history = payload.get("history")
        history = history if isinstance(history, list) else []
        for item in [*history, latest.get("completed"), latest.get("savepoint")]:
            if not isinstance(item, dict) or item.get("status") != "COMPLETED":
                continue
            triggered = milliseconds(item.get("trigger_timestamp"))
            acknowledged = milliseconds(item.get("latest_ack_timestamp"))
            if type(item.get("id")) is int and triggered and acknowledged:
                seen[item["id"]] = (triggered, acknowledged)
    return seen


def replay_windows(run, record):
    """When each recovery's replay began, from the snapshot it restored.

    The committer replays a restored checkpoint or savepoint while its task
    initializes, which follows Flink's restore and precedes the first
    checkpoint the restored job triggers, since Flink triggers one only once
    every task runs. So the replay began between those two instants, measured
    here from the restored snapshot's completion in milliseconds: that is
    what the recovery spent of each restored envelope's replay budget, beyond
    what ADR-0158 already sizes before the snapshot completes. The snapshot's
    last acknowledgement stands in for its completion, which follows it by
    Flink's finalization, so both figures may overstate the time spent. The first
    completed checkpoint stands in for the first triggered one, which may only
    widen the window. A recovery whose snapshot or later checkpoint no
    exported poll saw maps to None.
    """
    outcomes = record.get("outcomes") if isinstance(record, dict) else None
    outcomes = outcomes if isinstance(outcomes, dict) else {}
    snapshots = _snapshots(run)
    windows = {}
    for name in RECOVERIES:
        proof = outcomes.get(name)
        if not isinstance(proof, dict):
            continue
        restored = proof.get("restored")
        restored = restored if isinstance(restored, dict) else {}
        restored_at = milliseconds(restored.get("restore_timestamp"))
        identity = restored.get("id")
        snapshot = snapshots.get(identity) if type(identity) is int else None
        later = sorted(
            (triggered, number)
            for number, (triggered, _) in snapshots.items()
            if restored_at is not None and triggered >= restored_at
        )
        if restored_at is None or snapshot is None or not later:
            windows[name] = None
            continue
        completed = snapshot[1]
        windows[name] = {
            "restored_id": identity,
            "first_checkpoint_id": later[0][1],
            "from_ms": restored_at - completed,
            "to_ms": later[0][0] - completed,
        }
    return windows


def assess(run):
    """One Cloud Tasks run's verdict, beside its recoveries' timing and replay.

    Both are read from whatever recovery record the run exported, so a run
    whose verdict cannot be recomputed still reports the recoveries it proved.
    """
    record = recovery_record(run, COMPLETE_EVENT)
    return {
        **_verdict(run),
        "recoveries": recovery_timings(run, record),
        "replay": replay_windows(run, record),
    }


def _verdict(run):
    """One Cloud Tasks run's recomputed verdict, its stored one and its problems.

    A completed run is judged from the pages of the listing attempt its record
    names, recomputing the oracle report rather than reading the stored one; a
    page missing from the export leaves the run without a verdict. A run that
    never completed exported its account of itself, which says so, and is
    `inconclusive`.
    """
    trial = _trial(run)
    readings, malformed = measurements(run, MEASUREMENT_EVENT, COMPLETE_EVENT)
    samples = len(readings)
    result = run.result if isinstance(run.result, dict) else None
    if trial is None:
        return _unreadable(run, None, ["approval-without-cloudtasks-trial"], samples)
    if result is None:
        return _unreadable(run, None, ["missing-result.json"], samples)
    # A reading the fold cannot place would lower the rebuilt coverage, and
    # the verdict with it, for what is only a truncated download; a completed
    # record that is not an object is counted with them.
    if malformed:
        return _unreadable(run, None, [f"malformed-evidence:{malformed}"], samples)
    complete = bool(run.events_of(COMPLETE_EVENT))
    record = recovery_record(run, COMPLETE_EVENT)
    if not isinstance(record, dict):
        return _unreadable(run, None, ["missing-" + COMPLETE_EVENT], samples)
    if not complete and record.get("stage") == COMPLETE_STAGE:
        return _unreadable(run, record, ["missing-" + COMPLETE_EVENT], samples)
    # Decide over the coverage the readings support, never the coverage the
    # record claims.
    coverage = rebuild(readings, summarize)
    if missing := missing_samples(record.get("coverage"), coverage):
        return _unreadable(run, record, [f"missing-samples:{missing}"], samples)
    rebuilt = {**record, "coverage": coverage}
    if not complete:
        decided = verdict(rebuilt)
        return _result(
            run,
            record,
            decided,
            verdict_problems(record, decided, False),
            None,
            samples,
        )
    outcomes = record.get("outcomes")
    outcomes = outcomes if isinstance(outcomes, dict) else {}
    listing = outcomes.get("listing")
    if not isinstance(listing, dict):
        decided = verdict(rebuilt)
        return _result(
            run, record, decided, verdict_problems(record, decided, True), None, samples
        )
    attempt, count = listing.get("attempt"), listing.get("pages")
    if type(attempt) is not int or type(count) is not int:
        return _unreadable(run, record, ["malformed-listing-record"], samples)
    pages, malformed = _pages(run, attempt)
    if malformed:
        return _unreadable(run, record, [f"malformed-pages:{malformed}"], samples)
    if sorted(pages) != list(range(count)):
        missing = len(set(range(count)) - set(pages))
        extra = len(set(pages) - set(range(count)))
        counts = {"missing-pages": missing, "extra-pages": extra}
        return _unreadable(
            run, record, [f"{name}:{n}" for name, n in counts.items() if n], samples
        )
    report = assess_pages(trial, [pages[index] for index in range(count)])
    problems = []
    if listing.get("report") != report:
        problems.append("report-differs-from-pages")
    decided = verdict(
        {
            **rebuilt,
            "outcomes": {**outcomes, "listing": {**listing, "report": report}},
        }
    )
    problems.extend(verdict_problems(record, decided, True))
    return _result(run, record, decided, problems, report, samples)


def assess_runs(runs):
    """Every Cloud Tasks run in a discovered evidence directory, in the order given."""
    return [assess(run) for run in runs if run.scenario == SCENARIO]


def render(assessments):
    """The report's Cloud Tasks section, or nothing when it holds no such run."""
    if not assessments:
        return ""
    lines = [
        "",
        "## Deployed Cloud Tasks runs",
        "",
        (
            "A verdict recomputed from the exported evidence: the oracle report is "
            "rebuilt from the listing pages the run exported and the coverage from "
            "its measurements; the recoveries and the queue's paused state are "
            "read from the record."
        ),
        "",
        (
            "| run | verdict | stored | samples | listed | missing | foreign | "
            "duplicates | dispatched | reasons | problems |"
        ),
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in assessments:
        report = item["report"] if isinstance(item["report"], dict) else {}
        lines.append(
            "| {run} | {verdict} | {stored} | {samples} | {listed} | {missing} | "
            "{foreign} | {duplicates} | {dispatched} | {reasons} | {problems} |".format(
                run=item["run_id"],
                verdict=item["verdict"] or "-",
                stored=item["stored"] or "-",
                samples=item["samples"],
                reasons=", ".join(item["reasons"]) or "-",
                problems=", ".join(item["problems"]) or "-",
                **{
                    key: report.get(key, "-")
                    for key in (
                        "listed",
                        "missing",
                        "foreign",
                        "duplicates",
                        "dispatched",
                    )
                },
            )
        )
    lines.extend(
        [
            "",
            (
                "A usable verdict establishes that every input sequence became "
                "exactly one task in the paused queue through both recoveries; it "
                "makes no claim about a handler, since a paused queue dispatches "
                "nothing."
            ),
            (
                "Each family is required in the "
                + ", ".join(f"`{stage}`" for stage in SAMPLED_STAGES[:-1])
                + f" and `{SAMPLED_STAGES[-1]}` windows, and counts as observed "
                + "only where a reading returned it."
            ),
            (
                f"A run whose stage is not `{COMPLETE_STAGE}` did not finish, which "
                "is inconclusive rather than an evidence problem."
            ),
        ]
    )
    lines.extend(render_timings(assessments))
    lines.extend(render_replay(assessments))
    return "\n".join(lines) + "\n"


def render_replay(assessments):
    """A report table of when each recovery's replay began, or nothing without one."""
    rows = [
        (item["run_id"], name, window)
        for item in assessments
        for name, window in (item.get("replay") or {}).items()
    ]
    if not rows:
        return []
    lines = [
        "",
        (
            "Each recovery's replay of its restored snapshot began after Flink's "
            "restore and before the restored job's first checkpoint, measured from "
            "the snapshot's last acknowledgement: what the recovery spent of each "
            "restored envelope's replay budget, overstated by however long Flink "
            "took to finalize the snapshot."
        ),
        "",
        "| run | recovery | restored | replay began after | first checkpoint |",
        "| --- | --- | --- | --- | --- |",
    ]
    for run_id, name, window in rows:
        if window is None:
            lines.append(f"| {run_id} | {name} | unreadable | - | - |")
            continue
        lines.append(
            f"| {run_id} | {name} | {window['restored_id']} | "
            f"{window['from_ms'] / 1000:.1f} to {window['to_ms'] / 1000:.1f} s | "
            f"chk-{window['first_checkpoint_id']} |"
        )
    return lines
