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
"""Offline analysis of downloaded Cloud Tasks evidence."""

import gzip
import json
import subprocess
import sys
from fractions import Fraction
from pathlib import Path

import pytest
from flink_tier3 import analyze, cli, observe, protocol
from flink_tier3 import evidence as ev
from flink_tier3.analyze import RANDOM_CONTROL
from flink_tier3.common import utc
from flink_tier3.model import cell_records
from test_cloudtasks_evidence import (
    INC,
    INC2,
    NANOS,
    PROC,
    PROC2,
    SRC,
    SRC2,
    WALL,
    Synthetic,
)
from test_cloudtasks_session import CELL_A

RUN = "analyze-1246"
T_S = WALL // 1000
# The JobManager clock runs 2.5 s ahead of the supervisor clock in every fixture,
# so a checkpoint is placed correctly only if the recorded offset is applied.
OFFSET = 2.5
JM_MS = int(OFFSET * 1000)
SOURCE_START = WALL
LAST_MAPPED = WALL + 70_000
K03 = {
    **CELL_A,
    "id": "k03-delay-control",
    "offered_rate": 20,
    "control_delay_millis": 100,
}
K11 = {**CELL_A, "id": "interrupt-control-k11"}
K02 = {**CELL_A, "id": "k02-pace-25", "offered_rate": 25}
SRC_DIR = Path(analyze.__file__).resolve().parent


def sized(cell):
    """A session cell whose record and attempt limits match its parameters."""
    records = cell_records(cell)
    return {
        **cell,
        "record_limit": records,
        "attempt_limit": protocol.attempt_limit(cell, records),
    }


class Cell(Synthetic):
    """Receipts, rows and observations of one cell on a fixed timeline.

    The source starts at ``WALL`` and maps its last record at ``WALL + 70 s``;
    ten checkpoints complete one second apart from the end of warm-up, so the
    window is [WALL + 60 s, WALL + 70 s] and holds ten checkpoints.
    """

    def __init__(self, cell, run_id=RUN):
        super().__init__(sized(cell), run_id)
        self.lines, self.completions = [], []

    def sources(self, start=SOURCE_START, last=LAST_MAPPED, incarnation=SRC):
        records = cell_records(self.cell)
        base = {
            "records": records,
            "warmup_seconds": self.cell["warmup_seconds"],
            "observation_seconds": self.cell["observation_seconds"],
            "offered_rate": self.cell["offered_rate"],
            "monotonic_nanos": NANOS,
        }
        self.receipt(
            "source", incarnation, "start", sequence=0, wall_millis=start, **base
        )
        self.receipt(
            "source",
            incarnation,
            "last-mapped",
            sequence=records - 1,
            wall_millis=last,
            **base,
        )
        return self

    def row(
        self,
        sequence,
        origin,
        latency=5_000_000,
        status="OK",
        attempt=1,
        incarnation=INC,
        process=PROC,
        origin_process=PROC,
        name=None,
        cell=None,
        run_id=None,
    ):
        cell = cell or self.cell
        origin_nanos = NANOS + (origin - WALL) * 1_000_000
        line = ",".join(
            [
                "CT1246",
                run_id or self.run_id,
                cell["id"],
                cell["arm"],
                incarnation,
                process,
                origin_process,
                str(sequence),
                str(attempt),
                str(origin),
                str(origin_nanos),
                str(origin_nanos + 1_000_000),
                str(origin_nanos + latency),
                str(latency if process == origin_process else -1),
                status,
                name or f"projects/p/locations/l/queues/q/tasks/{'a' * 32}",
            ]
        )
        self.lines.append(line)
        if status == "OK":
            self.completions.append(Fraction(origin, 1000) + Fraction(latency, 10**9))
        return line

    def finish(self, per_part=25, incarnation=INC, terminal=True, complete=True):
        chunks = [
            self.lines[i : i + per_part] for i in range(0, len(self.lines), per_part)
        ]
        for index, chunk in enumerate(chunks, start=1):
            self.part(incarnation, index, chunk)
        self.creator(
            incarnation,
            rows=len(self.lines),
            parts=len(chunks),
            terminal=terminal,
            complete=complete,
        )
        return self

    # -- observations ----------------------------------------------------------

    def source_sum(self, at, drift=0):
        """numRecordsOut at ``at``: completions so far, five in flight, drift."""
        return sum(1 for c in self.completions if c <= at) + 5 + drift * (at - T_S)

    def observation(
        self,
        at,
        checkpoint_count=10,
        drift=0,
        restarted=False,
        timestamps=None,
        sink=None,
        heap="4096",
    ):
        done = [checkpoint(k) for k in range(checkpoint_count) if T_S + 61 + k <= at]
        cell_id = self.cell["id"]
        return {
            "at": float(at),
            "cell": cell_id,
            "job_id": "job",
            "job": {
                "state": "RUNNING",
                "timestamps": timestamps or {"RUNNING": jm(T_S - 100)},
                "now": jm(at),
            },
            "jm_offset_seconds": OFFSET,
            "restarted": restarted,
            "checkpoints": {
                "counts": {"in_progress": 1 if at % 2 else 0, "completed": len(done)},
                "completed": done[-1] if done else {},
                "restored": {},
                # The observer lists every checkpoint in history, whatever its
                # status, so an in-progress id is always the newest entry.
                "history": [d["id"] for d in done[-10:]] + [len(done) + 1],
            },
            "details": [{"status": "COMPLETED", **d, "tasks": {}} for d in done],
            "details_skipped": [],
            "sink": sink
            or [
                {"id": "Writer.busyTimeMsPerSecond", "min": 1, "max": 250, "sum": 300},
                {"id": "Writer.numRecordsIn", "min": 0, "max": 0, "sum": 0},
            ],
            "source": [
                {
                    "id": "numRecordsOut",
                    "min": 0,
                    "max": 0,
                    "sum": self.source_sum(at, drift),
                }
            ],
            "taskmanagers": [
                {"id": "tm-1", "metrics": [{"id": analyze.HEAP_USED, "value": heap}]}
            ],
            "queue": {"state": "PAUSED", "stats": {}},
            "pods": [
                {
                    "name": cell_id + "-taskmanager",
                    "resources": {"flink-main-container": {"requests": {"cpu": "1"}}},
                }
            ],
        }

    def observations(self, **options):
        return [
            ("observation", self.observation(T_S + second, **options))
            for second in range(59, 72)
        ]


def jm(seconds):
    """A JobManager millisecond timestamp for a supervisor-clock instant."""
    return int(seconds * 1000) + JM_MS


def checkpoint(k):
    trigger = WALL + 60_000 + k * 1000
    return {
        "id": k + 1,
        "trigger_timestamp": trigger + JM_MS,
        "latest_ack_timestamp": trigger + 1000 + JM_MS,
        "end_to_end_duration": 1000,
        "checkpointed_size": 100 + k,
        "state_size": 200 + k,
    }


def standard_rows(cell, latency_step=1_000_000):
    """40 in-window rows plus the edge cases the analysis must classify."""
    for i in range(40):
        cell.row(600 + i, WALL + 60_000 + i * 250, latency=(i + 1) * latency_step)
    cell.row(599, WALL + 59_999, latency=latency_step)  # before the window
    cell.row(640, WALL + 70_001, latency=latency_step)  # after the window
    cell.row(641, WALL + 65_000, status="UNAVAILABLE")  # retried inside
    cell.row(641, WALL + 65_000, attempt=2, latency=3 * latency_step)
    cell.row(642, WALL + 66_000, status="UNAVAILABLE")  # never succeeds
    cell.row(643, WALL + 67_000, process=PROC2)  # OK with latency -1
    return cell


def paced_rows(cell, count, spacing_ms):
    for i in range(count):
        cell.row(i, WALL + 60_000 + i * spacing_ms)
    return cell


def export_marker(root, cell, kind=None, at="2026-09-19T00:00:00Z"):
    """The marker the collector would have written, from the real reconciler."""
    kind = kind or ev.cell_kind(cell.cell)
    result = ev.Reconciler(ev.DirectorySource(root), cell.run_id, cell.cell, kind).run()
    objects = [
        {"name": o["name"], "size": o["size"], "sha256": o["sha256"]}
        for o in result.objects
    ]
    marker = {
        "version": 1,
        "run_id": cell.run_id,
        "cell_id": cell.cell["id"],
        "kind": kind,
        "arm": cell.cell["arm"],
        "outcome": "completed",
        "at": at,
        "reconciliation": result.summary(),
        "objects": objects,
        "manifest_sha256": ev.manifest_sha256(objects),
        "evidence_bytes": sum(o["size"] for o in objects),
        "read_ops": 0,
    }
    path = root / cell.prefix / ev.MARKER
    path.write_text(json.dumps(marker, sort_keys=True) + "\n")
    return marker


def write_run(root, cells, events=None, run_id=RUN, campaign="main-1246", line="2.2.1"):
    run_dir = root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    session = [c.cell for c in cells]
    approval = {
        "run_id": run_id,
        "campaign": campaign,
        "flink_version": line,
        "scenario": "cloudtasks",
        "cells": session,
    }
    (run_dir / "approval.json").write_text(json.dumps(approval))
    (run_dir / "session.json").write_text(
        json.dumps({"campaign": campaign, "cells": session})
    )
    (run_dir / "supervisor").mkdir(exist_ok=True)
    if events is None:
        events = [e for c in cells for e in c.observations()]
    for index, (event, payload) in enumerate(events):
        (run_dir / "supervisor" / f"{index:05d}.json").write_text(
            json.dumps(
                {"event": event, "at": utc(payload.get("at", 0)), "payload": payload}
            )
        )
    for cell in cells:
        cell.install_dir(root)
        export_marker(root, cell)
    return run_dir


def complete_cell(cell=CELL_A, **options):
    return standard_rows(Cell(cell, **options).sources()).finish()


def analyzed(run_dir, **options):
    report = analyze.analyze(run_dir, **options)
    return report, {c["cell_id"]: c for c in report["cells"]}


def part_path(root, cell, index=1, incarnation=INC):
    return root / cell.prefix / f"rows/{incarnation}-{index:06d}.csv.gz"


def rewrite_marker(root, cell, edit):
    path = root / cell.prefix / ev.MARKER
    marker = json.loads(path.read_text())
    edit(marker)
    path.write_text(json.dumps(marker, sort_keys=True) + "\n")


# --- one cell -------------------------------------------------------------------


def test_complete_cell_throughput_p95_and_window_by_hand(tmp_path):
    cell = complete_cell()
    run_dir = write_run(tmp_path, [cell])
    report, cells = analyzed(run_dir)
    record = cells[CELL_A["id"]]
    assert record["status"] == "usable" and record["reasons"] == []
    assert record["integrity"]["status"] == "verified"
    assert record["reconciliation"]["consistent"] is True
    # JM timestamps carry the 2.5 s offset; the window is in the supervisor
    # clock and runs between checkpoint completions, the first at or after
    # warm-up (61 s) and the last at or before the final mapped record (70 s).
    assert record["window"]["start"] == Fraction(T_S + 61)
    assert record["window"]["end"] == Fraction(T_S + 70)
    assert record["window"]["seconds"] == Fraction(9)
    assert record["window"]["checkpoints_inside"] == 10
    assert record["window"]["rule_holds"] is True
    # 36 paced rows from 61 s, the retried sequence and the -1 latency row;
    # the rows at 59.999 s and 70.001 s and the never-successful one are out.
    assert record["rows"]["distinct_ok_in_window"] == 38
    assert record["throughput"] == Fraction(38, 9)
    # 37 latency samples (5..40 ms and 3 ms): ceil(0.95 * 37) = 36 -> 39 ms.
    assert record["rows"]["p95_sample"] == 37
    assert record["rows"]["p95_nanos"] == 39_000_000
    assert record["rows"]["latency_excluded"] == 1
    assert record["rows"]["foreign_rows"] == 0
    assert record["checkpoints"]["max_in_progress"] == 1
    assert record["checkpoints"]["latest_completed"] == {
        "id": 10,
        "end_to_end_duration": 1000,
        "checkpointed_size": 109,
        "state_size": 209,
    }
    assert record["metrics"]["taskmanager_heap_used_max"] == 4096
    assert record["metrics"]["busy_time_ms_per_second_max"] == 250
    assert record["metrics"]["replay_budget_min"] == {
        "stagedReplayBudgetMillis": None,
        "currentCommitReplayBudgetMillis": None,
    }
    assert record["backlog"]["status"] == "evaluated"
    assert record["backlog"]["sustained_growth"] is False
    assert record["pod_resources"] == {
        CELL_A["id"] + "-taskmanager": {
            "flink-main-container": {"requests": {"cpu": "1"}}
        }
    }
    assert record["recovery"] is None
    assert report["groups"] == [] and report["unmatched_cells"] == [CELL_A["id"]]
    assert report["calibration"] == []


def test_replay_budget_minima_come_from_staged_gauges(tmp_path):
    staged = {**CELL_A, "arm": "STAGED_HASH"}
    cell = Cell(staged).sources()
    standard_rows(cell).finish()
    sink = [
        {"id": "Writer.stagedReplayBudgetMillis", "min": 3500, "max": 3600, "sum": 0},
        {
            "id": "Committer.currentCommitReplayBudgetMillis",
            "min": 90,
            "max": 1,
            "sum": 0,
        },
    ]
    events = [
        ("observation", cell.observation(T_S + s, sink=[dict(m) for m in sink]))
        for s in range(59, 72)
    ]
    events[3][1]["sink"][0]["min"] = 3400
    run_dir = write_run(tmp_path, [cell], events)
    _, cells = analyzed(run_dir)
    assert cells[staged["id"]]["metrics"]["replay_budget_min"] == {
        "stagedReplayBudgetMillis": 3400,
        "currentCommitReplayBudgetMillis": 90,
    }


def test_missing_terminal_is_incomplete_and_not_usable(tmp_path):
    cell = standard_rows(Cell(CELL_A).sources()).finish(terminal=False)
    run_dir = write_run(tmp_path, [cell])
    _, cells = analyzed(run_dir)
    record = cells[CELL_A["id"]]
    assert record["reconciliation"]["marker"] == {
        "status": "incomplete",
        "reasons": ["missing-terminal"],
    }
    assert record["reconciliation"]["consistent"] is True
    assert record["status"] == "incomplete" and "incomplete" in record["reasons"]
    # The measurement is still reported for the record, never as a result.
    assert record["throughput"] == Fraction(38, 9)


def test_sha256_mismatch_marks_the_cell_tampered_and_skips_rows(tmp_path):
    cell = complete_cell()
    run_dir = write_run(tmp_path, [cell])
    rewrite_marker(
        tmp_path, cell, lambda m: m["objects"][0].__setitem__("sha256", "0" * 64)
    )
    _, cells = analyzed(run_dir)
    record = cells[CELL_A["id"]]
    assert record["status"] == "tampered"
    assert record["integrity"]["problems"] == [
        "manifest-mismatch",
        "sha256-mismatch:"
        + json.loads((run_dir / "cells" / CELL_A["id"] / "exported.json").read_text())[
            "objects"
        ][0]["name"],
    ]
    assert record["rows"] is None and record["throughput"] is None


def test_truncated_part_is_tampered_or_inconsistent(tmp_path):
    cell = complete_cell()
    run_dir = write_run(tmp_path, [cell])
    path = part_path(tmp_path, cell)
    data = path.read_bytes()
    path.write_bytes(data[: len(data) // 2])
    _, cells = analyzed(run_dir)
    record = cells[CELL_A["id"]]
    assert record["status"] == "tampered"
    assert any(p.startswith("size-mismatch:") for p in record["integrity"]["problems"])
    # A marker that vouches for the truncated bytes passes integrity; the
    # reconciler then fails to decode the part and differs from the marker.

    def vouch(marker):
        for obj in marker["objects"]:
            if obj["name"].endswith(path.name):
                obj.update(size=path.stat().st_size, sha256=analyze.sha256_of(path))
        marker["manifest_sha256"] = ev.manifest_sha256(marker["objects"])

    rewrite_marker(tmp_path, cell, vouch)
    _, cells = analyzed(run_dir)
    record = cells[CELL_A["id"]]
    assert record["integrity"]["status"] == "verified"
    assert record["status"] == "inconsistent"
    assert record["reasons"][0].startswith("reconciler-failed:")
    assert record["reconciliation"]["recomputed"]["status"] == "invalid"


def test_marker_and_recomputed_reconciliation_must_agree(tmp_path):
    cell = complete_cell()
    run_dir = write_run(tmp_path, [cell])
    rewrite_marker(
        tmp_path,
        cell,
        lambda m: m["reconciliation"].__setitem__("status", "incomplete"),
    )
    _, cells = analyzed(run_dir)
    record = cells[CELL_A["id"]]
    assert record["status"] == "inconsistent"
    assert record["reasons"] == ["reconciliation-differs"]
    assert record["reconciliation"]["recomputed"]["status"] == "complete"


def test_unexported_and_missing_objects(tmp_path):
    cell = complete_cell()
    run_dir = write_run(tmp_path, [cell])
    (tmp_path / cell.prefix / ev.MARKER).unlink()
    _, cells = analyzed(run_dir)
    assert cells[CELL_A["id"]]["status"] == "unexported"
    export_marker(tmp_path, cell)
    part_path(tmp_path, cell).unlink()
    _, cells = analyzed(run_dir)
    assert cells[CELL_A["id"]]["status"] == "tampered"
    assert any(
        p.startswith("missing-object:")
        for p in cells[CELL_A["id"]]["integrity"]["problems"]
    )


def test_unlisted_local_object_is_tampered(tmp_path):
    cell = complete_cell()
    run_dir = write_run(tmp_path, [cell])
    extra = tmp_path / cell.prefix / f"rows/{INC}-000009.csv.gz"
    extra.write_bytes(gzip.compress(b""))
    _, cells = analyzed(run_dir)
    problems = cells[CELL_A["id"]]["integrity"]["problems"]
    assert cells[CELL_A["id"]]["status"] == "tampered"
    assert problems == ["unlisted-object:" + cell.prefix + f"rows/{INC}-000009.csv.gz"]


def test_duplicate_and_out_of_order_rows_follow_the_reconciler(tmp_path):
    duplicated = standard_rows(Cell(CELL_A).sources())
    duplicated.lines.append(duplicated.lines[0])
    duplicated.finish()
    run_dir = write_run(tmp_path, [duplicated])
    _, cells = analyzed(run_dir)
    record = cells[CELL_A["id"]]
    assert record["reconciliation"]["marker"]["reasons"] == ["duplicate-row"]
    assert record["status"] == "invalid" and record["reconciliation"]["consistent"]
    # Out-of-order completion is counted by the reconciler, not a verdict.
    shuffled = Cell(CELL_A).sources()
    for i in reversed(range(40)):
        shuffled.row(600 + i, WALL + 60_000 + i * 250)
    shuffled.finish(per_part=10)
    run_dir = write_run(tmp_path / "second", [shuffled])
    _, cells = analyzed(run_dir)
    record = cells[CELL_A["id"]]
    assert record["status"] == "usable"
    assert record["reconciliation"]["counts"]["out_of_order"] > 0
    assert record["throughput"] == 4


def test_foreign_cell_rows_are_excluded_from_the_measurement(tmp_path):
    cell = Cell(CELL_A).sources()
    standard_rows(cell)
    cell.row(700, WALL + 65_000, cell={**CELL_A, "id": "other-cell"})
    cell.finish()
    run_dir = write_run(tmp_path, [cell])
    _, cells = analyzed(run_dir)
    record = cells[CELL_A["id"]]
    assert record["reconciliation"]["marker"]["reasons"] == ["foreign-row"]
    assert record["status"] == "invalid"
    assert record["rows"]["foreign_rows"] == 1
    assert record["rows"]["distinct_ok_in_window"] == 38


def test_five_checkpoints_is_window_too_short(tmp_path):
    cell = complete_cell()
    events = [e for e in cell.observations(checkpoint_count=5)]
    run_dir = write_run(tmp_path, [cell], events)
    _, cells = analyzed(run_dir)
    record = cells[CELL_A["id"]]
    assert record["status"] == "inconclusive"
    assert record["reasons"] == ["window-too-short"]
    assert record["window"] is None and record["throughput"] is None
    assert record["backlog"] == {"status": "not-evaluable", "reason": "no-window"}


def test_six_checkpoints_is_the_shortest_window(tmp_path):
    cell = complete_cell()
    run_dir = write_run(tmp_path, [cell], cell.observations(checkpoint_count=6))
    _, cells = analyzed(run_dir)
    record = cells[CELL_A["id"]]
    assert record["status"] == "usable"
    assert record["window"]["checkpoints_inside"] == 6
    # Six completions one second apart: five seconds between the first and last.
    assert record["window"]["seconds"] == 5


def test_restarted_observation_makes_the_cell_inconclusive(tmp_path):
    cell = complete_cell()
    events = cell.observations()
    events[6][1]["restarted"] = True
    run_dir = write_run(tmp_path, [cell], events)
    _, cells = analyzed(run_dir)
    record = cells[CELL_A["id"]]
    assert record["status"] == "inconclusive" and record["reasons"] == ["restarted"]
    assert record["throughput"] == Fraction(38, 9)


def test_warmup_end_uses_the_source_start_receipt(tmp_path):
    # A source that started 3 s later pushes the first admissible checkpoint out.
    cell = standard_rows(Cell(CELL_A).sources(start=SOURCE_START + 3000)).finish()
    run_dir = write_run(tmp_path, [cell])
    _, cells = analyzed(run_dir)
    record = cells[CELL_A["id"]]
    assert record["window"]["first_checkpoint"] == 3
    assert record["window"]["start"] == Fraction(T_S + 63)
    assert record["window"]["checkpoints_inside"] == 8


# --- backlog --------------------------------------------------------------------


def test_backlog_growth_positive_and_negative(tmp_path):
    steady = complete_cell()
    growing = complete_cell(run_id="growing")
    write_run(tmp_path, [steady], steady.observations())
    write_run(tmp_path, [growing], growing.observations(drift=10), run_id="growing")
    report, _ = analyzed(tmp_path / "runs")
    assert [r["run_id"] for r in report["runs"]] == [RUN, "growing"]
    by_run = {c["run_id"]: c for c in report["cells"]}
    flat = by_run[RUN]["backlog"]
    assert flat["tolerance"] == 2  # max(0.05 * 10 * 1, 2 * 1 * 1)
    assert flat["boundaries"] == 10
    assert flat["values"] == [Fraction(5)] * 10
    assert flat["sustained_growth"] is False
    grown = by_run["growing"]["backlog"]
    assert grown["values"][1] - grown["values"][0] == 10
    assert grown["three_consecutive_over_tolerance"] is True
    assert grown["overall_over_slope"] is True
    assert grown["sustained_growth"] is True


def test_backlog_needs_four_boundaries():
    span = {
        "start": Fraction(0),
        "end": Fraction(3),
        "seconds": Fraction(3),
        "boundaries": [Fraction(1), Fraction(2), Fraction(3)],
    }
    observations = [
        {"at": float(t), "source": [{"id": "numRecordsOut", "sum": t}]}
        for t in range(5)
    ]
    result = analyze.backlog(CELL_A, span, observations, [])
    assert result == {
        "status": "not-evaluable",
        "reason": "fewer-than-four-boundaries",
        "boundaries": 3,
    }


# --- groups -------------------------------------------------------------------


def repetition(index, throughput, p95=100, offered_rate=10):
    return {
        "cell_id": f"c{index}",
        "status": "usable",
        "throughput": Fraction(throughput),
        "offered_rate": offered_rate,
        "repetition": index,
        "repeat": False,
        "rows": {"p95_nanos": p95},
    }


def arm(*throughputs, p95=100):
    return analyze.arm_statistics(
        [repetition(i, t, p95) for i, t in enumerate(throughputs, start=1)]
    )


def test_range_exactly_ten_percent_is_usable_and_above_is_inconclusive():
    base = arm(95, 100, 105)
    assert base["range_over_mean"] == Fraction(1, 10)
    assert analyze.compare(base, arm(100, 100, 100))["label"] == "general"
    wide = arm(Fraction("94.95"), 100, Fraction("105.05"))
    assert wide["range_over_mean"] == Fraction(101, 1000)
    result = analyze.compare(wide, arm(100, 100, 100))
    assert result["label"] == "inconclusive" and result["reasons"] == ["range-baseline"]
    result = analyze.compare(arm(100, 100, 100), wide)
    assert result["label"] == "inconclusive" and result["reasons"] == [
        "range-candidate"
    ]


@pytest.mark.parametrize(
    ("throughput", "p95", "label"),
    [
        (70, 200, "general"),
        (Fraction("69.999"), 200, "constrained"),
        (70, 201, "constrained"),
        (25, 400, "constrained"),
        (Fraction("24.999"), 400, "decline"),
        (25, 401, "decline"),
    ],
)
def test_ratio_thresholds_pass_exactly(throughput, p95, label):
    base = arm(100, 100, 100)
    result = analyze.compare(base, arm(throughput, throughput, throughput, p95=p95))
    assert result["label"] == label
    assert result["throughput_ratio"] == Fraction(throughput, 100)
    assert result["p95_ratio"] == Fraction(p95, 100)


def test_fewer_than_three_usable_repetitions_is_inconclusive():
    arms = {name: arm(100, 100, 100) for name in analyze.ARMS}
    arms["NAMED_HASH"] = arm(100, 100)
    verdict = analyze.group_verdict(arms)
    assert verdict["short_arms"] == ["NAMED_HASH"]
    assert verdict["verdict"] == "inconclusive"
    assert verdict["comparisons"]["STAGED_HASH"]["reasons"] == ["arm-short:NAMED_HASH"]
    # The incremental cost is still reported from what is there.
    assert verdict["incremental_cost"]["STAGED_HASH"]["baseline_usable"] == 2
    assert verdict["incremental_cost"]["STAGED_HASH"]["throughput_ratio"] == 1


def test_p95_is_the_mean_of_repetitions_never_pooled():
    statistics = analyze.arm_statistics(
        [repetition(1, 100, 100), repetition(2, 100, 400), repetition(3, 100, 100)]
    )
    assert statistics["mean_p95_nanos"] == Fraction(200)


def test_a_repeat_replaces_only_a_failed_repetition():
    original = repetition(1, 100)
    repeat = {**repetition(1, 50), "cell_id": "c1-x2", "repeat": True}
    assert analyze.arm_statistics([original, repeat])["cells"] == ["c1"]
    failed = {**original, "status": "inconclusive"}
    assert analyze.arm_statistics([failed, repeat])["cells"] == ["c1-x2"]


def s1_group():
    pinned = protocol.load()["cells"]
    return [
        c
        for c in pinned
        if c["flink"] == "2.2.1" and c["shape"] == "s1" and c["body_bytes"] == 1024
    ]


def test_group_verdict_end_to_end_across_two_runs_of_one_campaign(tmp_path):
    pinned = s1_group()
    assert len(pinned) == 15
    cells = [
        complete_cell(protocol.session_cell(pc, 10), run_id=f"run-{i % 2}")
        for i, pc in enumerate(pinned)
    ]
    for run_id in ("run-0", "run-1"):
        members = [c for c in cells if c.run_id == run_id]
        write_run(tmp_path, members, run_id=run_id)
    report, records = analyzed(tmp_path / "runs")
    assert report["unmatched_cells"] == []
    assert all(r["status"] == "usable" for r in records.values())
    [group] = report["groups"]
    assert (group["campaign"], group["flink"], group["shape"], group["body_bytes"]) == (
        "main-1246",
        "2.2.1",
        "s1",
        1024,
    )
    assert group["shape_parameters"] == {
        "parallelism": 1,
        "concurrency": 1,
        "checkpoint_seconds": 1,
        "distribution": "even",
    }
    assert group["short_arms"] == [] and group["verdict"] == "general"
    for name in analyze.CANDIDATES:
        comparison = group["comparisons"][name]
        assert comparison["label"] == "general"
        assert comparison["throughput_ratio"] == 1 and comparison["p95_ratio"] == 1
        assert group["incremental_cost"][name]["throughput_ratio"] == 1
    assert group["arms"]["UNNAMED"]["mean_throughput"] == Fraction(38, 9)
    assert group["arms"]["UNNAMED"]["range_over_mean"] == 0
    assert group["capacity"] is None


# --- capacity -----------------------------------------------------------------


def test_capacity_bracket_from_probe_cells(tmp_path):
    [unnamed] = [
        c for c in s1_group() if c["arm"] == "UNNAMED" and c["repetition"] == 1
    ]
    probes = []
    for index, (rate, rows, spacing) in enumerate(
        ((10, 100, 100), (20, 200, 50), (40, 300, 33)), start=1
    ):
        cell = Cell(protocol.session_cell(unnamed, rate, f"-q{index}")).sources()
        probes.append(paced_rows(cell, rows, spacing).finish())
    run_dir = write_run(tmp_path, probes)
    report, records = analyzed(run_dir)
    [group] = report["groups"]
    capacity = group["capacity"]
    assert [p["accepted"] for p in capacity["probes"]] == [True, True, False]
    assert capacity["probes"][2]["reasons"] == ["below-admitted-fraction"]
    # 269 of the 300 rows lie inside the nine-second window: 29.9 per second
    # against an offered 40, below the 95 % admission fraction.
    assert capacity["probes"][2]["throughput"] == Fraction(269, 9)
    assert capacity["state"] == {"initial": 10, "accepted": 20, "rejected": 40}
    assert (capacity["status"], capacity["next"]) == ("bracketed", 30)
    # Probes never count as repetitions.
    assert (
        group["arms"]["UNNAMED"]["usable"] == 0 and group["verdict"] == "inconclusive"
    )
    assert records[unnamed["cell_id"] + "-q3"]["protocol_cell_id"] == unnamed["cell_id"]


def test_probe_admission_needs_completeness_and_a_flat_backlog():
    accepted = {
        "status": "usable",
        "throughput": Fraction(19),
        "offered_rate": 20,
        "backlog": {"sustained_growth": False},
    }
    assert analyze.probe_accepted(accepted) == (True, [])
    assert analyze.probe_accepted(
        {**accepted, "throughput": Fraction(18999, 1000)}
    ) == (
        False,
        ["below-admitted-fraction"],
    )
    assert analyze.probe_accepted(
        {**accepted, "backlog": {"sustained_growth": True}}
    ) == (
        False,
        ["sustained-backlog-growth"],
    )
    # A window too short to hold four checkpoint boundaries measured nothing
    # about saturation, so the probe is repeated rather than rejected.
    assert analyze.probe_accepted(
        {**accepted, "backlog": {"status": "not-evaluable"}}
    ) == (
        None,
        ["backlog-not-evaluable"],
    )
    # Achieving less than the admitted fraction is evidence of saturation even
    # when the backlog could not be evaluated.
    assert analyze.probe_accepted(
        {
            **accepted,
            "throughput": Fraction(1),
            "backlog": {"status": "not-evaluable"},
        }
    ) == (False, ["below-admitted-fraction"])
    assert analyze.probe_accepted({**accepted, "status": "inconclusive"}) == (
        None,
        ["not-measured:inconclusive"],
    )
    assert analyze.probe_accepted({**accepted, "status": "restarted"}) == (
        None,
        ["not-measured:restarted"],
    )
    assert analyze.probe_accepted({**accepted, "status": "tampered"}) == (
        None,
        ["excluded:tampered"],
    )
    assert analyze.probe_accepted({**accepted, "throughput": None}) == (
        None,
        ["no-throughput"],
    )


# --- calibration --------------------------------------------------------------


def test_delay_control_detection_positive_and_negative(tmp_path):
    delayed = complete_cell(K03)
    assert delayed.cell["record_limit"] == 4860
    run_dir = write_run(
        tmp_path, [standard_rows(Cell(K03).sources(), 150_000_000).finish()]
    )
    report, records = analyzed(run_dir)
    record = records[K03["id"]]
    assert record["kind"] == "calibration" and record["status"] == "usable"
    control = report["calibration"][0]["delay_control"]
    assert control["throughput"] == Fraction(38, 9)
    assert control["p95_nanos"] == 39 * 150_000_000
    assert control["detected"] is True
    run_dir = write_run(tmp_path / "fast", [delayed])
    report, _ = analyzed(run_dir)
    assert report["calibration"][0]["delay_control"]["p95_nanos"] == 39_000_000
    assert report["calibration"][0]["delay_control"]["detected"] is False
    assert report["calibration"][0]["acceptance"][0] == {
        "item": "k03 detected and k02 admitted",
        "holds": None,
    }


def test_pacing_gauges_task_names_and_csv_pair(tmp_path):
    k02 = paced_rows(
        Cell({**CELL_A, "id": "k02-pace-25", "offered_rate": 25}).sources(), 250, 40
    )
    k04 = paced_rows(
        Cell(
            {
                **CELL_A,
                "id": "k04-pace-100",
                "offered_rate": 100,
                "parallelism": 4,
                "concurrency": 4,
            }
        ).sources(),
        900,
        11,
    )
    k07 = Cell(
        {
            **CELL_A,
            "id": "k07-counts-only",
            "offered_rate": 100,
            "parallelism": 4,
            "concurrency": 4,
            "emit_attempts": False,
        }
    ).sources()
    k09 = paced_rows(
        Cell({**CELL_A, "id": "k09-staged-gauges", "arm": "STAGED_HASH"}).sources(),
        100,
        100,
    )
    k10 = Cell(
        {**CELL_A, "id": "k10-random-control", "arm": "NAMED_RANDOM_CONTROL"}
    ).sources()
    for i in range(100):
        k10.row(
            i,
            WALL + 60_000 + i * 100,
            name=f"projects/p/locations/l/queues/q/tasks/{i:032x}",
        )
    cells = [c.finish() for c in (k02, k04, k09, k10)]
    k07.creator(INC, rows=0, parts=0)
    cells.append(k07)
    sink_on = [
        {"id": "Writer.busyTimeMsPerSecond", "min": 1, "max": 400, "sum": 0},
        {"id": "Writer.numRecordsIn", "min": 0, "max": 0, "sum": 0},
    ]
    events = [e for c in (k02, k09, k10) for e in c.observations()]
    events += k04.observations(sink=sink_on, heap="9000")
    events += [
        (
            "observation",
            k07.observation(
                T_S + s,
                heap="7000",
                sink=[
                    {
                        "id": "Writer.busyTimeMsPerSecond",
                        "min": 1,
                        "max": 300,
                        "sum": 0,
                    },
                    {
                        "id": "Writer.numRecordsIn",
                        "min": 0,
                        "max": 0,
                        "sum": 100 * (s - 59),
                    },
                ],
            ),
        )
        for s in range(59, 72)
    ]
    events.append(
        (
            "cell-metrics-discovered",
            {
                "cell": "k09-staged-gauges",
                "at": T_S + 50.0,
                "metrics": [
                    "Writer.stagedBytes",
                    "Writer.stagedReplayBudgetMillis",
                    "Writer.numRecordsIn",
                ],
            },
        )
    )
    events.append(
        (
            "cell-metrics-discovered",
            {
                "cell": "k04-pace-100",
                "at": T_S + 50.0,
                "metrics": ["Writer.numRecordsIn"],
            },
        )
    )
    run_dir = write_run(tmp_path, cells, events, campaign="calibration-1246")
    report, records = analyzed(run_dir, kind="calibration")
    calibration = report["calibration"][0]
    assert records["k07-counts-only"]["status"] == "usable"
    assert records["k07-counts-only"]["rows"]["rows"] == 0
    assert calibration["pacing"]["k02-pace-25"]["achieved_over_offered"] == 1
    assert calibration["pacing"]["k02-pace-25"]["admitted"] is True
    assert calibration["pacing"]["k04-pace-100"]["throughput"] == Fraction(809, 9)
    assert calibration["pacing"]["k04-pace-100"]["admitted"] is False
    assert calibration["gauges"]["k09-staged-gauges"]["staged_gauges_present"] is True
    assert calibration["gauges"]["k04-pace-100"]["staged_gauges_present"] is False
    assert calibration["task_names"] == {
        "k10-random-control": {"hex32": 100},
        "k09-staged-gauges": {"hex32": 100},
    }
    overhead = calibration["csv_overhead"]
    assert overhead["deltas"]["busy_time"] == -100
    assert overhead["deltas"]["taskmanager_heap"] == -2000
    assert overhead["deltas"]["achieved_rate"] == 100  # k07 sink rate 100/s, k04 0/s
    assert overhead["rows_throughput"] == Fraction(809, 9)
    holds = {item["item"]: item["holds"] for item in calibration["acceptance"]}
    assert holds["connector gauges discovered on k09 and absent on k04"] is True
    assert holds["k10 task names are 32 hex characters"] is True
    assert holds["every steady-state cell reconciles complete and verifies"] is True
    assert holds["pacing cells admitted without sustained backlog growth"] is None


def test_interrupt_control_is_refused_with_recovery_offsets(tmp_path):
    control = standard_rows(Cell(K11).sources()).finish(terminal=False)
    control.creator(INC2, rows=0, parts=0, wall=WALL + 90_000)
    events = control.observations()
    events.append(
        (
            "interrupt-issued",
            {
                "cell": K11["id"],
                "pod_uid": "u",
                "pod": "p",
                "at": T_S + 80.0,
                "deleted": True,
            },
        )
    )
    events.append(
        (
            "observation",
            control.observation(
                T_S + 85,
                restarted=True,
                timestamps={"RESTARTING": jm(T_S + 82), "RUNNING": jm(T_S - 100)},
            ),
        )
    )
    events.append(
        (
            "observation",
            control.observation(
                T_S + 95,
                timestamps={"RESTARTING": jm(T_S + 82), "RUNNING": jm(T_S + 91)},
            ),
        )
    )
    # The restart the control causes makes the source register a second
    # incarnation, exactly as the application does on a real interrupt.
    control.sources(start=641, last=LAST_MAPPED, incarnation=SRC2)
    run_dir = write_run(tmp_path, [control], events, campaign="calibration-1246")
    report, records = analyzed(run_dir)
    record = records[K11["id"]]
    assert record["reconciliation"]["recomputed"]["status"] == "restarted"
    assert "second-source" in record["reconciliation"]["recomputed"]["reasons"]
    assert record["status"] == "restarted"
    assert record["recovery"] == {
        "interrupt_issued": {"at": Fraction(T_S + 80), "clock": "supervisor"},
        "restarting": {"seconds_after_interrupt": 2, "clock": "jobmanager-via-offset"},
        "running": {"seconds_after_interrupt": 11, "clock": "jobmanager-via-offset"},
        "creator_start": {"seconds_after_interrupt": 10, "clock": "taskmanager-wall"},
    }
    section = report["calibration"][0]["interrupt_control"]
    assert section["reconciles_interrupted"] and section["steady_state_refused"]
    assert section["marker_status"] == "restarted"
    assert report["calibration"][0]["acceptance"][-1]["holds"] is True


def test_staged_gauge_names_match_the_observer():
    assert analyze.STAGED_GAUGES == observe.STAGED_REQUIRED
    assert set(analyze.REPLAY_BUDGET) <= observe.CONNECTOR_METRICS
    assert analyze.HEAP_USED in observe.JVM_METRICS


# --- reports and command -------------------------------------------------------


def test_reports_are_byte_identical_and_render_fractions(tmp_path):
    run_dir = write_run(tmp_path, [complete_cell()])
    first = analyze.write_reports(analyze.analyze(run_dir), tmp_path / "one")
    second = analyze.write_reports(analyze.analyze(run_dir), tmp_path / "two")
    for name in ("report.json", "report.md"):
        assert (first / name).read_bytes() == (second / name).read_bytes()
    report = json.loads((first / "report.json").read_text())
    [cell] = report["cells"]
    assert cell["throughput"] == {"fraction": "38/9", "float": 38 / 9}
    assert list(report) == sorted(report)
    markdown = (first / "report.md").read_text()
    assert all(
        line.endswith(".") or line.startswith("#") or not line
        for line in markdown.splitlines()
    )
    assert f"Cell {CELL_A['id']} of run {RUN} is usable" in markdown
    assert "support or release decision" in markdown


def test_command_runs_without_a_checkout_and_prints_one_line(
    tmp_path, monkeypatch, capsys
):
    run_dir = write_run(tmp_path, [complete_cell()])
    monkeypatch.chdir(tmp_path)
    assert cli.main(["analyze", "--evidence", str(run_dir)]) == 0
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        f"analyzed 1 cells in 1 runs (usable 1); 0 groups; reports in {run_dir / 'analysis'}"
    ]
    assert (run_dir / "analysis/report.json").is_file()
    assert cli.main(["analyze", "--evidence", str(tmp_path / "nowhere")]) == 1
    assert "analysis failed" in capsys.readouterr().out


def test_help_works_without_a_checkout(tmp_path):
    result = subprocess.run(
        [sys.executable, "-m", "flink_tier3", "analyze", "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )
    assert "usage: flink-tier3 analyze" in result.stdout
    assert "--evidence" in result.stdout


def test_analyze_module_does_not_import_google_cloud_or_kubernetes(tmp_path):
    # The package __init__ eagerly re-exports the cluster clients, so the module
    # is loaded under a bare package shim: what it imports is what it needs.
    code = """
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
source = Path(sys.argv[1])
spec = spec_from_file_location(
    "flink_tier3", source / "__init__.py", submodule_search_locations=[str(source)]
)
sys.modules["flink_tier3"] = module_from_spec(spec)  # never executed
import flink_tier3.analyze
loaded = sorted(
    m for m in sys.modules
    if m == "kubernetes" or m.startswith(("google.cloud", "kubernetes."))
)
print(sys.modules["flink_tier3.evidence"].__name__, loaded)
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(SRC_DIR)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "flink_tier3.evidence []"


# --- review round one: deduplicated creates and colliding calibration IDs ---------


def named(cell_id="named-dedup", arm="NAMED_HASH"):
    """A named cell whose retry is answered by the service's deduplication."""
    cell = Cell({**CELL_A, "id": cell_id, "arm": arm}).sources()
    for i in range(40):
        cell.row(600 + i, WALL + 60_000 + i * 250, latency=(i + 1) * 1_000_000)
    name = "projects/p/locations/l/queues/q/tasks/" + "b" * 32
    cell.row(640, WALL + 65_000, status="UNAVAILABLE", name=name)
    cell.row(640, WALL + 65_000, attempt=2, status="ALREADY_EXISTS", name=name)
    return cell.finish()


def test_a_deduplicated_create_counts_as_a_created_record(tmp_path):
    cell = named()
    _report, cells = analyzed(write_run(tmp_path, [cell]))
    record = cells["named-dedup"]
    assert record["status"] == "usable"
    assert record["reconciliation"]["counts"]["already_exists_explained"] == 1
    assert record["reconciliation"]["recomputed"]["status"] == "complete"
    # 36 of the paced records lie inside the nine-second window, plus the one
    # the service answered with ALREADY_EXISTS.
    assert record["rows"]["distinct_ok_in_window"] == 37
    assert record["throughput"] == Fraction(37, 9)


def test_an_unnamed_arm_never_counts_an_already_exists(tmp_path):
    cell = named("unnamed-dedup", "UNNAMED")
    _report, cells = analyzed(write_run(tmp_path, [cell]))
    record = cells["unnamed-dedup"]
    # The unnamed path cannot deduplicate, so the answer is a defect.
    assert record["status"] == "invalid" and record["reasons"] == ["invalid"]
    assert record["reconciliation"]["recomputed"] == {
        "status": "invalid",
        "reasons": ["unexplained-duplicate"],
    }
    # The measurement is still reported for the record, but an invalid cell
    # never contributes to an arm, and the sequence counts only its OK row.
    assert record["rows"]["distinct_ok_in_window"] == 36
    assert record["throughput"] == Fraction(36, 9)


def test_a_second_run_of_a_calibration_cell_is_not_masked_by_the_first(tmp_path):
    first = complete_cell(K03, run_id="cal-a")
    write_run(tmp_path, [first], run_id="cal-a", campaign="calibration")
    (tmp_path / first.prefix / ev.MARKER).unlink()
    second = complete_cell(K03, run_id="cal-b")
    write_run(tmp_path, [second], run_id="cal-b", campaign="calibration")
    report, _cells = analyzed(tmp_path / "runs", kind="calibration")
    delay = report["calibration"][0]["delay_control"]
    assert delay["status"] == "usable"  # the exported run wins over the unexported
    assert delay["cell_id"] == K03["id"]


def test_a_checkpoint_that_only_appears_in_history_does_not_open_a_window(tmp_path):
    cell = complete_cell()
    run_dir = write_run(tmp_path, [cell], events=cell.observations(checkpoint_count=5))
    _report, cells = analyzed(run_dir)
    record = cells[CELL_A["id"]]
    # Five completed checkpoints and a sixth listed but not completed.
    assert record["checkpoints"]["history_only"] == 1
    assert record["status"] == "inconclusive"
    assert record["reasons"] == ["window-too-short"]


def test_the_window_includes_a_row_and_a_completion_on_its_edges(tmp_path):
    cell = Cell(CELL_A).sources()
    for i in range(40):
        cell.row(600 + i, WALL + 60_000 + i * 250, latency=(i + 1) * 1_000_000)
    cell.row(700, WALL + 70_000, latency=1_000_000)  # exactly the window's end
    cell.finish()
    _report, cells = analyzed(write_run(tmp_path, [cell]))
    record = cells[CELL_A["id"]]
    assert record["window"]["start"] == Fraction(T_S + 61)
    assert record["window"]["end"] == Fraction(T_S + 70)
    # Both edges count: 36 rows from 61.000 s and the one at exactly 70.000 s.
    assert record["rows"]["distinct_ok_in_window"] == 37
    assert record["rows"]["p95_sample"] == 37


def test_p95_uses_the_nearest_rank_the_preregistration_names(tmp_path):
    # Thirty samples, all from the window's opening edge, with latencies of
    # 1 to 30 ms: nearest rank is ceil(0.95 * 30) = 29 -> 29 ms, which
    # floor(0.95 * 30) = 28 -> 28 ms and the largest, 30 ms, both miss.
    cell = Cell(CELL_A).sources()
    for i in range(30):
        cell.row(600 + i, WALL + 61_000 + i * 250, latency=(i + 1) * 1_000_000)
    cell.finish()
    _report, cells = analyzed(write_run(tmp_path, [cell]))
    record = cells[CELL_A["id"]]
    assert record["rows"]["p95_sample"] == 30
    assert record["rows"]["p95_nanos"] == 29_000_000


def test_an_interrupt_control_that_finished_cleanly_fails_its_acceptance(tmp_path):
    """A k11 cell with no interruption is not the control the protocol asked for."""
    control = complete_cell(K11)
    run_dir = write_run(
        tmp_path, [control], control.observations(), campaign="calibration-1246"
    )
    report, records = analyzed(run_dir)
    assert records[K11["id"]]["status"] == "interrupt-control"
    section = report["calibration"][0]["interrupt_control"]
    assert section["marker_status"] == "complete"
    assert section["reconciles_interrupted"] is False
    assert report["calibration"][0]["acceptance"][-1]["holds"] is False


# --- review round three: protocol conditions, outcomes and calibration lines ---


def test_a_cell_run_on_the_other_line_belongs_to_no_group(tmp_path):
    pinned = s1_group()
    cells = [complete_cell(protocol.session_cell(pc, 10)) for pc in pinned]
    # The IDs name 2.2.1 entries; the session ran on 1.20.4.
    run_dir = write_run(tmp_path, cells, line="1.20.4")
    report, records = analyzed(run_dir)
    [group] = report["groups"]
    for record in records.values():
        assert record["status"] == "inconclusive"
        assert "conditions-differ-from-protocol:flink" in record["reasons"]
    assert group["verdict"] == "inconclusive"
    assert group["arms"]["UNNAMED"]["usable"] == 0


def test_a_cell_run_with_another_body_belongs_to_no_group(tmp_path):
    [pinned] = [c for c in s1_group() if c["arm"] == "UNNAMED" and c["repetition"] == 1]
    executed = {**protocol.session_cell(pinned, 10), "body_bytes": 65536}
    run_dir = write_run(tmp_path, [complete_cell(executed)])
    _report, records = analyzed(run_dir)
    record = records[pinned["cell_id"]]
    assert record["status"] == "inconclusive"
    assert record["reasons"] == ["conditions-differ-from-protocol:body_bytes"]


def test_arms_measured_at_different_rates_do_not_compare(tmp_path):
    pinned = s1_group()
    cells = [
        complete_cell(
            protocol.session_cell(pc, 20 if pc["arm"] == "STAGED_HASH" else 10)
        )
        for pc in pinned
    ]
    report, records = analyzed(write_run(tmp_path, cells))
    assert all(r["status"] == "usable" for r in records.values())
    [group] = report["groups"]
    # Every arm is complete and identical, but one ran at a rate the capacity
    # search never froze for this group.
    assert group["offered_rates"] == [10, 20]
    assert group["frozen_rate"] is None
    assert group["reasons"] == ["offered-rate-differs-across-arms"]
    assert group["verdict"] == "inconclusive"


def test_one_frozen_rate_leaves_the_group_comparable(tmp_path):
    cells = [complete_cell(protocol.session_cell(pc, 10)) for pc in s1_group()]
    report, _records = analyzed(write_run(tmp_path, cells))
    [group] = report["groups"]
    assert (group["offered_rates"], group["frozen_rate"]) == ([10], 10)
    assert group["reasons"] == [] and group["verdict"] == "general"


def test_a_job_that_did_not_complete_carries_no_steady_state_result(tmp_path):
    cell = complete_cell()
    run_dir = write_run(tmp_path, [cell])
    rewrite_marker(tmp_path, cell, lambda m: m.update(outcome="failed"))
    _report, records = analyzed(run_dir)
    record = records[CELL_A["id"]]
    # The rows and receipts reconcile, but the supervisor recorded a job that
    # exhausted its deadline; its window is not a steady state.
    assert record["reconciliation"]["recomputed"]["status"] == "complete"
    assert record["outcome"] == "failed"
    assert record["status"] == "inconclusive"
    assert record["reasons"] == ["outcome:failed"]


def test_calibration_acceptance_is_reported_per_line(tmp_path):
    for line, run_id in (("2.2.1", "cal-221"), ("1.20.4", "cal-120")):
        cell = complete_cell(K03, run_id=run_id)
        write_run(tmp_path, [cell], run_id=run_id, campaign="calibration", line=line)
        if line == "1.20.4":
            # The 1.20.4 repeat of the same ID was never exported.
            (tmp_path / cell.prefix / ev.MARKER).unlink()
    report, _records = analyzed(tmp_path / "runs", kind="calibration")
    sections = {section["flink"]: section for section in report["calibration"]}
    assert sorted(sections) == ["1.20.4", "2.2.1"]
    assert sections["2.2.1"]["delay_control"]["status"] == "usable"
    # A usable 2.2.1 record never stands in for the missing 1.20.4 one.
    assert sections["1.20.4"]["delay_control"]["status"] == "unexported"


def test_an_interrupted_probe_does_not_freeze_the_capacity_search(tmp_path):
    [unnamed] = [
        c for c in s1_group() if c["arm"] == "UNNAMED" and c["repetition"] == 1
    ]
    probes = []
    for index, (rate, rows, spacing) in enumerate(
        ((10, 100, 100), (20, 200, 50), (40, 400, 25)), start=1
    ):
        cell = Cell(protocol.session_cell(unnamed, rate, f"-q{index}")).sources()
        probes.append(paced_rows(cell, rows, spacing).finish())
    # The third probe's TaskManager was preempted, so its source registered a
    # second incarnation: the rate was offered but never measured.
    probes[2].sources(incarnation=SRC2)
    run_dir = write_run(tmp_path, probes)
    report, _records = analyzed(run_dir)
    capacity = report["groups"][0]["capacity"]
    assert [p["accepted"] for p in capacity["probes"]] == [True, True, None]
    assert capacity["probes"][2]["reasons"] == ["not-measured:restarted"]
    # The interruption leaves the bracket open; the search offers 40 again
    # rather than freezing the group at 20.
    assert capacity["state"] == {"initial": 10, "accepted": 20, "rejected": None}
    assert (capacity["status"], capacity["next"]) == ("searching", 40)


# --- review round four: window conditions, rate sources and stalled searches ---


def test_a_cell_measured_over_another_window_belongs_to_no_group(tmp_path):
    [pinned] = [c for c in s1_group() if c["arm"] == "UNNAMED" and c["repetition"] == 1]
    executed = {
        **protocol.session_cell(pinned, 10),
        "warmup_seconds": 30,
        "observation_seconds": 90,
    }
    run_dir = write_run(tmp_path, [complete_cell(executed)])
    _report, records = analyzed(run_dir)
    record = records[pinned["cell_id"]]
    # The window is one of the conditions the protocol pins, so a cell observed
    # over a shorter one is not the pinned repetition however clean its rows.
    assert record["window"]["rule_holds"] is False
    assert record["status"] == "inconclusive"
    assert record["reasons"] == [
        "conditions-differ-from-protocol:observation_seconds,warmup_seconds"
    ]


def test_an_unexported_earlier_run_does_not_unfreeze_the_group_rate(tmp_path):
    pinned = s1_group()
    [first] = [c for c in pinned if c["arm"] == "UNNAMED" and c["repetition"] == 1]
    # An earlier session offered 20 and lost its export; the reviewed session
    # re-ran the whole group at the frozen 10.
    lost = complete_cell(protocol.session_cell(first, 20), run_id="run-a")
    write_run(tmp_path, [lost], run_id="run-a")
    (tmp_path / lost.prefix / ev.MARKER).unlink()
    cells = [
        complete_cell(protocol.session_cell(pc, 10), run_id="run-b") for pc in pinned
    ]
    write_run(tmp_path, cells, run_id="run-b")
    report, records = analyzed(tmp_path / "runs")
    assert records[first["cell_id"]]["status"] == "usable"  # the exported one wins
    [group] = report["groups"]
    assert (group["offered_rates"], group["frozen_rate"]) == ([10], 10)
    assert group["reasons"] == [] and group["verdict"] == "general"


def test_a_rate_offered_twice_without_a_measurement_stalls_the_search(tmp_path):
    [unnamed] = [
        c for c in s1_group() if c["arm"] == "UNNAMED" and c["repetition"] == 1
    ]
    probes = []
    for index, rate in ((1, 10), (2, 20), (3, 20)):
        cell = Cell(protocol.session_cell(unnamed, rate, f"-q{index}")).sources()
        probes.append(paced_rows(cell, 100, 100).finish())
    # Both probes at 20 were preempted, so neither measured the rate.
    probes[1].sources(incarnation=SRC2)
    probes[2].sources(incarnation=SRC2)
    report, _records = analyzed(write_run(tmp_path, probes))
    capacity = report["groups"][0]["capacity"]
    assert [p["accepted"] for p in capacity["probes"]] == [True, None, None]
    assert capacity["state"] == {"initial": 10, "accepted": 10, "rejected": None}
    # A third paid probe at the same rate would buy the same answer.
    assert (capacity["status"], capacity["next"]) == ("stalled", 20)


def test_a_calibration_cell_whose_job_failed_fails_its_acceptance(tmp_path):
    cell = complete_cell(K02, run_id="cal-pace")
    run_dir = write_run(
        tmp_path, [cell], run_id="cal-pace", campaign="calibration-1246"
    )
    rewrite_marker(tmp_path, cell, lambda m: m.update(outcome="failed"))
    report, records = analyzed(run_dir, kind="calibration")
    assert records[K02["id"]]["status"] == "inconclusive"
    [section] = report["calibration"]
    # Its numbers are still reported, with the status that disqualifies them.
    assert section["pacing"][K02["id"]]["status"] == "inconclusive"
    holds = {item["item"]: item["holds"] for item in section["acceptance"]}
    assert holds["every steady-state cell reconciles complete and verifies"] is False


def test_an_acceptance_item_needs_a_usable_cell_not_only_its_numbers():
    """A cell that met its rate but did not complete proves nothing about pacing."""

    def paced(status):
        return {
            "status": status,
            "offered_rate": 10,
            "throughput": Fraction(10),
            "achieved_over_offered": Fraction(1),
            "admitted": True,
            "sustained_backlog_growth": False,
        }

    section = {
        "pacing": {cell_id: paced("usable") for cell_id in analyze.PACING_ACCEPTANCE},
        "delay_control": None,
        "gauges": {},
        "task_names": {},
    }
    item = "pacing cells admitted without sustained backlog growth"
    holds = {i["item"]: i["holds"] for i in analyze.acceptance_items(section, {})}
    assert holds[item] is True
    section["pacing"]["k05-pace-1000"] = paced("inconclusive")
    holds = {i["item"]: i["holds"] for i in analyze.acceptance_items(section, {})}
    assert holds[item] is False


def test_only_the_events_the_analysis_reads_are_typed(tmp_path):
    """The supervisor records list payloads the analyzer never reads."""
    cell = complete_cell()
    run_dir = write_run(tmp_path, [cell])
    supervisor = run_dir / "supervisor"
    # An inventory receipt carries an array, and a stop receipt carries none.
    (supervisor / "90000.json").write_text(
        json.dumps({"event": "inventory", "at": utc(T_S + 90), "payload": [{"k": "v"}]})
    )
    (supervisor / "90001.json").write_text(
        json.dumps({"event": "stop-requested", "at": utc(T_S + 91)})
    )
    _report, records = analyzed(run_dir)
    assert records[CELL_A["id"]]["status"] == "usable"
    # What the analysis does read stays typed.
    (supervisor / "90002.json").write_text(
        json.dumps({"event": "observation", "at": utc(T_S + 92), "payload": []})
    )
    with pytest.raises(analyze.Failure, match="Malformed evidence event"):
        analyzed(run_dir)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("parallelism", 4),
        ("concurrency", 16),
        ("checkpoint_seconds", 10),
        ("distribution", "skew"),
        ("channel_pool_size", 2),
        ("body_bytes", 65536),
    ],
)
def test_every_pinned_condition_is_compared_with_what_ran(tmp_path, field, value):
    """The shape is four fields and the pool is a fifth; each is pinned."""
    [pinned] = [c for c in s1_group() if c["arm"] == "UNNAMED" and c["repetition"] == 1]
    executed = {**protocol.session_cell(pinned, 10), field: value}
    _report, records = analyzed(write_run(tmp_path, [complete_cell(executed)]))
    record = records[pinned["cell_id"]]
    assert record["status"] == "inconclusive"
    assert record["reasons"] == ["conditions-differ-from-protocol:" + field]


def test_a_calibration_report_renders_one_section_per_line(tmp_path):
    for line, run_id in (("2.2.1", "cal-221"), ("1.20.4", "cal-120")):
        cell = complete_cell(K03, run_id=run_id)
        write_run(tmp_path, [cell], run_id=run_id, campaign="calibration", line=line)
    report = analyze.analyze(tmp_path / "runs", kind="calibration")
    out = analyze.write_reports(report, tmp_path / "out")
    markdown = (out / "report.md").read_text()
    assert "campaign calibration on Flink 2.2.1." in markdown
    assert "campaign calibration on Flink 1.20.4." in markdown
    assert markdown.count("The delay control is not detected") == 2
    # The rendered calibration section obeys the report's own line style.
    assert all(
        line.endswith(".") or line.startswith("#") or not line
        for line in markdown.splitlines()
    )


# --- independent repair review: refusals reach the report ----------------------


def test_a_rate_conflict_refuses_every_comparison_and_cost(tmp_path):
    pinned = s1_group()
    cells = [
        complete_cell(
            protocol.session_cell(pc, 20 if pc["arm"] == "STAGED_HASH" else 10)
        )
        for pc in pinned
    ]
    report, _records = analyzed(write_run(tmp_path, cells))
    [group] = report["groups"]
    assert group["verdict"] == "inconclusive"
    for arm in analyze.CANDIDATES:
        comparison = group["comparisons"][arm]
        # No ratio and no label survives a group the rule refused to compare.
        assert comparison["label"] == "inconclusive"
        assert comparison["reasons"] == ["offered-rate-differs-across-arms"]
        assert comparison["throughput_ratio"] is None
        assert group["incremental_cost"][arm] is None
    markdown = analyze.render_markdown(report)
    assert "is general" not in markdown


def test_an_unusable_name_control_cannot_pass_its_acceptance_item():
    """A control needs evidence the analysis can trust, not a steady state."""

    def items(status):
        section = {
            "pacing": {},
            "delay_control": None,
            "gauges": {},
            "task_names": {RANDOM_CONTROL: ["hex32"]},
        }
        by_id = {RANDOM_CONTROL: {"status": status}}
        return {i["item"]: i["holds"] for i in analyze.acceptance_items(section, by_id)}

    item = "k10 task names are 32 hex characters"
    # A short window says nothing about the shape of a task name.
    assert items("inconclusive")[item] is True
    assert items("usable")[item] is True
    # Evidence that never reached the evidence bucket says nothing at all.
    assert items("unexported")[item] is False


def test_an_inconclusive_calibration_cell_fails_the_steady_state_item():
    section = {
        "pacing": {},
        "delay_control": None,
        "gauges": {},
        "task_names": {},
    }
    marker = {"marker": {"status": "complete"}}
    item = "every steady-state cell reconciles complete and verifies"
    by_id = {"k04-pace-100": {"status": "usable", "reconciliation": marker}}
    holds = {i["item"]: i["holds"] for i in analyze.acceptance_items(section, by_id)}
    assert holds[item] is True
    by_id["k04-pace-100"]["status"] = "inconclusive"
    holds = {i["item"]: i["holds"] for i in analyze.acceptance_items(section, by_id)}
    assert holds[item] is False
