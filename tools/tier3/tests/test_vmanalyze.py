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
"""The single-host analyzer, over campaigns a real controller produced.

Each campaign here is driven by ``vmcampaign.Controller`` forking a Python
stand-in for the probe, so the analyzer reads the directory layout, journal
and digests the controller really writes. The stand-in copies evidence this
module generated in the application's receipt and row grammar into the root
its command line names, writes checkpoint completions, and prints the probe's
summary line. None of it measures a service: a verdict here checks the
arithmetic and the classification, not Cloud Tasks.
"""

import contextlib
import gzip
import json
import os
import shutil
import signal
import sys
import textwrap
from fractions import Fraction
from pathlib import Path

import pytest
from flink_tier3 import campaign, cli, protocol, vmanalyze, vmcampaign
from flink_tier3.bundle import delivered_sources
from flink_tier3.common import Failure, digest, utc

RUN = "vm1246a"
LINE = "2.2.1"
RATE = 10
WALL = 1_760_000_000_000
NANOS = 5_000_000_000_000
SRC = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1"
SRC2 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2"
INC = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
PROC = "cccccccc-cccc-4ccc-8ccc-ccccccccccc1"
QUEUE = "projects/flink-gcp/locations/us-central1/queues/ct1246-vm1246a"
BUDGETS = {"bytes": 1_000_000_000, "seconds": 36_000}
# The arms the reduced assessment measures.
ARM_INDEX = {"UNNAMED": 0, "NAMED_HASH": 1, "STAGED_HASH": 3}

WORKER = textwrap.dedent(
    """
    import json, os, shutil, signal, sys, time
    directory, endpoint, *pairs = sys.argv[1:]
    options = dict(zip(pairs[::2], pairs[1::2]))
    spec = json.load(open(os.environ["VM_ANALYZE_SPEC"]))[options["--cell-id"]]
    os.makedirs(directory, exist_ok=True)
    if spec.get("tree"):
        root = options["--evidence-root"][len("file://"):]
        shutil.copytree(spec["tree"], root, dirs_exist_ok=True)
    if spec.get("checkpoints") is not None:
        with open(os.path.join(directory, "checkpoints.jsonl"), "w") as file:
            for key, at in spec["checkpoints"]:
                file.write(json.dumps({"id": key, "completedMillis": at}) + "\\n")
    def line(outcome="OBSERVATION", reason="-"):
        print(",".join(["PROBE1246", options["--run-id"], options["--cell-id"],
                        options["--arm"], outcome, reason, "1000", "1", "2048", "3",
                        "", "", "-"]), flush=True)
    mode = spec.get("mode", "observe")
    if mode == "observe":
        line()
    elif mode == "fail":
        line("FAILED", "TimeoutException: Checkpoint expired")
    elif mode == "crash":
        sys.exit(3)
    elif mode == "hang":
        time.sleep(120)
    elif mode == "linger":
        line()
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        time.sleep(120)
    """
)


# --- fixtures -------------------------------------------------------------------


def pinned():
    """The group every fixture measures: one line, shape and body, all arms."""
    return sorted(
        (
            cell
            for cell in protocol.load()["cells"]
            if cell["flink"] == LINE
            and cell["shape"] == "s1"
            and cell["body_bytes"] == 1024
        ),
        key=lambda cell: cell["cell_id"],
    )


def cell_of(arm, repetition):
    return next(
        c for c in pinned() if c["arm"] == arm and c["repetition"] == repetition
    )


def session(protocol_cell, suffix=""):
    return protocol.session_cell(protocol_cell, RATE, suffix)


def checkpoints(cell):
    """One completion a second, 500 ms after each second of input."""
    records = cell["record_limit"]
    return [[k + 1, WALL + k * 1000 + 500] for k in range(records // RATE)]


class Evidence:
    """Receipts and gzip rows of one cell, in the application's grammar."""

    def __init__(self, cell, run_id=RUN):
        self.cell, self.run_id = cell, run_id
        self.files = {}

    def receipt(self, role, incarnation, phase, **fields):
        document = {
            "version": 1,
            "run_id": self.run_id,
            "cell_id": self.cell["id"],
            "arm": self.cell["arm"],
            "role": role,
            "incarnation": incarnation,
            "process": PROC,
            "control_delay_millis": 0,
            "csv_enabled": True,
            **fields,
        }
        name = f"receipts/{role}-{incarnation}-{phase}.json"
        self.files[name] = (json.dumps(document) + "\n").encode()

    def build(self, ok=100, latency=5_000_000, restart=False):
        """``ok`` percent of sequences end OK, each after ``latency`` ns."""
        cell = self.cell
        records = cell["record_limit"]
        step = 1000 // RATE
        last = WALL + (records - 1) * step
        base = {
            "records": records,
            "warmup_seconds": cell["warmup_seconds"],
            "observation_seconds": cell["observation_seconds"],
            "offered_rate": RATE,
            "monotonic_nanos": NANOS,
        }
        sources = [SRC, SRC2] if restart else [SRC]
        for incarnation in sources:
            self.receipt(
                "source", incarnation, "start", sequence=0, wall_millis=WALL, **base
            )
            self.receipt(
                "source",
                incarnation,
                "last-mapped",
                sequence=records - 1,
                wall_millis=last,
                **base,
            )
        lines = []
        for sequence in range(records):
            origin = WALL + sequence * step
            origin_nanos = NANOS + sequence * step * 1_000_000
            status = "OK" if sequence % 100 < ok else "UNAVAILABLE"
            lines.append(
                ",".join(
                    [
                        "CT1246",
                        self.run_id,
                        cell["id"],
                        cell["arm"],
                        INC,
                        PROC,
                        PROC,
                        str(sequence),
                        "1",
                        str(origin),
                        str(origin_nanos),
                        str(origin_nanos + 1_000_000),
                        str(origin_nanos + latency),
                        str(latency),
                        status,
                        f"{QUEUE}/tasks/{sequence:032x}",
                    ]
                )
            )
        chunks = [lines[i : i + 1000] for i in range(0, len(lines), 1000)]
        for index, chunk in enumerate(chunks, start=1):
            self.files[f"rows/{INC}-{index:06d}.csv.gz"] = gzip.compress(
                ("\n".join(chunk) + "\n").encode(), mtime=0
            )
        self.receipt(
            "creator",
            INC,
            "start",
            attempt_limit=cell["attempt_limit"],
            wall_millis=WALL,
            monotonic_nanos=NANOS,
        )
        self.receipt(
            "creator",
            INC,
            "terminal",
            attempts=records,
            completed=records,
            observations=records,
            evidence_failed=False,
            limit_reached=False,
            client_close_failed=False,
            rows_exported=records,
            parts_closed=len(chunks),
            rows_flush_failed=False,
            complete=True,
            wall_millis=last + 1000,
            monotonic_nanos=NANOS + 300_000_000_000,
        )
        return self

    def write(self, root):
        """Under ``root`` as the application writes under its evidence root."""
        for name, data in self.files.items():
            path = root / self.run_id / "cells" / self.cell["id"] / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        return root


@pytest.fixture(scope="module")
def trees(tmp_path_factory):
    """Generated evidence, cached by what it contains."""
    root = tmp_path_factory.mktemp("trees")
    cache = {}

    def tree(cell, run_id=RUN, **options):
        key = (run_id, cell["id"], tuple(sorted(options.items())))
        if key not in cache:
            directory = root / f"t{len(cache)}"
            Evidence(cell, run_id).build(**options).write(directory)
            cache[key] = str(directory)
        return cache[key]

    return tree


@pytest.fixture
def worker(tmp_path, monkeypatch):
    path = tmp_path / "worker.py"
    path.write_text(WORKER)
    spec = tmp_path / "spec.json"
    spec.write_text("{}")
    monkeypatch.setenv("VM_ANALYZE_SPEC", str(spec))
    yield path
    for record in tmp_path.rglob("worker.json"):
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(json.loads(record.read_text())["pid"], signal.SIGKILL)


def inputs(worker, **overrides):
    return {
        "protocolSha256": protocol.protocol_sha256(),
        "flink": LINE,
        "launcher": [sys.executable, str(worker)],
        **overrides,
    }


def arguments(cell, run_id=RUN, **overrides):
    options = {
        "run-id": run_id,
        "cell-id": cell["id"],
        "queue": QUEUE,
        "target": "https://ct1246.invalid/task",
        "arm": cell["arm"],
        "body-bytes": cell["body_bytes"],
        "parallelism": cell["parallelism"],
        "concurrency": cell["concurrency"],
        "checkpoint-seconds": cell["checkpoint_seconds"],
        "channel-pool-size": cell["channel_pool_size"],
        "distribution": cell["distribution"],
        "offered-rate": cell["offered_rate"],
        "warmup-seconds": cell["warmup_seconds"],
        "observation-seconds": cell["observation_seconds"],
        "record-limit": cell["record_limit"],
        "attempt-limit": cell["attempt_limit"],
        **overrides,
    }
    result = []
    for name, value in options.items():
        result += [f"--{name}", str(value)]
    return result


class Campaign:
    """One campaign: what each cell's stand-in does, then a real controller."""

    def __init__(self, tmp_path, worker, trees, name="vm1246a", **inputs_overrides):
        self.tmp_path, self.worker, self.trees = tmp_path, worker, trees
        self.name = name
        self.directory = tmp_path / name
        self.inputs = inputs(worker, **inputs_overrides)
        self.runs = []

    def add(
        self,
        cell,
        mode="observe",
        ok=100,
        latency=5_000_000,
        restart=False,
        window=True,
        evidence=True,
        seconds=600,
        **overrides,
    ):
        order = len(self.runs) + 1
        entry = {
            "tree": self.trees(cell, ok=ok, latency=latency, restart=restart)
            if evidence
            else None,
            "checkpoints": checkpoints(cell) if window else None,
            "mode": mode,
        }
        specs = json.loads(Path(os.environ["VM_ANALYZE_SPEC"]).read_text())
        specs[cell["id"]] = entry
        with open(os.environ["VM_ANALYZE_SPEC"], "w") as file:
            json.dump(specs, file)
        self.runs.append(
            {
                "order": order,
                "cellId": cell["id"],
                "arguments": arguments(cell, **overrides),
                "reserves": {"bytes": 10_000_000, "seconds": seconds},
            }
        )
        return self

    def group(self, arms=None, repetitions=(1, 2, 3)):
        """Every arm and repetition of the group, with per-arm options."""
        arms = arms or {}
        for repetition in repetitions:
            for arm in ARM_INDEX:
                self.add(session(cell_of(arm, repetition)), **arms.get(arm, {}))
        return self

    def controller(self, directory=None):
        return vmcampaign.Controller(
            directory or self.directory,
            self.inputs,
            cadence=0.2,
            term_grace=0.5,
            kill_grace=2.0,
        )

    def drive(self):
        campaign.plan_campaign(
            self.directory, self.name, self.runs, self.inputs, BUDGETS
        )
        try:
            self.controller().drive()
        except Failure:
            pass
        return self.directory


def analyzed(*directories):
    return vmanalyze.analyze(list(directories))


def cells(report):
    return {cell["cell_id"]: cell for cell in report["cells"]}


def only_group(report):
    (group,) = report["groups"]
    return group


# --- verdicts -------------------------------------------------------------------


def test_equal_arms_are_general_and_the_window_is_the_preregistered_one(
    tmp_path, worker, trees
):
    directory = Campaign(tmp_path, worker, trees).group().drive()

    report = analyzed(directory)

    group = only_group(report)
    assert (group["flink"], group["shape"], group["body_bytes"]) == (LINE, "s1", 1024)
    assert group["verdict"] == "general"
    for comparison in group["comparisons"].values():
        assert comparison["throughput_ratio"] == 1
        assert comparison["p95_ratio"] == 1
    assert {c["status"] for c in report["cells"]} == {"usable"}
    window = report["cells"][0]["window"]
    assert window["clock"] == "probe-wall"
    # Completions every second at +0.5 s: the first at or after the 60 s
    # warm-up and the last at or before the final record at +242.9 s.
    assert window["start"] == Fraction(WALL + 60_500, 1000)
    assert window["end"] == Fraction(WALL + 242_500, 1000)
    assert window["checkpoints_inside"] == 183
    # Origins every 100 ms from +60.5 s to +242.5 s inclusive.
    assert report["cells"][0]["throughput"] == Fraction(1821, 182)
    assert report["cells"][0]["rows"]["p95_nanos"] == 5_000_000


def test_half_the_throughput_at_three_times_the_p95_is_constrained(
    tmp_path, worker, trees
):
    directory = (
        Campaign(tmp_path, worker, trees)
        .group({"STAGED_HASH": {"ok": 50, "latency": 15_000_000}})
        .drive()
    )

    group = only_group(analyzed(directory))

    hashed = group["comparisons"]["STAGED_HASH"]
    assert hashed["label"] == "constrained"
    assert hashed["p95_ratio"] == 3
    assert Fraction(45, 100) < hashed["throughput_ratio"] < Fraction(55, 100)
    assert group["verdict"] == "constrained"
    # The named path isolates staging: same names, same throughput as the baseline.
    assert group["incremental_cost"]["STAGED_HASH"]["p95_ratio"] == 3


def test_a_fifth_of_the_throughput_is_declined(tmp_path, worker, trees):
    directory = (
        Campaign(tmp_path, worker, trees).group({"STAGED_HASH": {"ok": 20}}).drive()
    )

    group = only_group(analyzed(directory))

    assert group["comparisons"]["STAGED_HASH"]["label"] == "decline"
    assert group["verdict"] == "decline"
    # Only the measured arms can be short; the random-name arms were never planned.
    assert group["short_arms"] == []
    assert set(group["comparisons"]) == {"STAGED_HASH"}


def test_a_baseline_whose_repetitions_spread_more_than_a_tenth_is_inconclusive(
    tmp_path, worker, trees
):
    runs = Campaign(tmp_path, worker, trees)
    runs.group(repetitions=(1, 2))
    for arm in ARM_INDEX:
        runs.add(session(cell_of(arm, 3)), **({"ok": 80} if arm == "UNNAMED" else {}))

    group = only_group(analyzed(runs.drive()))

    assert group["verdict"] == "inconclusive"
    assert "range-baseline" in group["comparisons"]["STAGED_HASH"]["reasons"]


def test_without_checkpoint_completions_there_is_no_window_and_no_verdict(
    tmp_path, worker, trees
):
    directory = (
        Campaign(tmp_path, worker, trees)
        .group({arm: {"window": False} for arm in ARM_INDEX})
        .drive()
    )

    report = analyzed(directory)

    assert {c["status"] for c in report["cells"]} == {"inconclusive"}
    assert {tuple(c["reasons"]) for c in report["cells"]} == {
        ("checkpoints-unrecorded",)
    }
    assert only_group(report)["verdict"] == "inconclusive"


def test_identical_input_gives_byte_identical_reports_wherever_it_was_collected(
    tmp_path, worker, trees
):
    directory = Campaign(tmp_path, worker, trees).group(repetitions=(1,)).drive()
    moved = tmp_path / "collected" / "elsewhere"
    shutil.copytree(directory, moved)

    first = vmanalyze.write_reports(analyzed(directory), tmp_path / "a")
    second = vmanalyze.write_reports(analyzed(directory), tmp_path / "b")
    third = vmanalyze.write_reports(analyzed(moved), tmp_path / "c")

    for name in ("report.json", "report.md"):
        assert (first / name).read_bytes() == (second / name).read_bytes()
        assert (first / name).read_bytes() == (third / name).read_bytes()


# --- runs that are not measurements ---------------------------------------------


def test_a_lost_run_is_reported_and_its_admitted_repeat_counts(tmp_path, worker, trees):
    lost = session(cell_of("STAGED_HASH", 1))
    first = Campaign(tmp_path, worker, trees, name="vm1246a")
    for repetition in (1, 2, 3):
        for arm in ARM_INDEX:
            cell = session(cell_of(arm, repetition))
            first.add(cell, mode="crash" if cell["id"] == lost["id"] else "observe")
    first.drive()
    stopped = first.controller()
    successor = tmp_path / "vm1246b"
    campaign.plan_successor(stopped.journal, successor, "vm1246b")
    first.controller(successor).drive()
    repeat = Campaign(tmp_path, worker, trees, name="vm1246c")
    repeat.add(session(cell_of("STAGED_HASH", 1), "-x2")).drive()

    report = analyzed(first.directory, successor, repeat.directory)

    by_id = cells(report)
    assert by_id[lost["id"]]["status"] == "lost"
    assert by_id[lost["id"] + "-x2"]["status"] == "usable"
    assert by_id[lost["id"] + "-x2"]["reasons"] == []
    summary = {c["campaign_id"]: c for c in report["campaigns"]}
    assert summary["vm1246a"]["lost"] == [lost["id"]]
    assert summary["vm1246b"]["predecessor"] == "vm1246a"
    assert only_group(report)["verdict"] == "general"


def test_a_successor_without_its_predecessor_is_refused(tmp_path, worker, trees):
    first = Campaign(tmp_path, worker, trees, name="vm1246a")
    first.add(session(cell_of("UNNAMED", 1)), mode="crash")
    first.add(session(cell_of("UNNAMED", 2)))
    first.drive()
    successor = tmp_path / "vm1246b"
    campaign.plan_successor(first.controller().journal, successor, "vm1246b")

    with pytest.raises(Failure, match="continues vm1246a, which was not given"):
        analyzed(successor)


def test_a_restarted_cell_is_not_repeated_and_leaves_its_arm_short(
    tmp_path, worker, trees
):
    restarted = session(cell_of("UNNAMED", 1))
    runs = Campaign(tmp_path, worker, trees).group({"UNNAMED": {}}, repetitions=(2, 3))
    runs.add(restarted, restart=True)
    for arm in list(ARM_INDEX)[1:]:
        runs.add(session(cell_of(arm, 1)))
    runs.add(session(cell_of("UNNAMED", 1), "-x2"))

    report = analyzed(runs.drive())

    by_id = cells(report)
    assert by_id[restarted["id"]]["status"] == "restarted"
    assert by_id[restarted["id"] + "-x2"]["status"] == "repeat-not-admitted"
    group = only_group(report)
    assert "UNNAMED" in group["short_arms"]
    assert group["verdict"] == "inconclusive"


def test_a_repeat_of_a_usable_cell_is_not_counted(tmp_path, worker, trees):
    cell = session(cell_of("UNNAMED", 1))
    runs = Campaign(tmp_path, worker, trees).add(cell)
    runs.add(session(cell_of("UNNAMED", 1), "-x2"))

    by_id = cells(analyzed(runs.drive()))

    assert by_id[cell["id"]]["status"] == "usable"
    assert by_id[cell["id"] + "-x2"]["status"] == "repeat-not-admitted"


def test_an_overrun_is_listed_and_a_slow_exit_keeps_its_observation(
    tmp_path, worker, trees
):
    overrun = session(cell_of("UNNAMED", 1))
    slow = session(cell_of("UNNAMED", 2))
    failed = session(cell_of("UNNAMED", 3))
    runs = Campaign(tmp_path, worker, trees)
    # Long enough that a slow runner still prints the lingering run's line
    # before its reservation ends.
    runs.add(overrun, mode="hang", seconds=6)
    runs.add(slow, mode="linger", seconds=6)
    runs.add(failed, mode="fail")
    early = session(cell_of("NAMED_HASH", 1))
    runs.add(early, mode="fail", evidence=False)

    report = analyzed(runs.drive())

    by_id = cells(report)
    assert by_id[overrun["id"]]["status"] == "reservation-overrun"
    assert by_id[overrun["id"]]["reasons"] == ["outlived its reservation of 6 seconds"]
    assert by_id[slow["id"]]["status"] == "usable"
    assert by_id[slow["id"]]["flags"] == ["slow-exit"]
    # A job that failed after its source started measured the cell's load.
    assert by_id[failed["id"]]["status"] == "failed-while-observing"
    assert by_id[early["id"]]["status"] == "failed"
    (summary,) = report["campaigns"]
    assert summary["reservation_overruns"] == [overrun["id"]]
    assert summary["slow_exits"] == [slow["id"]]


def test_a_plan_that_names_its_own_evidence_root_spends_no_cell(
    tmp_path, worker, trees
):
    cell = session(cell_of("UNNAMED", 1))
    runs = Campaign(tmp_path, worker, trees).add(
        cell, **{"evidence-root": f"file://{tmp_path}/elsewhere/runs/"}
    )

    report = analyzed(runs.drive())

    # The controller refuses the plan before claiming, so nothing ran.
    assert report["cells"] == []
    assert report["campaigns"][0]["runs"]["PENDING"] == 1
    assert report["campaigns"][0]["stopped"] is None


def test_conditions_that_differ_from_the_pinned_cell_are_inconclusive(
    tmp_path, worker, trees
):
    cell = session(cell_of("UNNAMED", 1))
    runs = Campaign(tmp_path, worker, trees).add(cell, **{"channel-pool-size": 4})

    (record,) = analyzed(runs.drive())["cells"]

    # No receipt carries the channel pool, so only the comparison with the
    # pinned cell can refuse this run.
    assert record["status"] == "inconclusive"
    assert record["reasons"] == ["conditions-differ-from-protocol:channel_pool_size"]


def test_a_different_offered_rate_across_arms_disqualifies_the_group(
    tmp_path, worker, trees
):
    runs = Campaign(tmp_path, worker, trees).group()
    report = analyzed(runs.drive())
    for record in report["cells"]:
        if record["arm"] == "NAMED_HASH":
            record["offered_rate"] = 20
    groups, _ = vmanalyze.build_groups(
        [dict(r, reasons=[]) for r in report["cells"]],
        vmanalyze.protocol_index(protocol.load()),
    )

    (group,) = groups
    vmanalyze.measured_arms(group)
    assert group["verdict"] == "inconclusive"
    assert group["reasons"] == ["offered-rate-differs-across-arms"]


# --- tampering and disagreement -------------------------------------------------


@pytest.fixture
def single(tmp_path, worker, trees):
    return Campaign(tmp_path, worker, trees).add(session(cell_of("UNNAMED", 1))).drive()


def run_directory(directory):
    (path,) = (directory / vmcampaign.RUNS).iterdir()
    return path


def test_a_changed_row_part_is_evidence_modified(single):
    part = next(run_directory(single).rglob("*.csv.gz"))
    part.write_bytes(part.read_bytes() + b"\0")

    (record,) = analyzed(single)["cells"]

    assert record["status"] == "evidence-modified"
    assert record["files"]["problems"] == [
        "changed:" + part.relative_to(run_directory(single)).as_posix()
    ]


def test_a_file_the_controller_never_digested_is_evidence_modified(single):
    extra = run_directory(single) / "probe" / "evidence" / "late.txt"
    extra.write_text("x")

    (record,) = analyzed(single)["cells"]

    assert record["status"] == "evidence-modified"
    assert record["files"]["problems"] == ["unrecorded:probe/evidence/late.txt"]


def test_the_settle_mark_is_not_a_modification(single):
    (run_directory(single) / vmcampaign.SETTLED).write_text("{}")

    (record,) = analyzed(single)["cells"]

    assert record["status"] == "usable"


@pytest.mark.parametrize(
    ("edit", "reason"),
    [
        (
            lambda o, s: s["runs"]["1"].update(detail="observation; teardown: x"),
            "detail-differs-from-journal",
        ),
        (lambda o, s: o.update(outcome="FAILED"), "outcome-differs-from-journal"),
        (lambda o, s: o["argv"].append("--extra"), "argv-differs-from-plan"),
    ],
)
def test_a_run_whose_records_disagree_is_inconsistent(single, edit, reason):
    outcome_path = run_directory(single) / vmcampaign.OUTCOME
    state_path = single / campaign.STATE
    outcome, state = (
        json.loads(outcome_path.read_text()),
        json.loads(state_path.read_text()),
    )
    edit(outcome, state)
    outcome_path.write_text(json.dumps(outcome))
    state_path.write_text(json.dumps(state))

    (record,) = analyzed(single)["cells"]

    assert record["status"] == "inconsistent"
    assert reason in record["reasons"]


def test_an_outcome_left_by_a_run_the_journal_calls_claimed_is_lost(single):
    state_path = single / campaign.STATE
    state = json.loads(state_path.read_text())
    state["runs"]["1"] = {"state": "CLAIMED", "cellId": state["runs"]["1"]["cellId"]}
    state["stopped"] = {"reason": "run 1 recorded no outcome", "at": "x"}
    state_path.write_text(json.dumps(state))

    (record,) = analyzed(single)["cells"]

    assert record["status"] == "lost"
    assert record["reasons"] == ["claimed-when-stopped", "outcome-never-finished"]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"protocolSha256": "a" * 64}, "planned against another protocol"),
        ({"flink": "1.19.0"}, "name no supported Flink line"),
    ],
)
def test_a_campaign_whose_inputs_do_not_pin_the_protocol_and_line_is_refused(
    tmp_path, worker, trees, overrides, message
):
    runs = Campaign(tmp_path, worker, trees, **overrides)
    runs.add(session(cell_of("UNNAMED", 1)))

    with pytest.raises(Failure, match=message):
        analyzed(runs.drive())


# --- the window rule ------------------------------------------------------------


def completions(count, first=60_500):
    return {k + 1: WALL + first + k * 1000 for k in range(count)}


def test_the_window_needs_six_checkpoints_inside_it():
    last = WALL + 70_000

    assert vmanalyze.observation_window(completions(6), WALL, last, 60)[1] is None
    assert vmanalyze.observation_window(completions(5), WALL, last, 60) == (
        None,
        "window-too-short",
    )


def test_the_window_edges_are_completions_inside_warm_up_and_input():
    span, _ = vmanalyze.observation_window(
        {
            1: WALL + 59_999,
            2: WALL + 60_000,
            **{k: WALL + 58_000 + k * 1000 for k in range(3, 11)},
            11: WALL + 90_001,
        },
        WALL,
        WALL + 90_000,
        60,
    )

    assert span["first_checkpoint"] == 2
    assert span["start"] == Fraction(WALL + 60_000, 1000)
    assert span["last_checkpoint"] == 10
    assert span["end"] == Fraction(WALL + 68_000, 1000)


def test_missing_receipts_are_named_before_missing_checkpoints():
    assert vmanalyze.observation_window(None, None, WALL, 60) == (
        None,
        "receipts-missing",
    )


@pytest.mark.parametrize(
    "text",
    [
        '{"id": 1}\n',
        '{"id": 1, "completedMillis": 2, "extra": 0}\n',
        '{"id": 1, "completedMillis": -2}\n',
        '{"id": true, "completedMillis": 2}\n',
        '{"id": 1, "completedMillis": 2}\n{"id": 1, "completedMillis": 3}\n',
        "not json\n",
    ],
)
def test_an_unreadable_completion_record_gives_no_completions(tmp_path, text):
    (tmp_path / "probe").mkdir()
    (tmp_path / "probe" / vmanalyze.CHECKPOINTS).write_text(text)

    assert vmanalyze.checkpoint_completions(tmp_path) is None


# --- packaging ------------------------------------------------------------------


def test_the_command_writes_both_reports(single, tmp_path, capsys):
    out = tmp_path / "out"

    assert cli.main(["vm-analyze", str(single), "--out", str(out)]) == 0

    assert "analyzed 1 cells in 1 campaigns (usable 1)" in capsys.readouterr().out
    assert (
        json.loads((out / "report.json").read_text())["cells"][0]["status"] == "usable"
    )
    assert (out / "report.md").read_text().startswith("# Cloud Tasks single-host")


def test_the_analyzer_is_not_delivered_to_the_supervisor_pod():
    delivered = delivered_sources()

    # A positive control, so "excluded" cannot be read from an empty walk.
    assert "runtime.py" in delivered
    assert "vmanalyze.py" not in delivered


# --- held by the mutation batch -------------------------------------------------


def test_a_completion_at_the_last_mapped_record_closes_the_window():
    last = WALL + 67_500

    span, _ = vmanalyze.observation_window(completions(8), WALL, last, 60)

    assert span["end"] == Fraction(last, 1000)


def test_a_limit_the_pinned_cell_does_not_have_is_inconclusive(tmp_path, worker, trees):
    cell = session(cell_of("UNNAMED", 1))
    runs = Campaign(tmp_path, worker, trees).add(
        cell, **{"record-limit": cell["record_limit"] + 1}
    )

    (record,) = analyzed(runs.drive())["cells"]

    # The receipts carry the records the window implies, not the limit, so the
    # reconciliation passes and only the comparison with the pinned cell refuses.
    assert record["reconciliation"]["status"] == "complete"
    assert record["status"] == "inconclusive"
    assert record["reasons"] == ["limits-differ-from-protocol:record_limit"]


def test_a_cell_executed_in_two_campaigns_counts_in_neither(tmp_path, worker, trees):
    cell = session(cell_of("UNNAMED", 1))
    first = Campaign(tmp_path, worker, trees, name="vm1246a").add(cell)
    second = Campaign(tmp_path, worker, trees, name="vm1246c").add(cell)

    report = analyzed(first.drive(), second.drive())

    assert [c["status"] for c in report["cells"]] == ["cell-executed-twice"] * 2


def test_a_repeat_whose_pinned_cell_never_ran_is_not_counted(tmp_path, worker, trees):
    runs = Campaign(tmp_path, worker, trees)
    runs.add(session(cell_of("UNNAMED", 1), "-x2"))

    (record,) = analyzed(runs.drive())["cells"]

    assert record["status"] == "repeat-not-admitted"


@pytest.fixture
def chain(tmp_path, worker, trees):
    first = Campaign(tmp_path, worker, trees, name="vm1246a")
    first.add(session(cell_of("UNNAMED", 1)), mode="crash")
    first.add(session(cell_of("UNNAMED", 2)))
    first.add(session(cell_of("UNNAMED", 3)))
    first.drive()
    successor = tmp_path / "vm1246b"
    campaign.plan_successor(first.controller().journal, successor, "vm1246b")
    return first.directory, successor


def test_a_successor_that_names_another_plan_is_refused(chain):
    first, successor = chain
    manifest = json.loads((successor / campaign.MANIFEST).read_text())
    manifest["predecessor"]["planSha256"] = "0" * 64
    (successor / campaign.MANIFEST).write_text(json.dumps(manifest))

    with pytest.raises(Failure, match="vm1246b does not continue vm1246a"):
        analyzed(first, successor)


def test_a_successor_that_does_not_carry_exactly_the_unspent_runs_is_refused(chain):
    first, successor = chain
    state = json.loads((first / campaign.STATE).read_text())
    state["runs"]["3"]["state"] = campaign.OBSERVED
    (first / campaign.STATE).write_text(json.dumps(state))

    with pytest.raises(Failure, match="does not carry its predecessor's unspent runs"):
        analyzed(first, successor)


def test_only_a_failure_before_the_source_started_is_repeated(tmp_path, worker, trees):
    early = session(cell_of("UNNAMED", 1))
    late = session(cell_of("UNNAMED", 2))
    runs = Campaign(tmp_path, worker, trees)
    runs.add(early, mode="fail", evidence=False)
    runs.add(late, mode="fail")
    runs.add(session(cell_of("UNNAMED", 1), "-x2"))
    runs.add(session(cell_of("UNNAMED", 2), "-x2"))

    by_id = cells(analyzed(runs.drive()))

    assert by_id[early["id"] + "-x2"]["status"] == "usable"
    assert by_id[late["id"] + "-x2"]["status"] == "repeat-not-admitted"


def test_a_repeat_handed_out_before_its_pinned_run_is_not_counted(
    tmp_path, worker, trees
):
    cell = session(cell_of("UNNAMED", 1))
    runs = Campaign(tmp_path, worker, trees)
    runs.add(session(cell_of("UNNAMED", 1), "-x2"))
    runs.add(cell, mode="crash")

    by_id = cells(analyzed(runs.drive()))

    assert by_id[cell["id"]]["status"] == "lost"
    assert by_id[cell["id"] + "-x2"]["status"] == "repeat-not-admitted"


def test_a_publish_that_died_midway_is_not_a_modification(single):
    (run_directory(single) / "settled.jsonabc123.writing").write_text("{")

    (record,) = analyzed(single)["cells"]

    assert record["status"] == "usable"


def test_a_state_holding_an_unknown_run_state_is_refused(single):
    state_path = single / campaign.STATE
    state = json.loads(state_path.read_text())
    state["runs"]["1"]["state"] = "DONE"
    state_path.write_text(json.dumps(state))

    with pytest.raises(Failure, match="holds a run in no known state"):
        analyzed(single)


# --- held for the independent review ---------------------------------------------


def test_a_summary_for_another_arm_is_inconsistent(single):
    outcome_path = run_directory(single) / vmcampaign.OUTCOME
    outcome = json.loads(outcome_path.read_text())
    outcome["summary"]["arm"] = "STAGED_HASH"
    outcome_path.write_text(json.dumps(outcome))

    (record,) = analyzed(single)["cells"]

    assert record["status"] == "inconsistent"
    assert record["reasons"] == ["summary-differs-from-arguments"]


def test_a_successor_that_changed_a_carried_run_is_refused(chain):
    first, successor = chain
    manifest = json.loads((successor / campaign.MANIFEST).read_text())
    manifest["plan"][0]["reserves"]["seconds"] += 1
    state = json.loads((successor / campaign.STATE).read_text())
    # A matching edit to both files, so only the chain check can refuse it.
    state["planSha256"] = digest(manifest["plan"])
    (successor / campaign.MANIFEST).write_text(json.dumps(manifest))
    (successor / campaign.STATE).write_text(json.dumps(state))

    with pytest.raises(Failure, match="does not carry its predecessor's unspent runs"):
        analyzed(first, successor)


def test_a_repeat_handed_out_before_its_pinned_run_failed_is_not_counted(
    tmp_path, worker, trees
):
    early = session(cell_of("UNNAMED", 1))
    runs = Campaign(tmp_path, worker, trees)
    runs.add(early, mode="fail", evidence=False)
    runs.add(session(cell_of("UNNAMED", 1), "-x2"))
    directory = runs.drive()
    # As if a second campaign had claimed the repeat while the pinned run was
    # still running: the pinned run's outcome lands after the repeat's claim.
    state = json.loads((directory / campaign.STATE).read_text())
    state["runs"]["1"]["finished"] = utc(state["runs"]["2"]["claimed"] + 1)
    (directory / campaign.STATE).write_text(json.dumps(state))

    by_id = cells(analyzed(directory))

    assert by_id[early["id"]]["status"] == "failed"
    assert by_id[early["id"] + "-x2"]["status"] == "repeat-not-admitted"


def test_a_chain_of_three_campaigns_is_joined(tmp_path, worker, trees):
    first = Campaign(tmp_path, worker, trees, name="vm1246a")
    first.add(session(cell_of("UNNAMED", 1)), mode="crash")
    first.add(session(cell_of("UNNAMED", 2)), mode="crash")
    first.add(session(cell_of("UNNAMED", 3)))
    first.drive()
    second, third = tmp_path / "vm1246b", tmp_path / "vm1246c"
    campaign.plan_successor(first.controller().journal, second, "vm1246b")
    with contextlib.suppress(Failure):
        first.controller(second).drive()
    campaign.plan_successor(first.controller(second).journal, third, "vm1246c")
    first.controller(third).drive()

    report = analyzed(first.directory, second, third)

    assert [(c["campaign_id"], c["status"]) for c in report["cells"]] == [
        ("vm1246a", "lost"),
        ("vm1246b", "lost"),
        ("vm1246c", "usable"),
    ]
