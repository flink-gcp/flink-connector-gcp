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
"""The campaign controller, driving real forked workers against a real journal.

The worker is a Python script standing in for the probe JVM. It takes the
probe's command line — directory, endpoint, ``--name value`` pairs — and a
``--mode`` that says which way to end, so each test forks a real process and
the controller sees real exit statuses, real signals and real output.
"""

import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import textwrap
import time

import pytest
from flink_tier3 import campaign, vmcampaign
from flink_tier3.bundle import delivered_sources
from flink_tier3.common import Failure

WORKER = textwrap.dedent(
    """
    import os, signal, subprocess, sys, time
    directory, endpoint, *pairs = sys.argv[1:]
    options = dict(zip(pairs[::2], pairs[1::2]))
    mode = options["--mode"]
    os.makedirs(os.path.join(directory, "checkpoints"), exist_ok=True)
    with open(os.path.join(directory, "checkpoints", "chk-1"), "wb") as file:
        file.write(b"x" * 100_000)
    with open(os.path.join(directory, "samples.jsonl"), "w") as file:
        file.write('{"elapsedNanos":1}\\n')
    run, cell = options["--run-id"], options["--cell-id"]
    def record(directory, name, pid):
        # Renamed into place, so a reader never sees the file empty.
        with open(os.path.join(directory, name + ".tmp"), "w") as file:
            file.write(str(pid))
        os.replace(os.path.join(directory, name + ".tmp"), os.path.join(directory, name))
    def line(outcome="OBSERVATION", reason="-", teardown="-", cell=cell, fields=None):
        parts = ["PROBE1246", run, cell, "STAGED_HASH", outcome, reason,
                 "1000", "1", "2048", "3", "stagedBytes=5", "", teardown]
        print(",".join(fields or parts), flush=True)
    if mode == "observe":
        line()
    elif mode == "fail":
        line("FAILED", "TimeoutException: Checkpoint expired")
    elif mode == "teardown":
        line(teardown="TimeoutException: cancel")
    elif mode == "silent":
        pass
    elif mode == "crash":
        sys.exit(3)
    elif mode == "twice":
        line(); line()
    elif mode == "short":
        print("PROBE1246," + run + "," + cell + ",STAGED_HASH,OBSERVATION", flush=True)
    elif mode == "other-cell":
        line(cell="zz99")
    elif mode == "unknown":
        line("MAYBE")
    elif mode == "disagree":
        line("OBSERVATION", "IOException: boom")
    elif mode == "slow":
        time.sleep(float(options["--sleep"]))
        line()
    elif mode == "hang":
        if options.get("--stubborn") == "true":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        record(directory, "hanging", os.getpid())
        time.sleep(120)
    elif mode == "linger":
        line()
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        time.sleep(120)
    elif mode == "straggler":
        left = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
        record(directory, "straggler", left.pid)
        line()
    """
)

BUDGETS = {"bytes": 10_000_000, "seconds": 3_600}


@pytest.fixture
def worker(tmp_path):
    path = tmp_path / "worker.py"
    path.write_text(WORKER)
    yield path
    # A test that fails before the controller stops its run would otherwise
    # leave the fake probe, or what it started, running for two minutes.
    for record in tmp_path.rglob("worker.json"):
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(json.loads(record.read_text())["pid"], signal.SIGKILL)
    for name in ("hanging", "straggler"):
        for record in tmp_path.rglob(name):
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.kill(int(record.read_text()), signal.SIGKILL)


def inputs(worker):
    return {
        "protocolSha256": "a" * 64,
        "launcher": [sys.executable, str(worker)],
    }


def run(order, mode, seconds=60, **options):
    arguments = ["--run-id", "cal1246d", "--cell-id", f"k{order:02d}", "--mode", mode]
    for name, value in options.items():
        arguments += [f"--{name}", str(value)]
    return {
        "order": order,
        "cellId": f"k{order:02d}",
        "arguments": arguments,
        "reserves": {"bytes": 100_000, "seconds": seconds},
    }


def planned(directory, worker, *runs, budgets=None):
    campaign.plan_campaign(
        directory, "cal1246d", list(runs), inputs(worker), budgets or BUDGETS
    )
    return directory


def controller(directory, worker, **options):
    options.setdefault("cadence", 0.2)
    options.setdefault("term_grace", 1.0)
    options.setdefault("kill_grace", 2.0)
    return vmcampaign.Controller(directory, inputs(worker), **options)


def state(directory):
    return json.loads((directory / campaign.STATE).read_text())


def outcome(directory, order, cell):
    return json.loads(
        (
            directory / vmcampaign.RUNS / f"{order:03d}-{cell}" / vmcampaign.OUTCOME
        ).read_text()
    )


def test_a_failed_run_is_recorded_and_the_campaign_proceeds(tmp_path, worker):
    directory = planned(
        tmp_path / "c", worker, run(1, "observe"), run(2, "fail"), run(3, "observe")
    )

    counts = controller(directory, worker).drive()

    assert counts["runs"] == {"PENDING": 0, "CLAIMED": 0, "OBSERVED": 2, "FAILED": 1}
    assert counts["stopped"] is None
    runs = state(directory)["runs"]
    assert runs["2"]["detail"] == "TimeoutException: Checkpoint expired"
    assert runs["1"]["detail"] == "observation"


@pytest.mark.parametrize(
    ("mode", "reason"),
    [
        ("silent", "printed no summary line"),
        ("crash", "printed no summary line"),
        ("twice", "printed 2 summary lines"),
        ("short", "printed a summary of 5 fields, not 13"),
        ("other-cell", "printed a summary for cal1246d/zz99, not cal1246d/k02"),
        ("unknown", "printed an unknown outcome 'MAYBE'"),
        ("disagree", "printed an outcome that disagrees with its reason"),
    ],
)
def test_a_run_with_no_recordable_outcome_stops_the_campaign(
    tmp_path, worker, mode, reason
):
    directory = planned(
        tmp_path / "c", worker, run(1, "observe"), run(2, mode), run(3, "observe")
    )

    with pytest.raises(Failure, match=f"Campaign stopped: run 2 {reason}"):
        controller(directory, worker).drive()

    runs = state(directory)["runs"]
    assert runs["1"]["state"] == campaign.OBSERVED
    assert runs["2"]["state"] == campaign.CLAIMED
    # The run after the stop was never forked.
    assert runs["3"]["state"] == campaign.PENDING
    assert not (directory / vmcampaign.RUNS / "003-k03").exists()
    assert state(directory)["stopped"]["reason"] == f"run 2 {reason}"
    assert outcome(directory, 2, "k02")["unrecordable"] == reason
    assert outcome(directory, 2, "k02")["exitStatus"] == (3 if mode == "crash" else 0)


def test_a_teardown_failure_does_not_undo_the_observation(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "teardown"))

    controller(directory, worker).drive()

    recorded = state(directory)["runs"]["1"]
    assert recorded["state"] == campaign.OBSERVED
    assert recorded["detail"] == "observation; teardown: TimeoutException: cancel"


def test_consumed_is_measured_rather_than_the_reservation(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "observe"))
    controller(directory, worker).drive()

    consumed = state(directory)["runs"]["1"]["consumed"]
    evidence = directory / vmcampaign.RUNS / "001-k01"
    # The settled marker is written after the run is charged, as bookkeeping.
    on_disk = sum(
        p.stat().st_size
        for p in evidence.rglob("*")
        if p.is_file() and p.name != vmcampaign.SETTLED
    )
    assert consumed["bytes"] == on_disk
    assert consumed["seconds"] < 60
    # The checkpoint scratch is gone before the run is measured.
    assert not (evidence / vmcampaign.PROBE / "checkpoints").exists()
    assert (evidence / vmcampaign.PROBE / "samples.jsonl").exists()
    assert state(directory)["consumed"] == consumed


def test_the_evidence_directory_carries_the_record_of_the_run(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "observe"))

    controller(directory, worker).drive()

    record = outcome(directory, 1, "k01")
    assert record["outcome"] == campaign.OBSERVED
    assert record["exitStatus"] == 0
    assert record["killed"] is False
    assert record["summary"]["samples"] == 1
    assert record["argv"][:2] == inputs(worker)["launcher"]
    # The endpoint is a frozen input too, and "-" means the real service.
    assert record["argv"][3] == "-"
    assert set(record["files"]) == {
        "probe/samples.jsonl",
        "stderr.log",
        "stdout.log",
        "worker.json",
    }


def test_a_run_past_its_reservation_is_stopped_and_recorded_failed(tmp_path, worker):
    directory = planned(
        tmp_path / "c", worker, run(1, "hang", seconds=2), run(2, "observe")
    )

    counts = controller(directory, worker).drive()

    assert counts["runs"]["FAILED"] == 1
    assert counts["runs"]["OBSERVED"] == 1
    assert counts["stopped"] is None
    recorded = state(directory)["runs"]["1"]
    assert recorded["detail"] == "outlived its reservation of 2 seconds"
    # Stopped at the reservation, not at the probe's own, far longer deadline.
    assert recorded["consumed"]["seconds"] <= 4
    assert outcome(directory, 1, "k01")["killed"] is True
    assert outcome(directory, 1, "k01")["exitStatus"] == -signal.SIGTERM
    hung = int(
        (directory / vmcampaign.RUNS / "001-k01" / "probe" / "hanging").read_text()
    )
    assert vmcampaign.start_time(hung) is None


def test_a_run_that_printed_its_line_and_would_not_exit_keeps_its_outcome(
    tmp_path, worker
):
    # The probe prints its summary and then waits for its threads; a JVM that
    # lingers past the reservation after that is a slow shutdown.
    directory = planned(tmp_path / "c", worker, run(1, "linger", seconds=2))

    controller(directory, worker).drive()

    recorded = state(directory)["runs"]["1"]
    assert recorded["state"] == campaign.OBSERVED
    assert recorded["detail"] == "observation; did not exit before its reservation"
    assert outcome(directory, 1, "k01")["exitStatus"] == -signal.SIGKILL


def test_a_run_that_would_not_exit_at_its_deadline_stops_the_campaign(
    tmp_path, worker, monkeypatch
):
    monkeypatch.setattr(vmcampaign.Controller, "_stop_group", lambda *_: False)
    directory = planned(
        tmp_path / "c", worker, run(1, "hang", seconds=1), run(2, "observe")
    )

    with pytest.raises(Failure, match="run 1 outlived its reservation and would not"):
        controller(directory, worker).drive()
    assert state(directory)["runs"]["2"]["state"] == campaign.PENDING


def test_a_process_left_behind_that_would_not_exit_stops_the_campaign(
    tmp_path, worker, monkeypatch
):
    monkeypatch.setattr(vmcampaign.Controller, "_stop_group", lambda *_: False)
    directory = planned(tmp_path / "c", worker, run(1, "straggler"), run(2, "observe"))

    with pytest.raises(Failure, match="run 1 left a process that would not exit"):
        controller(directory, worker).drive()
    assert state(directory)["runs"]["2"]["state"] == campaign.PENDING


def test_a_run_that_ignores_sigterm_is_killed(tmp_path, worker):
    directory = planned(
        tmp_path / "c", worker, run(1, "hang", seconds=2, stubborn="true")
    )

    controller(directory, worker).drive()

    assert outcome(directory, 1, "k01")["exitStatus"] == -signal.SIGKILL
    assert state(directory)["runs"]["1"]["state"] == campaign.FAILED


def test_a_process_left_behind_by_the_run_is_stopped(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "straggler"))

    controller(directory, worker).drive()

    assert state(directory)["runs"]["1"]["state"] == campaign.OBSERVED
    left = directory / vmcampaign.RUNS / "001-k01" / "probe" / "straggler"
    _until(lambda: vmcampaign.start_time(int(left.read_text())) is None)


def test_heartbeats_keep_a_run_longer_than_the_expiry(tmp_path, worker, monkeypatch):
    monkeypatch.setattr(campaign, "HEARTBEAT_EXPIRY_SECONDS", 1)
    directory = planned(tmp_path / "c", worker, run(1, "slow", seconds=30, sleep=3))

    controller(directory, worker, cadence=0.2).drive()

    assert state(directory)["runs"]["1"]["state"] == campaign.OBSERVED


def test_without_heartbeats_the_same_run_expires(tmp_path, worker, monkeypatch):
    # The firing control for the test above: waiting longer than the expiry
    # between heartbeats loses the claim, so the heartbeat is what kept it.
    monkeypatch.setattr(campaign, "HEARTBEAT_EXPIRY_SECONDS", 1)
    directory = planned(tmp_path / "c", worker, run(1, "slow", seconds=30, sleep=3))

    with pytest.raises(Failure, match="outlived its supervision"):
        controller(directory, worker, cadence=2.5).drive()


def test_a_failed_settle_keeps_the_outcome_and_stops_the_campaign(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "observe"), run(2, "observe"))

    def settle(run, record):
        raise RuntimeError("queue deletion refused")

    with pytest.raises(Failure, match="run 1 could not be settled: queue deletion"):
        controller(directory, worker, settle=settle).drive()
    runs = state(directory)["runs"]
    assert runs["1"]["state"] == campaign.OBSERVED
    assert runs["2"]["state"] == campaign.PENDING


def test_a_failed_prepare_forks_nothing_and_stops_the_campaign(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "observe"), run(2, "observe"))
    settled = []

    def prepare(run, heartbeat):
        raise RuntimeError("queue creation refused")

    with pytest.raises(Failure, match="run 1 could not be prepared: queue creation"):
        controller(
            directory,
            worker,
            prepare=prepare,
            settle=lambda run, record: settled.append(run),
        ).drive()
    assert not (directory / vmcampaign.RUNS / "001-k01" / vmcampaign.WORKER).exists()
    # Prepare undoes its own partial work; there is nothing to settle, now or
    # for a controller that opens the stopped campaign later.
    with pytest.raises(Failure, match="could not be prepared"):
        controller(
            directory, worker, settle=lambda run, record: settled.append(run)
        ).drive()
    assert settled == []
    assert state(directory)["runs"]["2"]["state"] == campaign.PENDING


def test_hooks_frame_each_run(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "observe"), run(2, "fail"))
    seen = []

    def prepare(run, heartbeat):
        evidence = directory / vmcampaign.RUNS / f"{run['order']:03d}-{run['cellId']}"
        # Before the fork, so the run's queue exists when the probe starts.
        assert not (evidence / vmcampaign.WORKER).exists()
        heartbeat()
        seen.append(("prepare", run["cellId"]))

    def settle(run, record):
        # After the outcome is recorded, so evidence can leave the host whole.
        order = str(run["order"])
        assert state(directory)["runs"][order]["state"] == record["outcome"]
        seen.append(("settle", run["cellId"], record["outcome"]))

    controller(directory, worker, prepare=prepare, settle=settle).drive()

    assert seen == [
        ("prepare", "k01"),
        ("settle", "k01", campaign.OBSERVED),
        ("prepare", "k02"),
        ("settle", "k02", campaign.FAILED),
    ]

    # A finished campaign reopened by another controller owes no settling.
    controller(directory, worker, prepare=prepare, settle=settle).drive()
    assert len(seen) == 4


def test_a_run_whose_outcome_is_unknown_is_settled_without_one(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "silent"))
    settled = []

    with pytest.raises(Failure, match="printed no summary line"):
        controller(
            directory, worker, settle=lambda run, record: settled.append(record)
        ).drive()
    assert settled == [None]
    # Settled on the way down, so a controller opening it later owes nothing.
    with pytest.raises(Failure, match="printed no summary line"):
        controller(
            directory, worker, settle=lambda run, record: settled.append(record)
        ).drive()
    assert settled == [None]


def test_the_launcher_comes_from_the_frozen_inputs(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "observe"))
    changed = {**inputs(worker), "launcher": [sys.executable, "-X", str(worker)]}

    with pytest.raises(Failure, match="inputs differ"):
        vmcampaign.Controller(directory, changed)
    with pytest.raises(Failure, match="no launcher"):
        vmcampaign.Controller(directory, {"protocolSha256": "a" * 64})


def test_a_second_controller_is_refused_while_the_first_drives(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "observe"))
    first = _detached()
    try:
        book = campaign.Journal(
            directory, inputs(worker), clock=time.time, alive=vmcampaign.alive
        )
        book.claim(vmcampaign.identity(first))

        with pytest.raises(Failure, match=f"driven by process {first} "):
            controller(directory, worker)
    finally:
        os.kill(first, signal.SIGKILL)


def test_a_reused_pid_is_not_the_owner(tmp_path):
    owner = vmcampaign.identity(os.getpid())

    assert vmcampaign.alive(owner)
    assert not vmcampaign.alive({**owner, "started": "Thu Jan  1 00:00:00 1970"})
    assert not vmcampaign.alive({**owner, "host": "elsewhere"})


def test_a_dead_process_is_not_alive():
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    owner = {**vmcampaign.identity(os.getpid()), "pid": child.pid}
    child.wait()

    assert not vmcampaign.alive(owner)


DRIVER = textwrap.dedent(
    """
    import json, sys
    from flink_tier3 import vmcampaign
    directory, inputs = sys.argv[1], json.loads(sys.argv[2])
    vmcampaign.Controller(directory, inputs, cadence=0.2).drive()
    """
)


def _driver(directory, worker):
    return subprocess.Popen(
        [sys.executable, "-c", DRIVER, str(directory), json.dumps(inputs(worker))]
    )


def _until(predicate, seconds=30):
    end = time.monotonic() + seconds
    while not predicate():
        assert time.monotonic() < end, "condition not reached"
        time.sleep(0.05)


def test_a_killed_controller_stops_the_campaign_and_its_run_is_reaped(tmp_path, worker):
    directory = planned(
        tmp_path / "c", worker, run(1, "hang", seconds=90), run(2, "observe")
    )
    hanging = directory / vmcampaign.RUNS / "001-k01" / "probe" / "hanging"
    driver = _driver(directory, worker)
    _until(hanging.exists)
    hung = int(hanging.read_text())

    driver.send_signal(signal.SIGKILL)
    driver.wait()
    # The run outlives the controller that forked it.
    assert vmcampaign.start_time(hung) is not None

    restarted = controller(directory, worker)

    assert restarted.orphans == ["001-k01"]
    _until(lambda: vmcampaign.start_time(hung) is None)
    assert restarted.journal.stopped()["reason"] == "run 1 lost its owner while claimed"
    with pytest.raises(Failure, match="lost its owner"):
        restarted.drive()


def _lost(directory, worker, pid, started):
    """A campaign whose run 1 was claimed by a dead controller and forked pid."""
    book = campaign.Journal(
        directory, inputs(worker), clock=time.time, alive=lambda owner: True
    )
    book.claim({"host": "gone", "pid": 1, "started": "never"})
    evidence = directory / vmcampaign.RUNS / "001-k01"
    evidence.mkdir(parents=True)
    (evidence / vmcampaign.WORKER).write_text(
        json.dumps({"host": socket.gethostname(), "pid": pid, "started": started})
    )


def _detached():
    """A sleeping process that is not this one's child, as an orphan is not."""
    # The grandchild must not hold the pipe, or reading it waits for its sleep.
    launcher = subprocess.run(
        [sys.executable, "-c", DETACH],
        capture_output=True,
        text=True,
        check=True,
    )
    return int(launcher.stdout)


DETACH = textwrap.dedent(
    """
    import subprocess, sys
    sleeper = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(120)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    print(sleeper.pid)
    """
)


@pytest.mark.parametrize("started", ["Thu Jan  1 00:00:00 1970", None])
def test_an_orphan_whose_pid_was_reused_is_left_alone(tmp_path, worker, started):
    directory = planned(tmp_path / "c", worker, run(1, "observe"))
    other = _detached()
    try:
        _lost(directory, worker, other, started)

        assert controller(directory, worker).orphans == []
        assert vmcampaign.start_time(other) is not None
    finally:
        os.kill(other, signal.SIGKILL)


def test_an_orphan_with_its_recorded_start_is_stopped(tmp_path, worker):
    # The positive control for the test above.
    directory = planned(tmp_path / "c", worker, run(1, "observe"))
    orphan = _detached()
    try:
        _lost(directory, worker, orphan, vmcampaign.start_time(orphan))

        assert controller(directory, worker).orphans == ["001-k01"]
        assert vmcampaign.start_time(orphan) is None
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.kill(orphan, signal.SIGKILL)


def test_an_interrupted_controller_finishes_its_run_and_a_restart_continues(
    tmp_path, worker
):
    directory = planned(
        tmp_path / "c",
        worker,
        run(1, "slow", sleep=2),
        run(2, "observe"),
        run(3, "observe"),
    )
    samples = directory / vmcampaign.RUNS / "001-k01" / "probe" / "samples.jsonl"
    driver = _driver(directory, worker)
    _until(samples.exists)

    driver.send_signal(signal.SIGTERM)
    assert driver.wait(timeout=30) == 0

    runs = state(directory)["runs"]
    assert runs["1"]["state"] == campaign.OBSERVED
    assert runs["2"]["state"] == campaign.PENDING
    counts = controller(directory, worker).drive()
    assert counts["runs"]["OBSERVED"] == 3
    assert counts["stopped"] is None


def test_a_stopped_campaign_continues_as_a_successor(tmp_path, worker):
    directory = planned(
        tmp_path / "c",
        worker,
        run(1, "observe"),
        run(2, "silent"),
        run(3, "fail"),
        run(4, "observe"),
    )
    with pytest.raises(Failure, match="run 2 printed no summary line"):
        controller(directory, worker).drive()
    stopped = controller(directory, worker)

    manifest = campaign.plan_successor(stopped.journal, tmp_path / "s", "cal1246e")

    # The lost run's cell was spent, so only the runs never claimed carry over.
    assert [
        (r["order"], r["cellId"], r["predecessorOrder"]) for r in manifest["plan"]
    ] == [
        (1, "k03", 3),
        (2, "k04", 4),
    ]
    assert manifest["predecessor"]["campaignId"] == "cal1246d"
    # Computed from the runs rather than from the journal's own arithmetic:
    # run 1 is charged what it used, and the lost run 2 its whole reservation.
    used = state(directory)["runs"]["1"]["consumed"]
    assert manifest["budgets"] == {
        "bytes": BUDGETS["bytes"] - used["bytes"] - 100_000,
        "seconds": BUDGETS["seconds"] - used["seconds"] - 60,
    }
    counts = controller(tmp_path / "s", worker).drive()
    assert counts["runs"] == {"PENDING": 0, "CLAIMED": 0, "OBSERVED": 1, "FAILED": 1}
    with pytest.raises(Failure, match="already continued as cal1246e"):
        campaign.plan_successor(stopped.journal, tmp_path / "t", "cal1246f")
    assert not (tmp_path / "t" / campaign.MANIFEST).exists()


def test_a_successor_can_be_given_a_new_budget_but_not_the_same_directory(
    tmp_path, worker
):
    directory = planned(tmp_path / "c", worker, run(1, "silent"), run(2, "observe"))
    with pytest.raises(Failure):
        controller(directory, worker).drive()
    stopped = controller(directory, worker)

    with pytest.raises(Failure, match="directory of its own"):
        campaign.plan_successor(stopped.journal, directory, "cal1246e")
    manifest = campaign.plan_successor(
        stopped.journal, tmp_path / "s", "cal1246e", budgets=BUDGETS
    )
    assert manifest["budgets"] == BUDGETS


def test_a_running_campaign_has_no_successor(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "observe"))

    with pytest.raises(Failure, match="Only a stopped campaign"):
        campaign.plan_successor(
            controller(directory, worker).journal, tmp_path / "s", "cal1246e"
        )


def test_the_cadence_leaves_room_under_the_expiry():
    longest = max(
        vmcampaign.CADENCE_SECONDS,
        vmcampaign.TERM_GRACE_SECONDS,
        vmcampaign.KILL_GRACE_SECONDS,
    )
    assert campaign.HEARTBEAT_EXPIRY_SECONDS >= 5 * longest


def test_the_controller_is_not_delivered_to_the_supervisor_pod():
    delivered = delivered_sources()

    # A positive control, so "excluded" cannot be read from an empty walk.
    assert "runtime.py" in delivered
    assert "vmcampaign.py" not in delivered


def _summary(reason="-", outcome="OBSERVATION"):
    return ",".join(
        ["PROBE1246", "cal1246d", "k01", "STAGED_HASH", outcome, reason]
        + ["1000", "1", "2048", "3", "stagedBytes=5", "", "-"]
    )


@pytest.mark.parametrize("separator", ["\x0c", "\x0b", "\x1c", "\x85", " "])
def test_a_failure_text_with_a_line_separator_is_still_one_line(separator):
    # The probe strips only commas, semicolons and line breaks from a failure
    # text; any other character a reader might split on stays in it.
    text = _summary(f"IOException: page{separator}break", "FAILED") + "\n"

    outcome, fields = vmcampaign.parse(text, "cal1246d", "k01")

    assert outcome == campaign.FAILED
    assert fields["reason"] == f"IOException: page{separator}break"


def test_a_summary_cut_short_is_not_a_line():
    with pytest.raises(vmcampaign.Unrecordable, match="no summary line"):
        vmcampaign.parse(_summary(), "cal1246d", "k01")


def test_a_count_in_another_script_is_not_a_number():
    text = _summary().replace(",1,2048,", ",²,2048,") + "\n"

    with pytest.raises(vmcampaign.Unrecordable, match="non-numeric field 8"):
        vmcampaign.parse(text, "cal1246d", "k01")


def test_recording_a_large_run_keeps_its_claim(tmp_path, worker, monkeypatch):
    # Each step after the run ends takes most of the expiry, on a clock the
    # test moves rather than one it waits on: only a heartbeat between every
    # two of them keeps the claim of a run that has already ended.
    monkeypatch.setattr(campaign, "HEARTBEAT_EXPIRY_SECONDS", 2)
    directory = planned(tmp_path / "c", worker, run(1, "observe"))
    driving = controller(directory, worker)
    skew = [0.0]
    driving.journal._clock = lambda: time.time() + skew[0]

    def slow(function, times=None):
        calls = [0]

        def call(*args):
            calls[0] += 1
            if times is None or calls[0] <= times:
                skew[0] += 1.2
            return function(*args)

        return call

    # The straggler check runs once, before the first heartbeat of recording.
    monkeypatch.setattr(vmcampaign, "_group_gone", slow(vmcampaign._group_gone, 1))
    monkeypatch.setattr(vmcampaign, "_files", slow(vmcampaign._files))
    monkeypatch.setattr(vmcampaign, "_size", slow(vmcampaign._size))

    driving.drive()

    assert state(directory)["runs"]["1"]["state"] == campaign.OBSERVED


SETTLE_CRASH = textwrap.dedent(
    """
    import json, os, sys
    from flink_tier3 import vmcampaign
    directory, inputs = sys.argv[1], json.loads(sys.argv[2])

    def settle(run, record):
        os._exit(9)

    vmcampaign.Controller(directory, inputs, cadence=0.2, settle=settle).drive()
    """
)


def test_a_settle_lost_to_a_crash_is_owed_to_the_next_controller(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "observe"), run(2, "observe"))
    crashed = subprocess.run(
        [
            sys.executable,
            "-c",
            SETTLE_CRASH,
            str(directory),
            json.dumps(inputs(worker)),
        ],
        check=False,
        timeout=60,
    )
    assert crashed.returncode == 9
    # The outcome was recorded and the claim released before settling began.
    assert state(directory)["runs"]["1"]["state"] == campaign.OBSERVED
    seen = []

    controller(
        directory,
        worker,
        settle=lambda run, record: seen.append(
            (run["cellId"], record and record["outcome"])
        ),
    ).drive()

    assert seen == [("k01", campaign.OBSERVED), ("k02", campaign.OBSERVED)]


GROUP = textwrap.dedent(
    """
    import subprocess, sys
    member = (
        "import subprocess, sys;"
        "p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'],"
        " stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,"
        " stderr=subprocess.DEVNULL);"
        "print(p.pid)"
    )
    leader = subprocess.Popen(
        [sys.executable, "-c", member],
        stdout=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    left = int(leader.stdout.readline())
    leader.wait()
    print(leader.pid, left)
    """
)


def test_what_an_orphan_left_in_its_group_is_stopped(tmp_path, worker):
    # The run's leader has exited and a process it started is still in its
    # group: the recorded PID answers nothing, and the group is the run's.
    directory = planned(tmp_path / "c", worker, run(1, "observe"))
    launched = subprocess.run(
        [sys.executable, "-c", GROUP], capture_output=True, text=True, check=True
    )
    leader, left = (int(value) for value in launched.stdout.split())
    try:
        _lost(directory, worker, leader, "the leader's start")

        assert controller(directory, worker).orphans == ["001-k01"]
        _until(lambda: vmcampaign.start_time(left) is None)
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.kill(left, signal.SIGKILL)


def _stopped_with_pending(tmp_path, worker):
    directory = planned(
        tmp_path / "c", worker, run(1, "silent"), run(2, "observe"), run(3, "observe")
    )
    with pytest.raises(Failure):
        controller(directory, worker).drive()
    return controller(directory, worker)


def test_a_successor_interrupted_after_its_mark_is_completed_not_duplicated(
    tmp_path, worker
):
    stopped = _stopped_with_pending(tmp_path, worker)
    # A process that died between marking the campaign and planning the
    # successor leaves the mark and no successor.
    target = {
        "campaignId": "cal1246e",
        "directory": str((tmp_path / "s").resolve()),
    }
    stopped.journal._locked(lambda state: state.update(successor=target))

    with pytest.raises(Failure, match="already continued as cal1246e"):
        campaign.plan_successor(stopped.journal, tmp_path / "t", "cal1246f")
    manifest = campaign.plan_successor(stopped.journal, tmp_path / "s", "cal1246e")
    assert [run["cellId"] for run in manifest["plan"]] == ["k02", "k03"]
    # Asked again, it answers with the successor it already planned.
    assert campaign.plan_successor(stopped.journal, tmp_path / "s", "cal1246e") == (
        manifest
    )


def test_a_successor_refused_by_planning_leaves_no_mark(tmp_path, worker):
    stopped = _stopped_with_pending(tmp_path, worker)

    with pytest.raises(Failure, match="positive integer"):
        campaign.plan_successor(
            stopped.journal,
            tmp_path / "s",
            "cal1246e",
            budgets={"bytes": 0, "seconds": 1},
        )
    assert "successor" not in state(tmp_path / "c")
    manifest = campaign.plan_successor(stopped.journal, tmp_path / "t", "cal1246f")
    assert manifest["campaignId"] == "cal1246f"


MARK_CRASH = textwrap.dedent(
    """
    import json, os, sys, time
    from flink_tier3 import campaign, vmcampaign
    directory, successor, inputs = sys.argv[1], sys.argv[2], json.loads(sys.argv[3])
    # The controller that stopped it is still alive, so a Controller here would
    # rightly refuse; the journal alone is enough to plan from.
    stopped = campaign.Journal(directory, inputs, clock=time.time, alive=vmcampaign.alive)
    # Killed while planning: no finally, no exception handler runs.
    campaign.plan_campaign = lambda *args, **kwargs: os._exit(9)
    campaign.plan_successor(stopped, successor, "cal1246e")
    """
)


def test_the_successor_mark_is_on_disk_before_the_successor_is(tmp_path, worker):
    _stopped_with_pending(tmp_path, worker)

    crashed = subprocess.run(
        [
            sys.executable,
            "-c",
            MARK_CRASH,
            str(tmp_path / "c"),
            str(tmp_path / "s"),
            json.dumps(inputs(worker)),
        ],
        check=False,
        timeout=60,
    )

    assert crashed.returncode == 9
    assert state(tmp_path / "c")["successor"] == {
        "campaignId": "cal1246e",
        "directory": str((tmp_path / "s").resolve()),
    }


def test_a_run_lost_with_its_controller_is_settled_without_an_outcome(tmp_path, worker):
    directory = planned(
        tmp_path / "c", worker, run(1, "hang", seconds=90), run(2, "observe")
    )
    hanging = directory / vmcampaign.RUNS / "001-k01" / "probe" / "hanging"
    driver = _driver(directory, worker)
    _until(hanging.exists)
    driver.send_signal(signal.SIGKILL)
    driver.wait()
    # Even an outcome written just before the controller died is not one the
    # journal recorded, so it is not the run's outcome.
    (directory / vmcampaign.RUNS / "001-k01" / vmcampaign.OUTCOME).write_text(
        json.dumps({"outcome": campaign.OBSERVED})
    )
    seen = []

    with pytest.raises(Failure, match="lost its owner"):
        controller(
            directory,
            worker,
            settle=lambda run, record: seen.append((run["cellId"], record)),
        ).drive()
    assert seen == [("k01", None)]


def test_a_settle_still_owed_that_fails_names_itself(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "observe"), run(2, "observe"))
    subprocess.run(
        [
            sys.executable,
            "-c",
            SETTLE_CRASH,
            str(directory),
            json.dumps(inputs(worker)),
        ],
        check=False,
        timeout=60,
    )

    def settle(run, record):
        raise RuntimeError("queue deletion refused")

    retrying = controller(directory, worker, settle=settle)
    with pytest.raises(Failure, match="run 1 could not be settled: queue deletion"):
        retrying.drive()
    assert retrying.journal.stopped()["reason"].startswith("run 1 could not be settled")
    assert state(directory)["runs"]["2"]["state"] == campaign.PENDING


def test_an_unknown_outcome_is_settled_once_or_left_owed(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "silent"))
    settled = []

    def failing(run, record):
        raise RuntimeError("queue deletion refused")

    with pytest.raises(Failure, match="printed no summary line"):
        controller(directory, worker, settle=failing).drive()
    # The failed settle is left owed, and the next controller pays it once.
    for _ in range(2):
        with pytest.raises(Failure):
            controller(
                directory, worker, settle=lambda run, record: settled.append(record)
            ).drive()
    assert settled == [None]


def test_a_second_controller_is_refused_when_the_first_claims_after_it_opened(
    tmp_path, worker
):
    directory = planned(tmp_path / "c", worker, run(1, "observe"))
    second = controller(directory, worker, settle=lambda run, record: None)
    first = _detached()
    try:
        book = campaign.Journal(
            directory, inputs(worker), clock=time.time, alive=vmcampaign.alive
        )
        book.claim(vmcampaign.identity(first))
        (directory / vmcampaign.RUNS / "001-k01").mkdir(parents=True)

        with pytest.raises(Failure, match=f"driven by process {first} "):
            second.drive()
        assert not (
            directory / vmcampaign.RUNS / "001-k01" / vmcampaign.SETTLED
        ).exists()
    finally:
        os.kill(first, signal.SIGKILL)


def test_an_orphan_whose_group_is_gone_is_not_reported(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "observe"))
    gone = subprocess.Popen([sys.executable, "-c", "pass"], start_new_session=True)
    gone.wait()
    _lost(directory, worker, gone.pid, "its start")

    assert controller(directory, worker).orphans == []


def test_a_retry_refuses_a_successor_it_did_not_plan(tmp_path, worker):
    stopped = _stopped_with_pending(tmp_path, worker)
    target = {"campaignId": "cal1246e", "directory": str((tmp_path / "s").resolve())}
    stopped.journal._locked(lambda state: state.update(successor=target))
    # Another campaign with the same plan already sits in the marked directory.
    campaign.plan_campaign(
        tmp_path / "s",
        "cal1246e",
        [
            dict(run, order=index)
            for index, run in enumerate(stopped.journal.plan[1:], 1)
        ],
        inputs(worker),
        BUDGETS,
        predecessor={
            "campaignId": "other",
            "planSha256": state(tmp_path / "c")["planSha256"],
        },
    )

    with pytest.raises(Failure, match="different campaign is already planned there"):
        campaign.plan_successor(stopped.journal, tmp_path / "s", "cal1246e")


def test_a_successor_that_cannot_be_written_leaves_no_mark(tmp_path, worker):
    stopped = _stopped_with_pending(tmp_path, worker)
    (tmp_path / "file").write_text("not a directory")

    with pytest.raises(OSError):
        campaign.plan_successor(stopped.journal, tmp_path / "file" / "s", "cal1246e")
    assert "successor" not in state(tmp_path / "c")


def test_runs_finished_without_a_settle_hook_are_not_owed_to_one(tmp_path, worker):
    directory = planned(tmp_path / "c", worker, run(1, "observe"), run(2, "observe"))
    first = controller(directory, worker)
    first.journal.claim(first.owner)
    first.execute(first.journal.plan[0])
    seen = []

    controller(
        directory, worker, settle=lambda run, record: seen.append(run["cellId"])
    ).drive()

    assert seen == ["k02"]


def test_a_second_controller_cannot_drive_while_the_first_settles(tmp_path, worker):
    # With no claim standing, only the drive lock keeps a second controller
    # from settling the same run or claiming the next one beside the first.
    directory = planned(tmp_path / "c", worker, run(1, "observe"), run(2, "observe"))
    refused = []

    def settle(run, record):
        if run["order"] == 1:
            with pytest.raises(Failure, match="driven by another controller") as caught:
                controller(directory, worker, settle=settle).drive()
            refused.append(str(caught.value))

    counts = controller(directory, worker, settle=settle).drive()

    assert refused == ["Campaign is driven by another controller"]
    assert counts["runs"]["OBSERVED"] == 2
