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
"""Recompute a Cloud Tasks run's verdict from a synthetic evidence directory."""

import json

import pytest
from flink_tier3 import analyze as shared
from flink_tier3.cloudtasks import analyze
from flink_tier3.cloudtasks.oracle import assess, page
from flink_tier3.cloudtasks.trial import Trial
from flink_tier3.cloudtasks.verdict import (
    COMPLETE_EVENT,
    MEASUREMENT_EVENT,
    OBSERVED,
    PAGE_EVENT,
    SAMPLED_STAGES,
    summarize,
    verdict,
)
from flink_tier3.runs import discover_runs

from .test_recovery_oracle import reading

TRIAL = Trial("ct-analyze", 4, 1)
PREFIX = TRIAL.queue_path + "/tasks/"


def tasks(sequences):
    return [{"name": PREFIX + TRIAL.task_id(n), "dispatchCount": 0} for n in sequences]


def write(directory, name, value):
    path = directory / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def run_directory(
    tmp_path,
    pages,
    *,
    stored_pages=None,
    attempt=2,
    complete=True,
    readings=None,
):
    """A completed run whose record names `attempt`, exporting `pages`.

    It exported one full reading in each sampled window unless `readings`,
    pairs of stage and reading, says otherwise.
    """
    directory = tmp_path / TRIAL.run_id
    write(
        directory,
        "approval.json",
        {
            "run_id": TRIAL.run_id,
            "scenario": "cloudtasks-recovery",
            "cloudtasks_trial": {"records": 4, "records_per_second": 1},
        },
    )
    items = [page(TRIAL, listed) for listed in (stored_pages or pages)]
    listing = {
        "attempt": attempt,
        "pages": len(items),
        "queue_state": "PAUSED",
        "report": assess(TRIAL, items),
    }
    outcomes = {"upgrade": {}, "failover": {}, "listing": listing}
    if readings is None:
        readings = [(stage, reading()) for stage in SAMPLED_STAGES]
    coverage = {}
    for index, (stage, value) in enumerate(readings):
        value = {**value, "window": stage}
        coverage = summarize(coverage, stage, value)
        write(
            directory,
            f"supervisor/measurement-{index}.json",
            {
                "event": MEASUREMENT_EVENT,
                "at": f"2026-10-13T00:00:{index:02d}Z",
                "payload": {"stage": stage, **value},
            },
        )
    record = {"stage": "complete", "outcomes": outcomes, "coverage": coverage}
    record.update(verdict(record))
    write(directory, "result.json", {"success": True, "idle": True, "recovery": record})
    for index, listed in enumerate(pages):
        write(
            directory,
            f"supervisor/page-{attempt}-{index}.json",
            {
                "event": PAGE_EVENT,
                "at": f"2026-10-13T00:00:{index:02d}Z",
                "payload": {"attempt": attempt, "page": index, **page(TRIAL, listed)},
            },
        )
    # A failed earlier attempt's page, which the record does not name.
    write(
        directory,
        "supervisor/page-1-0.json",
        {
            "event": PAGE_EVENT,
            "at": "2026-10-13T00:00:00Z",
            "payload": {"attempt": 1, "page": 0, **page(TRIAL, tasks([0]))},
        },
    )
    if complete:
        write(
            directory,
            "supervisor/complete.json",
            {"event": COMPLETE_EVENT, "at": "2026-10-13T00:01:00Z", "payload": record},
        )
    return directory


def test_a_usable_run_recomputes_to_its_stored_verdict(tmp_path):
    directory = run_directory(tmp_path, [tasks([0, 1]), tasks([2, 3])])
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] == "usable"
    assert result["stored"] == "usable"
    assert result["problems"] == []
    assert result["report"]["matched"] == 4
    assert result["samples"] == 3


def test_a_window_whose_readings_returned_nothing_is_inconclusive(tmp_path):
    readings = [(stage, reading()) for stage in SAMPLED_STAGES]
    readings[1] = ("upgraded", {"vertices": {"unavailable": "503"}})
    directory = run_directory(tmp_path, [tasks([0, 1, 2, 3])], readings=readings)
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] == "inconclusive"
    assert result["stored"] == "inconclusive"
    assert result["reasons"] == [
        f"unobserved-{family}-in-upgraded" for family in OBSERVED
    ]
    assert result["problems"] == []


def test_a_reading_missing_from_the_export_leaves_no_verdict(tmp_path):
    directory = run_directory(tmp_path, [tasks([0, 1, 2, 3])])
    (directory / "supervisor/measurement-2.json").unlink()
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] is None
    assert result["problems"] == ["missing-samples:1"]


def test_the_coverage_is_rebuilt_from_the_readings_not_the_record(tmp_path):
    readings = [(stage, reading()) for stage in SAMPLED_STAGES]
    directory = run_directory(tmp_path, [tasks([0, 1, 2, 3])], readings=readings)
    # The export holds a reading that returned no connector metric where the
    # record claims one did.
    measurement = json.loads((directory / "supervisor/measurement-0.json").read_text())
    measurement["payload"]["sink"] = measurement["payload"]["sink"][:2]
    (directory / "supervisor/measurement-0.json").write_text(json.dumps(measurement))
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] == "inconclusive"
    assert result["reasons"] == ["unobserved-connector-in-baseline"]
    assert result["problems"] == ["verdict-differs-from-the-record"]


def test_pages_that_contradict_the_stored_report_are_a_problem(tmp_path):
    directory = run_directory(
        tmp_path,
        [tasks([0, 1]), tasks([2, 2])],
        stored_pages=[tasks([0, 1]), tasks([2, 3])],
    )
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] == "inconclusive"
    assert result["reasons"] == ["oracle-not-passed"]
    assert "report-differs-from-pages" in result["problems"]
    assert "verdict-differs-from-the-record" in result["problems"]


def test_a_missing_page_leaves_the_run_without_a_verdict(tmp_path):
    directory = run_directory(tmp_path, [tasks([0, 1]), tasks([2, 3])])
    (directory / "supervisor/page-2-1.json").unlink()
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] is None
    assert result["problems"] == ["missing-pages:1"]


def test_an_unfinished_run_is_inconclusive_without_problems(tmp_path):
    directory = run_directory(tmp_path, [tasks([0, 1, 2, 3])], complete=False)
    result_path = directory / "result.json"
    result = json.loads(result_path.read_text())
    result["recovery"] = {"stage": "failover", "outcomes": {"upgrade": {}}}
    result_path.write_text(json.dumps(result))
    [run] = discover_runs(directory)
    assessed = analyze.assess(run)
    assert assessed["verdict"] == "inconclusive"
    assert assessed["problems"] == []


def test_the_shared_report_carries_the_section(tmp_path):
    run_directory(tmp_path, [tasks([0, 1, 2, 3])])
    report = shared.analyze(tmp_path)
    assert [item["verdict"] for item in report["cloudtasks"]] == ["usable"]
    assert "## Deployed Cloud Tasks runs" in shared.render_markdown(report)
    assert "cloudtasks runs: usable 1" in shared.summary_line(report, tmp_path)


def test_unreadable_evidence_leaves_the_run_without_a_verdict(tmp_path):
    directory = run_directory(tmp_path, [tasks([0, 1]), tasks([2, 3])])
    page = json.loads((directory / "supervisor/page-2-1.json").read_text())
    page["payload"]["ids"] = "not a list"
    (directory / "supervisor/page-2-1.json").write_text(json.dumps(page))
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] is None
    assert result["problems"] == ["malformed-pages:1"]
    assert result["samples"] == 3

    approval = json.loads((directory / "approval.json").read_text())
    del approval["cloudtasks_trial"]
    (directory / "approval.json").write_text(json.dumps(approval))
    [run] = discover_runs(directory)
    assert analyze.assess(run)["problems"] == ["approval-without-cloudtasks-trial"]


def test_a_page_beyond_the_recorded_count_is_reported(tmp_path):
    directory = run_directory(tmp_path, [tasks([0, 1]), tasks([2, 3])])
    write(
        directory,
        "supervisor/page-2-2.json",
        {
            "event": PAGE_EVENT,
            "at": "2026-10-13T00:00:30Z",
            "payload": {"attempt": 2, "page": 2, **page(TRIAL, tasks([0]))},
        },
    )
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] is None
    assert result["problems"] == ["extra-pages:1"]


def checkpoint(number, triggered, acknowledged, **fields):
    return {
        "id": number,
        "status": "COMPLETED",
        "is_savepoint": False,
        "trigger_timestamp": triggered,
        "latest_ack_timestamp": acknowledged,
        **fields,
    }


def with_recoveries(directory, upgrade, failover, polls):
    """Give the run's record these proofs and export these checkpoint polls."""
    for name in ("result.json", "supervisor/complete.json"):
        path = directory / name
        value = json.loads(path.read_text())
        record = value["recovery"] if name == "result.json" else value["payload"]
        record["outcomes"].update(upgrade=upgrade, failover=failover)
        path.write_text(json.dumps(value))
    for index, poll in enumerate(polls):
        write(
            directory,
            f"supervisor/checkpoints-{index}.json",
            {
                "event": analyze.CHECKPOINTS_EVENT,
                "at": f"2026-10-13T00:02:{index:02d}Z",
                "payload": {"job_id": "1" * 32, **poll},
            },
        )


def test_each_replay_began_between_the_restore_and_the_next_checkpoint(tmp_path):
    directory = run_directory(tmp_path, [tasks([0, 1, 2, 3])])
    savepoint = checkpoint(2, 100_000, 101_000, is_savepoint=True)
    with_recoveries(
        directory,
        upgrade={"restored": {"id": 2, "restore_timestamp": 130_000}},
        failover={"restored": {"id": 4, "restore_timestamp": 300_000}},
        polls=[
            # The initial job: its savepoint, in the latest statistics only.
            {"latest": {"savepoint": savepoint}, "history": []},
            # The upgraded job's history: two checkpoints after its restore.
            {"history": [checkpoint(3, 150_000, 152_000)]},
            {"history": [checkpoint(4, 180_000, 181_000), {"status": "FAILED"}]},
            # The recovered job's history starts again after the failover.
            {"history": [checkpoint(5, 340_000, 341_000)]},
        ],
    )
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] == "usable"
    assert result["replay"] == {
        "upgrade": {
            "restored_id": 2,
            "first_checkpoint_id": 3,
            "from_ms": 29_000,
            "to_ms": 49_000,
        },
        "failover": {
            "restored_id": 4,
            "first_checkpoint_id": 5,
            "from_ms": 119_000,
            "to_ms": 159_000,
        },
    }
    rendered = analyze.render([result])
    assert f"| {TRIAL.run_id} | upgrade | 2 | 29.0 to 49.0 s | chk-3 |" in rendered


@pytest.mark.parametrize(
    "restored",
    [
        # No exported poll saw the restored checkpoint complete.
        {"id": 9, "restore_timestamp": 300_000},
        # Nothing was checkpointed after the restore.
        {"id": 4, "restore_timestamp": 900_000},
        {"id": [4], "restore_timestamp": 300_000},
        {"id": 4, "restore_timestamp": "300000"},
        {"id": 4},
    ],
)
def test_a_replay_the_evidence_cannot_place_is_unreadable(tmp_path, restored):
    directory = run_directory(tmp_path, [tasks([0, 1, 2, 3])])
    with_recoveries(
        directory,
        upgrade="not a proof",
        failover={"restored": restored},
        polls=[{"history": [checkpoint(4, 180_000, 181_000)]}, {"history": 3}],
    )
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["replay"] == {"failover": None}
    assert f"| {TRIAL.run_id} | failover | unreadable |" in analyze.render([result])


def test_the_recoveries_are_timed_with_their_faults(tmp_path):
    """A rescale-in trial with a TaskManager replacement, its upgrade held."""
    directory = run_directory(tmp_path, [tasks([0, 1, 2, 3])])
    upgrade = {
        "restored": {
            "id": 2,
            "is_savepoint": True,
            "restore_timestamp": 1_760_000_040_000,
        },
        "checkpoint": {"id": 3, "latest_ack_timestamp": 1_760_000_070_000},
        "rescale": {"direction": "in", "subtasks_before": 3, "subtasks_after": 2},
    }
    failover = {
        "restored": {
            "id": 8,
            "is_savepoint": False,
            "restore_timestamp": 1_760_000_420_000,
        },
        "checkpoint": {"id": 9, "latest_ack_timestamp": 1_760_000_460_000},
        "replaced": {"uid": "tm-2-uid", "name": "tm-2"},
    }
    with_recoveries(directory, upgrade=upgrade, failover=failover, polls=[])
    # The held upgrade's own stage record shares the prefix and is not a start.
    for index, (stage, at) in enumerate(
        [
            ("upgrade", "2025-10-09T08:53:20Z"),
            ("upgraded", "2025-10-09T08:54:30Z"),
            ("failover", "2025-10-09T08:59:40Z"),
        ]
    ):
        write(
            directory,
            f"supervisor/stage-{index}.json",
            {
                "event": "recovery-" + stage,
                "at": at,
                "payload": {"stage": stage, "at": at},
            },
        )
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["recoveries"]["upgrade"] == {
        "fault": "rescale-in",
        "began_at": "2025-10-09T08:53:20Z",
        "restored_from": "savepoint",
        "restored_id": 2,
        "restored_ms": 40_000,
        "checkpoint_id": 3,
        "checkpoint_ms": 70_000,
    }
    assert result["recoveries"]["failover"]["fault"] == "tm-replacement"
    assert result["recoveries"]["failover"]["restored_ms"] == 40_000
    rendered = analyze.render([result])
    assert f"| {TRIAL.run_id} | upgrade | rescale-in | " in rendered
    assert f"| {TRIAL.run_id} | failover | tm-replacement | " in rendered


def test_a_replay_window_reads_every_place_a_poll_holds_a_checkpoint(tmp_path):
    directory = run_directory(tmp_path, [tasks([0, 1, 2, 3])])
    with_recoveries(
        directory,
        upgrade="not a proof",
        failover={"restored": {"id": 4, "restore_timestamp": 300_000}},
        polls=[
            # The restored checkpoint only as the latest completed one.
            {"latest": {"completed": checkpoint(4, 180_000, 181_000)}},
            # Not acknowledged: its trigger time is not used.
            {"history": [checkpoint(5, 300_000, None)]},
            # Triggered at the restore instant itself, which the replay follows.
            {"history": [checkpoint(6, 300_000, 301_000)]},
            {"history": [checkpoint(7, 330_000, 331_000)]},
        ],
    )
    [run] = discover_runs(directory)
    assert analyze.assess(run)["replay"] == {
        "failover": {
            "restored_id": 4,
            "first_checkpoint_id": 6,
            "from_ms": 119_000,
            "to_ms": 119_000,
        }
    }
