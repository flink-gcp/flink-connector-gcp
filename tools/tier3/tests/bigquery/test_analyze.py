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
"""Recomputing a deployed BigQuery verdict from an exported evidence set.

The seam between the exercise that writes this evidence and the analyzer that
reads it is crossed by `test_exercise`; these cases build the shapes
a real run does not produce.
"""

import json

import pytest
from flink_tier3 import analyze
from flink_tier3.bigquery.analyze import SCENARIO, render
from flink_tier3.bigquery.verdict import (
    COMPLETE_EVENT,
    COMPLETE_STAGE,
    FILE_LOADS_OBSERVED,
    MEASUREMENT_EVENT,
    OBSERVED,
    SAMPLED_STAGES,
)
from flink_tier3.common import INCONCLUSIVE, USABLE
from flink_tier3.recovery_analysis import counts

RUN = "bq-1423-0001"
ATTEMPTS = 3
WINDOW = {"attempts": ATTEMPTS, "observed": sorted(OBSERVED)}
SAMPLES = ATTEMPTS * len(SAMPLED_STAGES)
# One returned id per sink family, and a TaskManager that published a memory
# metric: the reading a healthy poll emits.
FULL = {
    "sink": [
        {"id": "busyTimeMsPerSecond"},
        {"id": "inPoolUsage"},
        {"id": "Sink__Writer.openDestinations"},
    ],
    "taskmanagers": [
        {"id": "tm-0", "metrics": [{"id": "Status.JVM.Memory.Direct.MemoryUsed"}]}
    ],
}
# What the same poll emits when the job exposed nothing.
BLIND = {"sink": {"unavailable": "503"}, "taskmanagers": {"unavailable": "503"}}


def record(**overrides):
    value = {
        "stage": COMPLETE_STAGE,
        "at": "2026-09-22T00:00:00Z",
        "outcomes": {
            "upgrade": {"checkpoint": {"id": 1}},
            "failover": {"checkpoint": {"id": 2}},
            "query": {"report": {"verdict": "pass"}},
        },
        "coverage": {stage: dict(WINDOW) for stage in SAMPLED_STAGES},
        "verdict": USABLE,
        "reasons": [],
    }
    value.update(overrides)
    return value


def samples_of(recovery, reading=None):
    """One measurement event per attempt the record accounts for, by stage.

    Each carries the reading the supervisor emitted beside the record, because
    that is what the analyzer folds: an event shaped only as `{"stage": ...}`
    would prove the counts agree and nothing about what was observed.
    """
    return [
        (MEASUREMENT_EVENT, {"stage": stage, **(FULL if reading is None else reading)})
        for stage, window in (recovery or {}).get("coverage", {}).items()
        for _ in range(window["attempts"])
    ]


def write_run(root, recovery=None, receipt=None, events=None, run_id=RUN):
    """One exported BigQuery run directory, as a download of `runs/<id>/`."""
    run_dir = root / run_id
    (run_dir / "supervisor").mkdir(parents=True, exist_ok=True)
    (run_dir / "approval.json").write_text(
        json.dumps({"run_id": run_id, "scenario": SCENARIO, "flink_version": "2.2.1"})
    )
    entries = list(events if events is not None else samples_of(recovery))
    if recovery is not None:
        entries.insert(0, (COMPLETE_EVENT, recovery))
    for index, (event, payload) in enumerate(entries):
        (run_dir / "supervisor" / f"{index:05d}.json").write_text(
            json.dumps(
                {
                    "event": event,
                    "at": f"2026-09-22T00:00:{index:02d}Z",
                    "payload": payload,
                }
            )
        )
    if receipt is not None:
        (run_dir / "result.json").write_text(json.dumps(receipt))
    return run_dir


def receipt_for(recovery, **overrides):
    value = {
        "scenario": SCENARIO,
        "idle": True,
        "success": recovery.get("verdict") == USABLE,
        "recovery": recovery,
    }
    value.update(overrides)
    return value


def assessed(root, expected=1):
    report = analyze.analyze(root)
    assert len(report["bigquery"]) == expected
    return report, report["bigquery"][0]


DIFFERS = "verdict-differs-from-the-record"


def test_a_complete_run_is_usable_and_its_verdict_is_recomputed(tmp_path):
    value = record()
    write_run(tmp_path, value, receipt_for(value))
    report, item = assessed(tmp_path)
    assert item == {
        "run_id": RUN,
        "verdict": USABLE,
        "stored": USABLE,
        "reasons": [],
        "problems": [],
        "samples": SAMPLES,
        # The fixture's proofs hold no timestamps and it exports no stage
        # records, so both proved recoveries are untimed.
        "recoveries": {"upgrade": None, "failover": None},
    }
    assert "Deployed BigQuery runs" in analyze.render_markdown(report)


def test_an_incomplete_observation_is_inconclusive_with_its_reasons(tmp_path):
    reasons = [f"unobserved-{name}-in-finishing" for name in OBSERVED]
    coverage = {
        "baseline": dict(WINDOW),
        "finishing": {"attempts": ATTEMPTS, "observed": []},
    }
    value = record(coverage=coverage, verdict=INCONCLUSIVE, reasons=reasons)
    # The readings say the same thing the record does: the finishing window
    # asked three times and the job exposed nothing.
    events = [
        *samples_of({"coverage": {"baseline": dict(WINDOW)}}),
        *samples_of({"coverage": {"finishing": dict(WINDOW)}}, reading=BLIND),
    ]
    write_run(tmp_path, value, receipt_for(value), events=events)
    _, item = assessed(tmp_path)
    assert item["verdict"] == INCONCLUSIVE
    assert item["problems"] == []
    assert item["reasons"] == reasons


def test_a_file_loads_run_is_held_to_the_families_its_trial_requires(tmp_path):
    """The receipt's trial decides the families, as the exercise's mode did."""
    trial = {"mode": "FILE_LOADS", "destinations": 10}
    # Read with the Storage Write families only, the run is short of two.
    value = record()
    write_run(tmp_path, value, receipt_for(value, bigquery_trial=trial))
    _, item = assessed(tmp_path)
    assert item["verdict"] == INCONCLUSIVE
    assert item["reasons"] == [
        f"unobserved-{name}-in-{stage}"
        for stage in SAMPLED_STAGES
        for name in FILE_LOADS_OBSERVED
    ]
    # The stored usable verdict is not the one the readings recompute to.
    assert item["problems"] == [DIFFERS]
    subtask = {
        "index": 0,
        "end_to_end_duration": 3,
        "checkpoint": {"sync": 1, "async": 2},
    }
    reading = {
        **FULL,
        "committer": [{"id": "BigQuery_FILE_LOADS__Committer.loadJobsSubmitted"}],
        "checkpoints": {"vertices": {"sink": [subtask], "committer": [subtask]}},
    }
    window = {
        "attempts": ATTEMPTS,
        "observed": sorted(OBSERVED + FILE_LOADS_OBSERVED),
    }
    value = record(coverage={stage: dict(window) for stage in SAMPLED_STAGES})
    write_run(
        tmp_path,
        value,
        receipt_for(value, bigquery_trial=trial),
        events=samples_of(value, reading),
        run_id=RUN + "-fl",
    )
    report = analyze.analyze(tmp_path)
    (item,) = [run for run in report["bigquery"] if run["run_id"] == RUN + "-fl"]
    assert (item["verdict"], item["reasons"], item["problems"]) == (USABLE, [], [])


def test_the_verdict_is_decided_over_the_readings_not_the_record_s_coverage(
    tmp_path,
):
    """A run whose sink was down for the whole trial read nothing.

    Its record claims every family in both windows; the readings beside it
    returned none, and they decide.
    """
    value = record()
    write_run(
        tmp_path, value, receipt_for(value), events=samples_of(value, reading=BLIND)
    )
    _, item = assessed(tmp_path)
    assert item["verdict"] == INCONCLUSIVE
    assert item["stored"] == USABLE
    assert item["problems"] == [DIFFERS]
    assert item["samples"] == SAMPLES


def test_a_completed_record_without_its_completion_evidence_has_no_verdict(tmp_path):
    """An absent object: only the record it should have accompanied names it."""
    value = record()
    write_run(tmp_path, None, receipt_for(value), events=samples_of(value))
    _, item = assessed(tmp_path)
    assert item["problems"] == ["missing-" + COMPLETE_EVENT]
    assert item["verdict"] is None
    assert item["stored"] == USABLE


def test_a_run_that_never_completed_is_inconclusive_not_an_evidence_problem(tmp_path):
    """The archetypal case: both recoveries proved, then the query slots ran out."""
    aborted = record(stage="visibility")
    del aborted["verdict"], aborted["reasons"]
    # The record was written at the run's last transition; the samples after
    # it are readings the record does not account for.
    events = [*samples_of(aborted), *samples_of(aborted)[:2]]
    write_run(tmp_path, None, receipt_for(aborted), events=events)
    _, item = assessed(tmp_path)
    assert item["verdict"] == INCONCLUSIVE
    assert item["stored"] is None
    assert item["reasons"] == ["recovery-incomplete"]
    assert item["problems"] == []
    assert item["samples"] == SAMPLES + 2


@pytest.mark.parametrize(
    "missing, problem",
    [
        ("result.json", "missing-result.json"),
        (COMPLETE_EVENT, "missing-" + COMPLETE_EVENT),
    ],
)
def test_an_evidence_set_missing_the_run_s_own_account_has_no_verdict(
    tmp_path, missing, problem
):
    value = record()
    receipt = receipt_for(value)
    receipt["recovery"] = None
    write_run(
        tmp_path,
        value if missing == "result.json" else None,
        None if missing == "result.json" else receipt,
        events=samples_of(value),
    )
    _, item = assessed(tmp_path)
    assert item["verdict"] is None
    assert item["problems"] == [problem]
    assert item["samples"] == SAMPLES


@pytest.mark.parametrize("reasons", [5, "unobserved-task-in-baseline", {}])
def test_a_reasons_field_that_is_not_a_list_differs_rather_than_raising(
    tmp_path, reasons
):
    """`recovery-complete` payloads are unvalidated, and this reads them."""
    value = record(reasons=reasons)
    write_run(tmp_path, value, receipt_for(value))
    _, item = assessed(tmp_path)
    assert item["verdict"] == USABLE
    assert item["problems"] == [DIFFERS]


@pytest.mark.parametrize(
    "stored, problems",
    [(USABLE, []), (INCONCLUSIVE, [DIFFERS]), ("passed", [DIFFERS])],
)
def test_only_the_recomputed_verdict_matches_the_record(tmp_path, stored, problems):
    value = record(verdict=stored)
    write_run(tmp_path, value, receipt_for(value))
    _, item = assessed(tmp_path)
    assert (item["verdict"], item["stored"], item["problems"]) == (
        USABLE,
        stored,
        problems,
    )


@pytest.mark.parametrize("event", [MEASUREMENT_EVENT, COMPLETE_EVENT])
@pytest.mark.parametrize("name", ["00001.json", "99999.json"])
def test_an_unreadable_record_is_an_incomplete_export(tmp_path, event, name):
    """Whatever that record held is missing from the rebuilt coverage.

    The verdict recomputed without it would be lowered by the download, not by
    the run, so there is none; fetch the evidence again.
    """
    value = record()
    write_run(tmp_path, value, receipt_for(value), events=samples_of(value))
    (tmp_path / RUN / "supervisor" / name).write_text(
        json.dumps(
            {"event": event, "at": "2026-09-22T00:01:00Z", "payload": "baseline"}
        )
    )
    _, item = assessed(tmp_path)
    assert item["problems"] == ["malformed-evidence:1"]
    assert (item["verdict"], item["stored"]) == (None, USABLE)


@pytest.mark.parametrize("lost", [1, ATTEMPTS])
def test_samples_the_record_counted_but_the_export_lacks_leave_no_verdict(
    tmp_path, lost
):
    """Fewer finishing samples would lower or keep the verdict by chance alone."""
    value = record()
    events = samples_of(value)
    finishing = [i for i, (_, p) in enumerate(events) if p["stage"] == "finishing"]
    for index in reversed(finishing[:lost]):
        del events[index]
    write_run(tmp_path, value, receipt_for(value), events=events)
    _, item = assessed(tmp_path)
    assert (item["verdict"], item["problems"]) == (None, [f"missing-samples:{lost}"])


def test_a_reading_the_fold_cannot_place_is_an_incomplete_export_too(tmp_path):
    """An object, but with no window to fold into: lost the same way."""
    value = record()
    events = [*samples_of(value), (MEASUREMENT_EVENT, dict(FULL))]
    write_run(tmp_path, value, receipt_for(value), events=events)
    _, item = assessed(tmp_path)
    assert item["problems"] == ["malformed-evidence:1"]
    assert (item["verdict"], item["stored"]) == (None, USABLE)


def test_a_completed_record_without_a_stored_verdict_differs(tmp_path):
    value = record()
    del value["verdict"]
    write_run(tmp_path, value, receipt_for(value))
    _, item = assessed(tmp_path)
    assert (item["verdict"], item["stored"], item["problems"]) == (
        USABLE,
        None,
        [DIFFERS],
    )


def test_the_later_of_two_completion_records_is_the_one_read(tmp_path):
    first = record(verdict=INCONCLUSIVE, reasons=["recovery-incomplete"])
    second = record()
    write_run(
        tmp_path,
        None,
        receipt_for(second),
        events=[(COMPLETE_EVENT, first), (COMPLETE_EVENT, second), *samples_of(second)],
    )
    _, item = assessed(tmp_path)
    assert (item["verdict"], item["stored"], item["problems"]) == (USABLE, USABLE, [])


def test_every_bigquery_run_is_assessed_in_the_order_the_analyzer_found_them(tmp_path):
    good = record()
    write_run(tmp_path, good, receipt_for(good), run_id="bq-0001")
    write_run(tmp_path, good, None, run_id="bq-0002")
    report, _ = assessed(tmp_path, expected=2)
    assert [item["run_id"] for item in report["bigquery"]] == ["bq-0001", "bq-0002"]
    assert [item["verdict"] for item in report["bigquery"]] == [USABLE, None]
    assert counts(report["bigquery"]) == "unreadable 1, usable 1"
    assert "bigquery runs: unreadable 1, usable 1" in analyze.summary_line(
        report, tmp_path
    )


def test_the_rendered_section_names_each_column_of_each_run(tmp_path):
    good = record()
    write_run(tmp_path, good, receipt_for(good), run_id="bq-0001")
    write_run(tmp_path, good, None, run_id="bq-0002")
    report, _ = assessed(tmp_path, expected=2)
    section = render(report["bigquery"])
    assert "| run | verdict | stored | samples | reasons | problems |" in section
    assert f"| bq-0001 | usable | usable | {SAMPLES} | - | - |" in section
    assert f"| bq-0002 | - | - | {SAMPLES} | - | missing-result.json |" in section
    assert "`baseline` and `finishing` windows" in section
    assert f"not `{COMPLETE_STAGE}`" in section


def test_a_directory_without_a_bigquery_run_reports_no_section(tmp_path):
    value = record()
    run_dir = write_run(tmp_path, value, receipt_for(value))
    (run_dir / "approval.json").write_text(
        json.dumps({"run_id": RUN, "scenario": "smoke"})
    )
    report = analyze.analyze(tmp_path)
    assert report["bigquery"] == []
    assert render([]) == ""
    assert counts([]) == ""
    rendered = analyze.render_markdown(report)
    assert "Deployed BigQuery runs" not in rendered
    assert "bigquery runs" not in analyze.summary_line(report, tmp_path)


def test_a_run_that_did_not_complete_still_times_what_it_proved(tmp_path):
    """Without a completion record, the receipt's record is the one timed."""
    proof = {
        "restored": {
            "id": 3,
            "is_savepoint": True,
            "restore_timestamp": 1_000_000_050_000,
        },
        "checkpoint": {"id": 4, "latest_ack_timestamp": 1_000_000_090_000},
    }
    value = record(
        stage="failover", outcomes={"upgrade": proof}, coverage={}, verdict=None
    )
    start = ("recovery-upgrade", {"stage": "upgrade", "at": "2001-09-09T01:46:40Z"})
    write_run(tmp_path, None, receipt_for(value), events=[start])
    _, item = assessed(tmp_path)
    assert item["verdict"] == INCONCLUSIVE
    assert item["recoveries"] == {
        "upgrade": {
            "fault": "savepoint-upgrade",
            "began_at": "2001-09-09T01:46:40Z",
            "restored_from": "savepoint",
            "restored_id": 3,
            "restored_ms": 50_000,
            "checkpoint_id": 4,
            "checkpoint_ms": 90_000,
        }
    }
