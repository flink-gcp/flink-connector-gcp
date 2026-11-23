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
from flink_tier3.bigquery.verdict import COMPLETE_EVENT, COMPLETE_STAGE
from flink_tier3.common import INCONCLUSIVE, USABLE
from flink_tier3.recovery_analysis import (
    SINGLE_FAULT,
    SINGLE_FAULT_NOTE,
    counts,
)

# The faults the single-fault trials' receipts named, written out so that
# dropping one from the analyzer's list fails a test here rather than
# shrinking it. A JobManager replacement named none.
ARCHIVED_FAULTS = ("tm-replacement", "rescale-out", "rescale-in")
RUN = "bq-1423-0001"
SEQUENCE_TRIAL = {
    "mode": "ALO",
    "destinations": 10,
    "fault": "sequence",
    "records": 5760,
}
FAULTS = ["upgrade", "jobmanager", "taskmanager", "rescale-in", "rescale-out"]


def record(report_verdict="pass", **overrides):
    """A fault-sequence run's record: its faults and its query."""
    value = {
        "stage": COMPLETE_STAGE,
        "at": "2026-09-22T00:00:00Z",
        "outcomes": {
            "faults": [{"kind": kind, "at": "2026-09-22T00:00:00Z"} for kind in FAULTS],
            "query": {"report": {"verdict": report_verdict}},
        },
        "verdict": USABLE if report_verdict == "pass" else INCONCLUSIVE,
        "reasons": [] if report_verdict == "pass" else ["oracle-not-passed"],
    }
    value.update(overrides)
    return value


def write_run(root, recovery=None, receipt=None, events=None, run_id=RUN):
    """One exported BigQuery run directory, as a download of `runs/<id>/`."""
    run_dir = root / run_id
    (run_dir / "supervisor").mkdir(parents=True, exist_ok=True)
    (run_dir / "approval.json").write_text(
        json.dumps({"run_id": run_id, "scenario": SCENARIO, "flink_version": "2.2.1"})
    )
    entries = list(events or ())
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
        "bigquery_trial": SEQUENCE_TRIAL,
    }
    value.update(overrides)
    return value


def assessed(root, expected=1):
    report = analyze.analyze(root)
    assert len(report["bigquery"]) == expected
    return report, report["bigquery"][0]


DIFFERS = "verdict-differs-from-the-record"


@pytest.mark.parametrize(
    "report_verdict,verdict,reasons",
    [("pass", USABLE, []), ("fail", INCONCLUSIVE, ["oracle-not-passed"])],
)
def test_a_sequence_run_is_decided_by_its_query_and_lists_its_faults(
    tmp_path, report_verdict, verdict, reasons
):
    """The query's report decides, and the faults are listed beside it."""
    value = record(report_verdict)
    write_run(tmp_path, value, receipt_for(value))
    report, item = assessed(tmp_path)
    assert item == {
        "run_id": RUN,
        "verdict": verdict,
        "stored": verdict,
        "reasons": reasons,
        "problems": [],
        "faults": value["outcomes"]["faults"],
    }
    section = render(report["bigquery"])
    assert f"| {RUN} | {verdict} | {verdict} | {', '.join(FAULTS)} |" in section
    assert "A `sequence` run (ADR-0204) is judged by its query oracle alone" in (
        section
    )
    assert "Deployed BigQuery runs" in analyze.render_markdown(report)


def test_a_sequence_run_stopped_before_its_query_is_incomplete(tmp_path):
    """A record the run left mid-sequence is judged by its own stage."""
    value = record(stage="taskmanager", verdict=None, reasons=[])
    del value["outcomes"]["query"]
    write_run(tmp_path, value, receipt_for(value))
    _, item = assessed(tmp_path)
    assert item["verdict"] == INCONCLUSIVE
    assert item["reasons"] == ["recovery-incomplete", "oracle-not-passed"]
    assert item["stored"] is None


@pytest.mark.parametrize(
    "trial",
    [
        # Archived single-fault trials, which no plan offers any more: the
        # first ones named no fault at all, and the very first carried the
        # version-1 schema's other fields too.
        {"mode": "ALO", "destinations": 10},
        {
            "version": 1,
            "mode": "EO",
            "destinations": 10,
            "repetition": 1,
            "query_slots": 12,
            "maximum_bytes_billed": 4294967296,
            "query_timeout_ms": 60000,
            "additional_cost_usd": "0",
        },
        *(
            {"mode": "EO", "destinations": 10, "fault": kind}
            for kind in ARCHIVED_FAULTS
        ),
    ],
)
def test_a_single_fault_run_has_no_verdict_here(tmp_path, trial):
    """It is judged at the revision that produced it, not by the sequence's rule."""
    value = record(verdict=USABLE)
    write_run(tmp_path, value, receipt_for(value, bigquery_trial=trial))
    report, item = assessed(tmp_path)
    assert (item["verdict"], item["stored"], item["problems"]) == (
        None,
        USABLE,
        [SINGLE_FAULT],
    )
    section = render(report["bigquery"])
    assert f"| {RUN} | - | usable | {', '.join(FAULTS)} | - | {SINGLE_FAULT} |" in (
        section
    )
    assert SINGLE_FAULT_NOTE in section


@pytest.mark.parametrize(
    "trial",
    [
        None,
        "sequence",
        {"mode": "ALO", "destinations": 10, "fault": None},
        {"mode": "ALO", "destinations": 10, "fault": "sequnce"},
        {"mode": "ALO", "destinations": 10, "fault": ""},
        {"mode": "ALO", "destinations": 10, "fault": ["tm-replacement"]},
        # A JobManager replacement named no fault, so a receipt naming it was
        # never written.
        {"mode": "EO", "destinations": 10, "fault": "jm-replacement"},
        # A trial without its delivery method or destination count is no
        # trial an earlier revision wrote, with or without a fault.
        {},
        {"destinations": 10},
        {"mode": "ALO", "fault": "tm-replacement"},
        # A sequence trial that lost its fault still names its record count,
        # which no single-fault trial named.
        {"mode": "ALO", "destinations": 10, "records": 5760},
        {"mode": "STORAGE_WRITE", "destinations": 10},
    ],
)
def test_a_receipt_without_a_readable_trial_is_not_a_single_fault_run(tmp_path, trial):
    """Only an archived trial's own shape is one; anything else is malformed."""
    value = record()
    receipt = receipt_for(value, bigquery_trial=trial)
    if trial is None:
        del receipt["bigquery_trial"]
    write_run(tmp_path, value, receipt)
    _, item = assessed(tmp_path)
    assert (item["verdict"], item["stored"], item["problems"]) == (
        None,
        None,
        ["receipt-without-bigquery-trial"],
    )


def test_a_single_fault_run_without_a_readable_record_stores_nothing(tmp_path):
    receipt = receipt_for(record(), bigquery_trial={"mode": "ALO", "destinations": 10})
    receipt["recovery"] = "malformed"
    write_run(tmp_path, None, receipt, events=[])
    _, item = assessed(tmp_path)
    assert (item["verdict"], item["stored"], item["problems"]) == (
        None,
        None,
        [SINGLE_FAULT],
    )


def test_a_completed_record_without_its_completion_evidence_has_no_verdict(tmp_path):
    """An absent object: only the record it should have accompanied names it."""
    value = record()
    write_run(tmp_path, None, receipt_for(value), events=[])
    _, item = assessed(tmp_path)
    assert item["problems"] == ["missing-" + COMPLETE_EVENT]
    assert item["verdict"] is None
    assert item["stored"] == USABLE


def test_a_run_that_never_completed_is_inconclusive_not_an_evidence_problem(tmp_path):
    """The archetypal case: the job never finished, so no query ran."""
    aborted = record(stage="rescale-out")
    del aborted["verdict"], aborted["reasons"], aborted["outcomes"]["query"]
    write_run(tmp_path, None, receipt_for(aborted), events=[])
    _, item = assessed(tmp_path)
    assert item["verdict"] == INCONCLUSIVE
    assert item["stored"] is None
    assert item["reasons"] == ["recovery-incomplete", "oracle-not-passed"]
    assert item["problems"] == []


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
        events=[],
    )
    _, item = assessed(tmp_path)
    assert item["verdict"] is None
    assert item["problems"] == [problem]


@pytest.mark.parametrize("reasons", [5, "oracle-not-passed", {}])
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


@pytest.mark.parametrize("name", ["00001.json", "99999.json"])
def test_an_unreadable_record_is_an_incomplete_export(tmp_path, name):
    """A record nothing can read is a download to fetch again, not a verdict."""
    value = record()
    write_run(tmp_path, value, receipt_for(value))
    (tmp_path / RUN / "supervisor" / name).write_text(
        json.dumps(
            {
                "event": COMPLETE_EVENT,
                "at": "2026-09-22T00:01:00Z",
                "payload": "baseline",
            }
        )
    )
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
        events=[(COMPLETE_EVENT, first), (COMPLETE_EVENT, second)],
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
    assert "| run | verdict | stored | faults | reasons | problems |" in section
    faults = ", ".join(FAULTS)
    assert f"| bq-0001 | usable | usable | {faults} | - | - |" in section
    assert f"| bq-0002 | - | - | {faults} | - | missing-result.json |" in section
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
