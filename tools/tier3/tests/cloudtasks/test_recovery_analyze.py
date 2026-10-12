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

from flink_tier3 import analyze as shared
from flink_tier3.cloudtasks import analyze
from flink_tier3.cloudtasks.oracle import assess, page
from flink_tier3.cloudtasks.trial import Trial
from flink_tier3.cloudtasks.verdict import COMPLETE_EVENT, PAGE_EVENT, verdict
from flink_tier3.runs import discover_runs

TRIAL = Trial("ct-analyze", 4, 1)
PREFIX = TRIAL.queue_path + "/tasks/"


def tasks(sequences):
    return [{"name": PREFIX + TRIAL.task_id(n), "dispatchCount": 0} for n in sequences]


def write(directory, name, value):
    path = directory / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def run_directory(tmp_path, pages, *, stored_pages=None, attempt=2, complete=True):
    """A completed run whose record names `attempt`, exporting `pages`."""
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
    record = {"stage": "complete", "outcomes": outcomes}
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
    assert "samples" not in result

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
