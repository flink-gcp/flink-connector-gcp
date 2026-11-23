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

import base64
import json

import pytest
from flink_tier3 import analyze as shared
from flink_tier3.cloudtasks import analyze
from flink_tier3.cloudtasks.oracle import assess, page
from flink_tier3.cloudtasks.trial import Trial
from flink_tier3.cloudtasks.verdict import PAGE_EVENT, data_reasons
from flink_tier3.recovery_analysis import (
    INCOMPLETE_NOTE,
    SINGLE_FAULT,
    SINGLE_FAULT_NOTE,
)
from flink_tier3.runs import discover_runs
from flink_tier3.verdict import COMPLETE_EVENT, sequence_verdict

# The kinds the single-fault trials named, written out so that dropping one
# from the analyzer's list fails a test here rather than shrinking it.
ARCHIVED_KINDS = ("jm-replacement", "tm-replacement", "rescale-out", "rescale-in")
TRIAL = Trial("ct-analyze", 4, 1, "sequence")
PREFIX = TRIAL.queue_path + "/tasks/"


def tasks(sequences, bodies=None):
    """Listed tasks; `bodies` maps a sequence to the record its body carries."""
    bodies = bodies or {}
    return [
        {
            "name": PREFIX + TRIAL.task_id(n),
            "dispatchCount": 0,
            "httpRequest": {
                "body": base64.b64encode(
                    f"{TRIAL.run_id}/{bodies.get(n, n)}".encode()
                ).decode()
            },
        }
        for n in sequences
    ]


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
    faults=None,
    kind="sequence",
):
    """A completed run whose record names `attempt`, exporting `pages`.

    Its approved trial is of `kind`, none when it is None, and its record
    lists `faults` when it names any.
    """
    directory = tmp_path / TRIAL.run_id
    write(
        directory,
        "approval.json",
        {
            "run_id": TRIAL.run_id,
            "scenario": "cloudtasks-recovery",
            "cloudtasks_trial": {
                "records": 4,
                "records_per_second": 1,
                **({"kind": kind} if kind is not None else {}),
            },
        },
    )
    items = [page(TRIAL, listed) for listed in (stored_pages or pages)]
    listing = {
        "attempt": attempt,
        "pages": len(items),
        "queue_state": "PAUSED",
        "report": assess(TRIAL, items),
    }
    outcomes = {"listing": listing}
    if faults is not None:
        outcomes["faults"] = faults
    record = {"stage": "complete", "outcomes": outcomes}
    record.update(sequence_verdict(record, data_reasons))
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


def test_pages_that_contradict_the_stored_report_are_a_problem(tmp_path):
    directory = run_directory(
        tmp_path,
        [tasks([0, 1]), tasks([2, 2])],
        stored_pages=[tasks([0, 1]), tasks([2, 3])],
        faults=FAULTS,
    )
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] == "inconclusive"
    assert result["reasons"] == ["oracle-not-passed"]
    assert "report-differs-from-pages" in result["problems"]
    assert "verdict-differs-from-the-record" in result["problems"]


def test_a_missing_page_leaves_the_run_without_a_verdict(tmp_path):
    directory = run_directory(tmp_path, [tasks([0, 1]), tasks([2, 3])], faults=FAULTS)
    (directory / "supervisor/page-2-1.json").unlink()
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] is None
    assert result["problems"] == ["missing-pages:1"]


def test_the_shared_report_carries_the_section(tmp_path):
    run_directory(tmp_path, [tasks([0, 1, 2, 3])], faults=FAULTS)
    report = shared.analyze(tmp_path)
    assert [item["verdict"] for item in report["cloudtasks"]] == ["usable"]
    assert "## Deployed Cloud Tasks runs" in shared.render_markdown(report)
    assert "cloudtasks runs: usable 1" in shared.summary_line(report, tmp_path)


def test_unreadable_evidence_leaves_the_run_without_a_verdict(tmp_path):
    directory = run_directory(tmp_path, [tasks([0, 1]), tasks([2, 3])], faults=FAULTS)
    page = json.loads((directory / "supervisor/page-2-1.json").read_text())
    page["payload"]["ids"] = "not a list"
    (directory / "supervisor/page-2-1.json").write_text(json.dumps(page))
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] is None
    assert result["problems"] == ["malformed-pages:1"]

    approval = json.loads((directory / "approval.json").read_text())
    del approval["cloudtasks_trial"]
    (directory / "approval.json").write_text(json.dumps(approval))
    [run] = discover_runs(directory)
    assert analyze.assess(run)["problems"] == ["approval-without-cloudtasks-trial"]


def test_a_malformed_completed_record_leaves_the_run_without_a_verdict(tmp_path):
    """An unreadable record is an export to fetch again; no stored verdict shows."""
    directory = run_directory(tmp_path, [tasks([0, 1, 2, 3])], faults=FAULTS)
    write(
        directory,
        "supervisor/complete-2.json",
        {"event": COMPLETE_EVENT, "at": "2026-10-13T00:02:00Z", "payload": "lost"},
    )
    [run] = discover_runs(directory)
    item = analyze.assess(run)
    assert (item["verdict"], item["stored"], item["problems"]) == (
        None,
        None,
        ["malformed-evidence:1"],
    )
    assert item["report"] is None
    [row] = [
        line for line in analyze.render([item]).splitlines() if TRIAL.run_id in line
    ]
    assert row.startswith(f"| {TRIAL.run_id} | - | - |")
    assert row.endswith("| malformed-evidence:1 |")


def test_a_page_beyond_the_recorded_count_is_reported(tmp_path):
    directory = run_directory(tmp_path, [tasks([0, 1]), tasks([2, 3])], faults=FAULTS)
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


FAULTS = [
    {"kind": "upgrade", "at": "2026-10-13T00:00:10Z"},
    {"kind": "jobmanager", "at": "2026-10-13T00:00:20Z"},
]


def test_a_sequence_run_is_assessed_by_its_listing_and_lists_its_faults(tmp_path):
    """A fault-sequence run (ADR-0204) is decided by its listing alone."""
    directory = run_directory(tmp_path, [tasks([0, 1]), tasks([2, 3])], faults=FAULTS)
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert (result["verdict"], result["stored"]) == ("usable", "usable")
    assert result["problems"] == []
    assert result["faults"] == FAULTS
    [row] = [
        line for line in analyze.render([result]).splitlines() if "ct-analyze" in line
    ]
    assert "| upgrade, jobmanager |" in row


def test_a_sequence_runs_duplicate_task_is_recomputed_from_its_pages(tmp_path):
    """The exactly-once rule: a task listed twice withholds `usable`."""
    directory = run_directory(
        tmp_path,
        [tasks([0, 1]), tasks([2, 3, 3])],
        stored_pages=[tasks([0, 1]), tasks([2, 3])],
        faults=FAULTS,
    )
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] == "inconclusive"
    assert result["reasons"] == ["oracle-not-passed"]
    assert result["report"]["duplicates"] == 1
    assert "report-differs-from-pages" in result["problems"]


def test_a_stopped_sequence_run_is_inconclusive_by_its_own_reasons(tmp_path):
    """A sequence run stopped before its listing names no single-fault reason."""
    directory = run_directory(tmp_path, [tasks([0, 1])], faults=FAULTS, complete=False)
    record = {"stage": "faults", "outcomes": {"faults": FAULTS}}
    record.update(sequence_verdict(record, data_reasons))
    write(
        directory, "result.json", {"success": False, "idle": True, "recovery": record}
    )
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] == "inconclusive"
    assert result["reasons"] == [
        "recovery-incomplete",
        "queue-not-paused",
        "oracle-not-passed",
    ]
    assert result["problems"] == []


def test_the_report_table_keeps_its_columns_for_both_kinds(tmp_path):
    """Header, separator and rows agree; a single-fault run lists no faults."""
    sequence = run_directory(
        tmp_path / "s", [tasks([0, 1]), tasks([2, 3])], faults=FAULTS
    )
    single = run_directory(
        tmp_path / "f", [tasks([0, 1]), tasks([2, 3])], kind="jm-replacement"
    )
    results = [
        analyze.assess(run) for d in (sequence, single) for run in discover_runs(d)
    ]
    lines = analyze.render(results).splitlines()
    table = [line for line in lines if line.startswith("| ")]
    header, separator, *rows = table[:4]
    assert header.count("|") == separator.count("|") == rows[0].count("|")
    assert rows[1].count("|") == header.count("|")
    faults = [cell.strip() for cell in header.split("|")].index("faults")
    assert [row.split("|")[faults].strip() for row in rows] == [
        "upgrade, jobmanager",
        "-",
    ]
    assert any("judged by its data alone" in line for line in lines)
    assert rows[1].split("|")[-2].strip() == SINGLE_FAULT


@pytest.mark.parametrize("kind", ARCHIVED_KINDS)
@pytest.mark.parametrize("complete", [True, False])
def test_a_single_fault_run_has_no_verdict_here(tmp_path, complete, kind):
    """Its approval names a kind the trial refuses; its record is still read."""
    directory = run_directory(
        tmp_path, [tasks([0, 1, 2, 3])], kind=kind, complete=complete
    )
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert (result["verdict"], result["stored"], result["reasons"]) == (
        None,
        "usable",
        [],
    )
    assert result["problems"] == [SINGLE_FAULT]
    assert result["report"] is None
    rendered = analyze.render([result])
    assert SINGLE_FAULT_NOTE in rendered
    assert INCOMPLETE_NOTE in rendered


@pytest.mark.parametrize("kind", [None, 3, [], "", "sequnce", "reactive-scale-out"])
def test_an_approval_without_a_known_kind_is_no_trial(tmp_path, kind):
    """A trial naming no kind, as the first runs' did, or one no trial had.

    Either is a malformed trial, not a single fault.
    """
    directory = run_directory(tmp_path, [tasks([0, 1, 2, 3])], kind=kind)
    [run] = discover_runs(directory)
    assert analyze.assess(run)["problems"] == ["approval-without-cloudtasks-trial"]


def test_a_completed_record_without_a_listing_has_no_verdict(tmp_path):
    """A completed run is decided by its listing, which this export lacks."""
    directory = run_directory(tmp_path, [tasks([0, 1])], faults=FAULTS)
    record = {"stage": "complete", "outcomes": {"faults": FAULTS}}
    record.update(sequence_verdict(record, data_reasons))
    write(
        directory, "result.json", {"success": False, "idle": True, "recovery": record}
    )
    write(
        directory,
        "supervisor/complete.json",
        {"event": COMPLETE_EVENT, "at": "2026-10-13T00:01:00Z", "payload": record},
    )
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert (result["verdict"], result["reasons"]) == (None, [])
    assert result["problems"] == ["missing-listing-record"]


def test_a_task_carrying_another_records_body_is_recomputed_as_failing(tmp_path):
    """The analyzer re-counts the wrong bodies the exported pages name."""
    directory = run_directory(
        tmp_path, [tasks([0, 1]), tasks([2, 3], bodies={3: 2})], faults=FAULTS
    )
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert (result["verdict"], result["reasons"]) == (
        "inconclusive",
        ["oracle-not-passed"],
    )
    assert result["report"]["mismatched"] == 1
    assert result["problems"] == []


def test_a_page_exported_without_its_mismatched_ids_has_no_verdict(tmp_path):
    """A page that omits the body check cannot be read as having passed it."""
    directory = run_directory(tmp_path, [tasks([0, 1]), tasks([2, 3])], faults=FAULTS)
    path = directory / "supervisor/page-2-1.json"
    event = json.loads(path.read_text())
    del event["payload"]["mismatched"]
    path.write_text(json.dumps(event))
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] is None
    assert result["problems"] == ["malformed-pages:1"]


@pytest.mark.parametrize(
    "mismatched",
    [
        # An id from another page.
        lambda ids: [TRIAL.task_id(0)],
        # Named more often than the page lists it.
        lambda ids: [ids[0], ids[0]],
    ],
)
def test_a_wrong_body_named_for_a_task_off_its_page_has_no_verdict(
    tmp_path, mismatched
):
    """The supervisor names a wrong body only for a task its page lists."""
    directory = run_directory(tmp_path, [tasks([0, 1]), tasks([2, 3])], faults=FAULTS)
    path = directory / "supervisor/page-2-1.json"
    event = json.loads(path.read_text())
    event["payload"]["mismatched"] = mismatched(event["payload"]["ids"])
    path.write_text(json.dumps(event))
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] is None
    assert result["problems"] == ["malformed-pages:1"]


@pytest.mark.parametrize("count", [0, -1])
def test_a_listing_record_of_no_pages_has_no_verdict(tmp_path, count):
    """Every listing exports at least one page, so none is a damaged record."""
    # No pages exported, and the record says so.
    directory = run_directory(tmp_path, [], faults=FAULTS)
    assert not list(directory.glob("supervisor/page-2-*.json"))
    for name in ("result.json", "supervisor/complete.json"):
        path = directory / name
        value = json.loads(path.read_text())
        record = value["recovery"] if name == "result.json" else value["payload"]
        record["outcomes"]["listing"]["pages"] = count
        path.write_text(json.dumps(value))
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["verdict"] is None
    assert result["problems"] == ["malformed-listing-record"]


def test_a_task_listed_twice_with_both_bodies_wrong_is_a_readable_page(tmp_path):
    """The supervisor names a task once per listing of it, so twice is valid."""
    directory = run_directory(
        tmp_path, [tasks([0, 1]), tasks([2, 3, 3], bodies={3: 2})], faults=FAULTS
    )
    [run] = discover_runs(directory)
    result = analyze.assess(run)
    assert result["problems"] == []
    assert (result["report"]["duplicates"], result["report"]["mismatched"]) == (1, 2)
