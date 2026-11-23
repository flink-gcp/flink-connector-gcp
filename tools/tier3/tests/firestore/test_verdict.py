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
"""The verdict, and the analyzer's account of a run from its record and events."""

import json

import pytest
from flink_tier3 import runs
from flink_tier3.common import USABLE
from flink_tier3.firestore import analyze, plan
from flink_tier3.firestore.verdict import COMPONENTS, data_reasons
from flink_tier3.recovery_analysis import (
    INCOMPLETE_NOTE,
    SINGLE_FAULT,
    SINGLE_FAULT_NOTE,
)
from flink_tier3.verdict import COMPLETE_EVENT, sequence_verdict

from ..test_analyze import write_run

# The kinds the single-fault trials named, written out so that dropping one
# from the analyzer's list fails a test here rather than shrinking it.
ARCHIVED_KINDS = ("jm-replacement", "tm-replacement", "rescale-out", "rescale-in")


def test_the_report_has_a_column_for_them():
    item = {
        "run_id": "fs-a1",
        "verdict": "usable",
        "stored": "usable",
        "oracles": None,
        "unplanned": ["Unplanned job restart"],
        "reasons": [],
        "problems": [],
        "faults": [],
    }
    row = next(line for line in analyze.render([item]).splitlines() if "fs-a1" in line)
    assert "| Unplanned job restart | - | - | - |" in row


def test_an_exported_run_reports_the_causes_its_events_name(tmp_path):
    """Read from the events, which a run stopped mid-wait still exported."""

    def waited(cause):
        return {
            "event": "unplanned-recovery",
            "at": "2026-11-11T01:42:00Z",
            "payload": {"stage": "finishing", "cause": cause},
        }

    run_dir = write_run(
        tmp_path,
        run_id="fs-a1",
        scenario="firestore-recovery",
        events=[
            waited("Unplanned workload interruption"),
            waited(None),
            waited(["not", "text"]),
            waited("Unplanned workload interruption"),
        ],
    )
    # The receipt keeps the record as the finishing stage last wrote it.
    result = json.loads((run_dir / "result.json").read_text())
    result["recovery"] = {"stage": "finishing", "outcomes": {}}
    (run_dir / "result.json").write_text(json.dumps(result))
    approval = json.loads((run_dir / "approval.json").read_text())
    approval["firestore_trial"] = plan.trial("sequence")
    (run_dir / "approval.json").write_text(json.dumps(approval))
    (assessed,) = analyze.assess_runs([runs.Run(run_dir)])
    assert assessed["unplanned"] == ["Unplanned workload interruption"]
    assert "| Unplanned workload interruption |" in analyze.render([assessed])


def test_a_completed_record_names_its_causes_without_their_events(tmp_path):
    """An export that lost the event files still has the record's causes."""
    run_dir = write_run(
        tmp_path,
        run_id="fs-a2",
        scenario="firestore-recovery",
        events=[
            {
                "event": "recovery-complete",
                "at": "2026-11-11T02:00:00Z",
                "payload": {
                    "stage": "complete",
                    "outcomes": {"unplanned": ["Unplanned job restart", None]},
                },
            }
        ],
    )
    approval = json.loads((run_dir / "approval.json").read_text())
    approval["firestore_trial"] = plan.trial("sequence")
    (run_dir / "approval.json").write_text(json.dumps(approval))
    (assessed,) = analyze.assess_runs([runs.Run(run_dir)])
    assert assessed["unplanned"] == ["Unplanned job restart"]


PASSED = {"verdict": "pass"}
RECORD = {
    "stage": "complete",
    "verdict": "usable",
    "reasons": [],
    "outcomes": {
        "oracles": {component: PASSED for component in COMPONENTS},
        "faults": [
            {"kind": "upgrade", "at": "2026-11-12T02:00:00Z"},
            {"kind": "jobmanager", "at": "2026-11-12T02:02:00Z"},
        ],
    },
}


def assessed(tmp_path, record=RECORD, extra=(), **trial):
    """The analyzer's account of a run that wrote `record` and `extra`, under `trial`."""
    run_dir = write_run(
        tmp_path,
        run_id="fs-s1",
        scenario="firestore-recovery",
        events=[
            {
                "event": "recovery-complete",
                "at": "2026-11-12T02:20:00Z",
                "payload": record,
            },
            *extra,
        ],
    )
    approval = json.loads((run_dir / "approval.json").read_text())
    approval["firestore_trial"] = {**plan.trial("sequence"), **trial}
    (run_dir / "approval.json").write_text(json.dumps(approval))
    (item,) = analyze.assess_runs([runs.Run(run_dir)])
    return item


def test_a_sequence_run_is_assessed_by_its_oracles_and_lists_its_faults(tmp_path):
    """A fault-sequence run (ADR-0204) is decided by its oracles alone."""
    item = assessed(tmp_path)
    assert item["verdict"] == USABLE
    assert item["problems"] == []
    assert item["faults"] == RECORD["outcomes"]["faults"]
    failing = {**PASSED, "verdict": "fail"}
    assert sequence_verdict(
        {
            **RECORD,
            "outcomes": {
                "oracles": {**RECORD["outcomes"]["oracles"], "native_sink": failing}
            },
        },
        data_reasons,
    )["reasons"] == ["native_sink-oracle-not-passed"]


def test_the_report_lists_a_sequence_runs_faults_and_claims_no_recovery(tmp_path):
    """The owner's ask in #2143: no recoveries or windows a sequence run never had."""
    rendered = analyze.render([assessed(tmp_path)])
    lines = rendered.splitlines()
    header = next(line for line in lines if line.startswith("| run |"))
    [row] = [line for line in lines if "fs-s1" in line]
    faults = [cell.strip() for cell in header.split("|")].index("faults")
    assert row.split("|")[faults].strip() == "upgrade, jobmanager"
    assert header.count("|") == row.count("|")
    assert "both recoveries" not in rendered
    assert "windows, and counts as observed" not in rendered
    assert "judged by its data alone" in rendered


@pytest.mark.parametrize("kind", ARCHIVED_KINDS)
def test_a_single_fault_run_has_no_verdict_here(tmp_path, kind):
    """Its approval names a kind the trial refuses; its stored verdict is kept."""
    item = assessed(tmp_path, kind=kind)
    assert (item["verdict"], item["stored"], item["problems"]) == (
        None,
        USABLE,
        [SINGLE_FAULT],
    )
    rendered = analyze.render([item])
    assert f"| {SINGLE_FAULT} |" in rendered
    assert SINGLE_FAULT_NOTE in rendered
    assert INCOMPLETE_NOTE in rendered


@pytest.mark.parametrize(
    "trial",
    [
        {"records": "many"},
        # A kind no trial had is malformed, not an archived single fault.
        {"kind": None},
        {"kind": 3},
        {"kind": []},
        {"kind": ""},
        {"kind": "sequnce"},
    ],
)
def test_a_trial_the_approval_does_not_hold_readably_is_no_trial(tmp_path, trial):
    item = assessed(tmp_path, **trial)
    assert (item["verdict"], item["problems"]) == (
        None,
        ["approval-without-firestore-trial"],
    )


def test_a_sequence_run_stopped_before_its_verdict_is_not_usable():
    """Four passing reports in a record that never completed decide nothing."""
    record = {
        "stage": "documents",
        "outcomes": {"oracles": {component: PASSED for component in COMPONENTS}},
    }
    assert sequence_verdict(record, data_reasons)["reasons"] == ["recovery-incomplete"]


def test_a_malformed_completed_record_leaves_the_run_without_a_verdict(tmp_path):
    """An unreadable record is an export to fetch again; no stored verdict shows."""
    item = assessed(tmp_path, extra=({"event": COMPLETE_EVENT, "payload": "baseline"},))
    assert (item["verdict"], item["stored"], item["problems"]) == (
        None,
        None,
        ["malformed-evidence:1"],
    )
    [row] = [line for line in analyze.render([item]).splitlines() if "fs-s1" in line]
    assert row.startswith("| fs-s1 | - | - |")
    assert row.endswith("| malformed-evidence:1 |")
