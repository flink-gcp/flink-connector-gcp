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
"""The verdict, and the analyzer's account of a run from its record."""

import json

import pytest
from flink_tier3 import runs
from flink_tier3.common import INCONCLUSIVE, USABLE
from flink_tier3.recovery_analysis import (
    SINGLE_FAULT,
    SINGLE_FAULT_NOTE,
)
from flink_tier3.spanner import analyze, plan
from flink_tier3.spanner.verdict import sequence_verdict

from ..test_analyze import write_run

# The kinds the single-fault trials named, written out so that dropping one
# from the analyzer's list fails a test here rather than shrinking it.
ARCHIVED_KINDS = ("jm-replacement", "tm-replacement", "rescale-out", "rescale-in")
FAULTS = [{"kind": "upgrade", "at": "2026-11-16T02:00:00Z"}]
RECORD = {
    "stage": "complete",
    "verdict": "usable",
    "reasons": [],
    "outcomes": {
        "oracles": {
            "sink": {"verdict": "pass"},
            "change_stream": {"verdict": "pass", "judged": True},
            "batch": {"verdict": "pass"},
        },
        "faults": FAULTS,
    },
}


def assessed(tmp_path, **trial):
    """The analyzer's account of one run that wrote `RECORD`, under `trial`."""
    run_dir = write_run(
        tmp_path,
        run_id="sp-s1",
        scenario="spanner-recovery",
        events=[
            {
                "event": "recovery-complete",
                "at": "2026-11-16T02:30:00Z",
                "payload": RECORD,
            }
        ],
    )
    approval = json.loads((run_dir / "approval.json").read_text())
    approval["spanner_trial"] = {
        **plan.trial("sequence", "2026-11-16T01:50:00Z"),
        **trial,
    }
    (run_dir / "approval.json").write_text(json.dumps(approval))
    (item,) = analyze.assess_runs([runs.Run(run_dir)])
    return item


def test_a_sequence_run_is_assessed_by_its_oracles_and_lists_its_faults(tmp_path):
    """A fault-sequence run (ADR-0204) is decided by its oracles alone."""
    item = assessed(tmp_path)
    assert item["verdict"] == USABLE
    assert item["problems"] == []
    assert item["faults"] == FAULTS
    rendered = analyze.render([item])
    [row] = [line for line in rendered.splitlines() if "sp-s1" in line]
    assert "| upgrade |" in row
    assert "watermark advanced" not in rendered


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
        ["approval-without-spanner-trial"],
    )


PASSED = {
    "sink": {"verdict": "pass"},
    "change_stream": {"verdict": "pass", "judged": True},
    "batch": {"verdict": "pass"},
}


@pytest.mark.parametrize(
    ("stage", "component", "report", "reason"),
    [
        ("tables", None, None, "recovery-incomplete"),
        ("complete", "sink", {"verdict": "fail"}, "sink-oracle-not-passed"),
        ("complete", "batch", {"verdict": "fail"}, "batch-oracle-not-passed"),
        (
            "complete",
            "change_stream",
            {"verdict": "fail", "judged": True},
            "change_stream-oracle-not-passed",
        ),
        (
            "complete",
            "change_stream",
            {"verdict": "pass", "judged": False},
            "change_stream-oracle-unjudged",
        ),
    ],
)
def test_a_sequence_run_is_usable_only_complete_with_every_oracle_passed(
    stage, component, report, reason
):
    """Lost data in any one table, or a relay not judged, withholds `usable`."""
    oracles = {**PASSED, **({component: report} if component else {})}
    decided = sequence_verdict({"stage": stage, "outcomes": {"oracles": oracles}})
    assert decided == {"verdict": INCONCLUSIVE, "reasons": [reason]}
    passed = sequence_verdict({"stage": "complete", "outcomes": {"oracles": PASSED}})
    assert passed == {"verdict": USABLE, "reasons": []}
