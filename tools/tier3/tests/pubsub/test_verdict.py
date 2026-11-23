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
"""What the Pub/Sub recovery verdict says, and what it refuses to say."""

import pytest
from flink_tier3.common import INCONCLUSIVE, USABLE
from flink_tier3.pubsub.verdict import application_pods, data_reasons
from flink_tier3.verdict import COMPLETE_STAGE, sequence_verdict

ORACLE = {
    "lines": 64,
    "rejected": None,
    "missing_inputs": 0,
    "logical_inputs": 60,
    "input_publication_duplicates": 0,
    "repeated_input_processing": 4,
    "output_publication_duplicates": 0,
    "repeated_output_delivery": 0,
}


def test_the_sequence_holds_the_jobmanager_two_taskmanagers_and_a_replacement():
    assert application_pods("sequence") == 4


@pytest.mark.parametrize("kind", ["jm-replacement", "rescale-out", None])
def test_no_other_kind_is_sized(kind):
    """Every trial runs the sequence (ADR-0204), so admission sizes no other."""
    with pytest.raises(ValueError, match="Unknown Pub/Sub trial kind"):
        application_pods(kind)


def sequence_record(**overrides):
    record = {
        "stage": COMPLETE_STAGE,
        "outcomes": {"faults": [{"kind": "upgrade"}]},
        "oracle": dict(ORACLE),
        "inputs": {"unpublished": 0, "unreceipted": 0},
    }
    record.update(overrides)
    return record


def test_a_complete_sequence_with_its_output_is_usable():
    assert sequence_verdict(sequence_record(), data_reasons) == {
        "verdict": USABLE,
        "reasons": [],
    }


def test_a_sequence_needs_no_recovery_outcome_or_replay():
    # Recovery outcomes and duplicates decide nothing (ADR-0204).
    record = sequence_record(outcomes={})
    record["oracle"]["repeated_input_processing"] = 900
    assert sequence_verdict(record, data_reasons)["verdict"] == USABLE


@pytest.mark.parametrize(
    "change, reasons",
    [
        ({"stage": "upgrade"}, ["recovery-incomplete"]),
        ({"oracle": None}, ["oracle-missing"]),
        ({"oracle": {"rejected": "Invalid output observation"}}, ["oracle-rejected"]),
        # An absent refusal is not an acceptance.
        (
            {"oracle": {k: v for k, v in ORACLE.items() if k != "rejected"}},
            ["oracle-rejected"],
        ),
        ({"oracle": {**ORACLE, "missing_inputs": 3}}, ["oracle-incomplete"]),
        ({"oracle": {**ORACLE, "missing_inputs": False}}, ["oracle-incomplete"]),
        ({"inputs": None}, ["inputs-missing"]),
        ({"inputs": {"unpublished": 1, "unreceipted": 0}}, ["input-ids-unpublished"]),
        ({"inputs": {"unpublished": 0, "unreceipted": 2}}, ["input-receipts-missing"]),
        # `False == 0` in Python, so a count is compared with its type.
        (
            {"inputs": {"unpublished": False, "unreceipted": None}},
            ["input-ids-unpublished", "input-receipts-missing"],
        ),
    ],
)
def test_each_sequence_shortfall_is_inconclusive_with_its_reason(change, reasons):
    assert sequence_verdict(sequence_record(**change), data_reasons) == {
        "verdict": INCONCLUSIVE,
        "reasons": reasons,
    }


@pytest.mark.parametrize("value", [None, [], "complete", 7])
def test_an_untyped_sequence_record_is_inconclusive_without_raising(value):
    assert sequence_verdict(value, data_reasons)["verdict"] == INCONCLUSIVE
