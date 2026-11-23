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
"""What the deployed verdict says, and what it refuses to say."""

import pytest
from flink_tier3.bigquery.verdict import (
    COMPLETE_EVENT,
    COMPLETE_STAGE,
    sequence_verdict,
)
from flink_tier3.common import INCONCLUSIVE, USABLE


def test_the_event_names_are_spelled_once_for_both_sides():
    assert COMPLETE_EVENT == "recovery-" + COMPLETE_STAGE


@pytest.mark.parametrize(
    "record,reasons",
    [
        # Only the data decides.
        (
            {
                "stage": COMPLETE_STAGE,
                "outcomes": {"query": {"report": {"verdict": "pass"}}},
            },
            [],
        ),
        (
            {
                "stage": COMPLETE_STAGE,
                "outcomes": {"query": {"report": {"verdict": "fail"}}},
            },
            ["oracle-not-passed"],
        ),
        (
            {"stage": "visibility", "outcomes": {}},
            ["recovery-incomplete", "oracle-not-passed"],
        ),
        # Untrusted JSON: a field of the wrong type is a reason, never a raise.
        (
            {"stage": COMPLETE_STAGE, "outcomes": {"query": "pass"}},
            ["oracle-not-passed"],
        ),
        ([], ["recovery-incomplete", "oracle-not-passed"]),
    ],
)
def test_a_sequence_verdict_is_its_data_alone(record, reasons):
    assert sequence_verdict(record) == {
        "verdict": INCONCLUSIVE if reasons else USABLE,
        "reasons": reasons,
    }
