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
"""What the shared reading of a record's oracle reports says, component by component."""

import pytest
from flink_tier3.verdict import component_reasons

COMPONENTS = ("sink", "relay")
# The relay's counts of a failure that timing cannot explain.
DEFINITIVE = ("foreign",)
PASSED = {"verdict": "pass", "judged": True}


def reasons(**oracles):
    record = {"outcomes": {"oracles": oracles}}
    return component_reasons(record, COMPONENTS, "relay", DEFINITIVE)


@pytest.mark.parametrize(
    "sink",
    [None, {}, {"verdict": None}, {"verdict": "fail"}, {"verdict": "PASS"}, "pass"],
    ids=["missing", "empty", "no-verdict", "failed", "other-spelling", "not-an-object"],
)
def test_only_a_passing_report_passes(sink):
    """A report that names no verdict is not one that passed."""
    oracles = {"relay": PASSED} if sink is None else {"sink": sink, "relay": PASSED}
    assert reasons(**oracles) == ["sink-oracle-not-passed"]


def test_every_component_passed_gives_no_reason():
    assert reasons(sink={"verdict": "pass"}, relay=PASSED) == []


@pytest.mark.parametrize(
    "relay,reason",
    [
        ({"verdict": "pass"}, "relay-oracle-unjudged"),
        ({"verdict": "pass", "judged": False}, "relay-oracle-unjudged"),
        ({"verdict": "pass", "judged": "true"}, "relay-oracle-unjudged"),
        ({"verdict": "pass", "judged": 1}, "relay-oracle-unjudged"),
        ({"verdict": "fail", "judged": False}, "relay-oracle-unjudged"),
        ({"verdict": "pass", "foreign": 1}, "relay-oracle-not-passed"),
        ({"verdict": "pass", "foreign": "0"}, "relay-oracle-unjudged"),
        ({"verdict": "pass", "foreign": True}, "relay-oracle-unjudged"),
        ({"verdict": "fail", "judged": True}, "relay-oracle-not-passed"),
        ({}, "relay-oracle-unjudged"),
    ],
    ids=[
        "judged-missing",
        "unjudged",
        "judged-a-string",
        "judged-a-number",
        "unjudged-and-failed",
        "unjudged-with-a-definitive-failure",
        "unjudged-with-a-count-that-is-a-string",
        "unjudged-with-a-count-that-is-a-boolean",
        "judged-and-failed",
        "missing",
    ],
)
def test_a_relay_is_decided_by_whether_it_was_judged_before_its_verdict(relay, reason):
    """Only a report that says it was judged is read for its verdict."""
    assert reasons(sink={"verdict": "pass"}, relay=relay) == [reason]


def test_a_component_without_a_relay_is_read_for_its_verdict_alone():
    record = {"outcomes": {"oracles": {"sink": {"verdict": "pass"}}}}
    assert component_reasons(record, ("sink",)) == []
