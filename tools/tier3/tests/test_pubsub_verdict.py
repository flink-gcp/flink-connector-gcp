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

import copy

import pytest
from flink_tier3.common import INCONCLUSIVE, USABLE
from flink_tier3.pubsub_verdict import (
    MEASUREMENT_EVENT,
    OBSERVED,
    SAMPLED_WINDOWS,
    WINDOWS,
    summarize,
    verdict,
)

# One sample that returned every family: a vertex's backpressure level and one
# returned id each from the source reader and the sink writer.
FULL = {
    "vertices": [
        {
            "id": "relay",
            "backpressure": {"status": "ok", "backpressureLevel": "ok"},
            "metrics": [
                {"id": "Source__Pub_Sub_input.pendingAcks", "sum": 3},
                {"id": "Sink__Writer.inFlightMessages", "sum": 0},
            ],
        }
    ],
    "backlog": {"requested": 20, "observed": 20, "outstanding": 0},
}
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


def covered():
    coverage = {}
    for stage in ("baseline", "after"):
        coverage = summarize(coverage, stage, FULL)
    return coverage


def complete(kind="jm-replacement", **overrides):
    replacement = kind.endswith("replacement")
    record = {
        "stage": "complete",
        "outcomes": {
            "checkpoint": {"id": 3},
            "fault": {"kind": kind},
            "recovery": {
                "replay": "observed" if replacement else "not-expected",
                "expected_replay": 4 if replacement else 0,
                "replayed": 4 if replacement else 0,
                "replay_ids_preserved": True,
            },
            "after": {
                "checkpoint": {"id": 5},
                "replay_by_new_attempts": 4 if replacement else 0,
                "replay_ids_preserved": True,
            },
        },
        "coverage": covered(),
        "oracle": dict(ORACLE),
    }
    record.update(overrides)
    return record


@pytest.mark.parametrize(
    "kind", ["jm-replacement", "tm-replacement", "rescale-out", "rescale-in"]
)
def test_a_complete_trial_with_its_evidence_is_usable(kind):
    assert verdict(complete(kind)) == {"verdict": USABLE, "reasons": []}


def test_duplicate_populations_decide_nothing():
    oracle = {
        **ORACLE,
        **{
            name: 7
            for name in (
                "input_publication_duplicates",
                "repeated_input_processing",
                "output_publication_duplicates",
                "repeated_output_delivery",
            )
        },
    }
    assert verdict(complete(oracle=oracle))["verdict"] == USABLE


@pytest.mark.parametrize(
    "change,reasons",
    [
        (lambda r: r.update(stage="after"), ["recovery-incomplete"]),
        (lambda r: r["outcomes"].pop("after"), ["missing-after"]),
        (lambda r: r["outcomes"].update(checkpoint=None), ["missing-checkpoint"]),
        # Without the fault or the recovery the replay cannot be judged, and
        # their absence is the reason.
        (lambda r: r["outcomes"].pop("fault"), ["missing-fault"]),
        (lambda r: r["outcomes"].update(recovery="observed"), ["missing-recovery"]),
        (
            lambda r: r["outcomes"]["recovery"].update(replay="unobserved"),
            ["replay-unobserved"],
        ),
        (
            lambda r: r["outcomes"]["recovery"].update(replay="not-expected"),
            ["replay-unobserved"],
        ),
        (
            lambda r: r["outcomes"]["recovery"].update(expected_replay=0, replayed=0),
            ["replay-empty"],
        ),
        (
            lambda r: r["outcomes"]["recovery"].update(expected_replay=True),
            ["replay-empty"],
        ),
        (
            lambda r: r["outcomes"]["recovery"].update(replayed=3),
            ["replay-incomplete"],
        ),
        (
            lambda r: r["outcomes"]["recovery"].update(
                expected_replay=1, replayed=True
            ),
            ["replay-incomplete"],
        ),
        (
            lambda r: r["outcomes"]["recovery"].update(replay_ids_preserved="true"),
            ["replay-ids-not-preserved"],
        ),
        # A replay collected after recovery was proven counts too.
        (
            lambda r: r["outcomes"]["after"].update(replay_ids_preserved=False),
            ["replay-ids-not-preserved"],
        ),
        (
            lambda r: r["outcomes"]["after"].pop("replay_ids_preserved"),
            ["replay-ids-not-preserved"],
        ),
        (lambda r: r["outcomes"]["fault"].update(kind="other"), ["unknown-trial"]),
        (lambda r: r.pop("oracle"), ["oracle-missing"]),
        (
            lambda r: r["oracle"].update(rejected="Invalid output observation"),
            ["oracle-rejected"],
        ),
        (lambda r: r["oracle"].pop("rejected"), ["oracle-rejected"]),
        (lambda r: r["oracle"].update(missing_inputs=2), ["oracle-incomplete"]),
        (lambda r: r["oracle"].pop("missing_inputs"), ["oracle-incomplete"]),
        (lambda r: r["oracle"].update(missing_inputs=False), ["oracle-incomplete"]),
    ],
)
def test_each_shortfall_is_inconclusive_with_its_reason(change, reasons):
    record = complete()
    change(record)
    assert verdict(record) == {"verdict": INCONCLUSIVE, "reasons": reasons}


@pytest.mark.parametrize("kind", ["rescale-out", "rescale-in"])
def test_a_rescale_record_claiming_a_replay_contradicts_its_trial(kind):
    """The exercise never writes this; a record that does was not written by it."""
    record = complete(kind)
    record["outcomes"]["recovery"]["replay"] = "observed"
    assert verdict(record)["reasons"] == ["replay-unexpected"]


@pytest.mark.parametrize("kind", ["rescale-out", "rescale-in"])
@pytest.mark.parametrize("replayed", [4, None, True, "0"])
def test_a_redelivery_after_a_savepoint_is_inconclusive_until_measured(kind, replayed):
    """Whether the service redelivers after a savepoint is #1435's to measure."""
    record = complete(kind)
    record["outcomes"]["after"]["replay_by_new_attempts"] = replayed
    assert verdict(record) == {
        "verdict": INCONCLUSIVE,
        "reasons": ["replay-after-savepoint"],
    }


def test_each_family_is_required_before_the_fault_and_after_recovery():
    for window in SAMPLED_WINDOWS:
        for family in OBSERVED:
            record = complete()
            observed = record["coverage"][window]["observed"]
            observed.remove(family)
            assert verdict(record)["reasons"] == [f"unobserved-{family}-in-{window}"]


@pytest.mark.parametrize("attempts", [0, None, True, "1"])
def test_a_window_that_took_no_sample_observed_nothing(attempts):
    record = complete()
    record["coverage"]["after"]["attempts"] = attempts
    assert verdict(record)["reasons"] == [
        "unsampled-after",
        *(f"unobserved-{family}-in-after" for family in OBSERVED),
    ]


@pytest.mark.parametrize("value", [None, [], "complete", 1])
def test_an_untyped_record_is_inconclusive_without_raising(value):
    decided = verdict(value)
    assert decided["verdict"] == INCONCLUSIVE
    assert decided["reasons"][:2] == ["recovery-incomplete", "missing-checkpoint"]


def test_malformed_fields_read_as_missing():
    record = complete(outcomes=[], coverage=[], oracle="pass")
    assert verdict(record)["reasons"] == [
        "missing-checkpoint",
        "missing-fault",
        "missing-recovery",
        "missing-after",
        "oracle-missing",
        *(
            reason
            for window in SAMPLED_WINDOWS
            for reason in (
                "unsampled-" + window,
                *(f"unobserved-{family}-in-{window}" for family in OBSERVED),
            )
        ),
    ]


def test_coverage_credits_what_a_sample_returned():
    assert summarize({}, "boundary", FULL)["before"]["observed"] == list(OBSERVED)
    unavailable = {
        "vertices": [
            {
                "id": "relay",
                "backpressure": {"unavailable": "503"},
                "metrics": {"unavailable": "no Pub/Sub connector metric is listed"},
            }
        ]
    }
    assert summarize({}, "after", unavailable) == {
        "after": {"attempts": 1, "observed": []}
    }
    # Another operator's metric, an item without an id, and the listing's own
    # shape, an id with no aggregate, which a skipped value query would leave.
    other = copy.deepcopy(FULL)
    other["vertices"][0]["metrics"] = [
        {"id": "Source__x.numRecordsIn", "sum": 1},
        {"sum": 1},
        {"id": "Source__Pub_Sub_input.pendingAcks"},
        {"id": "Sink__Writer.inFlightMessages"},
    ]
    assert summarize({}, "after", other)["after"]["observed"] == ["backpressure"]
    # An aggregate key without a finite number is no reading either.
    for value in (None, True, "3", float("nan"), float("inf"), [1]):
        empty = copy.deepcopy(FULL)
        for item in empty["vertices"][0]["metrics"]:
            item["sum"] = value
        assert summarize({}, "after", empty)["after"]["observed"] == ["backpressure"]
    for broken in ({}, {"vertices": {"unavailable": "x"}}, {"vertices": [None, 1]}):
        assert summarize({}, "after", broken)["after"]["observed"] == []


def test_coverage_is_monotonic_within_a_window_and_never_crosses_one():
    coverage = summarize({}, "baseline", FULL)
    coverage = summarize(coverage, "checkpoint", {"vertices": []})
    assert coverage == {"before": {"attempts": 2, "observed": list(OBSERVED)}}
    coverage = summarize(coverage, "after", {})
    assert coverage["after"] == {"attempts": 1, "observed": []}
    # A stage no window names counts nowhere, recovery's included.
    assert summarize(coverage, "recovering", FULL) == coverage
    assert summarize(coverage, "complete", FULL) == coverage


def test_windows_name_every_stage_before_the_fault_and_after_recovery():
    assert WINDOWS == {
        "baseline": "before",
        "checkpoint": "before",
        "boundary": "before",
        "after": "after",
    }
    # Reasons are reported in this order.
    assert SAMPLED_WINDOWS == ("before", "after")
    assert MEASUREMENT_EVENT == "pubsub-measurement"
