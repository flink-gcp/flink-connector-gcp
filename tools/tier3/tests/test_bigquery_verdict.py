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
"""What the deployed verdict says, and what it refuses to say."""

import pytest
from flink_tier3.bigquery_observe import (
    CONNECTOR_METRICS,
    MEMORY_METRICS,
    NETWORK_METRICS,
    SINK_FAMILIES,
    SINK_METRICS,
    TASK_METRICS,
)
from flink_tier3.bigquery_verdict import (
    COMPLETE_EVENT,
    COMPLETE_STAGE,
    MEASUREMENT_EVENT,
    MEMORY,
    OBSERVED,
    SAMPLED_STAGES,
    summarize,
    verdict,
)
from flink_tier3.common import INCONCLUSIVE, USABLE

# One returned metric id per family, and one TaskManager that published a
# memory value. These are the readings, not the discovery listing: the verdict
# credits what came back.
READINGS = {
    "task": {"sink": [{"id": "busyTimeMsPerSecond", "sum": "3"}]},
    "network": {"sink": [{"id": "inPoolUsage", "max": "0.5"}]},
    "connector": {"sink": [{"id": "Sink__Writer.openDestinations", "sum": "7"}]},
    MEMORY: {
        "taskmanagers": [
            {"id": "tm-0", "metrics": [{"id": "Status.JVM.Memory.Direct.MemoryUsed"}]}
        ]
    },
}
FULL = {
    "sink": [
        reading["sink"][0] for name, reading in READINGS.items() if name != MEMORY
    ],
    "taskmanagers": READINGS[MEMORY]["taskmanagers"],
}


def covered():
    """Coverage of a run that read everything in both sampled windows."""
    coverage = {}
    for stage in SAMPLED_STAGES:
        coverage = summarize(coverage, stage, FULL)
    return coverage


def complete(**overrides):
    record = {
        "stage": COMPLETE_STAGE,
        "outcomes": {
            "upgrade": {"checkpoint": {"id": 1}},
            "failover": {"checkpoint": {"id": 2}},
            "query": {"report": {"verdict": "pass"}},
        },
        "coverage": covered(),
    }
    record.update(overrides)
    return record


def without(key):
    outcomes = dict(complete()["outcomes"])
    outcomes.pop(key)
    return complete(outcomes=outcomes)


def unobserved(stage, names=OBSERVED):
    return [f"unobserved-{name}-in-{stage}" for name in OBSERVED if name in names]


def test_a_complete_run_with_every_observation_is_usable():
    decided = verdict(complete())
    assert decided["verdict"] == USABLE
    assert decided["reasons"] == []


@pytest.mark.parametrize(
    "key, reason",
    [
        ("upgrade", "missing-upgrade"),
        ("failover", "missing-failover"),
        ("query", "oracle-not-passed"),
    ],
)
def test_a_missing_proof_is_inconclusive_not_a_pass(key, reason):
    decided = verdict(without(key))
    assert decided["verdict"] == INCONCLUSIVE
    assert decided["reasons"] == [reason]


def test_a_failing_oracle_report_is_not_a_passing_one():
    """Not only an absent outcome: a retained record can carry a failed report."""
    outcomes = dict(complete()["outcomes"])
    outcomes["query"] = {"report": {"verdict": "fail"}}
    decided = verdict(complete(outcomes=outcomes))
    assert decided["reasons"] == ["oracle-not-passed"]


def test_a_passed_oracle_does_not_carry_a_run_whose_measurements_are_missing():
    """A `scope=query-result` report says the rows are right, and no more."""
    decided = verdict(complete(coverage={}))
    assert decided["verdict"] == INCONCLUSIVE
    assert decided["reasons"] == [
        reason
        for stage in SAMPLED_STAGES
        for reason in ("unsampled-" + stage, *unobserved(stage))
    ]


@pytest.mark.parametrize("name", OBSERVED)
@pytest.mark.parametrize("stage", SAMPLED_STAGES)
def test_one_unobserved_family_in_one_window_is_named_and_refused(name, stage):
    """Each family is required in each window, and named where it was missing."""
    coverage = {key: dict(window) for key, window in covered().items()}
    coverage[stage] = dict(
        coverage[stage],
        observed=[other for other in coverage[stage]["observed"] if other != name],
    )
    decided = verdict(complete(coverage=coverage))
    assert decided["verdict"] == INCONCLUSIVE
    assert decided["reasons"] == [f"unobserved-{name}-in-{stage}"]


@pytest.mark.parametrize("stage", SAMPLED_STAGES)
def test_a_window_that_took_no_sample_observed_nothing_whatever_it_claims(stage):
    """Otherwise a fabricated window passes by asserting its own coverage."""
    coverage = {key: dict(window) for key, window in covered().items()}
    coverage[stage] = dict(coverage[stage], attempts=0)
    decided = verdict(complete(coverage=coverage))
    assert decided["reasons"] == ["unsampled-" + stage, *unobserved(stage)]


def test_a_recovery_that_did_not_complete_is_not_rescued_by_its_observations():
    decided = verdict(complete(stage="visibility"))
    assert decided["verdict"] == INCONCLUSIVE
    assert decided["reasons"] == ["recovery-incomplete"]


@pytest.mark.parametrize(
    "record",
    [
        None,
        {},
        [],
        COMPLETE_STAGE,
        {"stage": COMPLETE_STAGE, "outcomes": ["upgrade", "failover"]},
        {"stage": COMPLETE_STAGE, "outcomes": {"query": "pass"}},
        {"stage": COMPLETE_STAGE, "coverage": "both"},
        {"stage": COMPLETE_STAGE, "coverage": {"baseline": "all"}},
        {
            "stage": COMPLETE_STAGE,
            "coverage": {s: {"attempts": 1, "observed": "all"} for s in SAMPLED_STAGES},
        },
        {
            "stage": COMPLETE_STAGE,
            "coverage": {s: {"attempts": "3", "observed": []} for s in SAMPLED_STAGES},
        },
    ],
)
def test_a_record_that_is_not_a_run_is_inconclusive_rather_than_raising(record):
    """The analyzer feeds this untrusted JSON, so a wrong type must not pass."""
    decided = verdict(record)
    assert decided["verdict"] == INCONCLUSIVE
    # Every family of every sampled window is unobserved, whatever the record
    # claims, plus whichever proofs it lacks.
    assert all(
        reason in decided["reasons"]
        for stage in SAMPLED_STAGES
        for reason in unobserved(stage)
    )


def test_the_verdict_reports_no_measured_value():
    """It says the instrument worked, never how fast the sink was."""
    for record in (complete(), complete(coverage={})):
        decided = verdict(record)
        assert set(decided) == {"verdict", "reasons"}
        # A figure smuggled into a reason would still be a string.
        assert not any(character.isdigit() for character in "".join(decided["reasons"]))


@pytest.mark.parametrize("name", OBSERVED)
def test_each_family_is_credited_only_by_its_own_reading(name):
    """A set-valued assertion cannot tell one family from another; this does."""
    summary = summarize({}, "baseline", READINGS[name])
    assert summary["baseline"]["observed"] == [name]


def test_coverage_is_monotonic_within_a_stage_and_does_not_cross_one():
    """A family read once in a window is read; another window says nothing."""
    first = summarize({}, "baseline", FULL)
    assert first["baseline"]["observed"] == sorted(OBSERVED)
    assert first["baseline"]["attempts"] == 1
    lost = summarize(first, "baseline", {"sink": {"unavailable": "503"}})
    assert lost["baseline"]["observed"] == sorted(OBSERVED)
    assert lost["baseline"]["attempts"] == 2
    assert set(lost) == {"baseline"}
    # The fold does not edit what it was given: a record already persisted
    # cannot be changed by a later sample.
    assert first["baseline"] == {"attempts": 1, "observed": sorted(OBSERVED)}
    disrupted = summarize(lost, "failover", FULL)
    assert verdict(complete(coverage=disrupted))["reasons"] == [
        "unsampled-finishing",
        *unobserved("finishing"),
    ]


@pytest.mark.parametrize(
    "reading",
    [
        {"sink": {"unavailable": "503"}},
        # Discovery never drops an id, so the request proves nothing was read.
        {"sink": [], "taskmanagers": []},
        {"sink": [{"sum": "7"}], "taskmanagers": [{"id": "tm-0"}]},
        {"sink": [{"id": "notAMetricThisSinkReports"}]},
        # The shape `taskmanagers()` actually produces when the read fails.
        {"taskmanagers": [{"id": "tm-0", "metrics": {"unavailable": "503"}}]},
        {"taskmanagers": [{"id": "tm-0", "metrics": []}]},
        {"taskmanagers": [{"id": "tm-0", "metrics": [{"id": "Status.JVM.CPU.Time"}]}]},
        {"taskmanagers": "none"},
    ],
)
def test_a_reading_that_returned_nothing_wanted_is_not_coverage(reading):
    summary = summarize({}, "baseline", reading)
    assert summary["baseline"] == {"attempts": 1, "observed": []}


def test_the_family_inventory_is_the_one_the_collector_reads():
    assert set(SINK_FAMILIES) == {"task", "network", "connector"}
    assert OBSERVED == ("connector", "network", "task", MEMORY)
    assert SAMPLED_STAGES == ("baseline", "finishing")
    # The discovery filter and the classifier are one declaration, so a metric
    # moved between families cannot be kept by one and lost by the other.
    assert SINK_FAMILIES["task"] == frozenset(TASK_METRICS)
    assert SINK_FAMILIES["network"] == frozenset(NETWORK_METRICS)
    assert SINK_FAMILIES["connector"] == frozenset(CONNECTOR_METRICS)
    assert SINK_METRICS == frozenset(TASK_METRICS + NETWORK_METRICS + CONNECTOR_METRICS)
    assert len(SINK_METRICS) == sum(len(ids) for ids in SINK_FAMILIES.values())
    assert "Status.JVM.Memory.Direct.MemoryUsed" in MEMORY_METRICS


def test_the_event_names_are_spelled_once_for_both_sides():
    assert COMPLETE_EVENT == "recovery-" + COMPLETE_STAGE
    assert MEASUREMENT_EVENT == "bigquery-measurement"
