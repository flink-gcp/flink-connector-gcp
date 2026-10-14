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
"""The task-listing oracle and the verdict decided from it."""

import pytest
from flink_tier3.cloudtasks.oracle import assess, page
from flink_tier3.cloudtasks.trial import Trial
from flink_tier3.cloudtasks.verdict import (
    COMPLETE_STAGE,
    OBSERVED,
    SAMPLED_STAGES,
    summarize,
    verdict,
)

TRIAL = Trial("ct-oracle", 5, 1)
PREFIX = TRIAL.queue_path + "/tasks/"


def task(sequence, **fields):
    return {"name": PREFIX + TRIAL.task_id(sequence), "dispatchCount": 0, **fields}


def test_page_keeps_ids_and_sets_aside_foreign_names_and_dispatches():
    other = "projects/flink-gcp/locations/us-central1/queues/ct1362-other/tasks/"
    item = page(
        TRIAL,
        [
            task(0),
            task(1, dispatchCount=1),
            {"name": other + TRIAL.task_id(2)},
            {"name": PREFIX + "not-a-digest"},
            {"other": "field"},
        ],
    )
    assert item["ids"] == [TRIAL.task_id(0), TRIAL.task_id(1)]
    assert item["dispatched"] == [TRIAL.task_id(1)]
    assert item["foreign"] == [
        other + TRIAL.task_id(2),
        PREFIX + "not-a-digest",
        "None",
    ]


def test_every_sequence_once_and_nothing_else_passes():
    pages = [page(TRIAL, [task(0), task(1)]), page(TRIAL, [task(2), task(3), task(4)])]
    assert assess(TRIAL, pages) == {
        "records": 5,
        "listed": 5,
        "matched": 5,
        "missing": 0,
        "foreign": 0,
        "duplicates": 0,
        "dispatched": 0,
        "verdict": "pass",
    }


@pytest.mark.parametrize(
    "tasks,field,count",
    [
        ([0, 1, 2, 3], "missing", 1),
        ([0, 1, 2, 3, 4, 4], "duplicates", 1),
        ([0, 1, 2, 3, 4, "x"], "foreign", 1),
    ],
)
def test_a_missing_repeated_or_foreign_task_fails(tasks, field, count):
    listed = [
        task(item) if isinstance(item, int) else {"name": PREFIX + "f" * 64}
        for item in tasks
    ]
    report = assess(TRIAL, [page(TRIAL, listed)])
    assert report[field] == count
    assert report["verdict"] == "fail"


def test_a_dispatched_task_fails_the_paused_queue():
    report = assess(
        TRIAL, [page(TRIAL, [task(n, dispatchCount=n == 3) for n in range(5)])]
    )
    assert report["dispatched"] == 1
    assert report["verdict"] == "fail"


# Every family read once in every sampled window.
COVERED = {
    stage: {"attempts": 1, "observed": sorted(OBSERVED)} for stage in SAMPLED_STAGES
}


def record(coverage=COVERED, **outcomes):
    return {"stage": COMPLETE_STAGE, "outcomes": outcomes, "coverage": coverage}


PASSED = {"queue_state": "PAUSED", "report": {"verdict": "pass"}}


def test_usable_needs_both_recoveries_a_paused_queue_and_the_oracle():
    assert verdict(record(upgrade={}, failover={}, listing=PASSED)) == {
        "verdict": "usable",
        "reasons": [],
    }
    assert verdict(record(upgrade={}, listing=PASSED))["reasons"] == [
        "missing-failover"
    ]
    assert verdict(
        record(upgrade={}, failover={}, listing={**PASSED, "queue_state": "RUNNING"})
    )["reasons"] == ["queue-not-paused"]
    assert verdict(
        record(
            upgrade={},
            failover={},
            listing={**PASSED, "report": {"verdict": "fail"}},
        )
    )["reasons"] == ["oracle-not-passed"]


def test_usable_needs_every_family_in_the_baseline_and_after_each_recovery():
    assert OBSERVED == ("connector", "network", "task", "memory", "checkpoint")
    assert SAMPLED_STAGES == ("baseline", "upgraded", "finishing")
    coverage = {**COVERED, "upgraded": {"attempts": 3, "observed": ["task"]}}
    del coverage["finishing"]
    reasons = verdict(record(coverage, upgrade={}, failover={}, listing=PASSED))[
        "reasons"
    ]
    assert reasons == [
        "unobserved-connector-in-upgraded",
        "unobserved-network-in-upgraded",
        "unobserved-memory-in-upgraded",
        "unobserved-checkpoint-in-upgraded",
        "unsampled-finishing",
        *(f"unobserved-{family}-in-finishing" for family in OBSERVED),
    ]
    # A recovery's own samples count toward no window.
    coverage = {**COVERED, "upgrade": {"attempts": 1, "observed": []}}
    assert verdict(record(coverage, upgrade={}, failover={}, listing=PASSED)) == {
        "verdict": "usable",
        "reasons": [],
    }


def reading(**changes):
    """One poll that returned every family, as the REST service answers it."""
    value = {
        "sink": [
            {"id": "numRecordsIn", "min": 1},
            {"id": "inPoolUsage", "min": 0},
            {"id": "Cloud_Tasks__Committer.tasksDeduplicated", "min": 0},
        ],
        "source": [{"id": "numRecordsOut"}],
        "taskmanagers": [
            {"id": "tm", "metrics": [{"id": "Status.JVM.Memory.Heap.Used"}]}
        ],
        "checkpoints": {
            "id": 3,
            "vertices": {
                "sink": [
                    {
                        "index": 0,
                        "end_to_end_duration": 40,
                        "checkpoint": {"sync": 1, "async": 2},
                    }
                ]
            },
        },
    }
    value.update(window="baseline", **changes)
    return value


@pytest.mark.parametrize(
    "changes,unobserved",
    [
        ({}, set()),
        # Asked for by its discovered id and not returned: not read.
        ({"sink": [{"id": "numRecordsIn"}, {"id": "inPoolUsage"}]}, {"connector"}),
        ({"sink": {"unavailable": "503"}}, {"task", "network", "connector"}),
        ({"taskmanagers": [{"id": "tm", "metrics": []}]}, {"memory"}),
        ({"taskmanagers": {"unavailable": "503"}}, {"memory"}),
        ({"checkpoints": {"unavailable": "no completed checkpoint"}}, {"checkpoint"}),
        # Each of a subtask's durations is required on its own.
        *(
            (
                {
                    "checkpoints": {
                        "id": 3,
                        "vertices": {
                            "sink": [
                                {
                                    "end_to_end_duration": 40,
                                    "checkpoint": {"sync": 1, "async": 2},
                                    **subtask,
                                }
                            ]
                        },
                    }
                },
                {"checkpoint"},
            )
            for subtask in (
                {"end_to_end_duration": "40"},
                {"end_to_end_duration": None},
                {"checkpoint": {"sync": 1}},
                {"checkpoint": {"async": 2}},
                {"checkpoint": {"sync": 1, "async": "2"}},
                {"checkpoint": None},
            )
        ),
        ({"checkpoints": {"id": 3, "vertices": {"committer": []}}}, {"checkpoint"}),
        ({"vertices": {"unavailable": "expected one source and one sink"}}, set()),
    ],
)
def test_a_family_counts_only_where_a_reading_returned_it(changes, unobserved):
    value = reading(**changes)
    if "vertices" in changes:
        value = {"window": "baseline", **changes}
        unobserved = set(OBSERVED)
    folded = summarize({}, "baseline", value)
    assert set(OBSERVED) - set(folded["baseline"]["observed"]) == unobserved
    assert folded["baseline"]["attempts"] == 1


@pytest.mark.parametrize("value", [None, [], {"stage": "listing"}, {"outcomes": 3}])
def test_an_unfinished_or_malformed_record_is_inconclusive(value):
    decided = verdict(value)
    assert decided["verdict"] == "inconclusive"
    assert "recovery-incomplete" in decided["reasons"]


def test_a_task_without_a_dispatch_count_was_never_dispatched():
    """Proto3 JSON omits a zero dispatch count, as the basic view returns it."""
    tasks = [{"name": PREFIX + TRIAL.task_id(n)} for n in range(5)]
    assert assess(TRIAL, [page(TRIAL, tasks)])["verdict"] == "pass"


def test_listed_counts_foreign_names_too():
    tasks = [task(n) for n in range(5)] + [{"name": "elsewhere"}]
    report = assess(TRIAL, [page(TRIAL, tasks)])
    assert (report["listed"], report["foreign"], report["matched"]) == (6, 1, 5)
