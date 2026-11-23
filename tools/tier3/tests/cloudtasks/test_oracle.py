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

import base64
import hashlib

import pytest
from flink_tier3.cloudtasks.oracle import assess, page
from flink_tier3.cloudtasks.trial import Trial
from flink_tier3.cloudtasks.verdict import COMPLETE_STAGE, sequence_verdict

TRIAL = Trial("ct-oracle", 5, 1, "sequence")
PREFIX = TRIAL.queue_path + "/tasks/"


def body(sequence, run_id=TRIAL.run_id):
    """The base64 body the application gives a task: its key."""
    return base64.b64encode(f"{run_id}/{sequence}".encode()).decode()


def task(sequence, **fields):
    return {
        "name": PREFIX + TRIAL.task_id(sequence),
        "dispatchCount": 0,
        "httpRequest": {"body": body(sequence)},
        **fields,
    }


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
        "mismatched": 0,
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
    listed = [task(item) if isinstance(item, int) else foreign_task() for item in tasks]
    report = assess(TRIAL, [page(TRIAL, listed)])
    assert report[field] == count
    # Each fails on its own: every listed body is its own task's key.
    assert report["mismatched"] == 0
    assert report["verdict"] == "fail"


def foreign_task():
    """Another run's record, well formed, under this run's queue."""
    key = b"ct-other/0"
    return {
        "name": PREFIX + hashlib.sha256(key).hexdigest(),
        "httpRequest": {"body": base64.b64encode(key).decode()},
    }


def test_a_dispatched_task_fails_the_paused_queue():
    report = assess(
        TRIAL, [page(TRIAL, [task(n, dispatchCount=n == 3) for n in range(5)])]
    )
    assert report["dispatched"] == 1
    assert report["verdict"] == "fail"


PASSED = {"queue_state": "PAUSED", "report": {"verdict": "pass"}}


@pytest.mark.parametrize("value", [None, [], {"stage": "listing"}, {"outcomes": 3}])
def test_an_unfinished_or_malformed_record_is_inconclusive(value):
    decided = sequence_verdict(value)
    assert decided["verdict"] == "inconclusive"
    assert "recovery-incomplete" in decided["reasons"]


def test_a_task_without_a_dispatch_count_was_never_dispatched():
    """Proto3 JSON omits a zero dispatch count."""
    tasks = [task(n) for n in range(5)]
    for item in tasks:
        del item["dispatchCount"]
    assert assess(TRIAL, [page(TRIAL, tasks)])["verdict"] == "pass"


def test_listed_counts_foreign_names_too():
    tasks = [task(n) for n in range(5)] + [{"name": "elsewhere"}]
    report = assess(TRIAL, [page(TRIAL, tasks)])
    assert (report["listed"], report["foreign"], report["matched"]) == (6, 1, 5)


@pytest.mark.parametrize(
    ("stage", "listing", "reason"),
    [
        ("listing", PASSED, "recovery-incomplete"),
        (COMPLETE_STAGE, {**PASSED, "queue_state": "RUNNING"}, "queue-not-paused"),
        (
            COMPLETE_STAGE,
            {**PASSED, "report": {"verdict": "fail"}},
            "oracle-not-passed",
        ),
        (COMPLETE_STAGE, {"queue_state": "PAUSED"}, "oracle-not-passed"),
    ],
)
def test_a_sequence_run_is_usable_only_complete_paused_and_listed_exactly_once(
    stage, listing, reason
):
    """A fault-sequence run (ADR-0204) is judged by its listing alone."""
    decided = sequence_verdict({"stage": stage, "outcomes": {"listing": listing}})
    assert decided == {"verdict": "inconclusive", "reasons": [reason]}


def test_a_completed_sequence_run_with_a_passing_listing_is_usable():
    """Nothing but the listing is asked of the record."""
    value = {"stage": COMPLETE_STAGE, "outcomes": {"listing": PASSED}}
    assert sequence_verdict(value) == {"verdict": "usable", "reasons": []}


@pytest.mark.parametrize(
    "request_",
    [
        # Sequence 3's task carrying sequence 4's record.
        {"body": body(4)},
        # Another run's record under this run's name.
        {"body": body(3, run_id="ct-other")},
        {},
        {"body": None},
        {"body": "not base64!"},
        None,
    ],
)
def test_a_task_whose_body_is_not_its_own_key_fails(request_):
    """ADR-0204: a wrong value for an expected record fails the trial."""
    tasks = [task(n) for n in range(5)]
    tasks[3]["httpRequest"] = request_
    item = page(TRIAL, tasks)
    assert item["mismatched"] == [TRIAL.task_id(3)]
    report = assess(TRIAL, [item])
    assert (report["matched"], report["mismatched"]) == (5, 1)
    assert report["verdict"] == "fail"


def test_mismatches_on_any_page_are_each_counted():
    """Two wrong bodies on the first of two pages, the second page clean."""
    first = [task(n) for n in range(3)]
    first[0]["httpRequest"]["body"] = body(4)
    first[2]["httpRequest"]["body"] = body(1)
    pages = [page(TRIAL, first), page(TRIAL, [task(3), task(4)])]
    report = assess(TRIAL, pages)
    assert (report["mismatched"], report["verdict"]) == (2, "fail")


def test_a_body_with_characters_outside_base64_is_not_its_key():
    """The service returns canonical base64; anything else is not decoded leniently."""
    tasks = [task(n) for n in range(5)]
    tasks[1]["httpRequest"]["body"] = body(1) + "!"
    assert page(TRIAL, tasks)["mismatched"] == [TRIAL.task_id(1)]
