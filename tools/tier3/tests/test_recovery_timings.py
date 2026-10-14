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
"""Reading each recovery's timing from the shared exercise's record.

The exercise writes these shapes and `bigquery/test_exercise` crosses that
seam; these cases build the shapes a real run does not produce.
"""

import copy

import pytest
from flink_tier3.recovery_analysis import recovery_timings, render_timings

# 2026-09-25T04:33:23.160467Z and 04:35:12.565908Z, in milliseconds.
UPGRADE_START = 1790310803160
FAILOVER_START = 1790310912566
STARTS = {
    "upgrade": "2026-09-25T04:33:23.160467Z",
    "failover": "2026-09-25T04:35:12.565908Z",
}


def proof(start, savepoint, restored_id, checkpoint_id):
    return {
        "restored": {
            "id": restored_id,
            "is_savepoint": savepoint,
            "restore_timestamp": start + 54_302,
        },
        "checkpoint": {"id": checkpoint_id, "latest_ack_timestamp": start + 88_722},
        "progress": {"processed": 10},
    }


OUTCOMES = {
    "upgrade": proof(UPGRADE_START, True, 29, 30),
    "failover": proof(FAILOVER_START, False, 30, 32),
}


class FakeRun:
    """The one method of a downloaded run the reader uses."""

    def __init__(self, starts=None):
        starts = STARTS if starts is None else starts
        self.events = [
            {
                "event": "recovery-" + name,
                "payload": {"stage": name, "at": at},
            }
            for name, ats in starts.items()
            for at in ([ats] if isinstance(ats, str | None) else ats)
        ]

    def events_of(self, event):
        return [e for e in self.events if e["event"] == event]


def timing(name, fault, restored_from, restored_id, checkpoint_id):
    return {
        "fault": fault,
        "began_at": STARTS[name],
        "restored_from": restored_from,
        "restored_id": restored_id,
        "restored_ms": 54_302,
        "checkpoint_id": checkpoint_id,
        "checkpoint_ms": 88_722,
    }


def test_both_recoveries_are_timed_from_their_stage_record():
    assert recovery_timings(FakeRun(), {"outcomes": OUTCOMES}) == {
        "upgrade": timing("upgrade", "savepoint-upgrade", "savepoint", 29, 30),
        "failover": timing("failover", "jm-replacement", "checkpoint", 30, 32),
    }


def test_the_proof_names_a_rescale_or_a_taskmanager_replacement():
    outcomes = copy.deepcopy(OUTCOMES)
    outcomes["upgrade"]["rescale"] = {"direction": "in"}
    outcomes["failover"]["replaced"] = {"uid": "u", "name": "tm-1"}
    timings = recovery_timings(FakeRun(), {"outcomes": outcomes})
    assert timings["upgrade"]["fault"] == "rescale-in"
    assert timings["failover"]["fault"] == "tm-replacement"
    outcomes["upgrade"]["rescale"] = {"direction": "out"}
    timings = recovery_timings(FakeRun(), {"outcomes": outcomes})
    assert timings["upgrade"]["fault"] == "rescale-out"


@pytest.mark.parametrize(
    "rescale",
    [
        {"direction": "sideways"},
        {"direction": []},
        {"direction": {}},
        {},
        None,
        "out",
    ],
)
def test_a_rescale_the_proof_names_unreadably_is_not_a_plain_upgrade(rescale):
    """Reading it as a savepoint upgrade would claim the sizes did not change."""
    outcomes = copy.deepcopy(OUTCOMES)
    outcomes["upgrade"]["rescale"] = rescale
    assert recovery_timings(FakeRun(), {"outcomes": outcomes})["upgrade"] is None


def test_a_recovery_the_record_did_not_prove_is_absent():
    record = {"outcomes": {"upgrade": OUTCOMES["upgrade"], "query": {}}}
    assert list(recovery_timings(FakeRun(), record)) == ["upgrade"]
    for record in (None, [], {}, {"outcomes": []}):
        assert recovery_timings(FakeRun(), record) == {}


@pytest.mark.parametrize(
    "path,value",
    [
        (("restored", "restore_timestamp"), 1.5e12),
        (("restored", "restore_timestamp"), True),
        (("restored", "restore_timestamp"), "1790310857462"),
        (("restored", "restore_timestamp"), 0),
        # Past what the report can turn into seconds.
        (("restored", "restore_timestamp"), 2**53),
        (("checkpoint", "latest_ack_timestamp"), 10**400),
        (("restored", "id"), None),
        (("restored", "is_savepoint"), "true"),
        (("checkpoint", "latest_ack_timestamp"), None),
        (("checkpoint", "id"), "30"),
        (("restored",), None),
        (("checkpoint",), []),
    ],
)
def test_a_proof_the_export_does_not_hold_readably_is_unreadable(path, value):
    outcomes = copy.deepcopy(OUTCOMES)
    target = outcomes["failover"]
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    timings = recovery_timings(FakeRun(), {"outcomes": outcomes})
    assert timings["failover"] is None
    assert timings["upgrade"] is not None


@pytest.mark.parametrize(
    "starts",
    [
        # No stage record, two of them, and one whose time is not UTC.
        {"upgrade": STARTS["upgrade"]},
        {**STARTS, "failover": [STARTS["failover"], STARTS["failover"]]},
        {**STARTS, "failover": "2026-09-25T04:35:12"},
        {**STARTS, "failover": None},
    ],
)
def test_a_recovery_without_one_readable_start_is_unreadable(starts):
    timings = recovery_timings(FakeRun(starts), {"outcomes": OUTCOMES})
    assert timings["failover"] is None
    assert timings["upgrade"] is not None


def test_the_table_renders_each_recovery_and_names_an_unreadable_one():
    items = [
        {
            "run_id": "bq-a1",
            "recoveries": recovery_timings(FakeRun(), {"outcomes": OUTCOMES}),
        },
        {"run_id": "bq-a2", "recoveries": {"upgrade": None}},
        {"run_id": "bq-a3", "recoveries": {}},
    ]
    lines = render_timings(items)
    assert lines[-3:] == [
        (
            "| bq-a1 | upgrade | savepoint-upgrade | 2026-09-25T04:33:23.160467Z "
            "| savepoint 29 | 54.3 s | chk-30 | 88.7 s |"
        ),
        (
            "| bq-a1 | failover | jm-replacement | 2026-09-25T04:35:12.565908Z "
            "| checkpoint 30 | 54.3 s | chk-32 | 88.7 s |"
        ),
        "| bq-a2 | upgrade | unreadable | - | - | - | - | - |",
    ]
    assert render_timings(items[2:]) == []
