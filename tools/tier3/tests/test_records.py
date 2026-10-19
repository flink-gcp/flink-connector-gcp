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
"""Conditional control updates."""

import pytest
from flink_tier3.common import ApiError, Failure, StorageRequestFailed
from flink_tier3.records import conditional_update


class Store:
    """One object whose writes fail with the queued statuses, then succeed.

    `"landed"` stores the write and then loses its answer; `"lost"` loses the
    request itself; both raise as a storage timeout does.
    """

    def __init__(self, *statuses):
        self.statuses = list(statuses)
        self.value, self.generation, self.writes = {"count": 0}, 1, 0

    def read(self):
        return dict(self.value), str(self.generation)

    def write(self, path, value, generation):
        self.writes += 1
        status = self.statuses.pop(0) if self.statuses else None
        if status == "landed":
            self.value, self.generation = value, self.generation + 1
        if status in ("landed", "lost"):
            raise StorageRequestFailed("POST storage request failed: ReadTimeout")
        if status:
            raise ApiError(status, "POST", path)
        assert generation == str(self.generation)
        self.value, self.generation = value, self.generation + 1


def update(store, sleeps, edit=None):
    def increment(value):
        if edit is not None:
            edit()
        value["count"] += 1

    return conditional_update(store, "o", store.read, increment, sleep=sleeps.append)


def test_a_rate_limited_write_is_edited_again_after_a_growing_backoff():
    store, sleeps, edits = Store(429, 429, 429), [], []
    assert update(store, sleeps, lambda: edits.append(len(sleeps))) == {"count": 1}
    assert store.value == {"count": 1} and store.writes == 4
    assert [int(delay) for delay in sleeps] == [1, 2, 4]
    assert all(0 <= delay - int(delay) < 1 for delay in sleeps)
    # Every write follows its own edit, taken after the preceding backoff.
    assert edits == [0, 1, 2, 3]


def test_rate_limiting_that_does_not_end_fails_after_six_backoffs():
    store, sleeps = Store(*[429] * 7), []
    with pytest.raises(ApiError) as raised:
        update(store, sleeps)
    assert raised.value.status == 429
    assert [int(delay) for delay in sleeps] == [1, 2, 4, 8, 16, 32]
    assert store.value == {"count": 0} and store.writes == 7


def test_a_lost_generation_race_rereads_after_a_jittered_wait():
    store, sleeps = Store(412, 409, 412), []
    assert update(store, sleeps) == {"count": 1}
    assert store.writes == 4 and len(sleeps) == 3
    # Full jitter under a ceiling that doubles from a quarter second.
    assert all(0 <= delay <= ceiling for delay, ceiling in zip(sleeps, (0.25, 0.5, 1)))


def test_race_waits_stop_doubling_at_four_seconds():
    store, sleeps = Store(*[412] * 19), []
    assert update(store, sleeps) == {"count": 1}
    assert store.writes == 20 and len(sleeps) == 19
    assert max(sleeps) <= 4


def test_twenty_lost_races_do_not_settle():
    store, sleeps = Store(*[412] * 20), []
    with pytest.raises(Failure, match="Concurrent control updates did not settle"):
        update(store, sleeps)
    # No wait follows the race that gives up.
    assert store.writes == 20 and len(sleeps) == 19


def test_refused_writes_do_not_spend_the_race_budget():
    store, sleeps = Store(*[412] * 19, *[429] * 6, 412), []
    with pytest.raises(Failure, match="did not settle"):
        update(store, sleeps)
    assert store.writes == 26 and len(sleeps) == 25


@pytest.mark.parametrize("status", [403, 500, 503])
def test_other_failures_are_not_retried(status):
    store, sleeps = Store(status), []
    with pytest.raises(ApiError):
        update(store, sleeps)
    assert store.writes == 1 and sleeps == []


def test_the_first_actor_to_begin_cleanup_names_the_stop():
    """The supervisor stops on the exercise's cause; the runner settles after it."""
    from flink_tier3.exercise import INTERRUPTED

    from .cloudtasks.test_recovery_lifecycle import Environment

    env = Environment()
    env.records.begin_cleanup(INTERRUPTED)
    env.records.begin_cleanup("external settlement")
    assert env.refresh().reason == INTERRUPTED


@pytest.mark.parametrize("fate", ["landed", "lost"])
def test_an_unanswered_update_that_reads_back_unchanged_is_applied_once(fate):
    """bt1855-tm-a4 stopped on one; the increment would show a double edit."""
    store, sleeps = Store(fate), []
    assert update(store, sleeps) == {"count": 1}
    assert store.value == {"count": 1}
    assert store.writes == (1 if fate == "landed" else 2)


def test_updates_that_never_answer_still_fail():
    store = Store("lost", "lost", "lost")
    with pytest.raises(StorageRequestFailed):
        update(store, [])
    assert store.writes == 3


@pytest.mark.parametrize("fates", [["landed"], ["lost"], ["lost", 503], ["lost"] * 3])
def test_an_unanswered_evidence_event_is_written_once_or_fails(fates):
    """A 412 on a repeat is the first attempt landing; nothing else is."""
    from .cloudtasks.test_recovery_lifecycle import Environment

    env = Environment()
    write = env.store.write
    sent = []

    def flaky(name, data):
        sent.append(name)
        fate = fates[len(sent) - 1] if len(sent) <= len(fates) else None
        if fate == "landed":
            write(name, data)
        if fate in ("landed", "lost"):
            raise StorageRequestFailed("POST storage request failed: ReadTimeout")
        if fate:
            raise ApiError(fate, "POST", name)
        return write(name, data)

    env.store.write = flaky
    stored = lambda: [key for key in env.store.data if "/supervisor/" in key[1]]
    if fates in (["lost", 503], ["lost"] * 3):
        with pytest.raises((ApiError, StorageRequestFailed)):
            env.records.evidence("probe", {"n": 1})
        assert len(sent) == len(fates) and not stored()
        return
    env.records.evidence("probe", {"n": 1})
    assert len(sent) == 2 and sent[0] == sent[1]
    assert len(stored()) == 1
