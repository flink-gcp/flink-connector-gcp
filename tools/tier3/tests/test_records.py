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
from flink_tier3.common import ApiError, Failure
from flink_tier3.records import conditional_update


class Store:
    """One object whose writes fail with the queued statuses, then succeed."""

    def __init__(self, *statuses):
        self.statuses = list(statuses)
        self.value, self.generation, self.writes = {"count": 0}, 1, 0

    def read(self):
        return dict(self.value), str(self.generation)

    def write(self, path, value, generation):
        self.writes += 1
        if self.statuses:
            raise ApiError(self.statuses.pop(0), "POST", path)
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


def test_an_update_makes_at_most_twenty_six_writes():
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
