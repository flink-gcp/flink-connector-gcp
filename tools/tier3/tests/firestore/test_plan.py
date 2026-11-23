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
from flink_tier3.exercise import RECORD_SECONDS, TIME_LIMIT_SECONDS
from flink_tier3.firestore import plan
from flink_tier3.policy import FIRESTORE_OBSERVATIONS, POLL

# How much later than their length the five faults made the first sequence
# runs' inputs finish: at most 406 seconds, in the first Pub/Sub run, rounded up.
FAULT_DELAY = 420
# An assumed time for the seed readers' first split: the latest input start
# of the first sequence runs, 677 seconds into the window, rounded up. Each
# reader asks for a split when its task starts, independently of the input;
# in `fs2147-seq-a1` the read time was planned at 363 seconds, and the late
# seed commits show both copies had started by 514. A read that begins after
# `latest_start` below still ends inside the read time, but may leave the
# oracles less than their full window before the time limit.
READ_START = 680


def test_the_slowest_seed_read_ends_in_time_and_leaves_the_oracles_their_window():
    """One reading subtask throughout: within 45 minutes of the first start,
    inside the hour the service keeps a read time, and the oracles' reads end
    within the run's time limit."""
    seed = plan.SEQUENCE_SEED
    read = seed["seed_records"] / seed["seed_records_per_second"] + FAULT_DELAY
    assert read <= 45 * 60
    # The copy outlasts the input, so the job finishes with the copy.
    rows = plan.SEQUENCE_INPUT
    assert rows["records"] / rows["records_per_second"] + FAULT_DELAY <= read
    # The application is created within its startup budget, before the
    # tasks start and the read begins.
    assert FIRESTORE_OBSERVATIONS["startup_seconds"] <= READ_START
    # Allow one supervisor poll after the copy ends before the job is seen
    # finished.
    latest_start = (
        TIME_LIMIT_SECONDS
        - RECORD_SECONDS
        - FIRESTORE_OBSERVATIONS["visibility_seconds"]
        - read
        - POLL
    )
    assert READ_START <= latest_start
