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
from flink_tier3.exercise import RECORD_SECONDS
from flink_tier3.firestore import plan
from flink_tier3.policy import CLEANUP_SECONDS, FIRESTORE_OBSERVATIONS

# The slowest recoveries of #1878 to their proof, in seconds, and a
# checkpoint interval read again after each restore.
DOWNTIME = 114 + 101 + 2 * 30


def test_the_slowest_read_ends_in_time_and_leaves_the_oracles_their_window():
    """One reading subtask throughout: within 45 minutes of the first start,
    which the service's one-hour read-time bound requires, and the oracles'
    reads end before the cleanup start."""
    seed = plan.SEED
    read = seed["seed_records"] / seed["seed_records_per_second"] + DOWNTIME
    assert read <= 45 * 60
    assert (
        FIRESTORE_OBSERVATIONS["startup_seconds"]
        + read
        + FIRESTORE_OBSERVATIONS["visibility_seconds"]
        <= plan.WINDOW_SECONDS - CLEANUP_SECONDS - RECORD_SECONDS
    )
