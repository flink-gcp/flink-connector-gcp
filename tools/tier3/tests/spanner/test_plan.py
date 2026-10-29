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
"""The change-stream end a dispatched trial carries, against the run's window."""

from flink_tier3.policy import CLEANUP_SECONDS, SPANNER_OBSERVATIONS
from flink_tier3.spanner import plan


def test_the_change_stream_end_leaves_the_oracles_time_before_cleanup():
    """A late end would leave the oracles no time before cleanup."""
    assert (
        plan.CHANGE_STREAM_END_SECONDS + SPANNER_OBSERVATIONS["visibility_seconds"]
        <= plan.WINDOW_SECONDS - CLEANUP_SECONDS
    )
