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

from flink_tier3.exercise import RECORD_SECONDS, TIME_LIMIT_SECONDS
from flink_tier3.policy import SPANNER_OBSERVATIONS
from flink_tier3.spanner import plan
from flink_tier3.spanner.exercise import COMMIT_SECONDS


def test_the_sequence_input_and_oracles_fit_around_its_change_stream_end():
    """An input that cannot finish before the end leaves no run usable, and an
    end too late leaves the oracles no time inside the run's time limit."""
    end = plan.SEQUENCE_CHANGE_STREAM_END_SECONDS
    rows = plan.SEQUENCE_INPUT
    assert rows["records"] / rows["records_per_second"] + COMMIT_SECONDS < end
    assert (
        end + SPANNER_OBSERVATIONS["visibility_seconds"]
        <= TIME_LIMIT_SECONDS - RECORD_SECONDS
    )
