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
"""The trial a dispatch approves: its kind, input and change-stream end."""

from flink_tier3.bigtable import plan
from flink_tier3.common import timestamp

START = "2026-10-20T00:00:00Z"


def test_the_change_stream_end_is_fixed_by_the_window_it_runs_in():
    """A stale end would let the source finish having read nothing (#1854)."""
    trial = plan.trial("rescale-out", START)
    assert timestamp(trial["change_stream_end"]) == timestamp(START) + 3900
    plan.validate_trial(trial, START)
