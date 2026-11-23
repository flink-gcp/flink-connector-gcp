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
"""Why a Spanner run's data withholds `usable` (ADR-0204)."""

from ..verdict import component_reasons

# The components the oracles judge, each by its own report under this name.
COMPONENTS = ("sink", "change_stream", "batch")
# The Change Streams relay's counts of a failure that timing cannot explain:
# a row no input names, and a commit after the change stream's end, which the
# source must not relay. A missing key is the one shortfall a commit after
# the end leaves.
RELAY_FAILURES = ("foreign", "after_end")


def data_reasons(record):
    """Whether all three oracles passed, the Change Streams one having been judged.

    An input that finished too close to the change stream's end leaves the
    relay unjudged, since a commit after the end is not relayed. The source's
    watermark, like every other metric, decides nothing.
    """
    return component_reasons(record, COMPONENTS, "change_stream", RELAY_FAILURES)
