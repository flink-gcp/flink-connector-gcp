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
"""Why a Bigtable run's data withholds `usable` (ADR-0204)."""

from ..verdict import component_reasons

# The components the oracles judge, each by its own report under this name.
COMPONENTS = ("eo", "alo", "relay")
# The relay's counts of a failure that timing cannot explain: a row the
# relay wrote that no input names, a cell outside its family, an identity
# relayed although `eo` did not apply it, a delivery that holds no commit
# time, and a commit after the change stream's end, which the source must not
# relay. A missing row is the one shortfall a commit after the end leaves.
RELAY_FAILURES = (
    "foreign",
    "foreign_families",
    "unapplied",
    "wrong_value",
    "after_end",
)


def data_reasons(record):
    """Whether all three oracles passed, the relay's having been judged.

    An input that finished too close to the change stream's end leaves the
    relay unjudged, since a commit after the end is not relayed.
    """
    return component_reasons(record, COMPONENTS, "relay", RELAY_FAILURES)
