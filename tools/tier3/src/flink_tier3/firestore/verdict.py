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
"""Why a Firestore run's data withholds `usable` (ADR-0204)."""

from ..verdict import component_reasons

# The components the oracles judge, each by its own report under this name.
COMPONENTS = (
    "native_sink",
    "datastore_sink",
    "native_batch",
    "datastore_batch",
)


def data_reasons(record):
    """Whether every component's oracle passed in both databases."""
    return component_reasons(record, COMPONENTS)
