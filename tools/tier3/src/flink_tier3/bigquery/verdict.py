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
"""Why a BigQuery run's data withholds `usable` (ADR-0204)."""

from ..verdict import outcome


def data_reasons(record):
    """Whether the query oracle passed.

    It passes with every input sequence in its table and no invalid row,
    duplicates being reported in ALO mode and failing the oracle in EO and
    FILE_LOADS modes.
    """
    report = outcome(record, "query").get("report")
    if not isinstance(report, dict) or report.get("verdict") != "pass":
        return ["oracle-not-passed"]
    return []
