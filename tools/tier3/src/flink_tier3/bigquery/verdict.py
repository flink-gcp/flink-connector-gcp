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
"""Whether a deployed BigQuery run is a usable recovery trial, and why not."""

from ..common import INCONCLUSIVE, USABLE

# The evidence event carrying the completed record. Both sides spell it from
# here: the exercise writes it and the offline analyzer reads it, and a rename
# reaching only one side would make the analyzer report missing evidence
# rather than fail.
COMPLETE_STAGE = "complete"
COMPLETE_EVENT = "recovery-" + COMPLETE_STAGE


def sequence_verdict(recovery):
    """A fault-sequence run's verdict: its data alone (ADR-0204).

    `usable` says the run completed and the query oracle passed: every input
    sequence in its table and no invalid row, duplicates being reported in
    ALO mode and failing the oracle in EO and FILE_LOADS modes.
    Interruptions, restarts, timings and metrics decide nothing.
    """
    record = recovery if isinstance(recovery, dict) else {}
    outcomes = record.get("outcomes")
    outcomes = outcomes if isinstance(outcomes, dict) else {}
    reasons = []
    if record.get("stage") != COMPLETE_STAGE:
        reasons.append("recovery-incomplete")
    query = outcomes.get("query")
    report = query.get("report") if isinstance(query, dict) else None
    if not isinstance(report, dict) or report.get("verdict") != "pass":
        reasons.append("oracle-not-passed")
    return {"verdict": INCONCLUSIVE if reasons else USABLE, "reasons": reasons}
