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
"""Whether a deployed Cloud Tasks run is a usable recovery trial, and why not."""

from ..common import INCONCLUSIVE, USABLE

# The evidence event carrying the completed record and the event each listing
# page emits. Both sides spell them from here: the exercise writes them and
# the offline analyzer reads them.
COMPLETE_STAGE = "complete"
COMPLETE_EVENT = "recovery-" + COMPLETE_STAGE
PAGE_EVENT = "cloudtasks-task-page"


def sequence_verdict(recovery):
    """A fault-sequence run's verdict: its data alone (ADR-0204).

    `usable` says the run completed, the queue read back paused and the
    oracle passed: every input sequence listed once as its own task with its
    own body, nothing else listed and nothing dispatched, which is the
    exactly-once rule with each record's value checked.
    Interruptions, restarts, timings and metrics decide nothing.
    """
    record = recovery if isinstance(recovery, dict) else {}
    outcomes = record.get("outcomes")
    outcomes = outcomes if isinstance(outcomes, dict) else {}
    reasons = []
    if record.get("stage") != COMPLETE_STAGE:
        reasons.append("recovery-incomplete")
    listing = outcomes.get("listing")
    listing = listing if isinstance(listing, dict) else {}
    if listing.get("queue_state") != "PAUSED":
        reasons.append("queue-not-paused")
    report = listing.get("report")
    if not isinstance(report, dict) or report.get("verdict") != "pass":
        reasons.append("oracle-not-passed")
    return {"verdict": INCONCLUSIVE if reasons else USABLE, "reasons": reasons}
