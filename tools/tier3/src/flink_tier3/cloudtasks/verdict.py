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
"""Why a Cloud Tasks run's data withholds `usable` (ADR-0204)."""

from ..verdict import outcome

# The event each listing page emits. Both sides spell it from here: the
# exercise writes it and the offline analyzer reads it.
PAGE_EVENT = "cloudtasks-task-page"


def data_reasons(record):
    """Whether the queue read back paused and the listing's oracle passed.

    The oracle passes with every input sequence listed once as its own task
    with its own body, nothing else listed and nothing dispatched, which is
    the exactly-once rule with each record's value checked.
    """
    listing = outcome(record, "listing")
    reasons = []
    if listing.get("queue_state") != "PAUSED":
        reasons.append("queue-not-paused")
    report = listing.get("report")
    if not isinstance(report, dict) or report.get("verdict") != "pass":
        reasons.append("oracle-not-passed")
    return reasons
