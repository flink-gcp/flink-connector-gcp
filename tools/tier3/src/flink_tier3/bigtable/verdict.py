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
"""Whether a deployed Bigtable run is a usable recovery trial, and why not."""

from ..common import INCONCLUSIVE, USABLE

# The evidence event carrying the completed record. Both sides spell it from
# here: the exercise writes it and the offline analyzer reads it.
COMPLETE_STAGE = "complete"
COMPLETE_EVENT = "recovery-" + COMPLETE_STAGE
# The components the oracles judge, each by its own report under this name.
COMPONENTS = ("eo", "alo", "relay")


def sequence_verdict(recovery):
    """A fault-sequence run's verdict: its data alone (ADR-0204).

    `usable` says the run completed and all three oracles passed, the relay's
    having been judged: an input that finished too close to the change
    stream's end leaves the relay unjudged, since a commit after the end is
    not relayed. Interruptions, restarts, timings and metrics decide nothing.
    """
    record = recovery if isinstance(recovery, dict) else {}
    outcomes = record.get("outcomes")
    outcomes = outcomes if isinstance(outcomes, dict) else {}
    reasons = []
    if record.get("stage") != COMPLETE_STAGE:
        reasons.append("recovery-incomplete")
    oracles = outcomes.get("oracles")
    oracles = oracles if isinstance(oracles, dict) else {}
    for component in COMPONENTS:
        report = oracles.get(component)
        report = report if isinstance(report, dict) else {}
        if component == "relay" and report.get("judged") is not True:
            reasons.append(component + "-oracle-unjudged")
        elif report.get("verdict") != "pass":
            reasons.append(component + "-oracle-not-passed")
    return {"verdict": INCONCLUSIVE if reasons else USABLE, "reasons": reasons}
