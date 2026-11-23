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
"""Whether a fault-sequence run is a usable recovery trial, and why not.

Every scenario decides its verdict the same way (ADR-0204): the run must have
completed, and its data must carry the claim. A scenario names only the
reasons its own data gives, which `sequence_verdict` adds after completion's.
"""

from .common import INCONCLUSIVE, USABLE

# The stage that completes a run, and the evidence event carrying its record.
# Both sides spell them from here: the exercise names each stage's event with
# `event_name` and the offline analyzer reads `COMPLETE_EVENT`, and a rename
# reaching only one side would make the analyzer report missing evidence rather
# than fail.
COMPLETE_STAGE = "complete"


def event_name(stage):
    """The evidence event that carries the record a stage's transition writes."""
    return "recovery-" + stage


COMPLETE_EVENT = event_name(COMPLETE_STAGE)


def sequence_verdict(recovery, data_reasons):
    """A fault-sequence run's verdict: its completion and its data alone.

    `data_reasons` takes the record, an empty one when `recovery` is not an
    object, and returns why the scenario's data does not carry the claim.
    Interruptions, restarts, timings and metrics decide nothing.
    """
    record = recovery if isinstance(recovery, dict) else {}
    reasons = [] if record.get("stage") == COMPLETE_STAGE else ["recovery-incomplete"]
    reasons.extend(data_reasons(record))
    return {"verdict": INCONCLUSIVE if reasons else USABLE, "reasons": reasons}


def outcome(record, key):
    """One entry of the record's outcomes, or an empty one when it is not an object."""
    outcomes = record.get("outcomes")
    value = outcomes.get(key) if isinstance(outcomes, dict) else None
    return value if isinstance(value, dict) else {}


def _positive(value):
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def component_reasons(record, components, relay=None, definitive=()):
    """Which of the record's oracle reports, one per component, did not pass.

    The `relay` component, when there is one, is judged only when the input
    finished long enough before the change stream's end and no restore
    followed it, and its report says whether it was. One that was not judged
    may lack rows only because timing cut them off, so it is reported as
    unjudged, and the run can run again as it is. It reads as not passed when
    its report counts a failure that timing cannot explain: any of the counts
    `definitive` names, each read only as a positive whole number.
    """
    oracles = outcome(record, "oracles")
    reasons = []
    for component in components:
        report = oracles.get(component)
        report = report if isinstance(report, dict) else {}
        if component == relay and report.get("judged") is not True:
            failed = any(_positive(report.get(kind)) for kind in definitive)
            reasons.append(
                component + ("-oracle-not-passed" if failed else "-oracle-unjudged")
            )
        elif report.get("verdict") != "pass":
            reasons.append(component + "-oracle-not-passed")
    return reasons
