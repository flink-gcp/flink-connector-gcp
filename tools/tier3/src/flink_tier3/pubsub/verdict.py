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
"""Whether a deployed Pub/Sub recovery trial carries its claim, and why not."""

import math

from ..common import INCONCLUSIVE, USABLE
from ..metrics import coverage_reasons, fold_coverage, metric_name
from .meter import malformed
from .observe import FAMILIES

COMPLETE_STAGE = "complete"
REQUEST_BUDGET = "request-budget-exhausted"
# The evidence events the exercise writes and the offline analysis reads.
COMPLETE_EVENT = "recovery-" + COMPLETE_STAGE
MEASUREMENT_EVENT = "pubsub-measurement"
REPLACEMENTS = ("jm-replacement", "tm-replacement")
RESCALES = ("rescale-out", "rescale-in")
# The windows a sample counts toward. The stages before the fault are one
# window, because the first of them can end within a poll of the job starting,
# before its operators have registered anything; the window after recovery is
# the last cohort's. Samples taken while recovering count toward neither.
WINDOWS = {
    "baseline": "before",
    "checkpoint": "before",
    "boundary": "before",
    "after": "after",
}
SAMPLED_WINDOWS = tuple(dict.fromkeys(WINDOWS.values()))
BACKPRESSURE = "backpressure"
OBSERVED = (BACKPRESSURE, *sorted(FAMILIES))
OUTCOMES = ("checkpoint", "fault", "recovery", "after")
# What Flink's aggregated subtask metrics answer carries for a returned id.
AGGREGATE_KEYS = ("min", "max", "avg", "sum", "skew")


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _read(reading):
    """Which families one sample actually returned a value for.

    The answer is credited, not the request: a listed metric id can come back
    absent, an endpoint that failed is named `unavailable` in the sample, and
    an id is read only where the answer carries a finite number for one of
    its aggregates.
    """
    observed = set()
    vertices = reading.get("vertices")
    for vertex in vertices if isinstance(vertices, list) else ():
        if not isinstance(vertex, dict):
            continue
        pressure = vertex.get("backpressure")
        if isinstance(pressure, dict) and isinstance(
            pressure.get("backpressureLevel"), str
        ):
            observed.add(BACKPRESSURE)
        metrics = vertex.get("metrics")
        names = {
            metric_name(item["id"])
            for item in (metrics if isinstance(metrics, list) else ())
            if isinstance(item, dict)
            and isinstance(item.get("id"), str)
            and any(_number(item.get(key)) for key in AGGREGATE_KEYS)
        }
        observed.update(family for family, ids in FAMILIES.items() if names & ids)
    return observed


def summarize(previous, stage, reading):
    """Fold one sample into its stage's window; a stage no window names counts nowhere."""
    return fold_coverage(previous, WINDOWS.get(stage), _read(reading))


def _replay(kind, recovery, after):
    """Why the recovery outcome does not prove the trial's replay, if it does not.

    A replacement must have seen what the fault displaced reappear: a trial
    whose boundary did not hold restored a checkpoint that already covered the
    replay cohort, so it exercised no redelivery, and is inconclusive rather
    than failed. A replay under new input message IDs came from a duplicate
    publication, not from the service redelivering what was leased. A rescale
    expects none, because its savepoint acknowledges what it covers; whether
    the service redelivers some of it anyway is unmeasured until #1435, so any
    replay-cohort output from the attempts it started leaves it inconclusive.
    """
    replay = recovery.get("replay")
    if kind in RESCALES:
        reasons = [] if replay == "not-expected" else ["replay-unexpected"]
        replayed = after.get("replay_by_new_attempts")
        if type(replayed) is not int or replayed != 0:
            reasons.append("replay-after-savepoint")
        return reasons
    if kind not in REPLACEMENTS:
        return ["unknown-trial"]
    if replay != "observed":
        return ["replay-unobserved"]
    expected = recovery.get("expected_replay")
    if type(expected) is not int or expected < 1:
        return ["replay-empty"]
    reasons = []
    replayed = recovery.get("replayed")
    if type(replayed) is not int or replayed != expected:
        reasons.append("replay-incomplete")
    # Asked at recovery and again over everything collected by the end.
    if (
        recovery.get("replay_ids_preserved") is not True
        or after.get("replay_ids_preserved") is not True
    ):
        reasons.append("replay-ids-not-preserved")
    return reasons


def started_by_fault(attempt, restored, phase, before, kind):
    """Whether an attempt is one the fault started, from restored state.

    A replacement restarts attempts in the same phase; a rescale's are the
    upgraded job's. The exercise and the offline analysis ask the same thing.
    """
    return (
        attempt not in before
        and restored is True
        and (kind in REPLACEMENTS or phase == "upgrade")
    )


def oracle_reasons(oracle):
    """Why the oracle's account does not carry the claim, if it does not."""
    if not isinstance(oracle, dict):
        return ["oracle-missing"]
    # An absent refusal is not an acceptance, and `False == 0` in Python, so
    # the count is compared with its type.
    if "rejected" not in oracle or oracle["rejected"] is not None:
        return ["oracle-rejected"]
    missing = oracle.get("missing_inputs")
    if type(missing) is not int or missing != 0:
        return ["oracle-incomplete"]
    return []


def request_reasons(requests, limit):
    """Why the run's request meter withholds success from its receipt.

    A run whose meter reached the ceiling stopped admitting work on it, so its
    evidence ends where the meter stopped it; a meter that is missing, not
    one the meter could have written for the approved `limit`, or begun by
    an actor other than the dispatching runner, did not count the whole run.
    """
    if not isinstance(requests, dict):
        return ["request-meter-missing"]
    if malformed(requests, limit):
        return ["request-meter-malformed"]
    if requests.get("incomplete") is not False:
        return ["request-meter-incomplete"]
    if requests.get("exhausted") is not False:
        return [REQUEST_BUDGET]
    return []


def verdict(recovery):
    """The trial's verdict and the reasons behind it, from its recovery record.

    `usable` says the trial carries its claim: it completed, the output oracle
    accepted every collected line and found every logical input of both
    subscriptions, the fault's replay population behaved as the trial requires,
    and each observation family was read before the fault and after recovery.
    Anything short of that is `inconclusive`. The oracle's duplicate counters
    are reported beside it and decide nothing, and no ordering is claimed.

    The record is durable JSON that nothing types on the way in, so every
    field is checked for its type before it is read.
    """
    record = recovery if isinstance(recovery, dict) else {}
    outcomes = record.get("outcomes")
    outcomes = outcomes if isinstance(outcomes, dict) else {}
    reasons = []
    if record.get("stage") != COMPLETE_STAGE:
        reasons.append("recovery-incomplete")
    reasons.extend(
        "missing-" + name
        for name in OUTCOMES
        if not isinstance(outcomes.get(name), dict)
    )
    fault = outcomes.get("fault")
    recovery_outcome = outcomes.get("recovery")
    after = outcomes.get("after")
    if all(isinstance(value, dict) for value in (fault, recovery_outcome, after)):
        reasons.extend(_replay(fault.get("kind"), recovery_outcome, after))
    reasons.extend(oracle_reasons(record.get("oracle")))
    reasons.extend(coverage_reasons(record.get("coverage"), SAMPLED_WINDOWS, OBSERVED))
    return {"verdict": INCONCLUSIVE if reasons else USABLE, "reasons": reasons}
