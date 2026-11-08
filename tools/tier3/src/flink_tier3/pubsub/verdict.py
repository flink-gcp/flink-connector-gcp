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
from ..exercise import FAULTS, REPLACEMENT_KINDS, RESCALE_KINDS
from ..metrics import coverage_reasons, fold_coverage, metric_name
from .observe import FAMILIES

COMPLETE_STAGE = "complete"
# The evidence events the exercise writes and the offline analysis reads.
COMPLETE_EVENT = "recovery-" + COMPLETE_STAGE
MEASUREMENT_EVENT = "pubsub-measurement"
REPLACEMENTS = tuple(REPLACEMENT_KINDS.values())
RESCALES = tuple(RESCALE_KINDS.values())
# The AdaptiveScheduler kinds (#2055, reworked by #2126), which only the
# Pub/Sub scenario runs. The scheduler restarts the whole job from its latest
# checkpoint for every rescale and failure. Each kind produces one case of
# restore deduplication (#2054) on purpose and runs only with the setting on:
# with it, a trial that shows the case shows the setting's effect, and one
# with the setting off would show only that the case duplicates.
# - `reactive-scale-out` runs in the Operator's standalone mode under reactive
#   mode and patches the TaskManager replica count, which the Operator applies
#   in place; the scheduler restarts the job when its next checkpoint
#   completes, the checkpoint-triggered rescale whose cancellation races the
#   completion notification.
# - `reactive-tm-kill`, in the same mode, deletes a TaskManager Pod without a
#   grace period once a checkpoint covering replay-cohort input has completed:
#   a failure that lands before the notification ran.
# - `adaptive-scale-in` runs in native mode under the AdaptiveScheduler
#   without reactive mode and patches the job vertex's parallelism override,
#   which the Operator applies in place through the resource-requirements API,
#   as an autoscaler does; the scheduler again rescales when its next
#   checkpoint completes. Reactive mode refuses that API, so this is the
#   checkpoint-triggered scale-in.
# `initial` and `recovery` are the job vertex's parallelism before and after
# the fault, which a reactive kind sets through its TaskManagers.
# `hold_millis` is how long the relay holds the first replay-cohort record
# after each snapshot of an attempt that restored nothing, which keeps that
# checkpoint's notification queued on the task thread; the fault must land
# within it for the trial to show its case.
# `kubernetes/lifecycle/pubsub.cue` renders the same sizes and holds, and a
# test holds it to this table.
ADAPTIVE = {
    "reactive-scale-out": {
        "scheduler": "reactive",
        "fault": "scale",
        "initial": 2,
        "recovery": 3,
        "hold_millis": 30000,
    },
    "reactive-tm-kill": {
        "scheduler": "reactive",
        "fault": "kill",
        "initial": 3,
        "recovery": 3,
        "hold_millis": 90000,
    },
    "adaptive-scale-in": {
        "scheduler": "adaptive",
        "fault": "override",
        "initial": 3,
        "recovery": 2,
        "hold_millis": 30000,
    },
}
# The AdaptiveScheduler kinds' delay per replay-cohort input and flow-control limit,
# which keep the replay cohort flowing while the fault lands; the Pub/Sub
# application README's "Restore-deduplication trials" section gives the
# reasons.
ADAPTIVE_REPLAY_DELAY_MILLIS = 100
ADAPTIVE_MAX_OUTSTANDING = 100000
# The flow-control limit every other kind renders: the SDK's default.
MAX_OUTSTANDING = 1000


def sizes(kind):
    """The job vertex's parallelism in a kind's initial and recovery phases."""
    if kind in ADAPTIVE:
        return ADAPTIVE[kind]["initial"], ADAPTIVE[kind]["recovery"]
    return FAULTS[kind].initial, FAULTS[kind].upgrade


def application_pods(kind):
    """The Pods admission lets a kind's application hold at once.

    The JobManager, the most TaskManagers either manifest runs, and one
    replacement: a terminating Pod holds its quota until it is gone, so a
    replaced Pod and its replacement overlap.
    """
    return 2 + max(sizes(kind))


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
    if kind in ADAPTIVE:
        # An AdaptiveScheduler restart restores a checkpoint, so what it displaced is
        # redelivered and the oracle's completeness counts it; what the
        # restored checkpoint already covered is the oracle's covered count.
        # The record carries the size the exercise read, which an offline
        # recomputation checks again.
        reasons = []
        resized = recovery.get("parallelism")
        if type(resized) is not int or resized != ADAPTIVE[kind]["recovery"]:
            reasons.append("rescale-unproven")
        if not held(recovery.get("first_restored"), recovery.get("hold")):
            reasons.append("case-unproduced")
        if not suppressed(after.get("deduplication")):
            reasons.append("suppression-unobserved")
        return reasons
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


def held(restored, hold):
    """Whether the case's restore was of a checkpoint whose notification was held.

    The relay holds the first replay-cohort record after a snapshot for
    `hold_millis`, and logs it, so until then the checkpoint's completion
    notification is queued behind that record. The case is the first restore
    after the fault: it must restore that checkpoint, at least one relay
    attempt must have logged a hold after it, and the fault must come before
    the hold ran out. The hold is counted from the checkpoint's trigger,
    which precedes every subtask's snapshot, so the bound errs short.
    `event_at` is the deletion for a TaskManager kill and the restore for a
    rescale, whose cancellation precedes it.
    """
    if not isinstance(restored, dict) or not isinstance(hold, dict):
        return False
    values = [
        hold.get(key)
        for key in ("checkpoint", "triggered_at", "hold_millis", "relay_holds")
    ]
    event = hold.get("event_at")
    if any(type(value) is not int for value in values) or type(event) not in (
        int,
        float,
    ):
        return False
    checkpoint, triggered, millis, holds = values
    return (
        type(restored.get("id")) is int
        and restored["id"] == checkpoint
        and holds > 0
        and millis > 0
        and triggered <= event < triggered + millis
    )


def suppressed(reading):
    """Whether the restored job's ledger suppressed at least one redelivery.

    Without one, the restored checkpoint either covered no unacknowledged
    input or Pub/Sub redelivered none of it, and the trial showed nothing
    about the setting.
    """
    if not isinstance(reading, dict):
        return False
    count = reading.get("redeliveriesSuppressed")
    return type(count) in (int, float) and math.isfinite(count) and count > 0


def started_by_fault(attempt, restored, phase, before, kind):
    """Whether an attempt is one the fault started, from restored state.

    A replacement or an AdaptiveScheduler restart starts attempts in the same phase; a
    savepoint rescale's are the upgraded job's. The exercise and the offline analysis ask the same thing.
    """
    return (
        attempt not in before
        and restored is True
        and (kind in REPLACEMENTS or kind in ADAPTIVE or phase == "upgrade")
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
