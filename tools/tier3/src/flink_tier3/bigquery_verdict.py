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
"""Whether a deployed BigQuery run is a usable measurement, and why not."""

from .bigquery_observe import MEMORY_METRICS, SINK_FAMILIES
from .common import INCONCLUSIVE, USABLE
from .metrics import metric_name

# What the run has to have observed to carry the measurement claim: the sink
# families #1312 asks for, and the TaskManager memory beside them. The verdict
# says whether each was read, never what it read. A throughput or a memory
# figure is a finding to publish, not a threshold to pass — reusing one as a
# target is what that issue forbids.
MEMORY = "memory"
OBSERVED = (*sorted(SINK_FAMILIES), MEMORY)
# The steady window and the post-recovery window. Each must have read every
# family on its own: a post-recovery figure with no baseline to compare it
# against is not the comparison the measurement exists to make, and a baseline
# whose reading actually came from mid-failover is not a steady state.
SAMPLED_STAGES = ("baseline", "finishing")
# The evidence event carrying the completed record, and the event each sample
# emits. Both sides spell them from here: the exercise writes them and the
# offline analyzer reads them, and a rename reaching only one side would make
# the analyzer report missing evidence rather than fail.
COMPLETE_STAGE = "complete"
COMPLETE_EVENT = "recovery-" + COMPLETE_STAGE
MEASUREMENT_EVENT = "bigquery-measurement"


def _returned(reading, key, wanted, name=metric_name):
    """Which of the wanted metric names one endpoint's answer actually carried.

    Credit the response rather than the request. Discovery lists a metric id
    once and never drops it, so asking for a gauge whose operator no longer
    publishes it still succeeds, with that gauge simply absent from the
    answer — and a family counted from the listing would be reported as
    observed when only its name was ever obtained.

    A vertex metric's id carries its operator name and is stripped back to the
    metric; a TaskManager's is the documented name itself, so it is compared
    whole.
    """
    value = reading.get(key)
    if not isinstance(value, list):
        return frozenset()
    return frozenset(
        name(item["id"])
        for item in value
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    ) & frozenset(wanted)


def _read(reading):
    """Which families one poll actually returned a value for."""
    names = _returned(
        reading, "sink", (name for ids in SINK_FAMILIES.values() for name in ids)
    )
    observed = {family for family, ids in SINK_FAMILIES.items() if names & ids}
    managers = reading.get("taskmanagers")
    if isinstance(managers, list) and any(
        _returned(manager, "metrics", MEMORY_METRICS, name=str)
        for manager in managers
        if isinstance(manager, dict)
    ):
        observed.add(MEMORY)
    return observed


def summarize(previous, stage, reading):
    """Fold one sample into its stage's coverage, keeping what it ever read.

    Coverage is monotonic within a stage and never crosses one: a family read
    during the baseline is read, even if a later baseline sample lost it to a
    restart, but it says nothing about the post-recovery window.
    """
    coverage = {
        name: dict(window)
        for name, window in previous.items()
        if isinstance(window, dict)
    }
    window = coverage.setdefault(stage, {"attempts": 0, "observed": []})
    window["attempts"] += 1
    window["observed"] = sorted(set(window["observed"]) | _read(reading))
    return coverage


def verdict(recovery):
    """The run's verdict and the reasons behind it, from its recovery record.

    `usable` says the instrument worked: both recoveries proved, the oracle
    passed, and every observation family read in both sampled windows.
    Anything short of that is `inconclusive` — a run that happened and cannot
    carry the claim — which is the same distinction the Cloud Tasks analyzer
    draws. The exercise decides it with the transition that completes the run;
    the offline analyzer recomputes it from the exported record, which is
    untrusted JSON, so every field is checked for its type before it is read.

    The recomputation is against the rule set in this file as it stands, not
    the one the Pod ran. Evidence re-analyzed after the required families or
    the sampled windows here change can disagree with its own stored verdict
    for that reason alone, and the analyzer cannot tell that from a forgery,
    so archived evidence is read with the revision that produced it.
    """
    record = recovery if isinstance(recovery, dict) else {}
    outcomes = record.get("outcomes")
    outcomes = outcomes if isinstance(outcomes, dict) else {}
    coverage = record.get("coverage")
    coverage = coverage if isinstance(coverage, dict) else {}
    reasons = []
    if record.get("stage") != COMPLETE_STAGE:
        reasons.append("recovery-incomplete")
    for proof in ("upgrade", "failover"):
        if proof not in outcomes:
            reasons.append("missing-" + proof)
    query = outcomes.get("query")
    report = query.get("report") if isinstance(query, dict) else None
    if not isinstance(report, dict) or report.get("verdict") != "pass":
        reasons.append("oracle-not-passed")
    for stage in SAMPLED_STAGES:
        window = coverage.get(stage)
        window = window if isinstance(window, dict) else {}
        attempts = window.get("attempts")
        observed = window.get("observed")
        observed = observed if isinstance(observed, list) else []
        # A window that took no sample observed nothing, whatever it claims.
        if not isinstance(attempts, int) or attempts < 1:
            reasons.append("unsampled-" + stage)
            observed = []
        reasons.extend(
            f"unobserved-{name}-in-{stage}" for name in OBSERVED if name not in observed
        )
    return {"verdict": INCONCLUSIVE if reasons else USABLE, "reasons": reasons}
