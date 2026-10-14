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
from ..metrics import (
    checkpoint_read,
    coverage_reasons,
    fold_coverage,
    memory_read,
    returned,
)
from .observe import SINK_FAMILIES

# The evidence event carrying the completed record, the event each listing
# page emits and the event each measurement emits. Both sides spell them from
# here: the exercise writes them and the offline analyzer reads them.
COMPLETE_STAGE = "complete"
COMPLETE_EVENT = "recovery-" + COMPLETE_STAGE
PAGE_EVENT = "cloudtasks-task-page"
MEASUREMENT_EVENT = "cloudtasks-measurement"
# What the run has to have read to carry the measurement claim: the families
# the BigQuery trials read. The verdict says whether each was read, never what
# it read; a figure is a finding, not a threshold.
MEMORY = "memory"
CHECKPOINT = "checkpoint"
OBSERVED = (*sorted(SINK_FAMILIES), MEMORY, CHECKPOINT)
# The baseline and the windows after each recovery, each of which must have
# read every family on its own: a figure after a recovery is compared with
# the baseline, and the two recoveries are different faults. A sample names
# the window it was taken in, which the exercise decides from its stage and
# the window's span.
SAMPLED_STAGES = ("baseline", "upgraded", "finishing")


def _read(reading):
    """Which families one poll actually returned a value for."""
    names = returned(
        reading, "sink", (name for ids in SINK_FAMILIES.values() for name in ids)
    )
    observed = {family for family, ids in SINK_FAMILIES.items() if names & ids}
    if checkpoint_read(reading, ("sink",)):
        observed.add(CHECKPOINT)
    if memory_read(reading):
        observed.add(MEMORY)
    return observed


def summarize(previous, stage, reading):
    """Fold one sample into its window's coverage, keeping what it ever read.

    The sample names its window, or none when it was taken outside every
    window; `stage` is only where the shared rebuild found it, and a stage
    and a window can share a name without being the same span.
    """
    window = reading.get("window")
    return fold_coverage(
        previous, window if isinstance(window, str) else None, _read(reading)
    )


def verdict(recovery):
    """The run's verdict and the reasons behind it, from its recovery record.

    `usable` says both recoveries proved, the queue stayed paused, the
    oracle passed — every input sequence listed once as its own task, nothing
    else listed and nothing dispatched — and every family was read in the
    baseline and in the windows after each recovery. The exercise decides it
    with the transition that completes the run; the offline analyzer
    recomputes it from the exported record, pages and measurements, which are
    untrusted JSON, so every field is checked for its type before it is read.

    The recomputation is against the rules in this file as they stand. The
    first trial, `ct-1362-a1`, ran before the families were read, so its
    archived evidence recomputes to `inconclusive` against them; archived
    evidence is read with the revision that produced it.
    """
    record = recovery if isinstance(recovery, dict) else {}
    outcomes = record.get("outcomes")
    outcomes = outcomes if isinstance(outcomes, dict) else {}
    reasons = []
    if record.get("stage") != COMPLETE_STAGE:
        reasons.append("recovery-incomplete")
    for proof in ("upgrade", "failover"):
        if proof not in outcomes:
            reasons.append("missing-" + proof)
    listing = outcomes.get("listing")
    listing = listing if isinstance(listing, dict) else {}
    if listing.get("queue_state") != "PAUSED":
        reasons.append("queue-not-paused")
    report = listing.get("report")
    if not isinstance(report, dict) or report.get("verdict") != "pass":
        reasons.append("oracle-not-passed")
    reasons.extend(coverage_reasons(record.get("coverage"), SAMPLED_STAGES, OBSERVED))
    return {"verdict": INCONCLUSIVE if reasons else USABLE, "reasons": reasons}
