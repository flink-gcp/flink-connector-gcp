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
from ..metrics import (
    WINDOWS,
    coverage_reasons,
    families_read,
    fold_coverage,
    observed_families,
    window_of,
)
from .observe import COMPONENTS, FAMILIES

# The evidence event carrying the completed record and the event each
# measurement emits. Both sides spell them from here: the exercise writes
# them and the offline analyzer reads them.
COMPLETE_STAGE = "complete"
COMPLETE_EVENT = "recovery-" + COMPLETE_STAGE
MEASUREMENT_EVENT = "bigtable-measurement"
# Every component's families and checkpoint share, and the job's memory. The
# verdict says whether each was read, never what it read.
OBSERVED = observed_families(FAMILIES)
SAMPLED_STAGES = WINDOWS


def summarize(previous, stage, reading):
    """Fold one sample into its window's coverage, keeping what it ever read."""
    return fold_coverage(
        previous, window_of(reading, stage), families_read(reading, FAMILIES)
    )


def verdict(recovery):
    """The run's verdict and the reasons behind it, from its recovery record.

    `usable` says both recoveries proved, each component's oracle passed — the
    relay's only when the input finished before the change stream's end — and
    every family was read in the baseline and in the windows after each
    recovery. The offline analyzer recomputes it from the exported record and
    measurements, which are untrusted JSON, so every field is checked for its
    type before it is read.
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
    oracles = outcomes.get("oracles")
    oracles = oracles if isinstance(oracles, dict) else {}
    for component in COMPONENTS:
        report = oracles.get(component)
        report = report if isinstance(report, dict) else {}
        # Only the relay is judged conditionally, and only a judgment the
        # report records counts.
        if component == "relay" and report.get("judged") is not True:
            reasons.append(component + "-oracle-unjudged")
        elif report.get("verdict") != "pass":
            reasons.append(component + "-oracle-not-passed")
    reasons.extend(coverage_reasons(record.get("coverage"), SAMPLED_STAGES, OBSERVED))
    return {"verdict": INCONCLUSIVE if reasons else USABLE, "reasons": reasons}
