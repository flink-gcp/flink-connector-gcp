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
"""Whether a deployed Spanner run is a usable recovery trial, and why not."""

from ..common import INCONCLUSIVE, USABLE
from ..metrics import (
    WINDOWS,
    coverage_reasons,
    families_read,
    fold_coverage,
    observed_families,
    window_of,
)
from .observe import (
    COMPONENTS,
    FAMILIES,
    partitions_discovered,
    partitions_reread,
    watermark,
)

# The evidence event carrying the completed record and the event each
# measurement emits. Both sides spell them from here: the exercise writes
# them and the offline analyzer reads them.
COMPLETE_STAGE = "complete"
COMPLETE_EVENT = "recovery-" + COMPLETE_STAGE
MEASUREMENT_EVENT = "spanner-measurement"
# Every component's families and checkpoint share, and the job's memory. The
# verdict says whether each was read, never what it read.
OBSERVED = observed_families(FAMILIES)
SAMPLED_STAGES = WINDOWS
# The figures each window keeps the greatest reading of: the Change Streams
# source's watermark, which its oracle requires to advance after each
# recovery, and the two partition counts the reports carry.
MAXIMA = {
    "watermark": watermark,
    "partitions_reread": partitions_reread,
    "partitions_discovered": partitions_discovered,
}
# Flink's watermark before the first one, Long.MIN_VALUE.
NO_WATERMARK = -(2**63)


def summarize(previous, stage, reading):
    """Fold one sample into its window's coverage, keeping what it ever read.

    The window also keeps the greatest of each figure in `MAXIMA` that a
    reading returned, so the offline analyzer rebuilds them with the
    coverage.
    """
    window = window_of(reading, stage)
    coverage = fold_coverage(previous, window, families_read(reading, FAMILIES))
    if window is None:
        return coverage
    entry = coverage[window]
    maxima = dict(entry.get("maxima") or {})
    for name, read in MAXIMA.items():
        value = read(reading)
        if value is not None:
            maxima[name] = max(maxima.get(name, value), value)
    entry["maxima"] = maxima
    return coverage


def maxima(coverage, name):
    """Each window's greatest reading of one figure, None where it read none."""
    coverage = coverage if isinstance(coverage, dict) else {}
    result = {}
    for window in SAMPLED_STAGES:
        entry = coverage.get(window)
        figures = entry.get("maxima") if isinstance(entry, dict) else None
        value = figures.get(name) if isinstance(figures, dict) else None
        result[window] = value if type(value) in (int, float) else None
    return result


def watermark_advanced(coverage):
    """Whether the watermark advanced after each recovery, or None unread.

    The window after each recovery must read a greater watermark than the
    window before it, and the baseline one at all. A window that read none
    leaves the rule unjudged, never passed.
    """
    figures = maxima(coverage, "watermark")
    if any(value is None for value in figures.values()):
        return None
    return (
        NO_WATERMARK < figures["baseline"] < figures["upgraded"] < figures["finishing"]
    )


def verdict(recovery):
    """The run's verdict and the reasons behind it, from its recovery record.

    `usable` says both recoveries proved, each component's oracle passed — the
    Change Streams one judged only when the input finished before the change
    stream's end and every window read the source's watermark, which must
    have advanced — and every family was read in the baseline and in the
    windows after each recovery. The offline analyzer recomputes it from the
    exported record and measurements, which are untrusted JSON, so every
    field is checked for its type before it is read.
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
    advanced = watermark_advanced(record.get("coverage"))
    for component in COMPONENTS:
        report = oracles.get(component)
        report = report if isinstance(report, dict) else {}
        # Only the Change Streams oracle is judged conditionally, and only a
        # judgment the report records counts.
        if component == "change_stream" and (
            report.get("judged") is not True or advanced is None
        ):
            reasons.append(component + "-oracle-unjudged")
        elif report.get("verdict") != "pass" or (
            component == "change_stream" and advanced is not True
        ):
            reasons.append(component + "-oracle-not-passed")
    reasons.extend(coverage_reasons(record.get("coverage"), SAMPLED_STAGES, OBSERVED))
    return {"verdict": INCONCLUSIVE if reasons else USABLE, "reasons": reasons}
