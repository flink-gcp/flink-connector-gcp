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
"""Per-cell rows and per-group verdicts under the preregistered #1246 rule.

The single-host analyzer, ``vmanalyze``, decides which runs are cells and what
window each one observed; this module turns a cell's rows into throughput and
p95, and groups usable repetitions into the verdict labels of
``docs/adr/evidence/0162-cloudtasks-assessment-1246.md``. Probe cells never
reach it: the capacity search was dropped from the assessment, and ``vmanalyze``
lists them without analyzing them. Every ratio stays a ``Fraction`` until
rendering.
"""

from __future__ import annotations

import math
import re
from array import array
from fractions import Fraction

from ..common import INCONCLUSIVE, USABLE
from .evidence import RECEIPT, Row, iter_rows
from .protocol import ARMS, SHAPES

SUFFIX = re.compile(r"-(x2|q[0-9]+)\Z")
PROBE = re.compile(r"-q([0-9]+)\Z")
CALIBRATION_CELL = re.compile(r"k[0-9]{2}-")
INTERRUPT_SUFFIX = "-k11"
BASELINE = "UNNAMED"
CANDIDATES = ("STAGED_HASH", "STAGED_RANDOM")
INCREMENTAL = {"STAGED_HASH": "NAMED_HASH", "STAGED_RANDOM": "NAMED_RANDOM_CONTROL"}
REPETITIONS = 3
# Thresholds of the preregistration; the protocol pin restates them as floats
# and the analyzer compares only these exact fractions.
GENERAL = (Fraction(7, 10), Fraction(2))
CONSTRAINED = (Fraction(1, 4), Fraction(4))
RANGE_MAX = Fraction(1, 10)
MIN_CHECKPOINTS = 6
HEX = re.compile(r"[0-9a-f]+\Z")
LABEL_ORDER = (INCONCLUSIVE, "decline", "constrained", "general")


def base_cell_id(cell_id):
    """The protocol cell id without a repeat (-x2) or probe (-qN) suffix."""
    return SUFFIX.sub("", cell_id)


def probe_index(cell_id):
    match = PROBE.search(cell_id)
    return int(match[1]) if match else None


def is_interrupt_control(cell_id):
    return cell_id.endswith(INTERRUPT_SUFFIX)


def is_calibration_cell(cell_id):
    return bool(CALIBRATION_CELL.match(cell_id)) or is_interrupt_control(cell_id)


def receipts_by_phase(receipts):
    """``(role, phase) -> [receipt]`` from the reconciliation's receipt names."""
    table = {}
    for name, receipt in receipts.items():
        match = RECEIPT.search(name)
        if match:
            role, _incarnation, phase = match.groups()
            table.setdefault((role, phase), []).append(receipt)
    return table


def creator_order(receipts):
    starts = receipts_by_phase(receipts).get(("creator", "start"), [])
    return [
        r["incarnation"]
        for r in sorted(starts, key=lambda r: (r["wall_millis"], r["incarnation"]))
    ]


def source_bounds(receipts):
    """Earliest source start and latest last-mapped wall millis (TM clock)."""
    table = receipts_by_phase(receipts)
    starts = [r["wall_millis"] for r in table.get(("source", "start"), [])]
    lasts = [r["wall_millis"] for r in table.get(("source", "last-mapped"), [])]
    return (min(starts) if starts else None, max(lasts) if lasts else None)


def observation_window(placed, completed_ids, start_ms, warmup_seconds, last_ms):
    """The preregistered window, or the reason there is none.

    ``start_ms`` and ``last_ms`` are wall millis of the source receipts, and
    the ``ack`` of each ``placed`` checkpoint is in seconds of the same clock;
    the caller names that clock.
    """
    if start_ms is None or last_ms is None:
        return None, "receipts-missing"
    warm_end = Fraction(start_ms, 1000) + warmup_seconds
    input_end = Fraction(last_ms, 1000)
    ordered = sorted(placed.values(), key=lambda c: c["id"])
    # Both edges are checkpoint completions, so the window covers whole
    # checkpoint intervals: the first completing at or after warm-up and the
    # last completing at or before the source's final mapped record.
    first = next((c for c in ordered if c["ack"] >= warm_end), None)
    last = next((c for c in reversed(ordered) if c["ack"] <= input_end), None)
    if first is None or last is None or last["id"] < first["id"]:
        return None, "window-too-short"
    inside = sorted(i for i in completed_ids if first["id"] <= i <= last["id"])
    if len(inside) < MIN_CHECKPOINTS or last["ack"] <= first["ack"]:
        return None, "window-too-short"
    return {
        "start": first["ack"],
        "end": last["ack"],
        "seconds": last["ack"] - first["ack"],
        "first_checkpoint": first["id"],
        "last_checkpoint": last["id"],
        "checkpoints_inside": len(inside),
        "boundaries": [placed[i]["ack"] for i in inside if i in placed],
    }, None


# A named arm's retry after a lost response is answered with ALREADY_EXISTS,
# which the reconciliation explains by the earlier attempt of the same
# sequence and name. The record was created, so it counts once; the unnamed
# baseline has no such answer and counts its retry's OK instead.
CREATED = ("OK", "ALREADY_EXISTS")


class _Ledger:
    """Eventual outcome per sequence in the reconciler's row order.

    Bounded state like ``evidence._Sequences``: arrays sized from the record
    limit, a dict past it. ``completed`` holds the completion instant of the
    eventual row in nanoseconds of the TaskManager wall clock.
    """

    def __init__(self, limit, arm="UNNAMED"):
        self.created = CREATED if arm != "UNNAMED" else ("OK",)
        self.limit = max(0, int(limit))
        self.ok = bytearray((self.limit + 7) // 8)
        self.origin = array("q", [0]) * self.limit
        self.completed = array("q", [0]) * self.limit
        self.beyond = {}

    def record(self, row: Row):
        completed = row.origin_millis * 1_000_000 + (
            row.completed_nanos - row.origin_nanos
        )
        ok = row.status in self.created
        if 0 <= row.sequence < self.limit:
            mask = 1 << (row.sequence & 7)
            if ok:
                self.ok[row.sequence >> 3] |= mask
            else:
                self.ok[row.sequence >> 3] &= ~mask & 0xFF
            self.origin[row.sequence] = row.origin_millis
            self.completed[row.sequence] = completed
        else:
            self.beyond[row.sequence] = (ok, row.origin_millis, completed)

    def ok_sequences(self):
        for sequence in range(self.limit):
            if self.ok[sequence >> 3] & (1 << (sequence & 7)):
                yield self.origin[sequence], self.completed[sequence]
        for ok, origin, completed in self.beyond.values():
            if ok:
                yield origin, completed


def name_class(name):
    segment = str(name).rsplit("/", 1)[-1]
    if HEX.fullmatch(segment) and len(segment) in (32, 64):
        return f"hex{len(segment)}"
    return "other"


def analyze_rows(run, cell, order, span):
    """Throughput inputs, p95 and completion instants from the cell's rows."""
    ledger = _Ledger(cell.get("record_limit", 0), cell.get("arm", "UNNAMED"))
    latencies = array("q")
    excluded = foreign = rows = 0
    names = {}
    start_ms = span["start"] * 1000 if span else None
    end_ms = span["end"] * 1000 if span else None
    stream = iter_rows(run.source, run.run_id, cell["id"], order=order)
    try:
        for _part, row in stream:
            rows += 1
            if (
                row.run_id != run.run_id
                or row.cell_id != cell["id"]
                or row.arm != cell["arm"]
            ):
                foreign += 1
                continue
            ledger.record(row)
            if row.status != "OK":
                continue
            kind = name_class(row.name)
            names[kind] = names.get(kind, 0) + 1
            if span is not None and start_ms <= row.origin_millis <= end_ms:
                if row.latency_nanos >= 0:
                    latencies.append(row.latency_nanos)
                else:
                    excluded += 1
    finally:
        stream.close()
    completions = array("q")
    distinct_ok = distinct_ok_inside = 0
    for origin, completed in ledger.ok_sequences():
        distinct_ok += 1
        completions.append(completed)
        if span is not None and start_ms <= origin <= end_ms:
            distinct_ok_inside += 1
    completions = array("q", sorted(completions))
    latencies = array("q", sorted(latencies))
    p95 = None
    if latencies:
        # Nearest rank: the ceil(0.95 n)-th smallest value.
        p95 = latencies[math.ceil(Fraction(95, 100) * len(latencies)) - 1]
    return {
        "rows": rows,
        "foreign_rows": foreign,
        "distinct_ok": distinct_ok,
        "distinct_ok_in_window": distinct_ok_inside,
        "p95_nanos": p95,
        "p95_sample": len(latencies),
        "latency_excluded": excluded,
        "task_names": dict(sorted(names.items())),
        "completions": completions,
    }


def protocol_index(protocol):
    return {cell["cell_id"]: cell for cell in protocol["cells"]}


def arm_statistics(cells):
    """Mean throughput, range over mean and mean p95 of usable repetitions.

    A repetition contributes once: the pinned cell when it is usable, else
    its ``-x2`` repeat. p95 is the mean of per-repetition values, never pooled.
    """
    usable = [c for c in cells if c["status"] == USABLE and c["throughput"] is not None]
    by_repetition = {}
    for cell in sorted(usable, key=lambda c: (c["repetition"], c["repeat"])):
        by_repetition.setdefault(cell["repetition"], cell)
    chosen = [by_repetition[r] for r in sorted(by_repetition)]
    result = {
        "usable": len(chosen),
        "cells": [c["cell_id"] for c in chosen],
        # The rates of the records that contribute, not of every record that
        # carries one of their IDs: an earlier unexported run of the same cell
        # may have offered a different rate and contributes nothing.
        "offered_rates": sorted(
            {c["offered_rate"] for c in chosen},
            key=lambda rate: (rate is None, rate),
        ),
        "throughputs": [c["throughput"] for c in chosen],
        "p95_nanos": [c["rows"]["p95_nanos"] for c in chosen],
        "mean_throughput": None,
        "range_over_mean": None,
        "mean_p95_nanos": None,
    }
    if not chosen:
        return result
    throughputs = result["throughputs"]
    mean = sum(throughputs) / len(throughputs)
    result["mean_throughput"] = mean
    if mean > 0:
        result["range_over_mean"] = (max(throughputs) - min(throughputs)) / mean
    p95s = [Fraction(p) for p in result["p95_nanos"] if p is not None]
    if len(p95s) == len(chosen):
        result["mean_p95_nanos"] = sum(p95s) / len(p95s)
    return result


def compare(base, candidate, short=(), disqualified=()):
    """One candidate arm against the baseline under the preregistered rule.

    ``disqualified`` names reasons the group as a whole cannot be compared.
    They travel into the comparison so no ratio or label is reported for a
    pair the rule has already refused.
    """
    result = {"throughput_ratio": None, "p95_ratio": None, "label": None, "reasons": []}
    reasons = result["reasons"]
    reasons.extend(disqualified)
    reasons.extend("arm-short:" + arm for arm in short)
    if base["range_over_mean"] is None or base["range_over_mean"] > RANGE_MAX:
        reasons.append("range-baseline")
    if candidate["range_over_mean"] is None or candidate["range_over_mean"] > RANGE_MAX:
        reasons.append("range-candidate")
    if base["mean_p95_nanos"] in (None, 0) or candidate["mean_p95_nanos"] is None:
        reasons.append("p95-undefined")
    if reasons:
        result["label"] = INCONCLUSIVE
        return result
    ratio_t = candidate["mean_throughput"] / base["mean_throughput"]
    ratio_p = candidate["mean_p95_nanos"] / base["mean_p95_nanos"]
    result.update(throughput_ratio=ratio_t, p95_ratio=ratio_p)
    if ratio_t >= GENERAL[0] and ratio_p <= GENERAL[1]:
        result["label"] = "general"
    elif ratio_t >= CONSTRAINED[0] and ratio_p <= CONSTRAINED[1]:
        result["label"] = "constrained"
    else:
        result["label"] = "decline"
    return result


def incremental(base, candidate):
    """Staging cost against the named arm of the same distribution; a report only."""
    if (
        base["usable"] == 0
        or candidate["usable"] == 0
        or base["mean_throughput"] == 0
        or base["mean_p95_nanos"] in (None, 0)
        or candidate["mean_p95_nanos"] is None
    ):
        return None
    return {
        "throughput_ratio": candidate["mean_throughput"] / base["mean_throughput"],
        "p95_ratio": candidate["mean_p95_nanos"] / base["mean_p95_nanos"],
        "baseline_usable": base["usable"],
        "candidate_usable": candidate["usable"],
    }


def group_verdict(arms, disqualified=()):
    """Labels for STAGED_HASH and STAGED_RANDOM against UNNAMED, plus costs."""
    short = [arm for arm in ARMS if arms[arm]["usable"] < REPETITIONS]
    comparisons = {
        arm: compare(arms[BASELINE], arms[arm], short, disqualified)
        for arm in CANDIDATES
    }
    labels = [c["label"] for c in comparisons.values()]
    return {
        "arms": arms,
        "short_arms": short,
        "comparisons": comparisons,
        # A cost ratio between arms the rule refuses to compare would be read
        # as a measurement of staging, which it is not.
        "incremental_cost": {
            arm: None
            if disqualified
            else incremental(arms[INCREMENTAL[arm]], arms[arm])
            for arm in CANDIDATES
        },
        "verdict": min(labels, key=LABEL_ORDER.index),
    }


def group_key(campaign, protocol_cell):
    return (
        campaign,
        protocol_cell["flink"],
        protocol_cell["shape"],
        protocol_cell["body_bytes"],
    )


def build_groups(cells, index):
    groups, unmatched = {}, []
    for record in cells:
        protocol_cell = index.get(record["protocol_cell_id"])
        if protocol_cell is None:
            if not is_calibration_cell(record["cell_id"]):
                unmatched.append(record["cell_id"])
            continue
        record["repetition"] = protocol_cell["repetition"]
        record["shape"] = protocol_cell["shape"]
        record["body_bytes"] = protocol_cell["body_bytes"]
        # The cell ID names a protocol entry; the run must have executed that
        # entry's conditions, or its result belongs to no group.
        mismatched = [
            field
            for field, executed, pinned in (
                ("arm", record["arm"], protocol_cell["arm"]),
                ("flink", record["flink"], protocol_cell["flink"]),
                (
                    "body_bytes",
                    record["body_bytes_executed"],
                    protocol_cell["body_bytes"],
                ),
                (
                    "parallelism",
                    record["parallelism"],
                    protocol_cell["parallelism"],
                ),
                ("concurrency", record["concurrency"], protocol_cell["concurrency"]),
                (
                    "checkpoint_seconds",
                    record["checkpoint_seconds"],
                    protocol_cell["checkpoint_seconds"],
                ),
                (
                    "distribution",
                    record["distribution"],
                    protocol_cell["distribution"],
                ),
                (
                    "channel_pool_size",
                    record["channel_pool_size"],
                    protocol_cell["channel_pool_size"],
                ),
                (
                    "warmup_seconds",
                    record["warmup_seconds"],
                    protocol_cell["warmup_seconds"],
                ),
                (
                    "observation_seconds",
                    record["observation_seconds"],
                    protocol_cell["observation_seconds"],
                ),
            )
            if executed != pinned
        ]
        if mismatched:
            record["reasons"].append(
                "conditions-differ-from-protocol:" + ",".join(sorted(mismatched))
            )
            if record["status"] == USABLE:
                record["status"] = INCONCLUSIVE
        key = group_key(record["campaign"], protocol_cell)
        groups.setdefault(key, []).append(record)
    result = []
    for key in sorted(groups, key=lambda k: tuple(str(part) for part in k)):
        members = groups[key]
        arms = {
            arm: arm_statistics([c for c in members if c["arm"] == arm]) for arm in ARMS
        }
        # The preregistration compares the five arms at one frozen rate, the
        # one the capacity search accepted for this shape, body and line.
        # Arms measured at different offered rates are not comparable, so the
        # refusal reaches every comparison rather than only the group label.
        rates = sorted(
            {rate for stats in arms.values() for rate in stats["offered_rates"]},
            key=lambda rate: (rate is None, rate),
        )
        conflict = ["offered-rate-differs-across-arms"] if len(rates) > 1 else []
        verdict = group_verdict(arms, conflict)
        verdict["offered_rates"] = rates
        verdict["frozen_rate"] = rates[0] if len(rates) == 1 else None
        verdict["reasons"] = conflict
        result.append(
            {
                "campaign": key[0],
                "flink": key[1],
                "shape": key[2],
                "shape_parameters": dict(
                    zip(
                        (
                            "parallelism",
                            "concurrency",
                            "checkpoint_seconds",
                            "distribution",
                        ),
                        SHAPES[key[2]],
                        strict=True,
                    )
                ),
                "body_bytes": key[3],
                "cells": sorted(c["cell_id"] for c in members),
                **verdict,
            }
        )
    return result, sorted(unmatched)


def show(value, digits=3):
    if isinstance(value, Fraction):
        return f"{float(value):.{digits}f} ({value.numerator}/{value.denominator})"
    return "n/a" if value is None else str(value)
