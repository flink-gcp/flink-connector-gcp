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
"""Offline analysis of downloaded #1246 Cloud Tasks evidence.

The analyzer reads a local mirror of ``gs://flink-gcp-tier3-evidence/runs/``
(one run directory or a directory of them), verifies every exported object
against its ``exported.json`` marker, re-runs the reconciler, and derives the
preregistered quantities of ``docs/adr/evidence/0162-cloudtasks-assessment-1246.md``:
the observation window, per-repetition throughput and p95, backlog across
checkpoint boundaries, recovery offsets, the per-group verdict labels, the
capacity bracket and the calibration acceptance items. It never contacts a
cluster or a bucket, needs only the standard library beside the evidence and
protocol modules, keeps every ratio a ``Fraction`` until rendering, and writes
byte-identical reports for identical input.

Clock domains: the JobManager clock enters the supervisor domain only through
the ``jm_offset_seconds`` each observation recorded at the same poll; receipt
and row wall clocks come from the TaskManager, for which no offset was recorded,
and are compared with the supervisor domain as they are. Every reported instant
names the domain it was measured in.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from array import array
from bisect import bisect_right
from fractions import Fraction
from itertools import pairwise
from pathlib import Path

from .bigquery_analyze import assess_runs, render
from .bigquery_analyze import counts as bigquery_counts
from .common import EXCLUDED, INCONCLUSIVE, USABLE, Failure
from .evidence import (
    MARKER,
    RECEIPT,
    RECEIPTS,
    ROWS,
    DirectorySource,
    Reconciler,
    Reconciliation,
    Row,
    cell_prefix,
    iter_rows,
    manifest_sha256,
)
from .protocol import SHAPES, load, next_probe, protocol_sha256, window

SUFFIX = re.compile(r"-(x2|q[0-9]+)\Z")
PROBE = re.compile(r"-q([0-9]+)\Z")
CALIBRATION_CELL = re.compile(r"k[0-9]{2}-")
PACING_CELL = re.compile(r"k[0-9]{2}-pace-[0-9]+\Z")
INTERRUPT_SUFFIX = "-k11"
DELAY_CONTROL = "k03-delay-control"
PACING_ACCEPTANCE = (
    "k01-pace-10",
    "k02-pace-25",
    "k04-pace-100",
    "k05-pace-1000",
    "k06-pace-2000",
)
CSV_PAIR = ("k04-pace-100", "k07-counts-only")
GAUGE_PAIR = ("k09-staged-gauges", "k04-pace-100")
RANDOM_CONTROL = "k10-random-control"
BASELINE = "UNNAMED"
CANDIDATES = ("STAGED_HASH", "STAGED_RANDOM")
INCREMENTAL = {"STAGED_HASH": "NAMED_HASH", "STAGED_RANDOM": "NAMED_RANDOM_CONTROL"}
ARMS = (BASELINE, "NAMED_HASH", "NAMED_RANDOM_CONTROL", *CANDIDATES)
REPETITIONS = 3
# Thresholds of the preregistration; the protocol pin restates them as floats
# and the analyzer compares only these exact fractions.
GENERAL = (Fraction(7, 10), Fraction(2))
CONSTRAINED = (Fraction(1, 4), Fraction(4))
RANGE_MAX = Fraction(1, 10)
ADMITTED = Fraction(95, 100)
BACKLOG_SLOPE = Fraction(5, 100)
MIN_CHECKPOINTS = 6
MIN_BOUNDARIES = 4
DELAY_THROUGHPUT_MAX = Fraction(12)
DELAY_P95_MIN_NANOS = 100_000_000
REPLAY_BUDGET = ("stagedReplayBudgetMillis", "currentCommitReplayBudgetMillis")
# The two gauges observe.STAGED_REQUIRED demands of a staged arm.
STAGED_GAUGES = ("stagedBytes", "stagedReplayBudgetMillis")
HEAP_USED = "Status.JVM.Memory.Heap.Used"
BUSY_TIME = "busyTimeMsPerSecond"
SOURCE_OUT = "numRecordsOut"
SINK_IN = "numRecordsIn"
HEX = re.compile(r"[0-9a-f]+\Z")
LABEL_ORDER = (INCONCLUSIVE, "decline", "constrained", "general")


def fraction(value):
    """An exact Fraction of an evidence number; floats go through their repr."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise Failure("Not a number: " + repr(value))
    return Fraction(repr(value)) if isinstance(value, float) else Fraction(value)


def at_of(observation):
    """The supervisor-clock instant of an observation or event payload."""
    return fraction(observation.get("at", 0))


def metric_name(metric_id):
    return str(metric_id).rsplit(".", 1)[-1]


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


def _int(value):
    return isinstance(value, int) and not isinstance(value, bool)


# --- input ------------------------------------------------------------------


class RunMirror:
    """``DirectorySource`` over one run directory, keyed by full object names.

    A ``gcloud storage cp --recursive`` of ``runs/RUN_ID/`` drops the bucket
    prefix, while the evidence module addresses objects as ``runs/RUN_ID/...``;
    this adapter restores the prefix on listing and strips it on open.
    """

    def __init__(self, run_dir, run_id):
        self.inner = DirectorySource(run_dir)
        self.prefix = f"runs/{run_id}/"

    def _strip(self, name):
        if not name.startswith(self.prefix):
            raise Failure("Object name outside this run: " + name)
        return name[len(self.prefix) :]

    def list(self, prefix):
        if not prefix.startswith(self.prefix):
            return []
        return [
            {**obj, "name": self.prefix + obj["name"]}
            for obj in self.inner.list(self._strip(prefix))
        ]

    def open(self, name, generation):
        return self.inner.open(self._strip(name), generation)


class Run:
    """One downloaded run: its approval, session, evidence events and cells."""

    def __init__(self, directory):
        self.directory = Path(directory)
        self.approval = read_json(self.directory / "approval.json")
        if not isinstance(self.approval, dict) or "run_id" not in self.approval:
            raise Failure("Not a run directory: " + str(directory))
        self.run_id = str(self.approval["run_id"])
        self.campaign = str(self.approval.get("campaign", ""))
        self.scenario = str(self.approval.get("scenario", ""))
        self.flink = str(self.approval.get("flink_version", ""))
        self.session = read_json(self.directory / "session.json")
        self.result = read_json(self.directory / "result.json")
        cells = self.approval.get("cells")
        if not cells and isinstance(self.session, dict):
            cells = self.session.get("cells")
        self.cells = list(cells or [])
        self.events = read_events(self.directory)
        self.source = RunMirror(self.directory, self.run_id)

    def events_of(self, event, cell_id=None):
        return [
            entry
            for entry in self.events
            if entry["event"] == event
            and isinstance(entry["payload"], dict)
            and (cell_id is None or entry["payload"].get("cell") == cell_id)
        ]

    def relative(self, name):
        return self.directory / name[len(f"runs/{self.run_id}/") :]


def read_json(path):
    path = Path(path)
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise Failure("Unreadable JSON evidence: " + str(path)) from error


def read_events(run_dir):
    """Every ``{event, at, payload}`` object of the supervisor and runner."""
    entries = []
    for actor in ("supervisor", "runner"):
        base = Path(run_dir) / actor
        if not base.is_dir():
            continue
        for path in sorted(base.glob("*.json")):
            value = read_json(path)
            if not isinstance(value, dict) or not isinstance(value.get("event"), str):
                raise Failure("Malformed evidence event: " + str(path))
            payload = value.get("payload")
            if value["event"] in ANALYZED_EVENTS and not isinstance(payload, dict):
                # Other receipts, such as the inventory list, are carried
                # through untouched; only what the analysis reads is typed.
                raise Failure("Malformed evidence event: " + str(path))
            entries.append(
                {
                    "event": value["event"],
                    "at": value.get("at"),
                    "payload": payload,
                    "actor": actor,
                    "name": path.name,
                }
            )
    entries.sort(key=lambda e: (str(e["at"]), e["actor"], e["name"]))
    return entries


def discover_runs(path):
    """One run directory, or every immediate child that is one, sorted by id."""
    path = Path(path)
    if not path.is_dir():
        raise Failure("Evidence directory does not exist: " + str(path))
    if (path / "approval.json").is_file():
        return [Run(path)]
    runs = [
        Run(child)
        for child in sorted(path.iterdir())
        if child.is_dir() and (child / "approval.json").is_file()
    ]
    if not runs:
        raise Failure("No run directory with approval.json under " + str(path))
    return sorted(runs, key=lambda run: run.run_id)


# --- integrity ----------------------------------------------------------------


def sha256_of(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 16):
            digest.update(chunk)
    return digest.hexdigest()


def check_integrity(run, cell_id, marker):
    """Problems between the marker and the local objects; empty means verified."""
    if (
        not isinstance(marker, dict)
        or marker.get("version") != 1
        or marker.get("run_id") != run.run_id
        or marker.get("cell_id") != cell_id
        or not isinstance(marker.get("objects"), list)
        or not isinstance(marker.get("reconciliation"), dict)
    ):
        return ["marker-identity"]
    problems, listed = [], set()
    prefix = cell_prefix(run.run_id, cell_id)
    for obj in marker["objects"]:
        name = str(obj.get("name", ""))
        listed.add(name)
        path = run.relative(name)
        if not name.startswith(prefix):
            problems.append("foreign-object:" + name)
        elif not path.is_file():
            problems.append("missing-object:" + name)
        elif path.stat().st_size != obj.get("size"):
            problems.append("size-mismatch:" + name)
        elif sha256_of(path) != obj.get("sha256"):
            problems.append("sha256-mismatch:" + name)
    if manifest_sha256(marker["objects"]) != marker.get("manifest_sha256"):
        problems.append("manifest-mismatch")
    for obj in run.source.list(prefix):
        relative = obj["name"][len(prefix) :]
        if relative.startswith((ROWS, RECEIPTS)) and obj["name"] not in listed:
            problems.append("unlisted-object:" + obj["name"])
    return sorted(problems)


# --- receipts -----------------------------------------------------------------


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


# --- observations -----------------------------------------------------------


def observations_of(run, cell_id):
    entries = [e["payload"] for e in run.events_of("observation", cell_id)]
    return sorted(entries, key=at_of)


def metric_rows(sample):
    if not isinstance(sample, list):
        return []
    return [m for m in sample if isinstance(m, dict)]


def aggregate(sample, name, key):
    """Values of ``key`` for the metrics whose id ends in ``name``."""
    return [
        fraction(metric[key])
        for metric in metric_rows(sample)
        if metric_name(metric.get("id", "")) == name
        and isinstance(metric.get(key), int | float)
        and not isinstance(metric.get(key), bool)
    ]


def taskmanager_values(observation, name):
    values = []
    for manager in observation.get("taskmanagers") or []:
        if not isinstance(manager, dict):
            continue
        for metric in metric_rows(manager.get("metrics")):
            if metric.get("id") != name:
                continue
            try:
                values.append(fraction(json.loads(str(metric.get("value")))))
            except (ValueError, Failure):
                continue
    return values


def placed_checkpoints(observations):
    """Completed checkpoints with timestamps, in the supervisor clock domain.

    Timestamps come from ``checkpoints.completed`` and COMPLETED ``details``;
    ids that were only listed (``history``, ``details_skipped``) stay unplaced.
    Each JM millisecond value is moved into the supervisor domain with the
    ``jm_offset_seconds`` of the observation that recorded it.
    """
    placed, completed_ids, history_ids = {}, set(), set()
    in_progress = None
    for observation in observations:
        checkpoints = observation.get("checkpoints")
        if not isinstance(checkpoints, dict) or "unavailable" in checkpoints:
            continue
        offset = observation.get("jm_offset_seconds")
        counts = checkpoints.get("counts") or {}
        if _int(counts.get("in_progress")):
            in_progress = max(in_progress or 0, counts["in_progress"])
        history_ids.update(i for i in checkpoints.get("history") or [] if _int(i))
        completed_ids.update(
            i for i in observation.get("details_skipped") or [] if _int(i)
        )
        candidates = [checkpoints.get("completed") or {}]
        candidates += [
            d
            for d in observation.get("details") or []
            if isinstance(d, dict) and d.get("status", "COMPLETED") == "COMPLETED"
        ]
        for entry in candidates:
            if not _int(entry.get("id")):
                continue
            completed_ids.add(entry["id"])
            if (
                entry["id"] in placed
                or not isinstance(offset, int | float)
                or not _int(entry.get("trigger_timestamp"))
                or not _int(entry.get("latest_ack_timestamp"))
            ):
                continue
            shift = fraction(offset)
            placed[entry["id"]] = {
                "id": entry["id"],
                "trigger": Fraction(entry["trigger_timestamp"], 1000) - shift,
                "ack": Fraction(entry["latest_ack_timestamp"], 1000) - shift,
                "end_to_end_duration": entry.get("end_to_end_duration"),
                "checkpointed_size": entry.get("checkpointed_size"),
                "state_size": entry.get("state_size"),
            }
    return placed, completed_ids, history_ids, in_progress


ANALYZED_EVENTS = (
    "observation",
    "cell-metrics-discovered",
    "metrics-unavailable",
    "interrupt-issued",
    "interrupt-intent",
)


def observation_window(placed, completed_ids, start_ms, warmup_seconds, last_ms):
    """The preregistered window, or the reason there is none.

    ``start_ms`` and ``last_ms`` are TaskManager wall millis of the source
    receipts; ``placed`` checkpoints are already in the supervisor domain.
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
        "clock": "supervisor",
        "start": first["ack"],
        "end": last["ack"],
        "seconds": last["ack"] - first["ack"],
        "first_checkpoint": first["id"],
        "last_checkpoint": last["id"],
        "checkpoints_inside": len(inside),
        "boundaries": [placed[i]["ack"] for i in inside if i in placed],
    }, None


def interpolate(samples, t):
    """Linear value of sorted ``(at, value)`` samples at ``t``; None outside."""
    if not samples or t < samples[0][0] or t > samples[-1][0]:
        return None
    index = bisect_right([s[0] for s in samples], t) - 1
    at, value = samples[index]
    if at == t or index + 1 >= len(samples):
        return value
    next_at, next_value = samples[index + 1]
    return value + (next_value - value) * (t - at) / (next_at - at)


def metric_series(observations, section, name):
    samples = []
    for observation in observations:
        values = aggregate(observation.get(section), name, "sum")
        if values:
            samples.append((at_of(observation), sum(values)))
    return samples


def series_rate(samples, span):
    if span is None or span["seconds"] <= 0:
        return None
    start = interpolate(samples, span["start"])
    end = interpolate(samples, span["end"])
    if start is None or end is None:
        return None
    return (end - start) / span["seconds"]


# --- rows -------------------------------------------------------------------


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


def completed_by(completions, t_seconds):
    """Distinct OK sequences whose completion (TM wall) is at or before t."""
    return bisect_right(completions, math.floor(t_seconds * 1_000_000_000))


def backlog(cell, span, observations, completions):
    """Backlog at checkpoint boundaries and the sustained-growth rule.

    ``B_k = S(b_k) - C(b_k)``: S is the source ``numRecordsOut`` sum
    interpolated between the observations around the boundary, C the distinct
    OK sequences completed by then. The tolerance is
    ``max(0.05 * R * I, 2 * P * c)``.
    """
    if span is None:
        return {"status": "not-evaluable", "reason": "no-window"}
    samples = metric_series(observations, "source", SOURCE_OUT)
    points = []
    for boundary in span["boundaries"]:
        emitted = interpolate(samples, boundary)
        if emitted is None:
            continue
        points.append((boundary, emitted - completed_by(completions, boundary)))
    if len(points) < MIN_BOUNDARIES:
        return {
            "status": "not-evaluable",
            "reason": "fewer-than-four-boundaries",
            "boundaries": len(points),
        }
    rate = Fraction(cell["offered_rate"])
    tau = max(
        BACKLOG_SLOPE * rate * Fraction(cell["checkpoint_seconds"]),
        2 * Fraction(cell["parallelism"]) * Fraction(cell["concurrency"]),
    )
    values = [b for _, b in points]
    growths = [b - a for a, b in pairwise(values)]
    consecutive = any(
        all(g > tau for g in growths[i : i + 3]) for i in range(len(growths) - 2)
    )
    slope_limit = BACKLOG_SLOPE * rate * (points[-1][0] - points[0][0])
    overall = values[-1] - values[0] > slope_limit
    return {
        "status": "evaluated",
        "tolerance": tau,
        "boundaries": len(points),
        "values": values,
        "max": max(values),
        "three_consecutive_over_tolerance": consecutive,
        "overall_over_slope": overall,
        "sustained_growth": consecutive or overall,
    }


# --- recovery ---------------------------------------------------------------


def recovery(run, cell_id, observations, receipts):
    """Offsets from the interrupt to RESTARTING, RUNNING and the new creator.

    RESTARTING and RUNNING are JobManager timestamps moved into the supervisor
    domain through the offset of the observation that carried them; the
    creator start is a TaskManager wall instant.
    """
    issued = run.events_of("interrupt-issued", cell_id)
    if not issued:
        return None
    issued_at = at_of(issued[0]["payload"])
    result = {
        "interrupt_issued": {"at": issued_at, "clock": "supervisor"},
        "restarting": None,
        "running": None,
        "creator_start": None,
    }
    restarting = None
    for observation in observations:
        offset = observation.get("jm_offset_seconds")
        if not isinstance(offset, int | float) or at_of(observation) < issued_at:
            continue
        timestamps = (observation.get("job") or {}).get("timestamps") or {}
        shift = fraction(offset)
        if restarting is None and _int(timestamps.get("RESTARTING")):
            candidate = Fraction(timestamps["RESTARTING"], 1000) - shift
            if candidate >= issued_at:
                restarting = candidate
                result["restarting"] = {
                    "seconds_after_interrupt": candidate - issued_at,
                    "clock": "jobmanager-via-offset",
                }
        if (
            restarting is not None
            and result["running"] is None
            and _int(timestamps.get("RUNNING"))
        ):
            running = Fraction(timestamps["RUNNING"], 1000) - shift
            if running >= restarting:
                result["running"] = {
                    "seconds_after_interrupt": running - issued_at,
                    "clock": "jobmanager-via-offset",
                }
    starts = sorted(
        r["wall_millis"]
        for r in receipts_by_phase(receipts).get(("creator", "start"), [])
    )
    later = [ms for ms in starts if Fraction(ms, 1000) > issued_at]
    if later:
        result["creator_start"] = {
            "seconds_after_interrupt": Fraction(later[0], 1000) - issued_at,
            "clock": "taskmanager-wall",
        }
    return result


# --- per cell ---------------------------------------------------------------


def pod_resources(observations):
    seen = {}
    for observation in observations:
        for pod in observation.get("pods") or []:
            if isinstance(pod, dict) and isinstance(pod.get("name"), str):
                seen[pod["name"]] = pod.get("resources") or {}
    return dict(sorted(seen.items()))


def observation_metrics(observations, span):
    heap = [v for o in observations for v in taskmanager_values(o, HEAP_USED)]
    busy = [v for o in observations for v in aggregate(o.get("sink"), BUSY_TIME, "max")]
    budgets = {}
    for name in REPLAY_BUDGET:
        values = [
            v for o in observations for v in aggregate(o.get("sink"), name, "min")
        ]
        budgets[name] = min(values) if values else None
    return {
        "taskmanager_heap_used_max": max(heap) if heap else None,
        "busy_time_ms_per_second_max": max(busy) if busy else None,
        "replay_budget_min": budgets,
        "source_records_out_rate": series_rate(
            metric_series(observations, "source", SOURCE_OUT), span
        ),
        "sink_records_in_rate": series_rate(
            metric_series(observations, "sink", SINK_IN), span
        ),
    }


def analyze_cell(run, cell, kind_override=None):
    cell_id = cell["id"]
    record = {
        "run_id": run.run_id,
        "campaign": run.campaign,
        "flink": run.flink,
        "cell_id": cell_id,
        "protocol_cell_id": base_cell_id(cell_id),
        "probe": probe_index(cell_id),
        "repeat": cell_id.endswith("-x2"),
        "arm": cell.get("arm"),
        "body_bytes_executed": cell.get("body_bytes"),
        "parallelism": cell.get("parallelism"),
        "concurrency": cell.get("concurrency"),
        "checkpoint_seconds": cell.get("checkpoint_seconds"),
        "distribution": cell.get("distribution"),
        "channel_pool_size": cell.get("channel_pool_size"),
        "warmup_seconds": cell.get("warmup_seconds"),
        "observation_seconds": cell.get("observation_seconds"),
        "outcome": None,
        "kind": kind_override,
        "offered_rate": cell.get("offered_rate"),
        "status": None,
        "reasons": [],
        "integrity": {"status": None, "problems": []},
        "reconciliation": None,
        "window": None,
        "throughput": None,
        "rows": None,
        "checkpoints": None,
        "backlog": None,
        "recovery": None,
        "metrics": None,
        "pod_resources": None,
        "observations": 0,
    }
    marker = read_json(run.relative(cell_prefix(run.run_id, cell_id) + MARKER))
    if marker is None:
        record.update(status="unexported", reasons=["no-marker"])
        record["integrity"]["status"] = "unexported"
        return record
    problems = check_integrity(run, cell_id, marker)
    record["integrity"] = {
        "status": "tampered" if problems else "verified",
        "problems": problems,
        "manifest_sha256": marker.get("manifest_sha256"),
    }
    if problems:
        record.update(status="tampered", reasons=["integrity"])
        return record
    if kind_override is None:
        record["kind"] = marker.get("kind")
    record["outcome"] = marker.get("outcome")
    expected = marker["reconciliation"]
    try:
        recomputed = Reconciler(
            run.source, run.run_id, cell, marker.get("kind", "main")
        ).run()
    except Failure as error:
        recomputed = Reconciliation("invalid", ["reconciler-failed"])
        record["reasons"].append("reconciler-failed:" + str(error))
    consistent = recomputed.status == expected.get("status") and list(
        recomputed.reasons
    ) == list(expected.get("reasons", []))
    record["reconciliation"] = {
        "marker": {
            "status": expected.get("status"),
            "reasons": list(expected.get("reasons", [])),
        },
        "recomputed": {
            "status": recomputed.status,
            "reasons": list(recomputed.reasons),
        },
        "counts": dict(recomputed.counts),
        "consistent": consistent,
    }
    if not consistent:
        record["status"] = "inconsistent"
        record["reasons"].append("reconciliation-differs")
        return record
    receipts = recomputed.receipts
    observations = observations_of(run, cell_id)
    record["observations"] = len(observations)
    placed, completed_ids, history_ids, in_progress = placed_checkpoints(observations)
    start_ms, last_ms = source_bounds(receipts)
    span, failure = observation_window(
        placed, completed_ids, start_ms, cell.get("warmup_seconds", 0), last_ms
    )
    latest = max(placed.values(), key=lambda c: c["id"], default=None)
    record["checkpoints"] = {
        "completed_observed": len(completed_ids),
        "placed": len(placed),
        "history_only": len(history_ids - completed_ids),
        "max_in_progress": in_progress,
        "latest_completed": None
        if latest is None
        else {
            k: latest[k]
            for k in ("id", "end_to_end_duration", "checkpointed_size", "state_size")
        },
    }
    warm, observation_seconds = window(int(cell.get("checkpoint_seconds", 0)))
    if span is not None:
        record["window"] = {
            **span,
            "rule_holds": cell.get("warmup_seconds") == warm
            and cell.get("observation_seconds") == observation_seconds,
        }
    record["source_start_ms"] = {"value": start_ms, "clock": "taskmanager-wall"}
    record["source_last_mapped_ms"] = {"value": last_ms, "clock": "taskmanager-wall"}
    restarted = any(o.get("restarted") is True for o in observations)
    rows = analyze_rows(run, cell, creator_order(receipts), span)
    completions = rows.pop("completions")
    record["rows"] = rows
    if span is not None:
        record["throughput"] = Fraction(rows["distinct_ok_in_window"]) / span["seconds"]
    record["metrics"] = observation_metrics(observations, span)
    record["backlog"] = backlog(cell, span, observations, completions)
    record["recovery"] = recovery(run, cell_id, observations, receipts)
    record["pod_resources"] = pod_resources(observations)
    reasons = record["reasons"]
    # The supervisor's outcome for the cell. A job that failed, was cancelled
    # or ran out of deadline may still export complete evidence for the rows
    # it did write, and that evidence describes an aborted run, not a
    # steady state.
    completed = record["outcome"] == "completed"
    if not completed:
        reasons.append("outcome:" + str(record["outcome"]))
    if recomputed.status != "complete":
        reasons.append(recomputed.status)
    if is_interrupt_control(cell_id):
        reasons.append("interrupt-control")
    if restarted:
        reasons.append("restarted")
    if failure is not None:
        reasons.append(failure)
    if recomputed.status != "complete":
        record["status"] = recomputed.status
    elif is_interrupt_control(cell_id):
        record["status"] = "interrupt-control"
    elif restarted or failure is not None or not completed:
        record["status"] = INCONCLUSIVE
    else:
        record["status"] = USABLE
    return record


# --- groups -------------------------------------------------------------------


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
        members = groups.setdefault(key, {"repetitions": [], "probes": []})
        members["probes" if record["probe"] is not None else "repetitions"].append(
            record
        )
    result = []
    for key in sorted(groups, key=lambda k: tuple(str(part) for part in k)):
        members = groups[key]
        arms = {
            arm: arm_statistics([c for c in members["repetitions"] if c["arm"] == arm])
            for arm in ARMS
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
                "cells": sorted(c["cell_id"] for c in members["repetitions"]),
                **verdict,
                "capacity": capacity(members["probes"]),
            }
        )
    return result, sorted(unmatched)


# --- capacity -----------------------------------------------------------------


def probe_accepted(record):
    """The admission rule of the capacity search for one probe cell.

    ``True`` accepts the offered rate, ``False`` rejects it and ``None`` means
    the probe measured nothing. Only evidence of the sink failing to keep up
    rejects a rate: an interrupted, restarted or unexported probe, and one
    whose window holds too few checkpoint boundaries to evaluate the backlog,
    must be repeated. Treating those as saturation would freeze the capacity
    at the last accepted rate on a Spot interruption alone.
    """
    if record["status"] in EXCLUDED:
        return None, ["excluded:" + record["status"]]
    if record["status"] != USABLE:
        return None, ["not-measured:" + record["status"]]
    throughput, offered = record["throughput"], record["offered_rate"]
    if throughput is None or offered is None:
        return None, ["no-throughput"]
    growth = (record.get("backlog") or {}).get("sustained_growth")
    reasons = []
    if throughput < ADMITTED * Fraction(offered):
        reasons.append("below-admitted-fraction")
    if growth:
        reasons.append("sustained-backlog-growth")
    if reasons:
        return False, reasons
    if growth is None:
        return None, ["backlog-not-evaluable"]
    return True, []


def capacity(probes):
    if not probes:
        return None
    entries = []
    for record in sorted(probes, key=lambda c: (c["probe"], c["cell_id"])):
        accepted, reasons = probe_accepted(record)
        entries.append(
            {
                "cell_id": record["cell_id"],
                "probe": record["probe"],
                "offered_rate": record["offered_rate"],
                "throughput": record["throughput"],
                "accepted": accepted,
                "reasons": reasons,
            }
        )
    accepted = [e["offered_rate"] for e in entries if e["accepted"] is True]
    rejected = [e["offered_rate"] for e in entries if e["accepted"] is False]
    state = {
        "initial": entries[0]["offered_rate"],
        "accepted": max(accepted) if accepted else None,
        "rejected": min(rejected) if rejected else None,
    }
    if (
        state["accepted"] is not None
        and state["rejected"] is not None
        and state["accepted"] >= state["rejected"]
    ):
        return {
            "probes": entries,
            "state": state,
            "status": "inconsistent",
            "next": None,
        }
    status, rate = next_probe(state)
    # A probe that measured nothing is repeated once, as the preregistration
    # says. A rate offered twice without a measurement stalls the search
    # rather than buying the same answer a third time.
    attempts = [e for e in entries if e["offered_rate"] == rate]
    if (
        status in ("searching", "bracketed")
        and len(attempts) >= 2
        and all(e["accepted"] is None for e in attempts)
    ):
        status = "stalled"
    return {"probes": entries, "state": state, "status": status, "next": rate}


# --- calibration ------------------------------------------------------------


def delta(after, before):
    return None if after is None or before is None else after - before


def gauge_evidence(runs, cell_id):
    discovered = [
        e["payload"]
        for run in runs
        for e in run.events_of("cell-metrics-discovered", cell_id)
    ]
    unavailable = [
        e["payload"]
        for run in runs
        for e in run.events_of("metrics-unavailable", cell_id)
    ]
    names = sorted({metric_name(m) for p in discovered for m in p.get("metrics", [])})
    return {
        "discovered_events": len(discovered),
        "metric_names": names,
        "staged_gauges_present": all(name in names for name in STAGED_GAUGES),
        "missing_reported": sorted(
            {m for p in unavailable for m in p.get("missing", [])}
        ),
    }


def calibration(cells, runs):
    """One acceptance section per campaign and Flink line.

    Both calibration sessions reuse ``k01``-``k04``, so a single section keyed
    by cell ID would let a usable 2.2.1 record stand in for an unexported or
    incomplete 1.20.4 repeat. Sections never merge across a line.
    """
    keys = sorted(
        {(c["campaign"], c["flink"]) for c in cells},
        key=lambda key: tuple(str(part) for part in key),
    )
    sections = []
    for campaign, flink in keys:
        section = calibration_section(
            [c for c in cells if (c["campaign"], c["flink"]) == (campaign, flink)],
            [r for r in runs if (r.campaign, r.flink) == (campaign, flink)],
        )
        if section is not None:
            sections.append({"campaign": campaign, "flink": flink, **section})
    return sections


def calibration_section(cells, runs):
    # Within one campaign and line a cell ID may still repeat, when an earlier
    # session left it unexported; the usable record wins over the unusable one.
    by_id = {}
    for record in cells:
        previous = by_id.get(record["cell_id"])
        if previous is None or (
            previous["status"] != USABLE and record["status"] == USABLE
        ):
            by_id[record["cell_id"]] = record
    if not by_id:
        return None
    section = {"pacing": {}, "delay_control": None, "csv_overhead": None}
    for cell_id, record in sorted(by_id.items()):
        if not PACING_CELL.fullmatch(cell_id):
            continue
        ratio = None
        if record["throughput"] is not None and record["offered_rate"]:
            ratio = record["throughput"] / Fraction(record["offered_rate"])
        section["pacing"][cell_id] = {
            "status": record["status"],
            "offered_rate": record["offered_rate"],
            "throughput": record["throughput"],
            "achieved_over_offered": ratio,
            "admitted": None if ratio is None else ratio >= ADMITTED,
            "sustained_backlog_growth": (record.get("backlog") or {}).get(
                "sustained_growth"
            ),
        }
    delay = by_id.get(DELAY_CONTROL)
    if delay is not None:
        p95 = (delay.get("rows") or {}).get("p95_nanos")
        section["delay_control"] = {
            "cell_id": DELAY_CONTROL,
            "status": delay["status"],
            "throughput": delay["throughput"],
            "p95_nanos": p95,
            "detected": delay["throughput"] is not None
            and p95 is not None
            and delay["throughput"] <= DELAY_THROUGHPUT_MAX
            and p95 >= DELAY_P95_MIN_NANOS,
        }
    rows_on, rows_off = (by_id.get(cell_id) for cell_id in CSV_PAIR)
    if rows_on is not None and rows_off is not None:
        on, off = rows_on.get("metrics") or {}, rows_off.get("metrics") or {}
        section["csv_overhead"] = {
            "cells": list(CSV_PAIR),
            "basis": "sink numRecordsIn rate, busyTimeMsPerSecond max, heap used max",
            "deltas": {
                "achieved_rate": delta(
                    off.get("sink_records_in_rate"), on.get("sink_records_in_rate")
                ),
                "busy_time": delta(
                    off.get("busy_time_ms_per_second_max"),
                    on.get("busy_time_ms_per_second_max"),
                ),
                "taskmanager_heap": delta(
                    off.get("taskmanager_heap_used_max"),
                    on.get("taskmanager_heap_used_max"),
                ),
            },
            "rows_throughput": rows_on["throughput"],
        }
    section["gauges"] = {
        cell_id: gauge_evidence(runs, cell_id)
        for cell_id in GAUGE_PAIR
        if cell_id in by_id
    }
    section["task_names"] = {
        cell_id: (by_id[cell_id].get("rows") or {}).get("task_names")
        for cell_id in (RANDOM_CONTROL, GAUGE_PAIR[0])
        if cell_id in by_id
    }
    control = next((r for i, r in by_id.items() if is_interrupt_control(i)), None)
    if control is not None:
        marker = (control.get("reconciliation") or {}).get("marker") or {}
        section["interrupt_control"] = {
            "cell_id": control["cell_id"],
            "status": control["status"],
            "marker_status": marker.get("status"),
            # The control's own restart makes the source register a second
            # incarnation, so the reconciliation is restarted rather than
            # merely incomplete; both say the cell is not steady state.
            "reconciles_interrupted": marker.get("status")
            in ("restarted", "incomplete"),
            "steady_state_refused": control["status"] != USABLE,
            "recovery": control.get("recovery"),
        }
    section["acceptance"] = acceptance_items(section, by_id)
    return section


def acceptance_items(section, by_id):
    """The instrument acceptance list of the preregistration, item by item."""
    delay, pacing, gauges = (
        section["delay_control"],
        section["pacing"],
        section["gauges"],
    )
    pace_25 = pacing.get("k02-pace-25")
    paced = [pacing.get(c) for c in PACING_ACCEPTANCE]
    names = section["task_names"].get(RANDOM_CONTROL)
    steady = [r for i, r in by_id.items() if not is_interrupt_control(i)]
    control = section.get("interrupt_control")

    def verified(cell_id):
        """The cell's evidence was exported and matches its marker.

        Metric discovery and name shape do not need a steady state, but they
        do need evidence the analysis can trust.
        """
        record = by_id.get(cell_id)
        return record is not None and record["status"] not in EXCLUDED

    return [
        {
            "item": "k03 detected and k02 admitted",
            "holds": None
            if delay is None or pace_25 is None
            else delay["status"] == USABLE
            and delay["detected"]
            and pace_25["status"] == USABLE
            and pace_25["admitted"] is True,
        },
        {
            "item": "pacing cells admitted without sustained backlog growth",
            "holds": None
            if any(p is None for p in paced)
            else all(
                p["status"] == USABLE
                and p["admitted"] is True
                and p["sustained_backlog_growth"] is False
                for p in paced
            ),
        },
        {
            "item": "connector gauges discovered on k09 and absent on k04",
            "holds": None
            if any(c not in gauges for c in GAUGE_PAIR)
            else all(verified(c) for c in GAUGE_PAIR)
            and gauges[GAUGE_PAIR[0]]["staged_gauges_present"]
            and not gauges[GAUGE_PAIR[1]]["staged_gauges_present"],
        },
        {
            "item": "k10 task names are 32 hex characters",
            "holds": None
            if not names
            else verified(RANDOM_CONTROL) and set(names) == {"hex32"},
        },
        {
            "item": "every steady-state cell reconciles complete and verifies",
            "holds": None
            if not steady
            else all(
                r["status"] == USABLE
                and ((r.get("reconciliation") or {}).get("marker") or {}).get("status")
                == "complete"
                for r in steady
            ),
        },
        {
            "item": "interrupt control reconciles restarted or incomplete and is refused",
            "holds": None
            if control is None
            else control["reconciles_interrupted"] and control["steady_state_refused"],
        },
    ]


# --- report -----------------------------------------------------------------


def plain(value):
    """JSON-ready copy: a Fraction becomes ``{fraction, float}``."""
    if isinstance(value, Fraction):
        return {
            "fraction": f"{value.numerator}/{value.denominator}",
            "float": float(value),
        }
    if isinstance(value, dict):
        return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [plain(v) for v in value]
    if isinstance(value, set | frozenset):
        return sorted(plain(v) for v in value)
    if isinstance(value, array):
        return list(value)
    return value


def show(value, digits=3):
    if isinstance(value, Fraction):
        return f"{float(value):.{digits}f} ({value.numerator}/{value.denominator})"
    return "n/a" if value is None else str(value)


def render_markdown(report):
    lines = [
        # The title and the preregistration below belong to the cells. A
        # directory holding only a deployed run has neither.
        "# Cloud Tasks evidence analysis"
        if report["cells"]
        else "# Tier-3 evidence analysis",
        "",
        (
            f"The analyzer read {len(report['runs'])} run directories holding "
            f"{len(report['cells'])} cells."
        ),
        *(
            [
                (
                    "Cell verdict labels follow the preregistration in "
                    "docs/adr/evidence/0162-cloudtasks-assessment-1246.md and "
                    "state no support or release decision."
                )
            ]
            if report["cells"]
            else []
        ),
        "",
        "## Cells",
        "",
    ]
    for cell in report["cells"]:
        reasons = ", ".join(cell["reasons"]) or "none"
        p95 = (cell.get("rows") or {}).get("p95_nanos")
        lines.append(
            f"Cell {cell['cell_id']} of run {cell['run_id']} is {cell['status']} "
            f"(reasons: {reasons}) with throughput {show(cell['throughput'])} records "
            f"per second and p95 {'n/a' if p95 is None else str(p95) + ' ns'}."
        )
    lines += ["", "## Groups", ""]
    if not report["groups"]:
        lines.append("No protocol group was found.")
    for group in report["groups"]:
        lines.append(
            f"Group {group['campaign']} {group['flink']} {group['shape']} "
            f"{group['body_bytes']} B is {group['verdict']}."
        )
        for arm, comparison in group["comparisons"].items():
            lines.append(
                f"{arm} against UNNAMED is {comparison['label']} with throughput ratio "
                f"{show(comparison['throughput_ratio'])} and p95 ratio "
                f"{show(comparison['p95_ratio'])}."
            )
        for arm, cost in group["incremental_cost"].items():
            if cost is not None:
                lines.append(
                    f"{arm} against {INCREMENTAL[arm]} costs throughput ratio "
                    f"{show(cost['throughput_ratio'])} and p95 ratio "
                    f"{show(cost['p95_ratio'])}."
                )
        if group["capacity"] is not None:
            state = group["capacity"]["state"]
            status, rate = group["capacity"]["status"], group["capacity"]["next"]
            ending = {
                "frozen": f"the frozen rate is {rate}",
                "ceiling": f"the search reached the protocol ceiling of {rate}",
                "stalled": f"{rate} was offered twice and measured neither time",
                "inconsistent": "no further rate follows",
            }.get(status, f"the next probe is {rate}")
            lines.append(
                f"The capacity search is {status} with accepted {state['accepted']} "
                f"and rejected {state['rejected']}; {ending}."
            )
    if report["unmatched_cells"]:
        lines += [
            "",
            "Cells outside the protocol pin and the calibration set: "
            + ", ".join(report["unmatched_cells"])
            + ".",
        ]
    if report["calibration"]:
        lines += ["", "## Calibration", ""]
        for section in report["calibration"]:
            lines.append(
                f"The items below are campaign {section['campaign']} on Flink "
                f"{section['flink']}."
            )
            for item in section["acceptance"]:
                verdict = {True: "holds", False: "fails", None: "is not evaluable"}
                lines.append(f"The item '{item['item']}' {verdict[item['holds']]}.")
            delay = section["delay_control"]
            if delay is not None:
                lines.append(
                    f"The delay control {'is' if delay['detected'] else 'is not'} "
                    f"detected with throughput {show(delay['throughput'])} and p95 "
                    f"{show(delay['p95_nanos'])} ns."
                )
    return "\n".join(lines) + "\n" + render(report.get("bigquery") or [])


def analyze(evidence, protocol_path=None, kind=None):
    runs = discover_runs(evidence)
    protocol = load(protocol_path)
    cells = [analyze_cell(run, cell, kind) for run in runs for cell in run.cells]
    cells.sort(key=lambda c: (c["campaign"], c["run_id"], c["cell_id"]))
    groups, unmatched = build_groups(cells, protocol_index(protocol))
    calibration_cells = [
        c
        for c in cells
        if kind == "calibration"
        or is_calibration_cell(c["cell_id"])
        or c.get("kind") == "calibration"
    ]
    return {
        "evidence": Path(evidence).name,
        "protocol_sha256": protocol_sha256(protocol_path),
        "runs": [
            {
                "run_id": run.run_id,
                "campaign": run.campaign,
                "flink": run.flink,
                "cells": [c["id"] for c in run.cells],
                "events": len(run.events),
                "result": None
                if not isinstance(run.result, dict)
                else {k: run.result.get(k) for k in ("success", "idle", "cells")},
            }
            for run in runs
        ],
        "cells": cells,
        "groups": groups,
        "unmatched_cells": unmatched,
        "calibration": calibration(calibration_cells, runs),
        # Recomputed from the exported evidence; empty unless the directory
        # holds a deployed BigQuery run.
        "bigquery": assess_runs(runs),
    }


def write_reports(report, out, markdown=render_markdown):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_bytes(
        (json.dumps(plain(report), sort_keys=True, indent=2) + "\n").encode()
    )
    (out / "report.md").write_bytes(markdown(report).encode())
    return out


def summary_line(report, out):
    counts = {}
    for cell in report["cells"]:
        counts[cell["status"]] = counts.get(cell["status"], 0) + 1
    status = ", ".join(f"{k} {v}" for k, v in sorted(counts.items()))
    return (
        f"analyzed {len(report['cells'])} cells in {len(report['runs'])} runs "
        f"({status or 'none'}); {len(report['groups'])} groups"
        + (
            f"; bigquery runs: {bigquery}"
            if (bigquery := bigquery_counts(report.get("bigquery") or []))
            else ""
        )
        + f"; reports in {out}"
    )


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="flink-tier3 analyze",
        description="Offline analysis of a downloaded evidence directory; "
        "never contacts a cluster or a bucket.",
    )
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--protocol", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--kind", choices=("main", "calibration"), default=None)
    args = parser.parse_args(argv)
    try:
        report = analyze(args.evidence, args.protocol, args.kind)
        out = write_reports(report, args.out or args.evidence / "analysis")
    except Failure as error:
        print("analysis failed: " + str(error))
        return 1
    print(summary_line(report, out))
    return 0
