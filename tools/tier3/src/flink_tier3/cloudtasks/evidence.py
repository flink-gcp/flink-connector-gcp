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
"""Cloud Tasks evidence: reconcile a cell's rows and receipts.

The measurement application leaves gzip CSV parts under ``rows/`` and JSON
receipts under ``receipts/`` below its evidence root. The reconciler streams
them once and decides whether they form a complete observation. Nothing here
accumulates rows: per-sequence state lives in bitmaps and a fixed array sized
from the cell's record limit.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import zlib
from array import array
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from ..common import Failure, utc
from ..policy import MIB
from .protocol import cell_records

ROWS = "rows/"
RECEIPTS = "receipts/"
# The row grammar of ObservationLog: the marker plus fifteen fields.
ROW_MARKER = "CT1246"
ROW_FIELDS = 16
PART_CEILING = 64 * MIB
RECEIPT_CEILING = 64 * 1024
# A row is about 400 bytes. A part that decodes without a newline inside four
# kibibytes is a decompression bomb rather than evidence.
ROW_CEILING = 4096
READ_CHUNK = 256 * 1024
# Receipt counts bound allocations and comparisons downstream, so a count no
# cell can have produced is rejected with the receipt.
COUNT_CEILING = 3 * 10_000_000
# 8 MiB or sixty seconds per part over the longest run, with room to spare.
PARTS_CEILING = 100000
KINDS = ("main", "calibration", "interrupt-control")
# Statuses after which a later ALREADY_EXISTS for the same name is the
# service's deduplication of an attempt whose outcome the client never saw.
EXPLAINING = frozenset(
    {"OK", "CANCELLED", "UNAVAILABLE", "DEADLINE_EXCEEDED", "UNKNOWN"}
)
_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
PART = re.compile(rf"(?:^|/)rows/({_UUID})-([0-9]{{6}})\.csv\.gz\Z")
RECEIPT = re.compile(
    rf"(?:^|/)receipts/(source|creator)-({_UUID})-(start|last-mapped|terminal)\.json\Z"
)
INTEGER = re.compile(r"-?[0-9]+\Z")
STATUS = re.compile(r"[A-Z][A-Z_]*\Z")
IDENTITY = ("run_id", "cell_id", "arm", "role", "incarnation", "process")
SOURCE_FIELDS = (
    "sequence",
    "records",
    "warmup_seconds",
    "observation_seconds",
    "offered_rate",
    "wall_millis",
    "monotonic_nanos",
)
# The application parses the offered rate as a double, so its receipts carry
# it as a JSON number that may be written `10.0`. The protocol admits only
# whole rates, so the value must be integral without having to be an integer.
INTEGRAL_FIELDS = ("offered_rate",)
TERMINAL_COUNTS = (
    "attempts",
    "completed",
    "observations",
    "rows_exported",
    "parts_closed",
)
TERMINAL_FLAGS = (
    "evidence_failed",
    "limit_reached",
    "client_close_failed",
    "rows_flush_failed",
    "complete",
)
# Verdict precedence: a later, weaker status never hides an earlier stronger one.
PRECEDENCE = {"complete": 0, "incomplete": 1, "restarted": 2, "invalid": 3}


def cell_prefix(run_id, cell_id):
    return f"runs/{run_id}/cells/{cell_id}/"


def cell_kind(cell):
    """The measurement kind a cell's controls imply."""
    if cell.get("control_delay_millis", 0) == 0 and cell.get("emit_attempts", True):
        return "main"
    return "calibration"


class Source(Protocol):
    """Where a cell's objects are read from."""

    def list(self, prefix) -> list[dict]: ...

    def open(self, name, generation): ...


class DirectorySource:
    """A local mirror whose relative paths are the object names; no generations."""

    def __init__(self, root):
        self.root = Path(root)

    def list(self, prefix):
        base = (
            self.root / prefix if prefix.endswith("/") else (self.root / prefix).parent
        )
        if not base.is_dir():
            return []
        result = []
        for path in sorted(path for path in base.rglob("*") if path.is_file()):
            name = path.relative_to(self.root).as_posix()
            if not name.startswith(prefix):
                continue
            stat = path.stat()
            result.append(
                {
                    "name": name,
                    "generation": "0",
                    "size": str(stat.st_size),
                    "created": utc(stat.st_mtime),
                }
            )
        return result

    def open(self, name, generation):
        return (self.root / name).open("rb")


@dataclass(frozen=True, slots=True)
class Row:
    run_id: str
    cell_id: str
    arm: str
    incarnation: str
    process: str
    origin_process: str
    sequence: int
    attempt: int
    origin_millis: int
    origin_nanos: int
    send_nanos: int
    completed_nanos: int
    latency_nanos: int
    status: str
    name: str


def parse_row(line):
    """One ObservationLog line as a typed row; anything else is a Failure."""
    fields = line.split(",")
    if len(fields) != ROW_FIELDS or fields[0] != ROW_MARKER:
        raise Failure("Malformed evidence row: field count or marker")
    numbers = fields[7:14]
    if not all(INTEGER.fullmatch(value) for value in numbers):
        raise Failure("Malformed evidence row: non-integer field")
    if not STATUS.fullmatch(fields[14]) or not all(fields[1:7]):
        raise Failure("Malformed evidence row: identity or status")
    return Row(
        fields[1],
        fields[2],
        fields[3],
        fields[4],
        fields[5],
        fields[6],
        *(int(value) for value in numbers),
        fields[14],
        fields[15],
    )


def part_index(name):
    """``(incarnation, index)`` of ``rows/<incarnation>-<NNNNNN>.csv.gz``."""
    match = PART.search(name)
    if not match:
        raise Failure("Not an evidence part name: " + name)
    return match[1], int(match[2])


class _Tap:
    """Bounds the transfer of one object by the size its listing reported."""

    def __init__(self, raw, limit, name):
        self.raw, self.limit, self.name = raw, limit, name
        self.count = 0

    def read(self, size=-1):
        data = self.raw.read(size)
        self.count += len(data)
        if self.count > self.limit:
            raise Failure("Part is longer than its listing: " + self.name)
        return data

    def readable(self):
        return True

    def seekable(self):
        return False

    def close(self):
        self.raw.close()


def read_object(source, obj, ceiling):
    """Whole bytes of one small object, refused before download above ``ceiling``."""
    if int(obj["size"]) > ceiling:
        raise Failure("Object exceeds its size ceiling: " + obj["name"])
    tap = _Tap(source.open(obj["name"], obj["generation"]), ceiling, obj["name"])
    try:
        chunks = []
        while chunk := tap.read(65536):
            chunks.append(chunk)
    finally:
        tap.close()
    data = b"".join(chunks)
    if len(data) != int(obj["size"]):
        raise Failure("Object length differs from its listing: " + obj["name"])
    return data


def iter_rows(source, run_id, cell_id, parts=None, order=None):
    """Stream ``(part_name, Row)`` over the cell's parts in incarnation order.

    ``parts`` is an optional listing already taken; ``order`` an optional incarnation sequence that ranks parts before
    the UUID order. Parts are decoded incrementally and never held whole.
    """
    if parts is None:
        parts = source.list(cell_prefix(run_id, cell_id) + ROWS)
    rank = {incarnation: index for index, incarnation in enumerate(order or ())}

    def key(obj):
        incarnation, index = part_index(obj["name"])
        return rank.get(incarnation, len(rank)), incarnation, index

    for obj in sorted(parts, key=key):
        name, size = obj["name"], int(obj["size"])
        if size > PART_CEILING:
            raise Failure("Part exceeds its size ceiling: " + name)
        tap = _Tap(source.open(name, obj["generation"]), size, name)
        try:
            with gzip.GzipFile(fileobj=tap, mode="rb") as unzipped:
                for number, line in enumerate(_lines(unzipped, name), start=1):
                    try:
                        row = parse_row(line.decode("utf-8"))
                    except Failure as error:
                        raise Failure(f"{error} (row {number} of {name})") from error
                    yield name, row
        except (EOFError, zlib.error, UnicodeDecodeError, OSError) as error:
            raise Failure("Evidence part failed to decode: " + name) from error
        finally:
            tap.close()
        if tap.count != size:
            raise Failure("Part length differs from its listing: " + name)


def _lines(stream, name, limit=ROW_CEILING):
    """Decoded lines of one part, refusing a line longer than ``limit``.

    A small compressed part can decode into an enormous newline-free string.
    Reading it as text would buffer that string whole before any field is
    validated, so the length is bounded while the bytes arrive.
    """
    pending = b""
    while True:
        chunk = stream.read(READ_CHUNK)
        if not chunk:
            break
        pending += chunk
        start = 0
        while (index := pending.find(b"\n", start)) != -1:
            if index - start > limit:
                raise Failure("Evidence part holds an over-long line: " + name)
            yield pending[start:index]
            start = index + 1
        pending = pending[start:]
        if len(pending) > limit:
            raise Failure("Evidence part holds an over-long line: " + name)
    if pending:
        yield pending


@dataclass
class Reconciliation:
    status: str = "complete"
    reasons: list = field(default_factory=list)
    counts: dict = field(default_factory=dict)
    receipts: dict = field(default_factory=dict)

    def flag(self, status, reason):
        if PRECEDENCE[status] > PRECEDENCE[self.status]:
            self.status = status
        if reason not in self.reasons:
            self.reasons.append(reason)


class _Sequences:
    """Per-sequence state: a bitmap per attempt ordinal, a bitmap of OK outcomes
    and one 64-bit slot naming the last explaining attempt; a dict past the limit.

    Attempt ordinals restart when a restored incarnation replays a record, so
    the ``(sequence, attempt)`` bitmaps are cleared per incarnation while the
    outcome bitmap and the name slots span the whole cell.
    """

    def __init__(self, limit):
        self.limit = max(0, int(limit))
        octets = (self.limit + 7) // 8
        self.seen = [bytearray(octets) for _ in range(3)]
        self.seen_beyond = set()
        self.ok = bytearray(octets)
        self.ok_beyond = set()
        self.ok_count = 0
        self.slots = array("Q", [0]) * self.limit
        self.slots_beyond = {}

    def next_incarnation(self):
        for bitmap in self.seen:
            bitmap[:] = bytes(len(bitmap))
        self.seen_beyond = set()

    def _inside(self, sequence, attempt):
        return 0 <= sequence < self.limit and 1 <= attempt <= 3

    def duplicate(self, sequence, attempt):
        if self._inside(sequence, attempt):
            bitmap, mask = self.seen[attempt - 1], 1 << (sequence & 7)
            if bitmap[sequence >> 3] & mask:
                return True
            bitmap[sequence >> 3] |= mask
            return False
        if (sequence, attempt) in self.seen_beyond:
            return True
        self.seen_beyond.add((sequence, attempt))
        return False

    def outcome(self, sequence, ok):
        if 0 <= sequence < self.limit:
            mask = 1 << (sequence & 7)
            was = bool(self.ok[sequence >> 3] & mask)
            if ok:
                self.ok[sequence >> 3] |= mask
            else:
                self.ok[sequence >> 3] &= ~mask & 0xFF
        else:
            was = sequence in self.ok_beyond
            (self.ok_beyond.add if ok else self.ok_beyond.discard)(sequence)
        self.ok_count += int(ok) - int(was)

    @staticmethod
    def _encode(name, rank, attempt):
        digest = hashlib.blake2b(name.encode(), digest_size=6).digest()
        return (
            (int.from_bytes(digest, "big") << 16)
            | (min(rank, 255) << 8)
            | min(attempt, 255)
        ) + 1

    def remember(self, sequence, name, rank, attempt):
        value = self._encode(name, rank, attempt)
        if 0 <= sequence < self.limit:
            self.slots[sequence] = value
        else:
            self.slots_beyond[sequence] = value

    def explained(self, sequence, name, rank, attempt):
        value = (
            self.slots[sequence]
            if 0 <= sequence < self.limit
            else self.slots_beyond.get(sequence, 0)
        )
        if not value:
            return False
        value -= 1
        earlier_rank, earlier_attempt = (value >> 8) & 255, value & 255
        same_name = value >> 16 == (self._encode(name, 0, 0) - 1) >> 16
        return same_name and (
            earlier_rank < min(rank, 255)
            or (earlier_rank == min(rank, 255) and earlier_attempt < min(attempt, 255))
        )


class Reconciler:
    """Decide whether one cell's receipts and rows form a complete observation."""

    def __init__(self, source, run_id, cell, kind="main", strict=True):
        if kind not in KINDS:
            raise Failure("Unknown measurement kind")
        self.source, self.run_id, self.cell, self.kind = source, run_id, cell, kind
        self.strict = strict
        self.prefix = cell_prefix(run_id, cell["id"])
        self.result = Reconciliation()
        self.sources = {}
        self.creators = {}

    def run(self):
        result = self.result
        listing = self.source.list(self.prefix)
        receipts, parts = [], []
        for obj in listing:
            relative = obj["name"][len(self.prefix) :]
            if relative.startswith(RECEIPTS):
                if not RECEIPT.search(obj["name"]):
                    result.flag("invalid", "unexpected-object")
                    continue
                receipts.append(obj)
            elif relative.startswith(ROWS):
                if not PART.search(obj["name"]):
                    result.flag("invalid", "unexpected-object")
                    continue
                parts.append(obj)
        for obj in receipts:
            self._receipt(obj)
        self._sources()
        order = self._creators()
        indices = self._parts(parts, order)
        self._rows(parts, order, indices)
        return result

    # -- receipts ------------------------------------------------------------

    def _receipt(self, obj):
        role, incarnation, phase = RECEIPT.search(obj["name"]).groups()
        data = read_object(self.source, obj, RECEIPT_CEILING)
        try:
            receipt = json.loads(data)
        except ValueError as error:
            if self.strict:
                raise Failure("Receipt failed to decode: " + obj["name"]) from error
            self.result.flag("invalid", "decode-failed")
            return
        if not isinstance(receipt, dict) or receipt.get("version") != 1:
            self.result.flag("invalid", "receipt-version")
            return
        self.result.receipts[obj["name"]] = receipt
        cell = self.cell
        if (
            receipt.get("run_id") != self.run_id
            or receipt.get("cell_id") != cell["id"]
            or receipt.get("arm") != cell["arm"]
        ):
            self.result.flag("invalid", "foreign-receipt")
            return
        if (
            receipt.get("role") != role
            or receipt.get("incarnation") != incarnation
            or not isinstance(receipt.get("process"), str)
        ):
            self.result.flag("invalid", "receipt-identity")
            return
        delay, csv = receipt.get("control_delay_millis"), receipt.get("csv_enabled")
        if (
            not _integer(delay)
            or not isinstance(csv, bool)
            or delay != cell["control_delay_millis"]
            or csv != cell["emit_attempts"]
            or (self.kind == "main" and (delay != 0 or csv is not True))
        ):
            self.result.flag("invalid", "control-flags")
        if role == "source":
            fields = SOURCE_FIELDS
        elif phase == "start":
            fields = ("attempt_limit", "wall_millis", "monotonic_nanos")
        else:
            fields = TERMINAL_COUNTS + ("wall_millis", "monotonic_nanos")
        if not all(
            _integral(receipt.get(name))
            if name in INTEGRAL_FIELDS
            else _integer(receipt.get(name))
            for name in fields
        ) or (
            role == "creator"
            and phase == "terminal"
            and not all(isinstance(receipt.get(name), bool) for name in TERMINAL_FLAGS)
        ):
            self.result.flag("invalid", "receipt-fields")
            return
        # A terminal count bounds later comparisons and allocations, so a
        # count outside what a session can produce is rejected here, where the
        # receipt is still the only thing that claims it.
        if role == "creator" and phase == "terminal" and not _bounded(receipt):
            self.result.flag("invalid", "receipt-counts-out-of-range")
            return
        table = self.sources if role == "source" else self.creators
        table.setdefault(incarnation, {})[phase] = receipt

    def _sources(self):
        result, cell = self.result, self.cell
        records = cell_records(cell)
        starts = [inc for inc, phases in self.sources.items() if "start" in phases]
        if not starts:
            result.flag("incomplete", "missing-source-start")
        elif len(starts) > 1:
            result.flag("restarted", "second-source")
        for phases in self.sources.values():
            if "start" not in phases:
                result.flag("invalid", "orphan-receipt")
                continue
            if "last-mapped" not in phases:
                result.flag("incomplete", "missing-last-mapped")
            for receipt in phases.values():
                if (
                    receipt["records"] != records
                    or receipt["warmup_seconds"] != cell["warmup_seconds"]
                    or receipt["observation_seconds"] != cell["observation_seconds"]
                    or receipt["offered_rate"] != cell["offered_rate"]
                ):
                    result.flag("invalid", "receipt-parameters")
            last = phases.get("last-mapped")
            if last is not None and last["sequence"] != records - 1:
                result.flag("invalid", "receipt-parameters")
            if (
                phases["start"]["sequence"] < 0
                or phases["start"]["sequence"] >= records
            ):
                result.flag("invalid", "receipt-parameters")

    def _creators(self):
        """Validate creator receipts and return incarnations in start order."""
        result, cell = self.result, self.cell
        for phases in self.creators.values():
            start, terminal = phases.get("start"), phases.get("terminal")
            if start is None:
                result.flag("invalid", "orphan-receipt")
                continue
            if start["attempt_limit"] != cell["attempt_limit"]:
                result.flag("invalid", "receipt-parameters")
            if terminal is None:
                result.flag("incomplete", "missing-terminal")
                continue
            if terminal["complete"] is not True or not (
                terminal["attempts"]
                == terminal["completed"]
                == terminal["observations"]
            ):
                result.flag("incomplete", "terminal-incomplete")
        return sorted(
            (inc for inc, phases in self.creators.items() if "start" in phases),
            key=lambda inc: (self.creators[inc]["start"]["wall_millis"], inc),
        )

    def _parts(self, parts, order):
        result = self.result
        indices = {}
        for obj in parts:
            incarnation, index = part_index(obj["name"])
            indices.setdefault(incarnation, set()).add(index)
        for incarnation in set(indices) - set(order):
            result.flag("invalid", "unregistered-incarnation")
        for incarnation in order:
            present = indices.get(incarnation, set())
            terminal = self.creators[incarnation].get("terminal")
            closed = (
                terminal["parts_closed"]
                if terminal
                else (max(present) if present else 0)
            )
            # ``1..closed`` without building it: the receipt names the count
            # and a contiguous run is pinned by its length and its two edges.
            if not (
                len(present) == closed
                and (not present or (min(present) == 1 and max(present) == closed))
            ):
                result.flag("invalid", "parts-noncontiguous")
        return indices

    # -- rows ----------------------------------------------------------------

    def _rows(self, parts, order, indices):
        result, cell = self.result, self.cell
        records = cell_records(cell)
        starts = [p["start"] for p in self.sources.values() if "start" in p]
        lasts = [p["last-mapped"] for p in self.sources.values() if "last-mapped" in p]
        low = min((s["sequence"] for s in starts), default=0)
        high = max((s["sequence"] for s in lasts), default=records - 1)
        last_mapped_wall = max((s["wall_millis"] for s in lasts), default=None)
        last_receipt_wall = max(
            (
                receipt["wall_millis"]
                for table in (self.sources, self.creators)
                for phases in table.values()
                for receipt in phases.values()
            ),
            default=None,
        )
        rank = {incarnation: index for index, incarnation in enumerate(order)}
        sequences = _Sequences(cell["record_limit"])
        rows_by_incarnation = dict.fromkeys(order, 0)
        counts = {
            "rows": 0,
            "parts": len(parts),
            "incarnations": len(order),
            "observations": sum(
                self.creators[inc]["terminal"]["observations"]
                for inc in order
                if "terminal" in self.creators[inc]
            ),
            "rows_exported": sum(
                self.creators[inc]["terminal"]["rows_exported"]
                for inc in order
                if "terminal" in self.creators[inc]
            ),
            "distinct_ok": 0,
            "already_exists_explained": 0,
            "already_exists_unexplained": 0,
            "late": 0,
            "out_of_order": 0,
        }

        current_part, current_incarnation = None, None
        previous_minimum, current_minimum = None, None
        rows = iter_rows(self.source, self.run_id, cell["id"], parts, order)
        try:
            for part, row in rows:
                if part != current_part:
                    incarnation, _ = part_index(part)
                    if incarnation != current_incarnation:
                        previous_minimum = None
                        sequences.next_incarnation()
                    else:
                        previous_minimum = current_minimum
                    current_part, current_incarnation, current_minimum = (
                        part,
                        incarnation,
                        None,
                    )
                counts["rows"] += 1
                rows_by_incarnation[current_incarnation] = (
                    rows_by_incarnation.get(current_incarnation, 0) + 1
                )
                if (
                    row.run_id != self.run_id
                    or row.cell_id != cell["id"]
                    or row.arm != cell["arm"]
                ):
                    result.flag("invalid", "foreign-row")
                    continue
                if row.incarnation != current_incarnation:
                    result.flag("invalid", "incarnation-mismatch")
                    continue
                if not low <= row.sequence <= high:
                    result.flag("invalid", "sequence-out-of-range")
                if row.attempt < 1:
                    result.flag("invalid", "sequence-out-of-range")
                if sequences.duplicate(row.sequence, row.attempt):
                    result.flag("invalid", "duplicate-row")
                    continue
                same_process = (
                    row.process == row.origin_process and row.latency_nanos >= 0
                )
                if (
                    same_process
                    and last_mapped_wall is not None
                    and last_receipt_wall is not None
                    and row.origin_millis <= last_mapped_wall
                ):
                    completion_wall = row.origin_millis + (
                        (row.completed_nanos - row.origin_nanos) / 1e6
                    )
                    if completion_wall > last_receipt_wall + 1000:
                        counts["late"] += 1
                if (
                    previous_minimum is not None
                    and row.completed_nanos < previous_minimum - 1_000_000_000
                ):
                    counts["out_of_order"] += 1
                if current_minimum is None or row.completed_nanos < current_minimum:
                    current_minimum = row.completed_nanos
                sequences.outcome(row.sequence, row.status == "OK")
                incarnation_rank = rank.get(current_incarnation, len(rank))
                if row.status == "ALREADY_EXISTS":
                    if cell["arm"] != "UNNAMED" and sequences.explained(
                        row.sequence, row.name, incarnation_rank, row.attempt
                    ):
                        counts["already_exists_explained"] += 1
                    else:
                        counts["already_exists_unexplained"] += 1
                        result.flag("invalid", "unexplained-duplicate")
                elif row.status in EXPLAINING:
                    sequences.remember(
                        row.sequence, row.name, incarnation_rank, row.attempt
                    )
        except Failure:
            if self.strict:
                raise
            result.flag("invalid", "decode-failed")
        finally:
            rows.close()
        counts["distinct_ok"] = sequences.ok_count
        for incarnation in order:
            terminal = self.creators[incarnation].get("terminal")
            if terminal is None:
                continue
            rows_seen = rows_by_incarnation.get(incarnation, 0)
            if self.creators[incarnation]["start"].get("csv_enabled") is False:
                if rows_seen or terminal["rows_exported"]:
                    result.flag("invalid", "row-count-mismatch")
            elif not (
                rows_seen == terminal["observations"] == terminal["rows_exported"]
            ):
                result.flag("invalid", "row-count-mismatch")
        for incarnation in set(rows_by_incarnation) - set(order):
            if rows_by_incarnation[incarnation]:
                result.flag("invalid", "unregistered-incarnation")
        result.counts = counts


def _integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _integral(value):
    """A whole number, whichever JSON type the writer chose for it."""
    if _integer(value):
        return True
    return isinstance(value, float) and value.is_integer()


def _bounded(terminal):
    """Every terminal count is a plausible session count, not just an integer."""
    return all(
        0
        <= terminal[name]
        <= (PARTS_CEILING if name == "parts_closed" else COUNT_CEILING)
        for name in TERMINAL_COUNTS
    )
