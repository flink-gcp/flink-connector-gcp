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
"""Cloud Tasks evidence: reconcile a cell's rows and receipts, then export them.

The measurement application leaves gzip CSV parts under ``rows/`` and JSON
receipts under ``receipts/`` in the one-day benchmark bucket. The collector
streams them once, decides whether they form a complete observation, copies
them server-side into the retained evidence bucket, verifies the copies, and
only then writes an ``exported.json`` marker and releases the benchmark prefix.
Nothing here accumulates rows: per-sequence state lives in bitmaps and a fixed
array sized from the cell's record limit.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import re
import zlib
from array import array
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

import google_crc32c

from .common import Failure, utc
from .model import cell_records
from .policy import BENCHMARK, CLOUDTASKS_CEILINGS, EVIDENCE, MIB

ROWS = "rows/"
RECEIPTS = "receipts/"
MARKER = "exported.json"
# The row grammar of ObservationLog: the marker plus fifteen fields.
ROW_MARKER = "CT1246"
ROW_FIELDS = 16
PART_CEILING = 64 * MIB
RECEIPT_CEILING = 64 * 1024
LISTING_MAXIMUM = 100000
# A row is about 400 bytes. A part that decodes without a newline inside four
# kibibytes is a decompression bomb rather than evidence, and the supervisor
# has 512 MiB.
ROW_CEILING = 4096
READ_CHUNK = 256 * 1024
# Receipt counts bound allocations and comparisons downstream, so a count no
# session can have produced is rejected with the receipt.
COUNT_CEILING = 3 * 10_000_000
# 8 MiB or sixty seconds per part over a five-hour session, with room to spare.
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
    """The measurement kind a session cell's controls imply."""
    if cell.get("control_delay_millis", 0) == 0 and cell.get("emit_attempts", True):
        return "main"
    return "calibration"


class Source(Protocol):
    """Where a cell's objects are read from: the benchmark bucket or a mirror."""

    def list(self, prefix) -> list[dict]: ...

    def open(self, name, generation): ...


class GcsSource:
    """Objects of one bucket; every listing and open ticks ``reads`` once."""

    def __init__(self, storage, bucket=BENCHMARK, reads=None):
        self.storage, self.bucket, self.reads = storage, bucket, reads

    def _tick(self):
        if self.reads is not None:
            self.reads()

    def list(self, prefix):
        self._tick()
        return self.storage.objects(prefix, self.bucket, LISTING_MAXIMUM)

    def open(self, name, generation):
        self._tick()
        return self.storage.open(name, generation, self.bucket)


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


class Digests:
    """SHA-256 for the marker, MD5 and CRC32C for the copy verification."""

    def __init__(self):
        self.sha256 = hashlib.sha256()
        self.md5 = hashlib.md5()
        self.crc32c = google_crc32c.Checksum()

    def update(self, data):
        self.sha256.update(data)
        self.md5.update(data)
        self.crc32c.update(data)

    def result(self):
        return {
            "sha256": self.sha256.hexdigest(),
            "md5": base64.b64encode(self.md5.digest()).decode(),
            "crc32c": base64.b64encode(self.crc32c.digest()).decode(),
        }


class _Tap:
    """Feeds every raw byte to a digest and bounds the transfer by the listing."""

    def __init__(self, raw, digest, limit, name):
        self.raw, self.digest, self.limit, self.name = raw, digest, limit, name
        self.count = 0

    def read(self, size=-1):
        data = self.raw.read(size)
        self.count += len(data)
        if self.count > self.limit:
            raise Failure("Part is longer than its listing: " + self.name)
        if self.digest is not None and data:
            self.digest.update(data)
        return data

    def readable(self):
        return True

    def seekable(self):
        return False

    def close(self):
        self.raw.close()


def read_object(source, obj, ceiling, digest=None):
    """Whole bytes of one small object, refused before download above ``ceiling``."""
    if int(obj["size"]) > ceiling:
        raise Failure("Object exceeds its size ceiling: " + obj["name"])
    tap = _Tap(
        source.open(obj["name"], obj["generation"]), digest, ceiling, obj["name"]
    )
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


def iter_rows(source, run_id, cell_id, hasher=None, parts=None, order=None):
    """Stream ``(part_name, Row)`` over the cell's parts in incarnation order.

    ``hasher(part_name)`` may return an object whose ``update`` receives the
    raw compressed bytes of that part. ``parts`` is an optional listing already
    taken; ``order`` an optional incarnation sequence that ranks parts before
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
        digest = hasher(name) if hasher is not None else None
        tap = _Tap(source.open(name, obj["generation"]), digest, size, name)
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
    objects: list = field(default_factory=list)

    def flag(self, status, reason):
        if PRECEDENCE[status] > PRECEDENCE[self.status]:
            self.status = status
        if reason not in self.reasons:
            self.reasons.append(reason)

    def summary(self):
        return {
            "status": self.status,
            "reasons": list(self.reasons),
            "counts": dict(self.counts),
        }


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
        digest = Digests()
        data = read_object(self.source, obj, RECEIPT_CEILING, digest)
        self.result.objects.append(
            {
                "name": obj["name"],
                "size": int(obj["size"]),
                "generation": obj["generation"],
                **digest.result(),
            }
        )
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
        digests = {}
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

        def hasher(name):
            digests[name] = Digests()
            return digests[name]

        current_part, current_incarnation = None, None
        previous_minimum, current_minimum = None, None
        rows = iter_rows(self.source, self.run_id, cell["id"], hasher, parts, order)
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
        for obj in parts:
            digest = digests.get(obj["name"])
            if digest is None:
                continue  # A non-strict decode failure stopped before this part.
            result.objects.append(
                {
                    "name": obj["name"],
                    "size": int(obj["size"]),
                    "generation": obj["generation"],
                    **digest.result(),
                }
            )
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


def manifest_sha256(objects):
    lines = sorted(f"{obj['name']} {obj['sha256']}" for obj in objects)
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


class _Reads:
    """Counts one export's read operations and charges them to the session meter."""

    def __init__(self, meter=None):
        self.meter, self.count = meter, 0

    def tick(self, count=1):
        self.count += count
        if self.meter is not None:
            self.meter.tick("read_ops", count)


class Collector:
    """Export each cell's evidence into the retained bucket and release the source."""

    def __init__(self, env, storage):
        self.env, self.storage = env, storage
        self.failed = {}

    @property
    def meter(self):
        return None if self.env.queues is None else self.env.queues.meter

    def marker_name(self, cell_id):
        return cell_prefix(self.env.approval.run_id, cell_id) + MARKER

    def marker(self, cell_id, reads=None):
        """The verified export marker in the evidence bucket, or None."""
        (reads or _Reads(self.meter)).tick()
        value, _ = self.storage.read(self.marker_name(cell_id), EVIDENCE)
        if value is None:
            return None
        if (
            not isinstance(value, dict)
            or value.get("version") != 1
            or value.get("run_id") != self.env.approval.run_id
            or value.get("cell_id") != cell_id
        ):
            raise Failure("Export marker belongs to another run or cell")
        return value

    def exported(self, cell_id):
        return self.marker(cell_id) is not None

    def export(self, cell, outcome="completed"):
        """Reconcile, copy, verify and mark one cell; returns the marker written."""
        cell_id, run_id = cell["id"], self.env.approval.run_id
        reads = _Reads(self.meter)
        existing = self.marker(cell_id, reads)
        if existing is not None:
            return existing
        kind = cell_kind(cell)
        source = GcsSource(self.storage, BENCHMARK, reads.tick)
        reconciliation = Reconciler(source, run_id, cell, kind).run()
        total = sum(obj["size"] for obj in reconciliation.objects)
        control = self.env.refresh()
        if control.evidence_bytes + total > CLOUDTASKS_CEILINGS["evidence_bytes"]:
            raise Failure(
                f"Session evidence ceiling reached: {cell_id} adds {total} bytes to "
                f"{control.evidence_bytes} of {CLOUDTASKS_CEILINGS['evidence_bytes']}"
            )
        exported = []
        for obj in reconciliation.objects:
            name = obj["name"]
            self.storage.rewrite(BENCHMARK, name, obj["generation"], name, EVIDENCE)
            reads.tick()
            metadata = self.storage.metadata(name, EVIDENCE)
            if metadata is None or int(metadata["size"]) != obj["size"]:
                raise Failure("Exported object differs from its source: " + name)
            checked = [
                key
                for key in ("crc32c", "md5")
                if metadata.get(key) is not None and obj.get(key) is not None
            ]
            if not checked:
                raise Failure("Exported object reports no checksum: " + name)
            for key in checked:
                if metadata[key] != obj[key]:
                    raise Failure("Exported object differs from its source: " + name)
            exported.append(
                {
                    "name": name,
                    "size": obj["size"],
                    "sha256": obj["sha256"],
                    "source_generation": obj["generation"],
                    "destination_generation": metadata["generation"],
                }
            )
        marker = {
            "version": 1,
            "run_id": run_id,
            "cell_id": cell_id,
            "kind": kind,
            "arm": cell["arm"],
            "outcome": outcome,
            "at": utc(self.env.clock()),
            "reconciliation": reconciliation.summary(),
            "objects": exported,
            "manifest_sha256": manifest_sha256(exported),
            "evidence_bytes": total,
            "read_ops": reads.count,
        }
        summary = {
            key: marker[key]
            for key in (
                "cell_id",
                "kind",
                "outcome",
                "at",
                "reconciliation",
                "manifest_sha256",
                "evidence_bytes",
                "read_ops",
            )
        }
        summary["objects"] = len(exported)
        # Account for the bytes before the marker authorizes their release: a
        # crash between the two may over-count the session, never under-count.
        self.env.records.record_export(cell_id, summary)
        name = self.marker_name(cell_id)
        self.storage.write(name, marker, "0", EVIDENCE)
        self.storage.write(name, marker, "0", BENCHMARK)
        self.env.emit("cell-exported", summary)
        return marker

    # -- session hooks -------------------------------------------------------

    def poll(self, session, cell, app, pods):
        """Rows travel through storage; nothing to observe per poll."""

    def after_cell(self, session, cell, outcome):
        try:
            self.export(cell, outcome)
        except Failure as error:
            self.failed[cell["id"]] = str(error)
            self.env.evidence_failed = True
            # The session stops here and never reaches its end hook; release
            # what earlier cells already exported so idle verification holds.
            try:
                self.release()
            except Failure as release_error:
                self.env.emit("benchmark-release-failed", {"cause": str(release_error)})
            raise

    def at_session_end(self, session, outcomes):
        for cell in self.env.approval.cells:
            cell_id = cell["id"]
            if cell_id in self.failed or self.exported(cell_id):
                continue
            prefix = cell_prefix(self.env.approval.run_id, cell_id)
            reads = _Reads(self.meter)
            reads.tick()
            objects = self.storage.objects(prefix, BENCHMARK, LISTING_MAXIMUM)
            if not any(
                obj["name"][len(prefix) :].startswith((ROWS, RECEIPTS))
                for obj in objects
            ):
                continue
            try:
                self.export(cell, outcomes.get(cell_id, "interrupted"))
            except Failure as error:
                self.failed[cell_id] = str(error)
                self.env.evidence_failed = True
        retained = self.release()
        if retained:
            self.env.emit("benchmark-evidence-retained", retained)

    def release(self):
        """Delete the benchmark prefix of every cell with a verified marker.

        Returns what remains for cells without one, keyed by cell ID.
        """
        return release_exported(self.env, self.storage, self.failed, self.meter)


def release_exported(env, storage, failed=(), meter=None):
    """Generation-checked release of exported benchmark prefixes; see Collector.

    Only what the marker authorizes is deleted: each evidence object at the
    exact generation that was copied and verified, the job's own state, and
    the benchmark copy of the marker. An object the export refused, and one
    replaced after the export read it, are kept and reported, because the
    downloaded evidence cannot reproduce a reason about an object that no
    longer exists.
    """
    run_id = env.approval.run_id
    retained = {}
    for cell_id in env.approval.cell_ids:
        prefix = cell_prefix(run_id, cell_id)
        reads = _Reads(meter)
        reads.tick()
        objects = storage.objects(prefix, BENCHMARK, LISTING_MAXIMUM)
        if not objects:
            continue
        reads.tick()
        marker, _ = storage.read(prefix + MARKER, EVIDENCE)
        if (
            cell_id in failed
            or marker is None
            or marker.get("version") != 1
            or marker.get("run_id") != run_id
            or marker.get("cell_id") != cell_id
        ):
            retained[cell_id] = {
                "objects": len(objects),
                "bytes": sum(int(obj["size"]) for obj in objects),
            }
            continue
        released, keep = authorized_release(marker, objects, prefix)
        released.sort(key=lambda obj: _release_order(obj, prefix))
        for obj in released:
            storage.delete(obj["name"], obj["generation"], BENCHMARK)
        if keep:
            retained[cell_id] = {
                "objects": len(keep),
                "bytes": sum(int(obj["size"]) for obj in keep),
                "unauthorized": sorted(obj["name"][len(prefix) :] for obj in keep),
            }
        env.emit(
            "benchmark-prefix-released",
            {"cell": cell_id, "objects": len(released), "retained": len(keep)},
        )
    return retained


def authorized_release(marker, objects, prefix):
    """Split a cell's benchmark objects into what a marker releases and the rest.

    A marker releases its own exported objects, each at the generation that was
    copied and verified, the job's checkpoint state and the benchmark copy of
    the marker. Anything else was never this export's to delete, and so is
    never its to prove released either: idle verification uses the same split.
    """
    authorized = {
        (obj.get("name"), str(obj.get("source_generation")))
        for obj in marker.get("objects", [])
    }
    released, retained = [], []
    for obj in objects:
        relative = obj["name"][len(prefix) :]
        target = (
            relative.startswith("state/")
            or relative == MARKER
            or (obj["name"], str(obj["generation"])) in authorized
        )
        (released if target else retained).append(obj)
    return released, retained


def _release_order(obj, prefix):
    """State first, then rows and receipts, the marker last."""
    relative = obj["name"][len(prefix) :]
    if relative == MARKER:
        return 4, relative
    for index, group in enumerate(("state/", ROWS, RECEIPTS)):
        if relative.startswith(group):
            return index, relative
    return 3, relative
