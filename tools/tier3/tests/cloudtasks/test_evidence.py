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
"""Cloud Tasks evidence reconciliation, from a fake bucket and a local tree."""

import gzip
import io
import json

import pytest
from flink_tier3.cloudtasks import evidence as ev
from flink_tier3.cloudtasks.protocol import cell_records

from ..test_lifecycle import Store, rt

CELL_A = {
    "id": "example-wiring-a",
    "arm": "UNNAMED",
    "body_bytes": 1024,
    "parallelism": 1,
    "concurrency": 1,
    "checkpoint_seconds": 1,
    "channel_pool_size": 1,
    "distribution": "even",
    "offered_rate": 10,
    "warmup_seconds": 60,
    "observation_seconds": 180,
    "record_limit": 2430,
    "attempt_limit": 3645,
    "control_delay_millis": 0,
    "emit_attempts": True,
}
CELL_B = {
    **CELL_A,
    "id": "example-wiring-b",
    "arm": "STAGED_HASH",
    "parallelism": 4,
    "concurrency": 4,
    "checkpoint_seconds": 10,
    "offered_rate": 100,
    "record_limit": 26100,
    "attempt_limit": 9788,
}
# Where the fake store keeps a cell's objects; a bucket the reconciler never names.
BUCKET = "cloudtasks-rows"
RUN = "test-1310"
INC = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
INC2 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2"
SRC = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1"
SRC2 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2"
PROC = "cccccccc-cccc-4ccc-8ccc-ccccccccccc1"
PROC2 = "cccccccc-cccc-4ccc-8ccc-ccccccccccc2"
QUEUE = "projects/flink-gcp/locations/us-central1/queues/ct1246-" + RUN
WALL = 1_760_000_000_000
NANOS = 5_000_000_000_000
CALIBRATION = {**CELL_A, "id": "example-wiring-c", "control_delay_millis": 100}
COUNTS_ONLY = {**CELL_A, "id": "example-wiring-d", "emit_attempts": False}


class Synthetic:
    """Receipts and gzip parts for one cell, written the way the application does."""

    def __init__(self, cell, run_id=RUN, delay=None, csv=None):
        self.run_id, self.cell = run_id, cell
        self.delay = cell["control_delay_millis"] if delay is None else delay
        self.csv = cell["emit_attempts"] if csv is None else csv
        self.prefix = ev.cell_prefix(run_id, cell["id"])
        self.files = {}
        # ObservedTaskCreator numbers every attempt of an incarnation, so a row
        # without an explicit ordinal takes the incarnation's next one.
        self.ordinals = {}

    def identity(self, role, incarnation):
        return {
            "version": 1,
            "run_id": self.run_id,
            "cell_id": self.cell["id"],
            "arm": self.cell["arm"],
            "role": role,
            "incarnation": incarnation,
            "process": PROC,
            "control_delay_millis": self.delay,
            "csv_enabled": self.csv,
        }

    def receipt(self, role, incarnation, phase, **fields):
        document = {**self.identity(role, incarnation), **fields}
        name = f"receipts/{role}-{incarnation}-{phase}.json"
        self.files[name] = (json.dumps(document) + "\n").encode()
        return name

    def source(self, incarnation=SRC, start=0, last=None, wall=WALL):
        records = cell_records(self.cell)
        base = {
            "records": records,
            "warmup_seconds": self.cell["warmup_seconds"],
            "observation_seconds": self.cell["observation_seconds"],
            "offered_rate": self.cell["offered_rate"],
            "monotonic_nanos": NANOS,
        }
        self.receipt(
            "source",
            incarnation,
            "start",
            sequence=start,
            wall_millis=wall - 1000,
            **base,
        )
        if last is not False:
            self.receipt(
                "source",
                incarnation,
                "last-mapped",
                sequence=records - 1 if last is None else last,
                wall_millis=wall + 10_000,
                **base,
            )

    def row(
        self,
        sequence,
        attempt=None,
        status="OK",
        incarnation=INC,
        name=None,
        process=PROC,
        origin_process=PROC,
        completed_offset=5_000_000,
        cell=None,
        run_id=None,
    ):
        cell = cell or self.cell
        if attempt is None:
            attempt = self.ordinals.get(incarnation, 0) + 1
        self.ordinals[incarnation] = max(self.ordinals.get(incarnation, 0), attempt)
        origin_millis = WALL + sequence * 100
        origin_nanos = NANOS + sequence * 100_000_000
        completed = origin_nanos + completed_offset
        latency = completed - origin_nanos if process == origin_process else -1
        return ",".join(
            [
                "CT1246",
                run_id or self.run_id,
                cell["id"],
                cell["arm"],
                incarnation,
                process,
                origin_process,
                str(sequence),
                str(attempt),
                str(origin_millis),
                str(origin_nanos),
                str(origin_nanos + 1_000_000),
                str(completed),
                str(latency),
                status,
                name or f"{QUEUE}/tasks/t{sequence}",
            ]
        )

    def part(self, incarnation, index, lines, data=None):
        name = f"rows/{incarnation}-{index:06d}.csv.gz"
        text = ("\n".join(lines) + "\n") if lines else ""
        # mtime 0 keeps a fixture's bytes, and so its digest, reproducible.
        self.files[name] = (
            gzip.compress(text.encode(), mtime=0) if data is None else data
        )
        return name

    def creator(
        self,
        incarnation=INC,
        rows=0,
        parts=0,
        observations=None,
        rows_exported=None,
        complete=True,
        terminal=True,
        wall=WALL,
    ):
        observations = rows if observations is None else observations
        self.receipt(
            "creator",
            incarnation,
            "start",
            attempt_limit=self.cell["attempt_limit"],
            wall_millis=wall,
            monotonic_nanos=NANOS,
        )
        if terminal:
            self.receipt(
                "creator",
                incarnation,
                "terminal",
                attempts=observations,
                completed=observations,
                observations=observations,
                evidence_failed=False,
                limit_reached=False,
                client_close_failed=False,
                rows_exported=rows if rows_exported is None else rows_exported,
                parts_closed=parts,
                rows_flush_failed=False,
                complete=complete,
                wall_millis=wall + 20_000,
                monotonic_nanos=NANOS + 20_000_000_000,
            )

    def install(self, store, bucket=BUCKET):
        for name, data in self.files.items():
            store.write_bytes(self.prefix + name, data, bucket)
        return self

    def install_dir(self, root):
        for name, data in self.files.items():
            path = root / self.prefix / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        return self


def complete_cell(cell=CELL_A, rows=6, per_part=2, incarnation=INC, **options):
    synthetic = Synthetic(cell, **options)
    synthetic.source()
    lines = [synthetic.row(i, incarnation=incarnation) for i in range(rows)]
    chunks = [lines[i : i + per_part] for i in range(0, rows, per_part)]
    for index, chunk in enumerate(chunks, start=1):
        synthetic.part(incarnation, index, chunk)
    synthetic.creator(incarnation, rows=rows, parts=len(chunks))
    return synthetic


class StoreSource:
    """The fake store's objects of one bucket, as a reconciler source."""

    def __init__(self, store, bucket=BUCKET):
        self.store, self.bucket = store, bucket

    def list(self, prefix):
        return self.store.objects(prefix, self.bucket, 100000)

    def open(self, name, generation):
        return self.store.open(name, generation, self.bucket)


def reconcile(synthetic, kind="main", store=None, **options):
    store = store or Store()
    synthetic.install(store)
    return ev.Reconciler(
        StoreSource(store), synthetic.run_id, synthetic.cell, kind, **options
    ).run()


# --- rows and parts -----------------------------------------------------------


def test_row_grammar_matches_the_observation_log():
    synthetic = Synthetic(CELL_A)
    row = ev.parse_row(synthetic.row(7, attempt=2, status="UNAVAILABLE"))
    assert (row.sequence, row.attempt, row.status) == (7, 2, "UNAVAILABLE")
    assert row.latency_nanos == row.completed_nanos - row.origin_nanos
    assert row.name == f"{QUEUE}/tasks/t7"
    line = synthetic.row(1)
    for bad in (
        line + ",extra",
        line.replace("CT1246", "CT0000", 1),
        line.replace(",1,", ",one,", 1),
        line.replace(",OK,", ",ok,", 1),
    ):
        with pytest.raises(rt.Failure, match="Malformed"):
            ev.parse_row(bad)
    assert ev.part_index(f"runs/r/cells/c/rows/{INC}-000042.csv.gz") == (INC, 42)
    with pytest.raises(rt.Failure, match="part name"):
        ev.part_index("rows/not-a-part.csv.gz")


def test_complete_cell_reconciles():
    synthetic = complete_cell()
    result = reconcile(synthetic)
    assert result.status == "complete" and result.reasons == []
    assert result.counts == {
        "rows": 6,
        "parts": 3,
        "incarnations": 1,
        "observations": 6,
        "rows_exported": 6,
        "distinct_ok": 6,
        "already_exists_explained": 0,
        "already_exists_unexplained": 0,
        "late": 0,
        "out_of_order": 0,
    }
    assert len(result.receipts) == 4


def test_missing_terminal_is_incomplete_and_second_source_is_restarted():
    synthetic = complete_cell()
    del synthetic.files[f"receipts/creator-{INC}-terminal.json"]
    result = reconcile(synthetic)
    assert result.status == "incomplete" and result.reasons == ["missing-terminal"]
    synthetic = complete_cell()
    synthetic.source(SRC2, start=3)
    result = reconcile(synthetic)
    assert result.status == "restarted" and "second-source" in result.reasons


def test_part_gap_and_unregistered_incarnation_are_invalid():
    synthetic = complete_cell()
    del synthetic.files[f"rows/{INC}-000002.csv.gz"]
    result = reconcile(synthetic)
    assert result.status == "invalid" and "parts-noncontiguous" in result.reasons
    synthetic = complete_cell()
    synthetic.part(INC2, 1, [synthetic.row(9, incarnation=INC2)])
    result = reconcile(synthetic)
    assert "unregistered-incarnation" in result.reasons


def test_truncated_part_fails_naming_the_part():
    synthetic = complete_cell()
    name = f"rows/{INC}-000002.csv.gz"
    synthetic.files[name] = synthetic.files[name][:-6]
    with pytest.raises(rt.Failure, match=name):
        reconcile(synthetic)
    other = complete_cell()
    garbage = other.part(INC, 3, [], data=b"not gzip at all")
    with pytest.raises(rt.Failure, match=garbage):
        reconcile(other)


def test_non_strict_reconciliation_records_the_decode_failure():
    synthetic = complete_cell()
    name = f"rows/{INC}-000002.csv.gz"
    synthetic.files[name] = b"\x1f\x8b\x08\x00garbage"
    result = reconcile(synthetic, strict=False)
    assert result.status == "invalid" and "decode-failed" in result.reasons


def test_row_count_mismatch_and_foreign_row():
    synthetic = complete_cell()
    synthetic.creator(INC, rows=7, parts=3)
    result = reconcile(synthetic)
    assert result.status == "invalid" and result.reasons == ["row-count-mismatch"]
    synthetic = complete_cell()
    synthetic.part(INC, 3, [synthetic.row(4), synthetic.row(5, cell=CELL_B)])
    result = reconcile(synthetic)
    assert "foreign-row" in result.reasons
    synthetic = complete_cell()
    synthetic.part(INC, 3, [synthetic.row(4), synthetic.row(5, incarnation=INC2)])
    assert "incarnation-mismatch" in reconcile(synthetic).reasons


def test_sequence_range_comes_from_the_source_receipts():
    synthetic = complete_cell()
    synthetic.source(start=3)
    result = reconcile(synthetic)
    assert result.status == "invalid" and "sequence-out-of-range" in result.reasons
    synthetic = complete_cell()
    beyond = CELL_A["record_limit"] + 5
    synthetic.part(INC, 3, [synthetic.row(4), synthetic.row(beyond)])
    assert "sequence-out-of-range" in reconcile(synthetic).reasons


def test_a_repeated_attempt_ordinal_is_a_duplicate_row():
    synthetic = complete_cell()
    line = synthetic.row(4)
    synthetic.part(INC, 3, [line, line])
    result = reconcile(synthetic)
    assert result.status == "invalid" and "duplicate-row" in result.reasons
    # The ordinal is the creator's, not the record's, so a second row that
    # repeats it is a duplicate whatever sequence it names.
    synthetic = complete_cell(rows=4)
    synthetic.part(INC, 3, [synthetic.row(4, attempt=5), synthetic.row(5, attempt=5)])
    synthetic.creator(INC, rows=6, parts=3)
    assert "duplicate-row" in reconcile(synthetic).reasons
    # A retry of the same record takes the creator's next ordinal.
    synthetic = complete_cell(rows=4)
    synthetic.part(INC, 3, [synthetic.row(1, attempt=5), synthetic.row(1, attempt=6)])
    synthetic.creator(INC, rows=6, parts=3)
    assert reconcile(synthetic).status == "complete"
    # The ordinal spans the incarnation, so a repeat in a later part counts too.
    synthetic = complete_cell(rows=4)
    synthetic.part(INC, 3, [synthetic.row(4, attempt=1), synthetic.row(5)])
    synthetic.creator(INC, rows=6, parts=3)
    assert reconcile(synthetic).reasons == ["duplicate-row"]


@pytest.mark.parametrize(
    "ordinal",
    [-7, 0, CELL_A["attempt_limit"] + 1, CELL_A["attempt_limit"] + 8],
)
def test_an_ordinal_outside_the_attempt_limit_is_out_of_range(ordinal):
    # Repeated, so a row that were remembered would also be a duplicate-row.
    synthetic = complete_cell(rows=4)
    line = synthetic.row(4, attempt=ordinal)
    synthetic.part(INC, 3, [line, line, synthetic.row(5, attempt=5)])
    synthetic.creator(INC, rows=7, parts=3)
    result = reconcile(synthetic)
    assert result.status == "invalid" and result.reasons == ["attempt-out-of-range"]


def test_the_last_ordinal_of_the_attempt_limit_is_in_range():
    synthetic = complete_cell(rows=4)
    synthetic.part(
        INC,
        3,
        [synthetic.row(4), synthetic.row(5, attempt=CELL_A["attempt_limit"])],
    )
    synthetic.creator(INC, rows=6, parts=3)
    assert reconcile(synthetic).status == "complete"


def test_duplicate_state_stays_bounded_over_a_creators_rows():
    # Every row of a creator carries a new ordinal, so per-row state would grow
    # with the cell; the ordinal bitmap is sized from the attempt limit instead.
    cell = CELL_B
    sequences = ev._Sequences(cell["record_limit"], cell["attempt_limit"])

    def sizes():
        return {
            key: len(value)
            for key, value in vars(sequences).items()
            if hasattr(value, "__len__")
        }

    before = sizes()
    for ordinal in range(1, cell["attempt_limit"] + 1):
        sequence = ordinal % cell["record_limit"]
        assert not sequences.duplicate(ordinal)
        sequences.outcome(sequence, True)
        sequences.remember(sequence, f"{QUEUE}/tasks/t{sequence}", 0, ordinal)
    assert sizes() == before
    assert sequences.duplicate(1) and sequences.duplicate(cell["attempt_limit"])
    sequences.next_incarnation()
    assert sizes() == before
    assert not sequences.duplicate(1)
    assert not sequences.duplicate(cell["attempt_limit"])


def test_already_exists_is_explained_only_by_an_earlier_named_attempt():
    synthetic = Synthetic(CELL_B)
    synthetic.source()
    name = f"{QUEUE}/tasks/fixed"
    synthetic.part(
        INC,
        1,
        [
            synthetic.row(0, 1, "UNAVAILABLE", name=name),
            synthetic.row(0, 2, "ALREADY_EXISTS", name=name),
            synthetic.row(1, 3, "OK"),
        ],
    )
    synthetic.creator(INC, rows=3, parts=1)
    result = reconcile(synthetic)
    assert result.status == "complete"
    assert result.counts["already_exists_explained"] == 1
    assert result.counts["distinct_ok"] == 1
    for earlier in ("INVALID_ARGUMENT", None):
        synthetic = Synthetic(CELL_B)
        synthetic.source()
        rows = [synthetic.row(0, 2, "ALREADY_EXISTS", name=name)]
        if earlier:
            rows.insert(0, synthetic.row(0, 1, earlier, name=name))
        synthetic.part(INC, 1, rows)
        synthetic.creator(INC, rows=len(rows), parts=1)
        result = reconcile(synthetic)
        assert result.status == "invalid" and "unexplained-duplicate" in result.reasons
        assert result.counts["already_exists_unexplained"] == 1
    # A different name for the same sequence is not the service's deduplication.
    synthetic = Synthetic(CELL_B)
    synthetic.source()
    synthetic.part(
        INC,
        1,
        [
            synthetic.row(0, 1, "OK", name=name),
            synthetic.row(0, 2, "ALREADY_EXISTS", name=name + "-other"),
        ],
    )
    synthetic.creator(INC, rows=2, parts=1)
    assert "unexplained-duplicate" in reconcile(synthetic).reasons


@pytest.mark.parametrize(
    "earlier",
    ["OK", "CANCELLED", "UNAVAILABLE", "DEADLINE_EXCEEDED", "UNKNOWN", "INTERNAL"],
)
def test_every_explaining_status_explains_a_later_already_exists(earlier):
    # OK created the task, and each other status leaves the client unable to
    # tell whether the service did, so a later attempt's ALREADY_EXISTS for the
    # same name is deduplication, not a defect.
    synthetic = Synthetic(CELL_B)
    synthetic.source()
    name = f"{QUEUE}/tasks/fixed"
    synthetic.part(
        INC,
        1,
        [
            synthetic.row(0, 1, earlier, name=name),
            synthetic.row(0, 2, "ALREADY_EXISTS", name=name),
        ],
    )
    synthetic.creator(INC, rows=2, parts=1)
    result = reconcile(synthetic)
    assert result.status == "complete", result.reasons
    assert result.counts["already_exists_explained"] == 1
    assert result.counts["already_exists_unexplained"] == 0


@pytest.mark.parametrize(
    "earlier, later",
    [(255, 256), (300, 301), (ev.COUNT_CEILING - 1, ev.COUNT_CEILING)],
)
def test_attempt_ordinals_past_one_octet_still_order(earlier, later):
    # A creator numbers every attempt it makes, so a retry deep into a cell
    # carries an ordinal far beyond one record's retry count. No protocol cell
    # has this limit; it admits the highest ordinal a creator receipt may count.
    cell = {**CELL_B, "attempt_limit": ev.COUNT_CEILING}
    name = f"{QUEUE}/tasks/fixed"
    for first, second, expected in (
        (earlier, later, "complete"),
        (later, earlier, "invalid"),
    ):
        synthetic = Synthetic(cell)
        synthetic.source()
        synthetic.part(
            INC,
            1,
            [
                synthetic.row(0, first, "INTERNAL", name=name),
                synthetic.row(0, second, "ALREADY_EXISTS", name=name),
            ],
        )
        synthetic.creator(INC, rows=2, parts=1)
        result = reconcile(synthetic)
        assert result.status == expected, (first, second, result.reasons)
        assert ("unexplained-duplicate" in result.reasons) == (expected == "invalid")


def test_already_exists_across_incarnations_follows_start_order():
    name = f"{QUEUE}/tasks/fixed"
    # INC sorts before INC2 by UUID; the creator start receipts decide instead.
    cases = (
        (INC, WALL, INC2, WALL + 5000, "complete"),
        (INC, WALL + 5000, INC2, WALL, "invalid"),
    )
    for ok_inc, ok_wall, ae_inc, ae_wall, expected in cases:
        synthetic = Synthetic(CELL_B)
        synthetic.source()
        synthetic.part(
            ok_inc, 1, [synthetic.row(0, 1, "OK", incarnation=ok_inc, name=name)]
        )
        synthetic.part(
            ae_inc,
            1,
            [synthetic.row(0, 1, "ALREADY_EXISTS", incarnation=ae_inc, name=name)],
        )
        synthetic.creator(ok_inc, rows=1, parts=1, wall=ok_wall)
        synthetic.creator(ae_inc, rows=1, parts=1, wall=ae_wall)
        result = reconcile(synthetic)
        assert result.status == expected, result.reasons
        assert result.counts["incarnations"] == 2
        # Each creator incarnation numbers its attempts from 1; that is no duplicate.
        assert "duplicate-row" not in result.reasons


def test_unnamed_arm_never_explains_already_exists():
    synthetic = complete_cell(rows=4)
    synthetic.part(
        INC,
        3,
        [synthetic.row(4, status="OK"), synthetic.row(4, status="ALREADY_EXISTS")],
    )
    synthetic.creator(INC, rows=6, parts=3)
    result = reconcile(synthetic)
    assert result.status == "invalid" and "unexplained-duplicate" in result.reasons


def test_control_flags_bind_main_cells_only():
    synthetic = complete_cell(CALIBRATION)
    result = reconcile(synthetic, kind="main")
    assert result.status == "invalid" and result.reasons == ["control-flags"]
    assert reconcile(synthetic, kind="calibration").status == "complete"
    # A receipt whose flags differ from the approved cell is not that cell's.
    synthetic = complete_cell(CELL_A, delay=100)
    assert "control-flags" in reconcile(synthetic, kind="calibration").reasons
    counts_only = Synthetic(COUNTS_ONLY)
    counts_only.source()
    counts_only.creator(INC, rows=0, parts=0, observations=6)
    result = reconcile(counts_only, kind="calibration")
    assert result.status == "complete" and result.counts["rows"] == 0
    counts_only.part(INC, 1, [counts_only.row(0)])
    counts_only.creator(INC, rows=0, parts=1, observations=6)
    assert "row-count-mismatch" in reconcile(counts_only, kind="calibration").reasons
    with pytest.raises(rt.Failure, match="kind"):
        ev.Reconciler(None, RUN, CELL_A, "other")


def test_receipt_identity_and_parameters_are_checked():
    synthetic = complete_cell()
    synthetic.receipt(
        "creator", INC, "start", attempt_limit=1, wall_millis=WALL, monotonic_nanos=1
    )
    assert "receipt-parameters" in reconcile(synthetic).reasons
    synthetic = complete_cell()
    synthetic.source(last=4)
    assert "receipt-parameters" in reconcile(synthetic).reasons
    synthetic = complete_cell()
    foreign = Synthetic(CELL_B)
    name = foreign.receipt(
        "creator",
        INC2,
        "start",
        attempt_limit=CELL_B["attempt_limit"],
        wall_millis=1,
        monotonic_nanos=1,
    )
    synthetic.files[name] = foreign.files[name]
    assert "foreign-receipt" in reconcile(synthetic).reasons
    synthetic = complete_cell()
    synthetic.files["receipts/creator-not-a-receipt.json"] = b"{}"
    assert "unexpected-object" in reconcile(synthetic).reasons
    synthetic = complete_cell()
    del synthetic.files[f"receipts/source-{SRC}-last-mapped.json"]
    assert "missing-last-mapped" in reconcile(synthetic).reasons


def test_late_and_out_of_order_rows_are_counted_not_flagged():
    synthetic = complete_cell(rows=4)
    synthetic.part(
        INC,
        3,
        [
            synthetic.row(4, completed_offset=25_000_000_000),
            synthetic.row(5, completed_offset=-3_000_000_000),
        ],
    )
    synthetic.creator(INC, rows=6, parts=3)
    result = reconcile(synthetic)
    assert result.status == "complete"
    assert result.counts["late"] == 1 and result.counts["out_of_order"] == 1
    # Latency across JVM incarnations is the -1 sentinel, never a late row.
    synthetic = complete_cell(rows=4)
    synthetic.part(
        INC,
        3,
        [
            synthetic.row(4, completed_offset=25_000_000_000, origin_process=PROC2),
            synthetic.row(5),
        ],
    )
    synthetic.creator(INC, rows=6, parts=3)
    assert reconcile(synthetic).counts["late"] == 0


def test_oversized_part_is_refused_before_download():
    class Listing:
        def __init__(self, objects):
            self.objects = objects

        def list(self, prefix):
            return [obj for obj in self.objects if obj["name"].startswith(prefix)]

        def open(self, name, generation):
            raise AssertionError("must not download " + name)

    prefix = ev.cell_prefix(RUN, CELL_A["id"])
    source = Listing(
        [
            {
                "name": prefix + f"rows/{INC}-000001.csv.gz",
                "generation": "1",
                "size": str(64 * rt.MIB + 1),
                "created": None,
            }
        ]
    )
    with pytest.raises(rt.Failure, match="size ceiling"):
        list(ev.iter_rows(source, RUN, CELL_A["id"]))
    source.objects[0]["name"] = prefix + f"receipts/creator-{INC}-start.json"
    with pytest.raises(rt.Failure, match="size ceiling"):
        ev.Reconciler(source, RUN, CELL_A).run()


def test_directory_source_round_trips_the_same_fixtures(tmp_path):
    synthetic = complete_cell()
    stored = reconcile(synthetic)
    synthetic.install_dir(tmp_path)
    local = ev.Reconciler(ev.DirectorySource(tmp_path), RUN, CELL_A).run()
    assert (local.status, local.reasons, local.counts, local.receipts) == (
        stored.status,
        stored.reasons,
        stored.counts,
        stored.receipts,
    )
    assert ev.DirectorySource(tmp_path).list("runs/absent/") == []


def test_a_receipt_count_no_session_could_produce_is_refused():
    synthetic = complete_cell()
    synthetic.creator(INC, rows=6, parts=1_000_000_000)
    result = reconcile(synthetic)
    assert result.status == "invalid"
    assert "receipt-counts-out-of-range" in result.reasons
    synthetic = complete_cell()
    synthetic.creator(INC, rows=-1, parts=3)
    assert "receipt-counts-out-of-range" in reconcile(synthetic).reasons


def test_a_part_without_a_newline_is_refused_before_it_is_buffered():
    synthetic = complete_cell()
    long_line = b"x" * (ev.ROW_CEILING + ev.READ_CHUNK + 1)
    synthetic.files[f"rows/{INC}-000001.csv.gz"] = gzip.compress(long_line)
    with pytest.raises(rt.Failure, match="over-long line"):
        reconcile(synthetic)


def test_a_long_line_is_refused_at_its_own_length_not_at_a_chunk_boundary():
    """The bound is the row ceiling, which is what the constant claims."""
    synthetic = complete_cell()
    over = b"x" * (ev.ROW_CEILING + 1) + b"\n" + b"short\n"
    synthetic.files[f"rows/{INC}-000001.csv.gz"] = gzip.compress(over)
    with pytest.raises(rt.Failure, match="over-long line"):
        reconcile(synthetic)
    stream = io.BytesIO(b"x" * ev.ROW_CEILING + b"\n")
    assert [len(line) for line in ev._lines(stream, "part")] == [ev.ROW_CEILING]


def test_a_cell_with_a_control_is_reconciled_as_calibration():
    assert ev.cell_kind(CELL_A) == "main"
    assert ev.cell_kind(CALIBRATION) == "calibration"
    assert ev.cell_kind(COUNTS_ONLY) == "calibration"
