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
"""Cloud Tasks evidence reconciliation, export, release and idle verification."""

import gzip
import hashlib
import json

import pytest
from flink_tier3 import evidence as ev
from flink_tier3.cleanup import verify_idle
from flink_tier3.model import cell_records
from flink_tier3.records import EnvironmentLock
from test_cloudtasks_session import (
    CELL_A,
    CELL_B,
    session_approval,
    session_environment,
)
from test_tier3_lifecycle import Store, rt, sdk_store
from test_tier3_lifecycle import env as env  # noqa: PLC0414 - re-export pytest fixture

RUN = "test-1310"
INC = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
INC2 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2"
SRC = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1"
SRC2 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2"
PROC = "cccccccc-cccc-4ccc-8ccc-ccccccccccc1"
PROC2 = "cccccccc-cccc-4ccc-8ccc-ccccccccccc2"
QUEUE = rt.queue_name(RUN)
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
        attempt=1,
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
        self.files[name] = gzip.compress(text.encode()) if data is None else data
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

    def install(self, store, bucket=rt.BENCHMARK):
        for name, data in self.files.items():
            store.write_bytes(self.prefix + name, data, bucket)
        return self

    def install_dir(self, root):
        for name, data in self.files.items():
            path = root / self.prefix / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        return self

    def sha256(self, name):
        return hashlib.sha256(self.files[name]).hexdigest()


def complete_cell(cell=CELL_A, rows=6, per_part=2, incarnation=INC, **options):
    synthetic = Synthetic(cell, **options)
    synthetic.source()
    lines = [synthetic.row(i, incarnation=incarnation) for i in range(rows)]
    chunks = [lines[i : i + per_part] for i in range(0, rows, per_part)]
    for index, chunk in enumerate(chunks, start=1):
        synthetic.part(incarnation, index, chunk)
    synthetic.creator(incarnation, rows=rows, parts=len(chunks))
    return synthetic


def reconcile(synthetic, kind="main", store=None, **options):
    store = store or Store()
    synthetic.install(store)
    source = ev.GcsSource(store, rt.BENCHMARK)
    return ev.Reconciler(
        source, synthetic.run_id, synthetic.cell, kind, **options
    ).run()


def collector_for(env, cells=(CELL_A, CELL_B)):
    session_approval(env, cells=cells)
    environment = session_environment(env)
    return ev.Collector(environment, env[1]), environment


def events(env, name):
    return [
        d["payload"]
        for (b, n), (d, _g) in env[1].data.items()
        if b == rt.EVIDENCE
        and n.startswith(f"runs/{RUN}/supervisor/")
        and d["event"] == name
    ]


def benchmark_names(store, prefix):
    return sorted(obj["name"] for obj in store.objects(prefix, rt.BENCHMARK, 100000))


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


def test_complete_cell_reconciles_with_streamed_digests():
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
    assert {obj["name"] for obj in result.objects} == {
        synthetic.prefix + name for name in synthetic.files
    }
    for obj in result.objects:
        assert obj["sha256"] == synthetic.sha256(obj["name"][len(synthetic.prefix) :])
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


def test_duplicate_sequence_attempt_inside_and_beyond_the_bitmap():
    synthetic = complete_cell()
    synthetic.part(INC, 3, [synthetic.row(4), synthetic.row(4)])
    result = reconcile(synthetic)
    assert result.status == "invalid" and "duplicate-row" in result.reasons
    synthetic = complete_cell(rows=4)
    synthetic.part(INC, 3, [synthetic.row(1, attempt=4), synthetic.row(1, attempt=4)])
    synthetic.creator(INC, rows=6, parts=3)
    assert "duplicate-row" in reconcile(synthetic).reasons
    synthetic = complete_cell(rows=4)
    synthetic.part(INC, 3, [synthetic.row(1, attempt=4), synthetic.row(1, attempt=5)])
    synthetic.creator(INC, rows=6, parts=3)
    assert reconcile(synthetic).status == "complete"


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
            synthetic.row(1, 1, "OK"),
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
        # A replayed record restarts its attempt ordinals; that is no duplicate.
        assert "duplicate-row" not in result.reasons


def test_unnamed_arm_never_explains_already_exists():
    synthetic = complete_cell(rows=4)
    synthetic.part(
        INC, 3, [synthetic.row(4, 1, "OK"), synthetic.row(4, 2, "ALREADY_EXISTS")]
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
    assert (local.status, local.reasons, local.counts) == (
        stored.status,
        stored.reasons,
        stored.counts,
    )
    assert sorted((o["name"], o["sha256"]) for o in local.objects) == sorted(
        (o["name"], o["sha256"]) for o in stored.objects
    )
    assert ev.DirectorySource(tmp_path).list("runs/absent/") == []


# --- collector ------------------------------------------------------------------


def test_complete_cell_export_writes_verified_markers(env):
    collector, environment = collector_for(env)
    store = env[1]
    synthetic = complete_cell().install(store)
    meter_before = environment.queues.meter.snapshot()["read_ops"]
    assert collector.exported(CELL_A["id"]) is False
    marker = collector.export(CELL_A, "completed")
    assert marker["reconciliation"]["status"] == "complete"
    assert marker["kind"] == "main" and marker["outcome"] == "completed"
    assert {o["name"] for o in marker["objects"]} == {
        synthetic.prefix + name for name in synthetic.files
    }
    for obj in marker["objects"]:
        relative = obj["name"][len(synthetic.prefix) :]
        assert obj["sha256"] == synthetic.sha256(relative)
        assert store.blobs[rt.EVIDENCE, obj["name"]][0] == synthetic.files[relative]
        assert obj["destination_generation"] == store.blobs[rt.EVIDENCE, obj["name"]][1]
    lines = sorted(f"{o['name']} {o['sha256']}" for o in marker["objects"])
    assert (
        marker["manifest_sha256"]
        == hashlib.sha256("\n".join(lines).encode()).hexdigest()
    )
    assert marker["evidence_bytes"] == sum(len(d) for d in synthetic.files.values())
    name = synthetic.prefix + ev.MARKER
    assert store.read(name)[0] == marker and store.read(name, rt.BENCHMARK)[0] == marker
    assert collector.exported(CELL_A["id"]) is True
    control = environment.refresh()
    assert control.exports[CELL_A["id"]]["objects"] == 7
    assert control.evidence_bytes == marker["evidence_bytes"]
    assert (
        events(env, "cell-exported")[0]["manifest_sha256"] == marker["manifest_sha256"]
    )
    # One marker read, one listing, seven opens, seven metadata readbacks.
    assert marker["read_ops"] == 16
    reads = environment.queues.meter.snapshot()["read_ops"] - meter_before
    assert reads == 16 + 2  # plus the two exported() probes above
    # A second export returns the existing marker without a second record.
    assert collector.export(CELL_A, "completed") == marker
    assert environment.refresh().evidence_bytes == marker["evidence_bytes"]


def test_incomplete_cells_are_still_exported_with_their_verdict(env):
    collector, _ = collector_for(env)
    synthetic = complete_cell()
    del synthetic.files[f"receipts/creator-{INC}-terminal.json"]
    synthetic.install(env[1])
    marker = collector.export(CELL_A, "failed")
    assert marker["reconciliation"] == {
        "status": "incomplete",
        "reasons": ["missing-terminal"],
        "counts": marker["reconciliation"]["counts"],
    }
    assert marker["outcome"] == "failed" and len(marker["objects"]) == 6


def test_rewrite_onto_an_existing_destination_is_verified_not_trusted(env):
    collector, _ = collector_for(env)
    store = env[1]
    synthetic = complete_cell().install(store)
    name = synthetic.prefix + f"rows/{INC}-000001.csv.gz"
    store.write_bytes(name, synthetic.files[f"rows/{INC}-000001.csv.gz"], rt.EVIDENCE)
    marker = collector.export(CELL_A)
    assert marker["reconciliation"]["status"] == "complete"
    other = complete_cell(CELL_B).install(store)
    name = other.prefix + f"rows/{INC}-000001.csv.gz"
    store.write_bytes(name, b"different bytes", rt.EVIDENCE)
    with pytest.raises(rt.Failure, match="differs"):
        collector.export(CELL_B)
    assert store.read(other.prefix + ev.MARKER) == (None, "0")
    assert store.read(other.prefix + ev.MARKER, rt.BENCHMARK) == (None, "0")


def test_evidence_budget_is_checked_before_any_copy(env):
    collector, environment = collector_for(env)
    store = env[1]
    complete_cell().install(store)
    path = f"_control/runs/{RUN}.json"
    value, generation = store.read(path)
    ceiling = rt.CLOUDTASKS_CEILINGS["evidence_bytes"]
    store.write(path, {**value, "evidence_bytes": ceiling - 10}, generation)
    with pytest.raises(rt.Failure, match="evidence ceiling"):
        collector.export(CELL_A)
    assert not [n for (b, n) in store.blobs if b == rt.EVIDENCE]
    assert environment.refresh().evidence_bytes == ceiling - 10


def test_after_cell_failure_stops_the_session_and_releases_earlier_exports(env):
    collector, environment = collector_for(env)
    store = env[1]
    first = complete_cell().install(store)
    collector.after_cell(None, CELL_A, "completed")
    second = complete_cell(CELL_B)
    bad = f"rows/{INC}-000002.csv.gz"
    second.files[bad] = second.files[bad][:-4]
    second.install(store)
    with pytest.raises(rt.Failure, match=bad):
        collector.after_cell(None, CELL_B, "completed")
    assert environment.evidence_failed
    assert benchmark_names(store, first.prefix) == []
    assert len(benchmark_names(store, second.prefix)) == len(second.files)
    assert store.read(first.prefix + ev.MARKER)[0]["cell_id"] == CELL_A["id"]
    assert [e["cell"] for e in events(env, "benchmark-prefix-released")] == [
        CELL_A["id"]
    ]
    assert store.read(second.prefix + ev.MARKER) == (None, "0")


def test_session_end_exports_interrupted_cells_and_retains_failed_ones(env):
    collector, environment = collector_for(env)
    store = env[1]
    first = complete_cell().install(store)
    collector.after_cell(None, CELL_A, "completed")
    state = first.prefix + "state/chk-1/_metadata"
    store.write(state, {"chk": 1}, bucket=rt.BENCHMARK)
    interrupted = complete_cell(CELL_B)
    del interrupted.files[f"receipts/creator-{INC}-terminal.json"]
    interrupted.install(store)
    collector.at_session_end(None, {CELL_A["id"]: "completed"})
    assert benchmark_names(store, first.prefix) == []
    assert benchmark_names(store, interrupted.prefix) == []
    marker = store.read(interrupted.prefix + ev.MARKER)[0]
    assert marker["outcome"] == "interrupted"
    assert marker["reconciliation"]["status"] == "incomplete"
    released = [e["cell"] for e in events(env, "benchmark-prefix-released")]
    assert sorted(released) == sorted([CELL_A["id"], CELL_B["id"]])
    assert events(env, "benchmark-evidence-retained") == []
    assert not environment.evidence_failed


def test_session_end_retains_a_cell_whose_export_failed(env):
    collector, environment = collector_for(env)
    store = env[1]
    failing = complete_cell(CELL_B)
    failing.files[f"rows/{INC}-000001.csv.gz"] = b"broken"
    failing.install(store)
    collector.at_session_end(None, {})
    assert environment.evidence_failed
    assert len(benchmark_names(store, failing.prefix)) == len(failing.files)
    retained = events(env, "benchmark-evidence-retained")
    assert retained == [
        {CELL_B["id"]: {"objects": 7, "bytes": retained[0][CELL_B["id"]]["bytes"]}}
    ]
    assert store.read(failing.prefix + ev.MARKER) == (None, "0")


def test_foreign_marker_is_refused(env):
    collector, _ = collector_for(env)
    prefix = ev.cell_prefix(RUN, CELL_A["id"])
    env[1].write(
        prefix + ev.MARKER, {"version": 1, "run_id": "other", "cell_id": CELL_A["id"]}
    )
    with pytest.raises(rt.Failure, match="another run"):
        collector.exported(CELL_A["id"])


def test_verify_idle_refuses_an_exported_but_unreleased_prefix(env):
    _collector, environment = collector_for(env)
    store = env[1]
    verify_idle(environment)
    prefix = ev.cell_prefix(RUN, CELL_A["id"])
    store.write_bytes(
        prefix + f"receipts/creator-{INC}-start.json", b"{}", rt.BENCHMARK
    )
    verify_idle(environment)  # Without a marker the one-day expiry is the backstop.
    store.write(prefix + ev.MARKER, {"version": 1, "cell_id": CELL_A["id"]})
    with pytest.raises(rt.Failure, match="not released"):
        verify_idle(environment)
    for obj in store.objects(prefix, rt.BENCHMARK):
        store.delete(obj["name"], obj["generation"], rt.BENCHMARK)
    verify_idle(environment)


# --- lock scan --------------------------------------------------------------------


@pytest.mark.parametrize("settled", [False, True])
def test_lock_scan_reads_two_objects_per_run_however_large_the_run_is(settled):
    store = Store()
    store.write("runs/other/approval.json", {"run_id": "other"})
    if settled:
        store.write("runs/other/result.json", {"success": True})
    prefix = f"runs/other/cells/c/rows/{INC}-"
    for index in range(200_000):
        store.blobs[rt.EVIDENCE, f"{prefix}{index:06d}.csv.gz"] = (b"x", "1")
    listed = []
    original = store.objects

    def objects(prefix, bucket=rt.EVIDENCE, maximum=20000):
        listed.append(prefix)
        return original(prefix, bucket, maximum)

    store.objects = objects
    lock = EnvironmentLock(store)
    if settled:
        assert lock.acquire({"nonce": "n"}) != "0"
    else:
        with pytest.raises(rt.Failure, match="lacks a final idle receipt"):
            lock.acquire({"nonce": "n"})
    assert listed == ["_control/runs/"]


def test_run_record_round_trips_exports():
    record = rt.RunRecord("n", exports={"c": {"evidence_bytes": 3}}, evidence_bytes=3)
    assert rt.RunRecord.from_dict(record.to_dict()) == record
    assert rt.RunRecord.from_dict({"nonce": "n", "phase": "approved"}).exports == {}


# --- SDK level ------------------------------------------------------------------


def test_storage_sdk_open_streams_one_generation_in_chunks():
    store, adapter = sdk_store(
        [
            (206, b"abcd"),
            (416, {"error": {"message": "range"}}),
        ]
    )
    with pytest.raises(rt.Failure, match="observed"):
        store.open("runs/x/part.gz", "0")
    stream = store.open("runs/x/part.gz", "17", rt.BENCHMARK, chunk_size=4)
    assert stream.read(4) == b"abcd"
    assert stream.read(4) == b""
    stream.close()
    request = adapter.calls[0][0]
    assert "ifGenerationMatch=17" in request.url and rt.BENCHMARK in request.url
    assert request.headers["Range"].startswith("bytes=0-")
    assert all(options["timeout"] == rt.HTTP_TIMEOUT for _, options in adapter.calls)
    assert len(adapter.calls) == 2


def test_storage_sdk_open_translates_a_replaced_generation():
    store, adapter = sdk_store([(412, {"error": {"message": "replaced"}})])
    stream = store.open("runs/x/part.gz", "17", chunk_size=4)
    with pytest.raises(rt.ApiError) as raised:
        stream.read(4)
    assert raised.value.status == 412 and len(adapter.calls) == 1


def test_storage_sdk_rewrite_loops_tokens_with_both_preconditions():
    store, adapter = sdk_store(
        [
            (
                200,
                {
                    "done": False,
                    "rewriteToken": "t1",
                    "totalBytesRewritten": "1",
                    "objectSize": "2",
                },
            ),
            (
                200,
                {
                    "done": True,
                    "totalBytesRewritten": "2",
                    "objectSize": "2",
                    "resource": {"generation": "42", "size": "2"},
                },
            ),
        ]
    )
    assert store.rewrite(rt.BENCHMARK, "runs/x/a", "17", "runs/x/a") == "42"
    first, second = (call[0] for call in adapter.calls)
    assert first.method == "POST" and "/rewriteTo/" in first.url
    assert rt.BENCHMARK in first.url and rt.EVIDENCE in first.url
    assert "ifGenerationMatch=0" in first.url
    assert "ifSourceGenerationMatch=17" in first.url
    assert "sourceGeneration=17" in first.url
    assert "rewriteToken=t1" in second.url
    with pytest.raises(rt.Failure, match="observed"):
        store.rewrite(rt.BENCHMARK, "runs/x/a", "0", "runs/x/a")


def test_storage_sdk_rewrite_returns_existing_destination_metadata_on_412():
    replies = [
        (412, {"error": {"message": "exists"}}),
        (
            200,
            {
                "generation": "9",
                "size": "2",
                "md5Hash": "md5==",
                "crc32c": "crc==",
                "timeCreated": "2026-09-19T00:00:00.000Z",
            },
        ),
    ]
    store, adapter = sdk_store(replies)
    assert store.rewrite(rt.BENCHMARK, "runs/x/a", "17", "runs/x/a") == {
        "size": "2",
        "crc32c": "crc==",
        "md5": "md5==",
        "generation": "9",
        "created": "2026-09-19T00:00:00.000Z",
    }
    assert adapter.calls[1][0].method == "GET" and len(adapter.calls) == 2
    # A 412 without a destination is the source precondition: it propagates.
    store, adapter = sdk_store(
        [
            (412, {"error": {"message": "source"}}),
            (404, {"error": {"message": "absent"}}),
        ]
    )
    with pytest.raises(rt.ApiError) as raised:
        store.rewrite(rt.BENCHMARK, "runs/x/a", "17", "runs/x/a")
    assert raised.value.status == 412


def test_storage_sdk_metadata_and_prefixes():
    store, adapter = sdk_store(
        [
            (404, {"error": {"message": "absent"}}),
            (200, {"generation": "3", "size": "5"}),
            (200, {"prefixes": ["runs/a/", "runs/b/"], "items": []}),
        ]
    )
    assert store.metadata("runs/x/a") is None
    assert store.metadata("runs/x/a") == {
        "size": "5",
        "crc32c": None,
        "md5": None,
        "generation": "3",
        "created": None,
    }
    assert store.prefixes("runs/") == {"runs/a/", "runs/b/"}
    request = adapter.calls[2][0]
    assert "delimiter=%2F" in request.url and "prefix=runs%2F" in request.url
    assert all(options["timeout"] == rt.HTTP_TIMEOUT for _, options in adapter.calls)


def test_storage_sdk_list_reports_creation_time():
    store, _ = sdk_store(
        [
            (
                200,
                {
                    "items": [
                        {
                            "name": "runs/one",
                            "generation": "17",
                            "size": "2",
                            "timeCreated": "2026-09-19T01:02:03.456Z",
                        }
                    ]
                },
            )
        ]
    )
    assert store.objects("runs/") == [
        {
            "name": "runs/one",
            "generation": "17",
            "size": "2",
            "created": "2026-09-19T01:02:03.456Z",
        }
    ]
