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
"""The reconciler against receipts the measurement application really wrote.

Every other test in this suite builds its receipts from the same file that
reads them, so a disagreement between the Java writer and the Python reader
cannot appear. It did: the application declares the offered rate as a double,
so its source receipts carry `10.0`, and the reader required an `int`. Every
cell of the first calibration session reconciled `invalid` on that one field
while its rows were perfect.

The fixtures under `receipts/` are the four receipts of cell `k01-pace-10` of
run `cal1246-221-20260920g`, copied unaltered from the evidence bucket. They
are the contract's other side, and they are what makes this test able to fail.
"""

import gzip
import json
import math
import shutil
from pathlib import Path

from flink_tier3 import evidence as ev

RECEIPTS = Path(__file__).parent / "receipts"
# The pinned k01 cell of the calibration session, as the approval carried it.
CELL = {
    "id": "k01-pace-10",
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
    "control_delay_millis": 0,
    "emit_attempts": True,
    "record_limit": 2430,
    "attempt_limit": 3645,
}
RUN = "cal1246-221-20260920g"
# The fixture carries the receipts and none of the 2,430 rows they account
# for, so exactly one complaint is legitimate: the rows do not add up.
# Anything else, named or not, is the reader and the writer disagreeing about
# a receipt. The four empty parts below are synthetic, and they are what makes
# `parts_closed` legible: a reader that took another count would raise
# `parts-noncontiguous` over them, which the expected list refuses.
ROWLESS_REASONS = ["row-count-mismatch"]
# The creator incarnation of the captured receipts, which owns their parts.
CREATOR = "510fa4c8-82b3-4a80-bc2b-d9c014ab38b9"


def install(root, parts=4):
    cell = root / ev.cell_prefix(RUN, CELL["id"])
    receipts = cell / ev.RECEIPTS.rstrip("/")
    receipts.mkdir(parents=True, exist_ok=True)
    for source in sorted(RECEIPTS.glob("*.json")):
        shutil.copy(source, receipts / source.name)
    rows = cell / ev.ROWS.rstrip("/")
    rows.mkdir(parents=True, exist_ok=True)
    for index in range(1, parts + 1):
        # Empty, so the parts are countable without carrying 2,430 rows.
        (rows / f"{CREATOR}-{index:06d}.csv.gz").write_bytes(
            gzip.compress(b"", mtime=0)
        )
    return root


def test_the_reconciler_accepts_the_receipts_the_application_writes(tmp_path):
    result = ev.Reconciler(
        ev.DirectorySource(install(tmp_path)), RUN, CELL, "main"
    ).run()
    assert sorted(result.reasons) == ROWLESS_REASONS, result.reasons


def test_the_captured_receipts_are_the_ones_that_broke_the_reader():
    """The fixture keeps its point: a float rate and whole-number counts."""
    start = json.loads(next(RECEIPTS.glob("source-*-start.json")).read_text())
    assert isinstance(start["offered_rate"], float)
    assert not isinstance(start["offered_rate"], int)
    assert math.isclose(start["offered_rate"], CELL["offered_rate"])
    assert start["records"] == ev.cell_records(CELL) == 2430
    creator = json.loads(next(RECEIPTS.glob("creator-*-start.json")).read_text())
    assert creator["attempt_limit"] == CELL["attempt_limit"] == 3645
    terminal = json.loads(next(RECEIPTS.glob("creator-*-terminal.json")).read_text())
    assert terminal["rows_exported"] == terminal["observations"] == 2430
    assert terminal["attempts"] == terminal["completed"] == 2430
    assert terminal["parts_closed"] == 4
    assert terminal["complete"] is True


def test_a_fractional_offered_rate_is_refused(tmp_path):
    """Whole is the point: the protocol admits no fractional rate."""
    root = install(tmp_path)
    start = next(
        (root / ev.cell_prefix(RUN, CELL["id"])).glob("receipts/source-*-start.json")
    )
    receipt = json.loads(start.read_text())
    receipt["offered_rate"] = 10.5
    start.write_text(json.dumps(receipt))
    result = ev.Reconciler(ev.DirectorySource(root), RUN, CELL, "main").run()
    assert "receipt-fields" in result.reasons
