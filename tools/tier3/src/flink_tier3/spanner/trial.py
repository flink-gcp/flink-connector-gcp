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
"""A Spanner recovery trial's input, its instance and its row keys."""

import hashlib
from dataclasses import dataclass

from ..common import Failure, timestamp, utc
from ..exercise import DEFAULT_FAULT, FAULTS
from ..policy import PROJECT, RUN_ID

# The application's bounds (kubernetes/apps/spanner RecoveryOptions) and the
# CUE package's 30-minute input ceiling.
MAX_RECORDS = 360000
MAX_RECORDS_PER_SECOND = 100
MAX_INPUT_SECONDS = 1800
MAX_SEED_ROWS_PER_SECOND = 1000
# Spanner allows 64 characters in an instance ID, so any run ID fits.
INSTANCE_PREFIX = "sp1360-"
# The application's database, tables and change stream (RecoveryOptions).
DATABASE = "recovery"
INPUT_TABLE = "input"
RELAY_TABLE = "relay"
SEED_TABLE = "seed"
BATCH_TABLE = "batch_out"
CHANGE_STREAM = "input_changes"
# The rows the supervisor adds to `seed` after the batch read has planned its
# snapshot, numbered after the runner's, which the copy must not read.
LATE_ROWS = 16
# Seven digits number every seed row, the late ones included.
MAX_SEED_ROWS = 10**7 - LATE_ROWS
KINDS = tuple(FAULTS)


def row_key(n):
    """Row `n`'s key in every table: `<n mod 16 in hex>/<n, seven digits>`."""
    return f"{n % 16:x}/{n:07d}"


@dataclass(frozen=True)
class Trial:
    """One run's input and seed, its instance and the row keys they take.

    Record ``n`` has the key ``row_key(n)`` in `input`, and so does seed row
    ``n`` in `seed`; the instance is ``sp1360-<run-id>``, as the CUE delivery
    renders it. ``change_stream_end`` is the UTC instant the change-stream
    source stops at, which the plan chooses from the run's window.
    """

    run_id: str
    records: int
    records_per_second: int
    change_stream_end: str
    seed_rows: int
    seed_rows_per_second: int
    kind: str = DEFAULT_FAULT

    def __post_init__(self):
        if not isinstance(self.run_id, str) or not RUN_ID.fullmatch(self.run_id):
            raise ValueError("Spanner trial requires a valid run ID")
        if type(self.records_per_second) is not int or not (
            1 <= self.records_per_second <= MAX_RECORDS_PER_SECOND
        ):
            raise ValueError("Spanner rate must be 1 to 100 records per second")
        if type(self.records) is not int or not (
            2 <= self.records <= MAX_RECORDS
            and self.records <= self.records_per_second * MAX_INPUT_SECONDS
        ):
            raise ValueError(
                "Spanner records must be 2 to 360,000 and 30 minutes of input"
            )
        try:
            end = timestamp(self.change_stream_end)
        except Failure as error:
            raise ValueError(
                "Spanner change-stream end must be a UTC instant"
            ) from error
        if utc(end) != self.change_stream_end:
            raise ValueError("Spanner change-stream end must be a UTC instant")
        if type(self.seed_rows) is not int or not 1 <= self.seed_rows <= MAX_SEED_ROWS:
            raise ValueError("Spanner seed rows must fit seven-digit keys")
        if type(self.seed_rows_per_second) is not int or not (
            1 <= self.seed_rows_per_second <= MAX_SEED_ROWS_PER_SECOND
        ):
            raise ValueError("Spanner seed pace must be 1 to 1,000 rows per second")
        if self.kind not in KINDS:
            raise ValueError("Unknown Spanner trial kind")

    @property
    def failover(self):
        """The Pod the second recovery deletes, as the shared exercise names it."""
        return FAULTS[self.kind].failover

    @property
    def rescale(self):
        """The direction the upgrade rescales the job in, or None."""
        return FAULTS[self.kind].rescale

    def parallelism(self, phase):
        """Every vertex but the input's subtasks, and the TaskManagers running them."""
        return FAULTS[self.kind].parallelism(phase)

    @property
    def instance(self):
        return INSTANCE_PREFIX + self.run_id

    @property
    def display_name(self):
        # Spanner requires a display name unique in the project and 4 to 30
        # characters long. The run ID makes it unique, and a run ID too long to
        # fit keeps its first 14 characters and a hash of the whole.
        name = "sp1360 " + self.run_id
        if len(name) <= 30:
            return name
        digest = hashlib.sha256(self.run_id.encode()).hexdigest()[:8]
        return f"sp1360 {self.run_id[:14]} {digest}"

    @property
    def instance_path(self):
        return f"projects/{PROJECT}/instances/{self.instance}"

    @property
    def database_path(self):
        return f"{self.instance_path}/databases/{DATABASE}"

    def arguments(self, phase):
        """The application arguments the CUE package renders for a phase."""
        return [
            "--run-id",
            self.run_id,
            "--instance",
            self.instance,
            "--phase",
            phase,
            "--records",
            str(self.records),
            "--records-per-second",
            str(self.records_per_second),
            "--change-stream-end",
            self.change_stream_end,
            "--seed-rows-per-second",
            str(self.seed_rows_per_second),
            "--require-restored",
            str(phase == "upgrade").lower(),
            "--parallelism",
            str(self.parallelism(phase)),
        ]

    def row_keys(self):
        """Every input sequence by the key its record writes."""
        return {row_key(n): n for n in range(self.records)}

    def seed_keys(self):
        """Every seed row the runner writes, by its key."""
        return {row_key(n): n for n in range(self.seed_rows)}

    def late_keys(self):
        """Every seed row the supervisor writes late, by its key."""
        return {
            row_key(n): n for n in range(self.seed_rows, self.seed_rows + LATE_ROWS)
        }
