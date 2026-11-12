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
"""A Firestore recovery trial's input, its two databases and their keys."""

from dataclasses import dataclass

from ..exercise import DEFAULT_FAULT, FAULTS
from ..policy import PROJECT, RUN_ID

# The application's bounds (kubernetes/apps/firestore RecoveryOptions) and the
# CUE package's 30-minute input ceiling.
MAX_RECORDS = 360000
MAX_RECORDS_PER_SECOND = 100
MAX_INPUT_SECONDS = 1800
MAX_SEED_RECORDS_PER_SECOND = 200
# The run's databases, one per mode; a 40-character run ID keeps each within
# the 63 characters Firestore allows.
DATABASE_PREFIX = "fs1879-"
MODES = ("native", "datastore")
SUFFIXES = {"native": "-n", "datastore": "-d"}
# The collections of the Native-mode database and the kinds of the
# Datastore-mode one (kubernetes/apps/firestore README).
INPUT = "input"
SEED = "seed"
BATCH = "batch_out"
# The records the supervisor adds to each database's `seed` after its batch
# read has planned its read time, numbered after the runner's, which the copy
# must not read.
LATE_RECORDS = 16
# Seven digits number every seed record, the late ones included.
MAX_SEED_RECORDS = 10**7 - LATE_RECORDS
# The fault-sequence trial of ADR-0204, which injects every kind in one run.
SEQUENCE_KIND = "sequence"
KINDS = (*FAULTS, SEQUENCE_KIND)


def record_key(n):
    """Record `n`'s document ID and key name: `<n mod 16 in hex>-<n, seven digits>`."""
    return f"{n % 16:x}-{n:07d}"


@dataclass(frozen=True)
class Trial:
    """One run's input and seed, its two databases and the keys they take.

    Record ``n`` has the key ``record_key(n)`` in `input`, and so does seed
    record ``n`` in `seed`, in both databases; the databases are
    ``fs1879-<run-id>-n`` and ``fs1879-<run-id>-d``, as the CUE delivery
    renders them. Each database holds a seed of ``seed_records``.
    """

    run_id: str
    records: int
    records_per_second: int
    seed_records: int
    seed_records_per_second: int
    kind: str = DEFAULT_FAULT

    def __post_init__(self):
        if not isinstance(self.run_id, str) or not RUN_ID.fullmatch(self.run_id):
            raise ValueError("Firestore trial requires a valid run ID")
        if type(self.records_per_second) is not int or not (
            1 <= self.records_per_second <= MAX_RECORDS_PER_SECOND
        ):
            raise ValueError("Firestore rate must be 1 to 100 records per second")
        if type(self.records) is not int or not (
            2 <= self.records <= MAX_RECORDS
            and self.records <= self.records_per_second * MAX_INPUT_SECONDS
        ):
            raise ValueError(
                "Firestore records must be 2 to 360,000 and 30 minutes of input"
            )
        if type(self.seed_records) is not int or not (
            1 <= self.seed_records <= MAX_SEED_RECORDS
        ):
            raise ValueError("Firestore seed records must fit seven-digit keys")
        if type(self.seed_records_per_second) is not int or not (
            1 <= self.seed_records_per_second <= MAX_SEED_RECORDS_PER_SECOND
        ):
            raise ValueError("Firestore seed pace must be 1 to 200 records per second")
        if self.kind not in KINDS:
            raise ValueError("Unknown Firestore trial kind")

    @property
    def failover(self):
        """The Pod the second recovery deletes, as the shared exercise names it."""
        return FAULTS[self.fault].failover

    @property
    def rescale(self):
        """The direction the upgrade rescales the job in, or None."""
        return FAULTS[self.fault].rescale

    @property
    def fault(self):
        """The kind whose sizes the run starts and upgrades at.

        The fault sequence starts and upgrades at two, as a JobManager
        replacement does, and patches its rescales itself.
        """
        return DEFAULT_FAULT if self.kind == SEQUENCE_KIND else self.kind

    def parallelism(self, phase):
        """Every vertex but the input's subtasks, and the TaskManagers running them."""
        return FAULTS[self.fault].parallelism(phase)

    def database(self, mode):
        """The database ID of one mode."""
        return DATABASE_PREFIX + self.run_id + SUFFIXES[mode]

    def database_path(self, mode):
        return f"projects/{PROJECT}/databases/{self.database(mode)}"

    def arguments(self, phase):
        """The application arguments the CUE package renders for a phase."""
        return [
            "--run-id",
            self.run_id,
            "--native-database",
            self.database("native"),
            "--datastore-database",
            self.database("datastore"),
            "--phase",
            phase,
            "--records",
            str(self.records),
            "--records-per-second",
            str(self.records_per_second),
            "--seed-records-per-second",
            str(self.seed_records_per_second),
            "--require-restored",
            str(phase == "upgrade").lower(),
            "--parallelism",
            str(self.parallelism(phase)),
        ]

    def record_keys(self):
        """Every input sequence by the key its record writes."""
        return {record_key(n): n for n in range(self.records)}

    def seed_keys(self):
        """Every seed record the runner writes, by its key."""
        return {record_key(n): n for n in range(self.seed_records)}

    def late_keys(self):
        """Every seed record the supervisor writes late, by its key."""
        return {
            record_key(n): n
            for n in range(self.seed_records, self.seed_records + LATE_RECORDS)
        }
