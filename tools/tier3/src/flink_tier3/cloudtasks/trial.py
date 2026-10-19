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
"""A Cloud Tasks recovery trial's input, its queue and its task names."""

import hashlib
from dataclasses import dataclass

from ..exercise import DEFAULT_FAULT, FAULTS
from ..policy import PROJECT, REGION, RUN_ID

# The application's bounds (kubernetes/apps/cloudtasks RecoveryOptions) and the
# CUE package's 30-minute input ceiling.
MAX_RECORDS = 360000
MAX_RECORDS_PER_SECOND = 100
MAX_INPUT_SECONDS = 1800
QUEUE_PREFIX = "ct1362-"
# Every trial runs a savepoint upgrade and then a Pod deletion; each shared
# fault kind but the default replaces one of the two with the fault it names.
KINDS = tuple(FAULTS)


def parent():
    return f"projects/{PROJECT}/locations/{REGION}"


@dataclass(frozen=True)
class Trial:
    """One run's input, its queue and the names its tasks take.

    The application names each task with the SHA-256 hex digest of the key
    ``<run-id>/<sequence>``, so a listed name maps back to exactly one sequence
    of this run; ``queue`` is ``ct1362-<run-id>``, as the CUE delivery renders it.

    ``kind`` defaults to the trial the first run was, whose approval names
    none, so its archived evidence still reads as the trial it ran.
    """

    run_id: str
    records: int
    records_per_second: int
    kind: str = DEFAULT_FAULT

    def __post_init__(self):
        if not isinstance(self.run_id, str) or not RUN_ID.fullmatch(self.run_id):
            raise ValueError("Cloud Tasks trial requires a valid run ID")
        if type(self.records_per_second) is not int or not (
            1 <= self.records_per_second <= MAX_RECORDS_PER_SECOND
        ):
            raise ValueError("Cloud Tasks rate must be 1 to 100 records per second")
        if type(self.records) is not int or not (
            2 <= self.records <= MAX_RECORDS
            and self.records <= self.records_per_second * MAX_INPUT_SECONDS
        ):
            raise ValueError(
                "Cloud Tasks records must be 2 to 360,000 and 30 minutes of input"
            )
        if self.kind not in KINDS:
            raise ValueError("Unknown Cloud Tasks trial kind")

    @property
    def failover(self):
        """The Pod the second recovery deletes, as the shared exercise names it."""
        return FAULTS[self.kind].failover

    @property
    def rescale(self):
        """The direction the upgrade rescales the sink in, or None."""
        return FAULTS[self.kind].rescale

    def parallelism(self, phase):
        """The sink's subtasks in one phase, and the TaskManagers running them."""
        return FAULTS[self.kind].parallelism(phase)

    @property
    def queue(self):
        return QUEUE_PREFIX + self.run_id

    @property
    def queue_path(self):
        return f"{parent()}/queues/{self.queue}"

    def arguments(self, phase):
        """The application arguments the CUE package renders for a phase."""
        return [
            "--run-id",
            self.run_id,
            "--queue",
            self.queue,
            "--phase",
            phase,
            "--records",
            str(self.records),
            "--records-per-second",
            str(self.records_per_second),
            "--require-restored",
            str(phase == "upgrade").lower(),
            "--sink-parallelism",
            str(self.parallelism(phase)),
        ]

    def task_id(self, sequence):
        """The id the sink gives the task of one input sequence."""
        if type(sequence) is not int or not 0 <= sequence < self.records:
            raise ValueError("Sequence is outside this run's input")
        key = f"{self.run_id}/{sequence}".encode()
        return hashlib.sha256(key).hexdigest()

    def task_ids(self):
        """Every input sequence by the task id its key names."""
        return {self.task_id(sequence): sequence for sequence in range(self.records)}
