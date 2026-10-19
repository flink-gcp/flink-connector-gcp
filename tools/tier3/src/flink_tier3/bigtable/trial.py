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
"""A Bigtable recovery trial's input, its instance and its row keys."""

from dataclasses import dataclass

from ..common import Failure, timestamp, utc
from ..exercise import DEFAULT_FAULT, FAULTS
from ..policy import PROJECT, RUN_ID

# The application's bounds (kubernetes/apps/bigtable RecoveryOptions) and the
# CUE package's 30-minute input ceiling.
MAX_RECORDS = 360000
MAX_RECORDS_PER_SECOND = 100
MAX_INPUT_SECONDS = 1800
# Bigtable allows 33 characters in an instance ID, so the run ID has 26.
INSTANCE_PREFIX = "bt1316-"
MAX_RUN_ID = 33 - len(INSTANCE_PREFIX)
# Cluster IDs allow 30 characters, too few for the instance's name, so every
# run's one cluster takes this fixed ID inside its own instance.
CLUSTER = "recovery"
ZONE = "us-central1-b"
APP_PROFILE = "recovery"
# The application's tables and families (RecoveryOptions).
EXACTLY_ONCE_TABLE = "eo"
AT_LEAST_ONCE_TABLE = "alo"
RELAY_TABLE = "relay"
SUM_FAMILY = "agg"
SUM_QUALIFIER = "count"
MARKER_FAMILY = "flink_commit"
RELAY_FAMILY = "r"
KINDS = tuple(FAULTS)


@dataclass(frozen=True)
class Trial:
    """One run's input, its instance and the row keys its records take.

    Record ``n`` has the row key ``<n mod 16 in hex>/<n, seven digits>`` in
    every table, and the instance is ``bt1316-<run-id>``, as the CUE delivery
    renders it. ``change_stream_end`` is the UTC instant the change-stream
    source stops at, which the plan chooses from the run's window.
    """

    run_id: str
    records: int
    records_per_second: int
    change_stream_end: str
    kind: str = DEFAULT_FAULT

    def __post_init__(self):
        if (
            not isinstance(self.run_id, str)
            or not RUN_ID.fullmatch(self.run_id)
            or len(self.run_id) > MAX_RUN_ID
        ):
            raise ValueError(
                "Bigtable trial requires a run ID of at most 26 characters"
            )
        if type(self.records_per_second) is not int or not (
            1 <= self.records_per_second <= MAX_RECORDS_PER_SECOND
        ):
            raise ValueError("Bigtable rate must be 1 to 100 records per second")
        if type(self.records) is not int or not (
            2 <= self.records <= MAX_RECORDS
            and self.records <= self.records_per_second * MAX_INPUT_SECONDS
        ):
            raise ValueError(
                "Bigtable records must be 2 to 360,000 and 30 minutes of input"
            )
        try:
            end = timestamp(self.change_stream_end)
        except Failure as error:
            raise ValueError(
                "Bigtable change-stream end must be a UTC instant"
            ) from error
        if utc(end) != self.change_stream_end:
            raise ValueError("Bigtable change-stream end must be a UTC instant")
        if self.kind not in KINDS:
            raise ValueError("Unknown Bigtable trial kind")

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
    def instance_path(self):
        return f"projects/{PROJECT}/instances/{self.instance}"

    def table_path(self, table):
        return f"{self.instance_path}/tables/{table}"

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
            "--require-restored",
            str(phase == "upgrade").lower(),
            "--parallelism",
            str(self.parallelism(phase)),
        ]

    def row_keys(self):
        """Every input sequence by the row key its record writes."""
        return {f"{n % 16:x}/{n:07d}": n for n in range(self.records)}
