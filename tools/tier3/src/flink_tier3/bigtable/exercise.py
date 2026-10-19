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
"""Bigtable recovery scheduling, built by the production supervisor entrypoint."""

from __future__ import annotations

from ..common import Failure, timestamp, utc
from ..exercise import FiniteInputExercise
from ..policy import BIGTABLE, BIGTABLE_OBSERVATIONS, BIGTABLE_STATE, POLL
from .observe import observation, vertices
from .oracle import AtLeastOnce, ExactlyOnce, Relay
from .trial import AT_LEAST_ONCE_TABLE, EXACTLY_ONCE_TABLE, RELAY_TABLE
from .verdict import MEASUREMENT_EVENT, summarize, verdict

# The stage between the finished application and the read tables.
ORACLE_STAGE = "tables"
# From the input's last record to its staged commit: a checkpoint interval and
# the commit, with margin. A commit later than the change stream's end is not
# relayed, so the relay is judged only when the input finished this long
# before it.
COMMIT_SECONDS = 120


class BigtableExercise(FiniteInputExercise):
    """Recover the finite input, then read all three tables against it.

    The caller supplies the supervisor's instance controller. The approved
    trial names the fault, as it does for Cloud Tasks; the job finishes once
    the change-stream source reaches its end, after the input. The verdict is
    decided from the recoveries, the families each window read and the three
    oracle reports, and written with the transition that completes the run.
    """

    namespace = BIGTABLE
    state_bucket = BIGTABLE_STATE
    timing = BIGTABLE_OBSERVATIONS
    service = "Bigtable"
    oracle_stage = ORACLE_STAGE
    progress_event = "bigtable-progress"
    measurement_event = MEASUREMENT_EVENT
    baseline_event = "bigtable-baseline-window"

    def __init__(self, env, upgrade, instance):
        if env.approval.scenario != "bigtable-recovery":
            raise Failure("Bigtable exercise requires a Bigtable approval")
        trial = env.approval.bigtable_plan
        if instance is None or instance.env is not env or instance.trial != trial:
            raise Failure("Bigtable exercise requires the run's instance controller")
        self.instance = instance
        super().__init__(env, upgrade, trial)

    def discover(self, rest, known):
        return vertices(rest, known)

    def read(self, rest, state):
        return observation(rest, state)

    summarize = staticmethod(summarize)

    def verify_tables(self):
        """Read every table once and judge each component on its own table.

        A ReadRows call that fails transiently is repeated until the read
        window closes, and any other failure ends the run, whose record then
        stops at this stage.
        """
        if self.stage != ORACLE_STAGE:
            raise Failure("Bigtable oracles require both recoveries and the input")
        until = self.oracle_window()
        api = self.instance.listing(until)
        checks = {
            EXACTLY_ONCE_TABLE: ExactlyOnce(self.trial),
            AT_LEAST_ONCE_TABLE: AtLeastOnce(self.trial),
            RELAY_TABLE: Relay(self.trial),
        }
        checked = self.env.clock()
        for table, check in checks.items():
            after = None
            while True:
                if self.env.clock() - checked >= POLL:
                    self.check_open()
                    self.env.records.heartbeat()
                    checked = self.env.clock()
                rows, after = self.retried(
                    lambda after=after, table=table: api.read_rows(table, after),
                    until,
                )
                check.add(rows)
                if after is None:
                    break
        eo = checks[EXACTLY_ONCE_TABLE]
        finished = min(
            sample["at"]
            for sample in self.fresh_progress("upgrade")
            if sample["processed"] == self.records
        )
        judged = finished + COMMIT_SECONDS <= timestamp(self.trial.change_stream_end)
        self.outcomes["oracles"] = {
            "input_finished_at": utc(finished),
            "change_stream_end": self.trial.change_stream_end,
            "eo": eo.report(),
            "alo": checks[AT_LEAST_ONCE_TABLE].report(),
            "relay": checks[RELAY_TABLE].report(eo.applied, judged),
        }
        self.complete(verdict)
