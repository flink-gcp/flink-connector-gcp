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
"""Spanner recovery scheduling, built by the production supervisor entrypoint."""

from __future__ import annotations

import re

from ..common import Failure, timestamp, utc
from ..exercise import FiniteInputExercise
from ..policy import POLL, SPANNER, SPANNER_OBSERVATIONS, SPANNER_STATE
from .instance import retryable_commit
from .observe import observation, vertices
from .oracle import BatchCopy, ChangeStream, Seed, Sink, micros
from .trial import BATCH_TABLE, INPUT_TABLE, LATE_ROWS, RELAY_TABLE, SEED_TABLE
from .verdict import MEASUREMENT_EVENT, maxima, summarize, verdict, watermark_advanced

# The stage between the finished application and the read tables.
ORACLE_STAGE = "tables"
# From the input's last record to its sink's commit: a checkpoint interval and
# the commit, with margin. A commit later than the change stream's end is not
# relayed, so the relay is judged only when the input finished this long
# before it, as for the Bigtable trial.
COMMIT_SECONDS = 120
# The line the batch read's enumerator logs on the JobManager once Spanner has
# planned its partitions (SpannerBatchReadSplitEnumerator.logPlan), in its
# shape for several partitions and for one; the count is the service's.
PLANNED = re.compile(
    r"Spanner planned (?:(?P<partitions>\d+) partition\(s\) for the .* of \S+"
    r" at read timestamp (?P<read_timestamp>\S+) and parallelism \d+\."
    r"|the .* of \S+ as (?P<one>\d+) partition\(s\) at read timestamp"
    r" (?P<one_timestamp>\S+);)"
)


class SpannerExercise(FiniteInputExercise):
    """Recover the finite input, then read the four tables against it.

    The caller supplies the supervisor's instance controller. The approved
    trial names the fault; the job finishes once the change-stream source
    reaches its end and the batch copy is done, after the input. Before the
    upgrade the supervisor writes the late seed rows, once a copied row shows
    the batch read planned. The verdict is decided from the recoveries, the
    families each window read, the source's watermark and the three oracle
    reports, and written with the transition that completes the run.
    """

    namespace = SPANNER
    state_bucket = SPANNER_STATE
    timing = SPANNER_OBSERVATIONS
    service = "Spanner"
    oracle_stage = ORACLE_STAGE
    progress_event = "spanner-progress"
    measurement_event = MEASUREMENT_EVENT
    baseline_event = "spanner-baseline-window"
    # The input with its sink, each relay subtask and each copy subtask: a lost
    # TaskManager restarts the input's region and its own relay and copy
    # subtasks, which share no edge with any other.
    failover_regions = 3

    def __init__(self, env, upgrade, instance):
        if env.approval.scenario != "spanner-recovery":
            raise Failure("Spanner exercise requires a Spanner approval")
        trial = env.approval.spanner_plan
        if instance is None or instance.env is not env or instance.trial != trial:
            raise Failure("Spanner exercise requires the run's instance controller")
        self.instance = instance
        # The supervisor's one multiplexed session, made at its first call.
        self.session = None
        # Each plan line read, by its Pod and its text, which starts with its
        # time: overlapping log reads return a line more than once.
        self.plan_lines = set()
        super().__init__(env, upgrade, trial)

    def discover(self, rest, known):
        return vertices(rest, known)

    def read(self, rest, state):
        return observation(rest, state)

    summarize = staticmethod(summarize)

    def progress(self, pod, text):
        """Read the input's progress, and the batch read's plan from the JobManager.

        The plan is logged once, when the read is planned at the job's first
        start; a restored read is not planned again. It is reported, never
        judged, so the last plan read is kept, and a line read again counts
        once.
        """
        super().progress(pod, text)
        if pod["metadata"].get("labels", {}).get("component") != "jobmanager":
            return
        for line in text.splitlines():
            match = PLANNED.search(line)
            key = (pod["metadata"]["uid"], line)
            if match and key not in self.plan_lines:
                self.plan_lines.add(key)
                plans = self.outcomes.get("batch_plan", {}).get("plans", 0)
                self.outcomes["batch_plan"] = {
                    "partitions": int(match["partitions"] or match["one"]),
                    "read_timestamp": match["read_timestamp"] or match["one_timestamp"],
                    "plans": plans + 1,
                }

    def _api(self, deadline):
        """The supervisor's client and its session, made once."""
        api = self.instance.listing(deadline)
        if self.session is None:
            self.session = api.session()
        return api

    def begin_upgrade(self, app, cp, progress, pods):
        """Write the late seed rows first; the upgrade waits for their record."""
        if "late_seed" not in self.outcomes:
            self.seed_late()
        if "late_seed" in self.outcomes:
            super().begin_upgrade(app, cp, progress, pods)

    def seed_late(self):
        """Add the late seed rows once the batch read has planned its snapshot.

        A copied row exists only after the read was planned, so a commit after
        one is read is after the snapshot, which the copy must not read. The
        commit is an insert-or-update of fixed values, so one repeated after a
        response that never arrived, or after the commit aborted with 409,
        writes nothing new. A call that fails so is left to the next poll; the
        record holds the commit before the upgrade may begin.
        """
        try:
            api = self._api(self.deadline)
            if not api.copied(self.session):
                return
            first = self.trial.seed_rows
            response = api.write_seed(self.session, range(first, first + LATE_ROWS))
        except Failure as error:
            if not retryable_commit(error):
                raise
            self.env.emit("spanner-late-seed-pending", {"cause": str(error)})
            return
        committed = response.get("commitTimestamp")
        # A commit answered without its time is one the oracle cannot place.
        micros(committed)
        self.outcomes["late_seed"] = {"commit_timestamp": committed, "rows": LATE_ROWS}
        self.persist("baseline")

    def verify_tables(self):
        """Read every table at one snapshot and judge each component on its own.

        The first page is read strongly and every later one at its read
        timestamp. A query that fails transiently is repeated until the read
        window closes, and any other failure ends the run, whose record then
        stops at this stage.
        """
        if self.stage != ORACLE_STAGE:
            raise Failure("Spanner oracles require both recoveries and the input")
        until = self.oracle_window()
        api = self.retried(lambda: self._api(until), until)
        checks = {
            INPUT_TABLE: Sink(self.trial),
            RELAY_TABLE: ChangeStream(self.trial),
            SEED_TABLE: Seed(self.trial),
            BATCH_TABLE: BatchCopy(self.trial),
        }
        checked, snapshot = self.env.clock(), None
        for table, check in checks.items():
            after = None
            while True:
                if self.env.clock() - checked >= POLL:
                    self.check_open()
                    self.env.records.heartbeat()
                    checked = self.env.clock()
                rows, after, snapshot = self.retried(
                    lambda after=after, table=table, snapshot=snapshot: api.read_page(
                        self.session, table, after, snapshot
                    ),
                    until,
                )
                check.add(rows)
                if after is None:
                    break
        finished = min(
            sample["at"]
            for sample in self.fresh_progress("upgrade")
            if sample["processed"] == self.records
        )
        judged = finished + COMMIT_SECONDS <= timestamp(self.trial.change_stream_end)
        relay = checks[RELAY_TABLE]
        stream = relay.report(judged)
        stream.update(
            watermark=maxima(self.coverage, "watermark"),
            watermark_advanced=watermark_advanced(self.coverage),
            partitions_discovered=maxima(self.coverage, "partitions_discovered"),
        )
        batch = checks[BATCH_TABLE].report(checks[SEED_TABLE].report())
        batch.update(
            plan=self.outcomes.get("batch_plan")
            or {"unavailable": "no plan line read from the JobManager"},
            late_after_plan=self.late_after_plan(),
            partitions_reread=maxima(self.coverage, "partitions_reread"),
        )
        self.outcomes["oracles"] = {
            "read_timestamp": snapshot,
            "input_finished_at": utc(finished),
            "change_stream_end": self.trial.change_stream_end,
            "sink": checks[INPUT_TABLE].report(relay),
            "change_stream": stream,
            "batch": batch,
        }
        self.complete(verdict)

    def late_after_plan(self):
        """Whether the late commit followed the plan's read timestamp, or None unread."""
        late, plan = self.outcomes.get("late_seed"), self.outcomes.get("batch_plan")
        if not late or not plan:
            return None
        try:
            return micros(late["commit_timestamp"]) > micros(plan["read_timestamp"])
        except Failure:
            return None
