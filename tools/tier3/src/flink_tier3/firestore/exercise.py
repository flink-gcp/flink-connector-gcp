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
"""Firestore recovery scheduling, built by the production supervisor entrypoint."""

from __future__ import annotations

import re

from ..common import Failure, timestamp
from ..exercise import FiniteInputExercise
from ..policy import FIRESTORE, FIRESTORE_OBSERVATIONS, FIRESTORE_STATE, POLL
from ..spanner.instance import retryable_commit
from .observe import observation, vertices
from .oracle import BatchCopy, Seed, Sink
from .trial import BATCH, INPUT, LATE_RECORDS, MODES, SEED
from .verdict import MEASUREMENT_EVENT, summarize, verdict

# The stage between the finished application and the read collections.
ORACLE_STAGE = "documents"
# The line each batch read's enumerator logs on the JobManager once the
# service has planned its splits (FirestoreBatchSplitEnumerator and
# DatastoreBatchSplitEnumerator onPlanned), in its shape for several splits
# and for one; the count is the service's, and the database names the mode.
# A plan of fewer splits than the parallelism is logged as a warning, whose
# parallelism a semicolon follows.
PLANNED = re.compile(
    r"Planned (?:(?P<splits>\d+) split\(s\)|(?P<one>one) split) for .* of"
    r" projects/[^/\s]+/databases/(?P<database>[a-z0-9-]+)"
    r" at read time (?P<read_time>\S+?Z)(?: and parallelism \d+[.;]|:)"
)


class FirestoreExercise(FiniteInputExercise):
    """Recover the finite input, then read both databases' collections against it.

    The caller supplies the supervisor's database controller. The approved
    trial names the fault; the job finishes once the input is complete and
    both copies are done. Before the upgrade the supervisor writes each
    database's late seed records, once a copy there shows its batch read
    planned. The verdict is decided from the recoveries, the families each
    window read and the four oracle reports, and written with the transition
    that completes the run.
    """

    namespace = FIRESTORE
    state_bucket = FIRESTORE_STATE
    timing = FIRESTORE_OBSERVATIONS
    service = "Firestore"
    oracle_stage = ORACLE_STAGE
    progress_event = "firestore-progress"
    measurement_event = MEASUREMENT_EVENT
    baseline_event = "firestore-baseline-window"
    # The input with both sinks, and each copy subtask: a lost TaskManager
    # restarts the input's region and its own two copy subtasks, which share
    # no edge with any other.
    failover_regions = 3
    # What is left after the last window is the job's end and the oracles,
    # which check every record through another recovery too, and report its
    # extra copies.
    waits_out_interruptions = True

    def __init__(self, env, upgrade, databases):
        if env.approval.scenario != "firestore-recovery":
            raise Failure("Firestore exercise requires a Firestore approval")
        trial = env.approval.firestore_plan
        if databases is None or databases.env is not env or databases.trial != trial:
            raise Failure("Firestore exercise requires the run's database controller")
        self.databases = databases
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
        """Read the input's progress, and each batch read's plan from the JobManager.

        A plan is logged once, when its read is planned at the job's first
        start; a restored read is not planned again. It is reported, never
        judged, so the last plan read of each mode is kept, and a line read
        again counts once.
        """
        super().progress(pod, text)
        if pod["metadata"].get("labels", {}).get("component") != "jobmanager":
            return
        modes = {self.trial.database(mode): mode for mode in MODES}
        for line in text.splitlines():
            match = PLANNED.search(line)
            key = (pod["metadata"]["uid"], line)
            if not match or match["database"] not in modes or key in self.plan_lines:
                continue
            self.plan_lines.add(key)
            plans = self.outcomes.setdefault("batch_plan", {})
            mode = modes[match["database"]]
            plans[mode] = {
                "splits": int(match["splits"] or 1),
                "read_time": match["read_time"],
                "plans": plans.get(mode, {}).get("plans", 0) + 1,
            }

    def begin_upgrade(self, app, cp, progress, pods):
        """Write the late seed records first; the upgrade waits for their record."""
        late = self.outcomes.get("late_seed", {})
        for mode in MODES:
            if mode not in late:
                self.seed_late(mode)
        if all(mode in self.outcomes.get("late_seed", {}) for mode in MODES):
            super().begin_upgrade(app, cp, progress, pods)

    def seed_late(self, mode):
        """Add one database's late seed records once its read has planned its time.

        A copy exists only after the read was planned, so a commit after one
        is read is after the read time, which the copy must not read. The
        commit sets fixed values, so one repeated after a response that never
        arrived, or after the commit aborted with 409, writes nothing new. A
        call that fails so is left to the next poll; the record holds the
        commit before the upgrade may begin.
        """
        try:
            api = self.databases.listing(self.deadline)
            if not api.copied(mode):
                return
            first = self.trial.seed_records
            committed = api.write_seed(mode, range(first, first + LATE_RECORDS))
        except Failure as error:
            if not retryable_commit(error):
                raise
            self.env.emit("firestore-late-seed-pending", {"cause": str(error)})
            return
        self.outcomes.setdefault("late_seed", {})[mode] = {
            "commit_time": committed,
            "records": LATE_RECORDS,
        }
        self.persist("baseline")

    def verify_tables(self):
        """Read every collection of each database at one read time and judge each.

        A database's first page is read now and every later one at its read
        time. A query that fails transiently is repeated until the read window
        closes, and any other failure ends the run, whose record then stops at
        this stage.
        """
        if self.stage != ORACLE_STAGE:
            raise Failure("Firestore oracles require both recoveries and the input")
        until = self.oracle_window()
        api = self.retried(lambda: self.databases.listing(until), until)
        checked = self.env.clock()
        plans = self.outcomes.get("batch_plan", {})
        oracles = {"read_times": {}}
        for mode in MODES:
            checks = {
                INPUT: Sink(self.trial),
                SEED: Seed(self.trial),
                BATCH: BatchCopy(self.trial),
            }
            read_time = None
            for collection, check in checks.items():
                after = None
                while True:
                    if self.env.clock() - checked >= POLL:
                        self.check_open()
                        self.env.records.heartbeat()
                        checked = self.env.clock()
                    rows, after, read_time = self.retried(
                        lambda m=mode, c=collection, a=after, t=read_time: (
                            api.read_page(m, c, a, t)
                        ),
                        until,
                    )
                    check.add(rows)
                    if after is None:
                        break
            plan = plans.get(mode)
            batch = checks[BATCH].report(
                checks[SEED].report(), plan["read_time"] if plan else None
            )
            batch.update(
                plan=plan or {"unavailable": "no plan line read from the JobManager"},
                late_after_plan=self.late_after_plan(mode),
            )
            oracles["read_times"][mode] = read_time
            oracles[mode + "_sink"] = checks[INPUT].report()
            oracles[mode + "_batch"] = batch
        self.outcomes["oracles"] = oracles
        self.complete(verdict)

    def late_after_plan(self, mode):
        """Whether a database's late commit followed its plan's read time, or None unread."""
        late = self.outcomes.get("late_seed", {}).get(mode)
        plan = self.outcomes.get("batch_plan", {}).get(mode)
        if not late or not plan:
            return None
        try:
            return timestamp(late["commit_time"]) > timestamp(plan["read_time"])
        except Failure:
            return None
