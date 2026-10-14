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
"""Cloud Tasks recovery scheduling, built by the production supervisor entrypoint."""

from __future__ import annotations

import uuid

from ..common import Failure, transient, utc
from ..exercise import RecoveryExercise
from ..policy import CLOUDTASKS, CLOUDTASKS_OBSERVATIONS, CLOUDTASKS_STATE, POLL
from .observe import observation, vertices
from .oracle import assess, page
from .verdict import (
    COMPLETE_STAGE,
    MEASUREMENT_EVENT,
    PAGE_EVENT,
    summarize,
    verdict,
)

# The stage between the finished application and the passing listing.
LISTING_STAGE = "listing"
# The wait before repeating a listing call that failed transiently.
RETRY_SECONDS = 5
# What the listing window leaves before the cleanup start, for writing the
# record's transition, which the cleanup start refuses.
RECORD_SECONDS = 60


class WindowClosed(Failure):
    """A listing call the listing window ran out on, however it failed."""


class CloudTasksExercise(RecoveryExercise):
    """Recover the finite input, then list the paused queue against it.

    The caller supplies the supervisor's queue controller. The approved trial
    names the fault: the upgrade may rescale the sink, and the failover may
    delete a TaskManager rather than the JobManager. The exercise samples the
    job once a measurement interval, holds a baseline, a window after the
    upgrade and one after the failover, and decides the verdict from the
    recoveries it proved, the families each window read and the listing it
    exported, and writes it with the transition that completes the run.
    """

    namespace = CLOUDTASKS
    state_bucket = CLOUDTASKS_STATE
    timing = CLOUDTASKS_OBSERVATIONS
    progress_event = "cloudtasks-progress"

    def __init__(self, env, upgrade, queue):
        if env.approval.scenario != "cloudtasks-recovery":
            raise Failure("Cloud Tasks exercise requires a Cloud Tasks approval")
        self.trial = env.approval.cloudtasks_plan
        if queue is None or queue.env is not env or queue.trial != self.trial:
            raise Failure("Cloud Tasks exercise requires the run's queue controller")
        self.records = self.trial.records
        # The JobManager and one single-slot TaskManager per sink subtask.
        self.expected_pods = 1 + self.trial.sink_parallelism("initial")
        self.queue = queue
        self.vertices = None
        super().__init__(
            env, upgrade, failover=self.trial.failover, rescale=self.trial.rescale
        )
        self.deadline = min(
            env.schedule.started + self.timing["input_seconds"],
            env.schedule.cleanup_at,
        )
        self.baseline_until = None
        self.post_until = None

    def progress(self, pod, text):
        super().progress(pod, text)
        lineage = self.env.refresh().lineage
        for sample in self.samples.values():
            try:
                valid = str(uuid.UUID(sample["lineage"])) == sample["lineage"]
            except ValueError:
                valid = False
            if sample["at"] > self.env.clock():
                raise Failure("Cloud Tasks progress is dated in the future")
            if not valid or (lineage is not None and sample["lineage"] != lineage):
                raise Failure("Cloud Tasks input lineage changed or is invalid")
            if lineage is None:
                self.env.records.record_lineage(sample["lineage"])
                lineage = sample["lineage"]

    def active_input(self, phase):
        """Whether the phase's input is still flowing, so a fault lands mid-input."""
        samples = self.fresh_progress(phase)
        if not samples:
            return False
        latest = max(samples, key=lambda sample: sample["at"])
        return (
            latest["processed"] < self.records and self.env.clock() - latest["at"] <= 60
        )

    def begin_upgrade(self, app, cp, progress, pods):
        """Upgrade once the baseline window, opened when input flows, has passed.

        The window opens on the first poll that sees the initial input flowing
        on the whole set of Pods, and the upgrade waits for a checkpoint
        triggered after it closes, so the baseline is a steady state.
        """
        if (
            not self.active_input("initial")
            or self.jobmanager(pods) is None
            or self.stable_pods is None
        ):
            return
        if self.baseline_until is None:
            self.baseline_until = (
                self.env.clock()
                + self.timing["warmup_seconds"]
                + self.timing["baseline_seconds"]
            )
            # A checkpoint interval and a poll to see it complete, twice over.
            self.deadline = min(self.baseline_until + 120, self.env.schedule.cleanup_at)
            self.env.records.evidence(
                "cloudtasks-baseline-window",
                {
                    "started_at": utc(self.env.clock()),
                    "until": utc(self.baseline_until),
                },
            )
        if (
            self.env.clock() < self.baseline_until
            or cp["trigger_timestamp"] / 1000 < self.baseline_until
        ):
            return
        super().begin_upgrade(app, cp, progress, pods)

    def begin_failover(self, app, proof, pods):
        if self.active_input("upgrade"):
            super().begin_failover(app, proof, pods)

    def persist(self, stage, **details):
        if stage == "finishing":
            self.post_until = self.env.clock() + self.timing["post_recovery_seconds"]
        # A finished application is complete only once its listing passes.
        if stage == COMPLETE_STAGE:
            stage = LISTING_STAGE
        super().persist(stage, **details)

    def attach_rest(self, service, job_id):
        if self.track_rest(service, job_id):
            self.vertices = None

    def window(self):
        """The measurement window the exercise is in now, or None outside one.

        The baseline starts after its warm-up and lasts until the upgrade
        begins; the window after the upgrade is its held stage; the one after
        the failover ends `post_recovery_seconds` after its proof. A sample
        taken during startup, a recovery or the rest of the input counts
        toward none of them.
        """
        now = self.env.clock()
        if self.stage == "baseline" and self.baseline_until is not None:
            if now >= self.baseline_until - self.timing["baseline_seconds"]:
                return "baseline"
        elif self.stage == "upgraded":
            return "upgraded"
        elif self.stage == "finishing" and now < self.post_until:
            return "finishing"
        return None

    def measure(self):
        """Sample the job once a measurement interval, labelled with its window.

        Every sample is exported with the stage it was taken in; only one
        inside a window counts toward that window's coverage. Discovery runs
        at every sample and keeps the metric ids found before, as the BigQuery
        exercise's does.
        """
        if not self.measurement_due():
            return
        self.vertices = vertices(self.rest, self.vertices)
        reading = {
            "stage": self.stage,
            "window": self.window(),
            **observation(self.rest, self.vertices),
        }
        self.coverage = summarize(self.coverage, self.stage, reading)
        self.env.emit(MEASUREMENT_EVENT, reading)

    def observe(self, app, rest, pods):
        self.measure()
        state = app.get("status", {}).get("jobStatus", {}).get("state")
        if self.stage == "finishing" and state == "FINISHED":
            completed = [
                sample["at"]
                for sample in self.fresh_progress("upgrade")
                if sample["processed"] == self.records
            ]
            if not completed or min(completed) < self.post_until:
                raise Failure(
                    "Cloud Tasks input finished before the post-recovery window"
                )
        return super().observe(app, rest, pods)

    def verify_tasks(self):
        """List the paused queue until the oracle passes or the window closes.

        Each attempt exports its pages before deciding, so the offline analyzer
        recomputes the report the record holds from the same pages. Another
        attempt starts only while one as long as the last still fits the
        listing window, and one that runs out of it anyway leaves the last
        whole attempt in place, so a listing that keeps failing is written as
        the trial's finding rather than lost to the deadline.
        """
        if self.stage != LISTING_STAGE:
            raise Failure("Cloud Tasks listing requires both recoveries and the input")
        until = min(
            self.env.clock() + self.timing["visibility_seconds"],
            self.env.schedule.cleanup_at - RECORD_SECONDS,
        )
        # The listing keeps its own window; the record's transition needs
        # only the cleanup start.
        self.deadline = self.env.schedule.cleanup_at
        listing, attempt = None, 0
        while True:
            attempt += 1
            started = self.env.clock()
            try:
                current = self._list(attempt, until)
            except WindowClosed:
                # Only the window running out on a listing call keeps the last
                # whole attempt; any other failure ends the run.
                if listing is None:
                    raise
                break
            listing, took = current, self.env.clock() - started
            if listing["report"]["verdict"] == "pass" or (
                self.env.clock() + POLL + took >= until
            ):
                break
            self.env.sleep(POLL)
        self.outcomes["listing"] = listing
        # Decided over the record the transition is about to write, which the
        # offline analyzer recomputes once the evidence is exported.
        coverage = self._coverage()
        decided = verdict(
            {"stage": COMPLETE_STAGE, "outcomes": self.outcomes, "coverage": coverage}
        )
        super().persist(
            COMPLETE_STAGE, processed=self.records, coverage=coverage, **decided
        )

    def _retried(self, call, until):
        """One listing call, repeated while it fails without saying anything.

        Any failure once the window is over is the window closing: a call
        refused before its request, a deadline met while its response arrived,
        and a timeout alike, since the queue's deadline for these calls is the
        window's end. Before then, a non-transient failure says something about
        the listing.
        """
        while True:
            try:
                return call()
            except Failure as error:
                if self.env.clock() >= until:
                    raise WindowClosed(str(error)) from error
                if not transient(error):
                    raise
            self.env.sleep(RETRY_SECONDS)

    def _list(self, attempt, until):
        """One whole listing, exported page by page, and its oracle report.

        The run's state is checked, and the supervisor's heartbeat written,
        once a poll interval rather than once a page: a listing of the trial
        is 180 pages or more.
        """
        self.check_open()
        api = self.queue.listing(until)
        queue = self._retried(api.get, until)
        state = queue.get("state") if isinstance(queue, dict) else None
        pages, token, checked = [], None, self.env.clock()
        while True:
            if self.env.clock() - checked >= POLL:
                self.check_open()
                self.env.records.heartbeat()
                checked = self.env.clock()
            tasks, token = self._retried(lambda token=token: api.tasks(token), until)
            item = page(self.trial, tasks)
            self.env.records.evidence(
                PAGE_EVENT,
                {
                    "attempt": attempt,
                    "page": len(pages),
                    "listed_at": utc(self.env.clock()),
                    **item,
                },
            )
            pages.append(item)
            if token is None:
                break
        return {
            "attempt": attempt,
            "pages": len(pages),
            "queue_state": state,
            "report": assess(self.trial, pages),
        }
