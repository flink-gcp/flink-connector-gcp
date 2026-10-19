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

from ..common import Failure, utc
from ..exercise import FiniteInputExercise
from ..policy import CLOUDTASKS, CLOUDTASKS_OBSERVATIONS, CLOUDTASKS_STATE, POLL
from .observe import observation, vertices
from .oracle import assess, page
from .verdict import (
    MEASUREMENT_EVENT,
    PAGE_EVENT,
    summarize,
    verdict,
)

# The stage between the finished application and the passing listing.
LISTING_STAGE = "listing"


class WindowClosed(Failure):
    """A listing call the listing window ran out on, however it failed."""


class CloudTasksExercise(FiniteInputExercise):
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
    service = "Cloud Tasks"
    oracle_stage = LISTING_STAGE
    progress_event = "cloudtasks-progress"
    measurement_event = MEASUREMENT_EVENT
    baseline_event = "cloudtasks-baseline-window"

    def __init__(self, env, upgrade, queue):
        if env.approval.scenario != "cloudtasks-recovery":
            raise Failure("Cloud Tasks exercise requires a Cloud Tasks approval")
        trial = env.approval.cloudtasks_plan
        if queue is None or queue.env is not env or queue.trial != trial:
            raise Failure("Cloud Tasks exercise requires the run's queue controller")
        self.queue = queue
        super().__init__(env, upgrade, trial)

    def discover(self, rest, known):
        return vertices(rest, known)

    def read(self, rest, state):
        return observation(rest, state)

    summarize = staticmethod(summarize)

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
        until = self.oracle_window()
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
        self.complete(verdict)

    def _list(self, attempt, until):
        """One whole listing, exported page by page, and its oracle report.

        The run's state is checked, and the supervisor's heartbeat written,
        once a poll interval rather than once a page: a listing of the trial
        is 180 pages or more.
        """
        self.check_open()
        api = self.queue.listing(until)
        queue = self.retried(api.get, until, WindowClosed)
        state = queue.get("state") if isinstance(queue, dict) else None
        pages, token, checked = [], None, self.env.clock()
        while True:
            if self.env.clock() - checked >= POLL:
                self.check_open()
                self.env.records.heartbeat()
                checked = self.env.clock()
            tasks, token = self.retried(
                lambda token=token: api.tasks(token), until, WindowClosed
            )
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
