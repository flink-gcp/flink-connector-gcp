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
"""BigQuery recovery scheduling, built by the production supervisor entrypoint."""

from __future__ import annotations

import google.auth.exceptions
import requests

from ..common import Failure, transient, utc
from ..exercise import FiniteInputExercise, RecoveryExercise
from ..policy import (
    BIGQUERY,
    BIGQUERY_OBSERVATIONS,
    BIGQUERY_STATE,
    POLL,
    QUERY_EVIDENCE_BYTES,
)
from .observe import observation, vertices
from .oracle import EXACTLY_ONCE, assess
from .verdict import MEASUREMENT_EVENT, summarize, verdict


def require_handoff(env, handoff):
    """Bind the internal execution path to its approved plan and reserved budget."""
    if (
        handoff is None
        or handoff.env is not env
        or handoff.controller.plan != env.approval.bigquery_plan
        or handoff.binding["query_until"] != env.schedule.cleanup_at
        or handoff.binding["evidence_bytes"] != QUERY_EVIDENCE_BYTES
    ):
        raise Failure(
            "BigQuery execution requires a bound handoff with its fixed 10 MiB query budget"
        )


def _said_nothing(error):
    """Whether a failed listing says nothing about the run's jobs.

    The listing reads the control record and the environment lock before it
    asks BigQuery, and the storage adapter wraps a transport error there, or a
    token refresh that failed in transport, as a plain failure; so the causes
    are followed. Any other failure is an invariant and stays fatal.
    """
    if transient(error):
        return True
    cause = error.__cause__
    while cause is not None:
        if isinstance(
            cause,
            requests.exceptions.RequestException
            | google.auth.exceptions.TransportError,
        ):
            return True
        cause = cause.__cause__
    return False


class BigQueryExercise(FiniteInputExercise):
    """Recover the finite input, then obtain a bounded query-visible result.

    The caller supplies the actors and the cleanup quiescence barrier.
    It decides the deployed measurement verdict from the recovery evidence it
    records and the observation coverage it accumulated, and writes it with the
    transition that completes the run.
    """

    namespace = BIGQUERY
    state_bucket = BIGQUERY_STATE
    timing = BIGQUERY_OBSERVATIONS
    service = "BigQuery"
    oracle_stage = "visibility"
    progress_event = "bigquery-progress"
    measurement_event = MEASUREMENT_EVENT
    baseline_event = "bigquery-baseline-window"

    def __init__(self, env, upgrade, connector_jobs):
        if env.approval.scenario != "bigquery-recovery":
            raise Failure("BigQuery exercise requires a BigQuery approval")
        trial = env.approval.bigquery_plan.trial
        # The supervisor's listing of the run's connector jobs that are not
        # DONE; a FILE_LOADS failover waits for one to be RUNNING.
        self.connector_jobs = connector_jobs
        super().__init__(env, upgrade, trial)

    def begin_failover(self, app, proof, pods):
        # The FILE_LOADS wait and its record precede the deletion, so they
        # wait for a Pod to delete as the failover itself does.
        if not self.active_input("upgrade") or self.failover_target(pods) is None:
            return
        if self.trial.mode != "FILE_LOADS":
            # The input check above already passed; skip the base's repeat.
            RecoveryExercise.begin_failover(self, app, proof, pods)
            return
        listed_at, running, errors = self._await_running_load()
        # Written before the deletion, so the failover record holds it even
        # when the recovery then fails.
        record = {
            "listed_at": utc(listed_at),
            "running_before": running,
            "listing_errors": errors,
        }
        self.outcomes["failover_in_flight"] = record
        RecoveryExercise.begin_failover(self, app, proof, pods)
        record["failover_at"] = utc(self.boundary)
        # A load listed RUNNING before the delete and again after it was
        # running when the delete landed, since a job's state only moves
        # forward; one that finished in between leaves the intersection empty.
        try:
            after = self._running_loads()
        except Failure as error:
            if not _said_nothing(error):
                raise
            after = None
            record["relisting_error"] = str(error)
        record.update(
            relisted_at=utc(self.env.clock()),
            running_after=after,
            running_through=None
            if after is None
            else sorted(set(running) & set(after)),
        )
        self.env.records.evidence("bigquery-failover-in-flight", record)

    def _running_loads(self):
        return sorted(
            job["id"]
            for job in self.connector_jobs(deadline=self.deadline)
            if job["kind"] == "load" and job["state"] == "RUNNING"
        )

    def _await_running_load(self):
        """List the run's jobs without a gap until a load is RUNNING.

        The wait replaces the deadline of the window after the upgrade and blocks the
        supervisor loop, which is why it is one checkpoint interval and a
        margin: a stop is still checked once a poll, and a transient listing
        error counts as a listing that saw nothing.
        """
        poll = self.timing["in_flight_poll_seconds"]
        self.deadline = min(
            self.env.clock() + self.timing["in_flight_seconds"],
            self.env.schedule.cleanup_at,
        )
        errors, checked = 0, None
        while True:
            started = self.env.clock()
            if started >= self.deadline:
                raise Failure("No load job of the run ran within the in-flight wait")
            if checked is None or started - checked >= POLL:
                self.env.require_running("Recovery exercise has been stopped")
                checked = started
            try:
                running = self._running_loads()
            except Failure as error:
                if not _said_nothing(error):
                    raise
                errors, running = errors + 1, []
            if running:
                return started, running, errors
            now = self.env.clock()
            self.env.sleep(max(0, min(started + poll, self.deadline) - now))

    def discover(self, rest, known):
        return vertices(rest, known, self.trial.mode)

    def read(self, rest, state):
        return observation(rest, state)

    summarize = staticmethod(summarize)

    def require_settled_loads(self, supervisor):
        """Refuse to read while a job the committer issued is still running.

        The committer commits synchronously, so a FINISHED application has no
        job left by the connector's own account. The oracle checks that
        rather than waiting it out: rows still arriving after FINISHED are the
        defect, and waiting would let them land and pass.
        """
        jobs = supervisor.connector_jobs(deadline=self.deadline)
        if jobs:
            self.env.records.evidence(
                "bigquery-unfinished-connector-jobs", {"jobs": jobs}
            )
            raise Failure(
                "A connector-issued BigQuery job outlived the finished application"
            )

    def verify_rows(self, supervisor):
        """Use new slots for visibility retries, without moving the final deadline."""
        if self.stage != "visibility":
            raise Failure(
                "BigQuery visibility requires both recoveries and finished input"
            )
        self.deadline = min(
            self.env.clock() + self.timing["visibility_seconds"],
            self.env.schedule.cleanup_at,
        )
        for slot in range(self.env.approval.bigquery_plan.query_slots):
            self.check_open()
            if self.trial.mode == "FILE_LOADS":
                self.require_settled_loads(supervisor)
            name = f"final-{slot}"
            result = supervisor.query(name, deadline=self.deadline)
            self.check_open()
            if not isinstance(result, dict) or "rows" not in result:
                raise Failure("BigQuery archived observation has no rows")
            report = assess(self.trial, result["rows"])
            if result.get("report") != report:
                raise Failure("BigQuery archived oracle differs from its rows")
            if report["invalid_rows"] or (
                self.trial.mode in EXACTLY_ONCE and report["duplicate_rows"]
            ):
                raise Failure("BigQuery final rows violate routing or uniqueness")
            if report["verdict"] == "pass":
                self.outcomes["query"] = {"observation": name, "report": report}
                # Decide the verdict from what this run holds, and write it with
                # the transition rather than after it, so it reaches the
                # `recovery-complete` evidence artifact and not only the final
                # receipt. It is decided over the record the transition is
                # about to write, because the offline analyzer recomputes it
                # from that same record once the evidence is exported.
                self.complete(lambda record: verdict(record, self.trial.mode))
                return
            self.env.sleep(min(POLL, max(0, self.deadline - self.env.clock())))
        raise Failure("BigQuery final visibility exhausted its approved query slots")
