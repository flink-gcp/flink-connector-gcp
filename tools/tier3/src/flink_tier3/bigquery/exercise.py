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

from ..common import Failure, StorageRequestFailed, transient
from ..exercise import FiniteInputExercise
from ..policy import (
    BIGQUERY,
    BIGQUERY_OBSERVATIONS,
    BIGQUERY_STATE,
    QUERY_EVIDENCE_BYTES,
)
from .oracle import assess
from .verdict import data_reasons


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


class BigQueryExercise(FiniteInputExercise):
    """The BigQuery trial as one fault sequence, judged by its data (ADR-0204).

    The job finishes with its input, and one query then reads every table,
    through the supervisor's query handoff. Its report is the verdict: rows
    that are invalid or missing, or in an exactly-once mode duplicated, fail
    the run through the verdict rather than ending it, and the query is not
    repeated until it passes, since the finished job, not the tables, says the
    input was committed. No load is awaited before a fault: the faults land
    wherever the commits happen to be (#2148). A FILE_LOADS run refuses to
    read while a job its committer issued is not DONE, as every FILE_LOADS
    trial does, and lists them again while a listing gets no answer.
    """

    namespace = BIGQUERY
    state_bucket = BIGQUERY_STATE
    timing = BIGQUERY_OBSERVATIONS
    service = "BigQuery"
    oracle_stage = "visibility"
    progress_event = "bigquery-progress"

    def __init__(self, env, upgrade):
        if env.approval.scenario != "bigquery-recovery":
            raise Failure("BigQuery exercise requires a BigQuery approval")
        super().__init__(env, upgrade, env.approval.bigquery_plan.trial)

    def refuse_unsettled(self, jobs):
        """Stop the run over the connector jobs a listing found not DONE."""
        if jobs:
            self.env.records.evidence(
                "bigquery-unfinished-connector-jobs", {"jobs": jobs}
            )
            raise Failure(
                "A connector-issued BigQuery job outlived the finished application"
            )

    def read_rows(self, supervisor, name, deadline):
        """One archived query result's report, checked against the rows it came with."""
        result = supervisor.query(name, deadline=deadline)
        self.check_open()
        if not isinstance(result, dict) or "rows" not in result:
            raise Failure("BigQuery archived observation has no rows")
        report = assess(self.trial, result["rows"])
        if result.get("report") != report:
            raise Failure("BigQuery archived oracle differs from its rows")
        return report

    def verify_rows(self, supervisor):
        """Read the tables once, within the read window, and complete the run."""
        if self.stage != "visibility":
            raise Failure("BigQuery visibility requires the fault sequence and input")
        self.check_open()
        until = self.oracle_window()
        if until <= self.env.clock():
            # The job finished within the minute the record's write keeps.
            raise Failure("Run reached its time limit at stage " + self.stage)
        if self.trial.mode == "FILE_LOADS":
            self.refuse_unsettled(self._listed_jobs(supervisor, until))
        name = "final-0"
        report = self.read_rows(supervisor, name, until)
        self.outcomes["query"] = {"observation": name, "report": report}
        self.complete(data_reasons)

    def _listed_jobs(self, supervisor, until):
        """The connector jobs not DONE, listed again while a listing gets no answer.

        Only the listing repeats: a job it found is refused even when
        recording it fails. A listing that failed part-way loses what it had
        found, so a job that then finishes before the next one goes unseen as
        a job; whatever it wrote was in the table before the query, which
        judges it, and the verdict is the data (ADR-0204). A storage read without
        an answer is told apart by the adapter, as the query handoff tells it.
        Between listings a stop is honoured, and a listing due after the read
        window is not begun, so its control reads cannot spend the minute the
        record keeps.
        """

        def listing():
            if self.env.clock() >= until:
                raise Failure(
                    "BigQuery read window closed while listing connector jobs"
                )
            self.check_open()
            return supervisor.connector_jobs(deadline=until)

        return self.retried(
            listing,
            until,
            said_nothing=lambda error: (
                transient(error) or isinstance(error, StorageRequestFailed)
            ),
        )
