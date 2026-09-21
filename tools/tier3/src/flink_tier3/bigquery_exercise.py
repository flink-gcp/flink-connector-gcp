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
"""Internal BigQuery recovery scheduling; entrypoints remain disabled."""

from __future__ import annotations

import uuid

from .bigquery import assess
from .bigquery_plan import OBSERVATIONS
from .common import Failure, utc
from .exercise import RecoveryExercise
from .policy import BIGQUERY, BIGQUERY_STATE, MIB, POLL


def require_handoff(env, handoff):
    """Bind the internal execution path to its approved plan and reserved budget."""
    if (
        handoff is None
        or handoff.env is not env
        or handoff.controller.plan != env.approval.bigquery_plan
        or handoff.binding["query_until"] != env.schedule.cleanup_at
        or handoff.binding["evidence_bytes"] != 10 * MIB
    ):
        raise Failure(
            "BigQuery execution requires a bound handoff with its fixed 10 MiB query budget"
        )


class BigQueryExercise(RecoveryExercise):
    """Recover the finite input, then obtain a bounded query-visible result.

    The caller supplies authenticated actors and the cleanup quiescence barrier.
    This exercise records recovery evidence, not a deployed measurement verdict.
    """

    namespace = BIGQUERY
    state_bucket = BIGQUERY_STATE
    timing = OBSERVATIONS
    progress_event = "bigquery-progress"
    expected_pods = 3

    def __init__(self, env, upgrade):
        if env.approval.scenario != "bigquery-recovery":
            raise Failure("BigQuery exercise requires a BigQuery approval")
        self.trial = env.approval.bigquery_plan.trial
        self.records = self.trial.records
        super().__init__(env, upgrade)
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
                raise Failure("BigQuery progress is dated in the future")
            if not valid or (lineage is not None and sample["lineage"] != lineage):
                raise Failure("BigQuery input lineage changed or is invalid")
            if lineage is None:
                self.env.records.record_lineage(sample["lineage"])
                lineage = sample["lineage"]

    def active_input(self, phase):
        samples = self.fresh_progress(phase)
        if not samples:
            return False
        latest = max(samples, key=lambda sample: sample["at"])
        if latest["at"] > self.env.clock():
            raise Failure("BigQuery progress is dated in the future")
        return (
            latest["processed"] < self.records and self.env.clock() - latest["at"] <= 60
        )

    def begin_upgrade(self, app, cp, progress, pods):
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
            self.deadline = min(self.baseline_until + 60, self.env.schedule.cleanup_at)
            self.env.records.evidence(
                "bigquery-baseline-window",
                {
                    "started_at": utc(self.env.clock()),
                    "until": utc(self.baseline_until),
                },
            )
        if self.env.clock() < self.baseline_until:
            return
        if cp["trigger_timestamp"] / 1000 < self.baseline_until:
            return
        super().begin_upgrade(app, cp, progress, pods)

    def begin_failover(self, app, proof, pods):
        if self.active_input("upgrade"):
            super().begin_failover(app, proof, pods)

    def persist(self, stage, **details):
        if stage == "finishing":
            self.post_until = self.env.clock() + self.timing["post_recovery_seconds"]
        if stage == "complete":
            stage = "visibility"
        super().persist(stage, **details)

    def observe(self, app, rest, pods):
        state = app.get("status", {}).get("jobStatus", {}).get("state")
        if self.stage == "finishing" and state == "FINISHED":
            completed = [
                sample["at"]
                for sample in self.fresh_progress("upgrade")
                if sample["processed"] == self.records
            ]
            if not completed or min(completed) < self.post_until:
                raise Failure("BigQuery input finished before the post-recovery window")
        return super().observe(app, rest, pods)

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
            name = f"final-{slot}"
            result = supervisor.query(name, deadline=self.deadline)
            self.check_open()
            if not isinstance(result, dict) or "rows" not in result:
                raise Failure("BigQuery archived observation has no rows")
            report = assess(self.trial, result["rows"])
            if result.get("report") != report:
                raise Failure("BigQuery archived oracle differs from its rows")
            if report["invalid_rows"] or (
                self.trial.mode == "EO" and report["duplicate_rows"]
            ):
                raise Failure("BigQuery final rows violate routing or uniqueness")
            if report["verdict"] == "pass":
                self.outcomes["query"] = {"observation": name, "report": report}
                # Bypass the FINISHED-to-visibility transition only after the oracle passes.
                super().persist("complete", processed=self.records)
                return
            self.env.sleep(min(POLL, max(0, self.deadline - self.env.clock())))
        raise Failure("BigQuery final visibility exhausted its approved query slots")
