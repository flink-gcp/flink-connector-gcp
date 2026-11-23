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
"""Inject and observe one approved Pub/Sub recovery trial."""

import re

from ..common import Failure, timestamp, utc
from ..exercise import SEQUENCE, SEQUENCE_KIND, UNCONFIRMED, RecoveryExercise
from ..policy import PUBSUB, PUBSUB_EXERCISE, PUBSUB_STATE
from ..verdict import COMPLETE_STAGE, sequence_verdict
from .messages import COHORTS, input_path, message_prefix, publications
from .oracle import input_ids, published_ids, reconcile
from .output import OutputCollector
from .verdict import data_reasons

# The line an attempt logs on its initialization, before any record.
ATTEMPT = re.compile(
    r"event=pubsub-attempt run_id=(\S+) phase=(\S+) attempt=(\S+) "
    r"restored=(true|false)"
)


def summary(collector):
    """What a collector has seen so far, which the record counts."""
    observations = collector.observations
    return {
        "observations": len(observations),
        "foreign": len(collector.foreign),
        "attempts": sorted({item["attempt"] for item in observations}),
        "last_output_at": utc(max(item["at"] for item in observations))
        if observations
        else None,
    }


# The relay's progress line, which an attempt logs at its first record and
# then at least every ten seconds while it processes: how many records it has
# processed.
PROGRESS = re.compile(
    r"event=pubsub-progress run_id=(\S+) phase=(\S+) attempt=(\S+) "
    r"restored=(true|false) origin=([0-9]+) processed=([0-9]+)"
)
# Pub/Sub's longest acknowledgement deadline, to which the client library caps
# each lease extension. A lease the attempts a restart stopped held has lapsed
# this long after the restore (ADR-0204). The wait counts from the job's latest
# restore or progress line, whichever is later, so the paced input is through
# before it starts.
REDELIVERY_SECONDS = 600
# How many pulls the last round may take before the verdict, once the
# redelivery horizon has passed with records still missing.
FINAL_PULLS = 200


class PubSubExercise(RecoveryExercise):
    """The Pub/Sub trial as one fault sequence, judged by its output (ADR-0204).

    The runner publishes the whole input, the admission cohort and then the
    other two as soon as the one before is out, and the relay paces it. The
    faults land while it flows, as in the other sequences; the relay's
    progress lines, by the attempt that logged them, show the job running
    again after each.

    The source never ends, so the job never finishes. It completes once it
    runs again after the last fault and either every expected record is on
    the output subscription, or `REDELIVERY_SECONDS` have passed since its
    latest restore or progress line, whichever came later, and a checkpoint
    triggered after them has completed: the input is through by then, and
    every lease the stopped attempts held has lapsed, so a redelivery would
    have logged progress and moved the wait on; a record still missing is
    lost. The output only ends the wait early when nothing is missing.

    Every output is checked against the runner's publication receipts, and
    the verdict is the oracle's and that check's alone. A pull the service
    answers with a transient error is tried again on the next poll.
    """

    namespace = PUBSUB
    state_bucket = PUBSUB_STATE
    timing = PUBSUB_EXERCISE

    def __init__(self, env, upgrade, handoff):
        if env.approval.scenario != "pubsub-recovery":
            raise Failure("Pub/Sub exercise requires a Pub/Sub approval")
        trial = env.approval.pubsub_trial
        if trial["trial"] != SEQUENCE_KIND:
            raise Failure("Pub/Sub fault sequence requires a sequence trial")
        self.records = trial["records_per_subscription"]
        super().__init__(env, upgrade)
        self.handoff = handoff
        self.collector = OutputCollector(handoff)
        # Attempt UUID to the Pod whose progress lines named it, and to when it
        # announced itself on its initialization.
        self.attempts = {}
        self.announced = {}
        # How many cohorts have been asked for; admission publishes the first.
        self.requested = 1

    def progress(self, pod, text):
        """Read the relay's progress lines as the job's progress."""
        uid = pod["metadata"]["uid"]
        for line in text.splitlines():
            match = ATTEMPT.search(line)
            if match and match.group(1) == self.env.approval.run_id:
                at = timestamp(line.split(" ", 1)[0])
                attempt = match.group(3)
                self.announced[attempt] = min(self.announced.get(attempt, at), at)
        for line in text.splitlines():
            match = PROGRESS.search(line)
            if not match:
                continue
            run_id, phase, attempt, restored, origin, processed = match.groups()
            at = timestamp(line.split(" ", 1)[0])
            if run_id != self.env.approval.run_id or phase not in (
                "initial",
                "upgrade",
            ):
                raise Failure("Pub/Sub progress belongs to another run or phase")
            if at < self.env.schedule.started:
                raise Failure("Pub/Sub progress predates this run")
            if at > self.env.clock():
                raise Failure("Pub/Sub progress is dated in the future")
            if phase == "upgrade" and (self.step == 0 or restored != "true"):
                raise Failure("Upgrade progress appeared before its approved operation")
            known = self.attempts.get(attempt)
            if known and known["pod_uid"] != uid:
                raise Failure("One Pub/Sub attempt was announced by two Pods")
            self.attempts[attempt] = {
                "pod_uid": uid,
                "pod": pod["metadata"]["name"],
                "phase": phase,
                "restored": restored == "true",
                "origin": int(origin),
            }
            self.samples[(uid, at, attempt, int(processed))] = {
                "at": at,
                "phase": phase,
                "processed": int(processed),
                "attempt": attempt,
                "pod_uid": uid,
            }

    def observed(self):
        """Every logical input seen on the output subscription."""
        return {
            (item["input_index"], item["sequence"])
            for item in self.collector.observations
        }

    def input_complete(self):
        """Whether every record of both subscriptions is on the output."""
        return len(self.observed()) == 2 * self.records

    def finished(self):
        """Never: the source does not end, so the job does not finish."""
        return False

    def ran_after_restore(self):
        """Whether an attempt that started after the awaited fault has logged progress.

        After a TaskManager's loss only its own region restores, so a line from
        the attempt on the other TaskManager, which never stopped, is not the
        job running again, even when that attempt logs its first progress only
        then. An attempt started after the fault when it announced itself, on
        its initialization, after the fault.
        """
        at, fault = self.restored_at, self.awaiting["at"]
        return (
            at is not None
            and at > fault
            and any(
                s["phase"] == "upgrade"
                and s["at"] > at
                and self.announced.get(s["attempt"], fault) > fault
                for s in self.samples.values()
            )
        )

    def restored_idle(self, app, status, pods):
        """Whether the job runs again after the awaited fault but logs no progress.

        Everything that shows a job running again holds except its progress:
        an applied patch deployed, the restore after the fault, the JobManager
        and the TaskManagers.
        """
        return (
            self.restored_at is not None
            and self.restored_at > self.awaiting["at"]
            and status == "RUNNING"
            and self.reconciled(app)
            and self.jobmanager(pods) is not None
            and len(self.running(pods, "taskmanager")) >= self.parallelism
        )

    def observe(self, app, rest, pods):
        self.check_open()
        self.collector.drain(self.timing["drain_pulls"])
        # The pulls can outlast the time limit, which is enforced first.
        self.check_open()
        self.request_input()
        if super().observe(app, rest, pods):
            raise Failure("Pub/Sub fault sequence completed without its output")
        return self.settle(app, rest, pods)

    def request_input(self):
        """Ask the runner for the next cohort once the one before it is out."""
        if self.requested == len(COHORTS):
            return
        previous = self.handoff.cohorts().get(COHORTS[self.requested - 1])
        if previous is None or previous["published_at"] is None:
            return
        self.handoff.request_cohort(
            COHORTS[self.requested],
            deadline=min(
                self.env.clock() + self.timing["cohort_seconds"],
                self.env.schedule.cleanup_at,
            ),
        )
        self.requested += 1

    def settle(self, app, rest, pods):
        """Complete the run once the job ran again after the last fault and its output is settled.

        The input is through when every record is on the output, and the job
        has nothing left to process when it logs no progress for
        `REDELIVERY_SECONDS` after a restore. Either, before the job is seen
        running again after the last fault, leaves the run unconfirmed: no
        fault goes ahead, and no patch not yet sent is sent, once the input is
        through, and a restored job with nothing to process logs no progress.
        """
        status = app.get("status", {}).get("jobStatus", {}).get("state", "")
        awaiting = self.awaiting
        unapplied = awaiting is not None and awaiting.get("generation", 0) is None
        if (unapplied or (awaiting is None and self.step < len(SEQUENCE))) and (
            self.input_complete()
        ):
            raise Failure(UNCONFIRMED)
        if (
            awaiting is not None
            and not unapplied
            and self.restored_idle(app, status, pods)
            and (
                self.input_complete()
                # Only statistics read this poll show the latest restore; a
                # read that failed may hide one the horizon counts from.
                or (rest and self.env.clock() >= self.restored_at + REDELIVERY_SECONDS)
            )
        ):
            raise Failure(UNCONFIRMED)
        if awaiting is not None or self.step < len(SEQUENCE) or status != "RUNNING":
            return False
        if self.input_complete():
            completion = "all-observed"
        else:
            # The input still flowing would read as lost; it logs progress.
            latest = max((s["at"] for s in self.samples.values()), default=None)
            marks = [at for at in (self.restored_at, latest) if at is not None]
            if not marks or not self.completed(rest, max(marks) + REDELIVERY_SECONDS):
                return False
            completion = "redelivery-horizon"
            refused = len(self.collector.retried)
            self.collector.drain(FINAL_PULLS)
            if len(self.collector.retried) > refused:
                # The last round was cut short; the next poll pulls again.
                return False
        details = {
            "completion": completion,
            "oracle": reconcile(
                self.collector.lines, self.env.approval.run_id, self.records
            ),
            "inputs": input_ids(self.collector.observations, self.published()),
            "attempts": self.attempts,
            # Batches a refused pull left without observations, which an
            # offline analysis does not count as lost.
            "retried_pulls": list(self.collector.retried),
        }
        decided = sequence_verdict(
            {"stage": COMPLETE_STAGE, "outcomes": self.outcomes, **details},
            data_reasons,
        )
        self.persist(
            COMPLETE_STAGE, observed=summary(self.collector), **details, **decided
        )
        return True

    def published(self):
        """Each logical input's published IDs, from the runner's receipts.

        A receipt that is missing or does not cover its interval leaves its
        inputs unreceipted, which the verdict reports.
        """
        prefix = message_prefix(self.env.approval.run_id, self.env.approval.nonce)
        receipts = []
        for index, start, count in publications(self.records):
            value, _ = self.env.store.read(
                input_path(prefix, index, start, count) + "/response.json"
            )
            ids = value.get("messageIds") if isinstance(value, dict) else None
            if isinstance(ids, list) and len(ids) == count:
                receipts.append((index, start, ids))
        return published_ids(receipts)
