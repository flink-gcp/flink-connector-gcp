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
"""One approved savepoint upgrade and one UID-scoped Pod disruption.

The upgrade may rescale the job, and the disruption deletes the JobManager or
one TaskManager, as the approved trial names them. A scenario whose timing
names a ``post_upgrade_seconds`` window holds the proved upgrade that long
before the disruption, so it can measure the job after each recovery.
"""

from __future__ import annotations

import copy
import re

from .common import ApiError, Failure, TransportError, contains, timestamp, utc
from .metrics import FlinkRest

# The Pod the second recovery deletes, by the name the approved trial gives it,
# and the role its deletion messages name.
FAILOVERS = {"jobmanager": "JM", "taskmanager": "TM"}
# The directions a rescale moves the checkpointed subtasks in.
RESCALE_DIRECTIONS = ("out", "in")
# The manifest fields an approved rescale's upgrade may change beside the
# arguments; an upgrade that is not a rescale leaves them as they are.
RESCALE_SIZES = (("job", "parallelism"), ("taskManager", "replicas"))


class RecoveryExercise:
    """The supervisor advances this exercise once; recovery only cleans it up.

    A service exercise names its namespace, state bucket and timing, and one
    that reads application progress also its event, Pod count and records.
    Its approved trial decides, when the exercise is built, which Pod the
    second recovery deletes and whether the upgrade is a rescale, and in which
    direction; nothing at run time chooses either.

    With a ``post_upgrade_seconds`` window the stages are ``baseline``,
    ``upgrade``, ``upgraded``, ``failover`` and ``finishing``; without one
    there is no ``upgraded`` stage, and the failover begins as soon as the
    upgrade is proved.
    """

    namespace: str
    state_bucket: str
    timing: dict
    progress_event: str
    expected_pods: int
    records: int

    def __init__(self, env, upgrade, *, failover="jobmanager", rescale=None):
        if upgrade is None:
            raise Failure("Recovery requires the approved upgrade manifest")
        if failover not in FAILOVERS or (
            rescale is not None and rescale not in RESCALE_DIRECTIONS
        ):
            raise Failure("Recovery requires an approved fault kind")
        self.env, self.upgrade = env, upgrade
        self.failover, self.rescale = failover, rescale
        self.stage = "baseline"
        self.deadline = min(
            env.schedule.started + self.timing["startup_seconds"],
            env.schedule.cleanup_at,
        )
        self.boundary = env.schedule.started
        self.samples = {}
        self.events = {}
        self.before = {}
        self.outcomes = {}
        self.initialized = False
        self.stable_pods = None
        self.retiring_pods = set()
        self.generation = None
        self.rest, self.measured_at = None, None
        self.coverage = {}
        self.upgraded_at, self.upgraded_until = None, None
        # The completed checkpoint a held upgrade's failover must not restore
        # behind: the latest one triggered since the window opened.
        self.failover_floor = None

    @property
    def recovering(self):
        return self.stage in ("upgrade", "failover")

    def admitted(self):
        """Called once, when the loop sees the application admitted."""

    def prefix(self, kind):
        """The run's own location for ``checkpoints`` or ``savepoints``."""
        return f"gs://{self.state_bucket}/runs/{self.env.approval.run_id}/{kind}/"

    def track_rest(self, service, job_id):
        """Keep a REST reader for the job the loop verified; True when replaced.

        No service resolved means the job is between states; measuring against
        the last one would attribute a reading to a job that is not the one
        running.
        """
        if service is None:
            self.rest = None
            return True
        replaced = self.rest is None or self.rest.job_id != job_id
        if replaced:
            self.rest = FlinkRest(self.env, service, job_id)
        self.rest.service = service
        return replaced

    def measurement_due(self):
        """Whether a REST reader exists and a measurement interval has passed."""
        if self.rest is None:
            return False
        now = self.env.clock()
        if (
            self.measured_at is not None
            and now - self.measured_at < self.timing["measure_seconds"]
        ):
            return False
        self.measured_at = now
        return True

    def rebaseline(self, pods):
        """After a proven recovery, the Pods running now are the stable set."""
        self.retiring_pods = {
            p["metadata"]["uid"]
            for p in pods
            if p["metadata"]["namespace"] == self.namespace
            and p["metadata"].get("deletionTimestamp")
        }
        self.stable_pods = {
            p["metadata"]["uid"]
            for p in pods
            if p["metadata"]["namespace"] == self.namespace
        } - self.retiring_pods

    def check_open(self):
        self.env.require_running("Recovery exercise has been stopped")
        if self.env.clock() >= min(self.deadline, self.env.schedule.cleanup_at):
            raise Failure("Recovery exercise deadline expired: " + self.stage)

    def _coverage(self):
        return {name: dict(window) for name, window in self.coverage.items()}

    def persist(self, stage, **details):
        # For a run that stops before its verdict, the record is the only
        # account of what it read.
        details.setdefault("coverage", self._coverage())
        self.check_open()
        value = {
            "stage": stage,
            "at": utc(self.env.clock()),
            "outcomes": copy.deepcopy(self.outcomes),
            **details,
        }
        self.env.records.evidence("recovery-" + stage, value)
        self.env.records.recovery_step(self.stage if self.initialized else None, value)
        self.stage, self.initialized = stage, True

    def progress(self, pod, text):
        pattern = (
            rf"event={self.progress_event} run_id=(\S+) phase=(\S+) lineage=(\S+) "
            r"restored=(true|false) processed=([0-9]+) sequence=([0-9]+)"
        )
        for line in text.splitlines():
            match = re.search(pattern, line)
            if not match:
                continue
            run_id, phase, lineage, restored, processed, sequence = match.groups()
            at = timestamp(line.split(" ", 1)[0])
            if run_id != self.env.approval.run_id or phase not in (
                "initial",
                "upgrade",
            ):
                raise Failure("Recovery progress belongs to another run or phase")
            if at < self.env.schedule.started:
                raise Failure("Recovery progress predates this run")
            if phase == "upgrade" and self.stage == "baseline":
                raise Failure("Upgrade progress appeared before its approved operation")
            if (phase == "initial" and restored != "false") or (
                phase == "upgrade" and restored != "true"
            ):
                raise Failure("Unexpected fresh start or unplanned initial recovery")
            processed, sequence = int(processed), int(sequence)
            if not 0 < processed <= self.records or sequence != processed - 1:
                raise Failure("Recovery progress violates the deterministic input")
            sample = {
                "at": at,
                "phase": phase,
                "lineage": lineage,
                "processed": processed,
                "sequence": sequence,
                "pod_uid": pod["metadata"]["uid"],
            }
            self.samples[(sample["pod_uid"], at, phase, processed)] = sample

    def scheduling(self, pods):
        owned = set(self.env.observed)
        new_events = []
        interrupted = False
        for event in self.env.kube.items("Event", self.namespace):
            if event.get("involvedObject", {}).get("uid") not in owned:
                continue
            meta = event["metadata"]
            if self.events.get(meta["uid"]) == meta["resourceVersion"]:
                continue
            self.events[meta["uid"]] = meta["resourceVersion"]
            new_events.append(event)
            if event.get("reason") in (
                "Preempted",
                "Evicted",
                "Shutdown",
                "NodeNotReady",
            ):
                interrupted = True
        if new_events:
            self.env.emit("scheduling-events", new_events)
        smoke = [p for p in pods if p["metadata"]["namespace"] == self.namespace]
        for pod in smoke:
            status = pod.get("status", {})
            if (
                status.get("reason") in ("Evicted", "Shutdown", "NodeLost")
                or any(
                    c.get("restartCount", 0)
                    for c in status.get("containerStatuses", [])
                )
                or any(
                    c.get("type") == "DisruptionTarget" and c.get("status") == "True"
                    for c in status.get("conditions", [])
                )
            ):
                interrupted = True
        if interrupted:
            raise Failure("Unplanned workload interruption; trial is inconclusive")
        identities = {p["metadata"]["uid"] for p in smoke} - self.retiring_pods
        if (
            self.stable_pods is None
            and self.stage == "baseline"
            and len(smoke) == self.expected_pods
            and all(p.get("status", {}).get("phase") == "Running" for p in smoke)
        ):
            self.stable_pods = identities
        if (
            self.stable_pods is not None
            and identities != self.stable_pods
            and not self.recovering
            # Completed input can release idle Pods before the Operator reports FINISHED.
            and not (
                self.stage == "finishing"
                and self.input_complete()
                and identities < self.stable_pods
            )
        ):
            raise Failure("Unplanned workload replacement; trial is inconclusive")

    def running(self, pods, component):
        """This namespace's running Pods of one Flink component, none terminating."""
        return [
            p
            for p in pods
            if p["metadata"]["namespace"] == self.namespace
            and p["metadata"].get("labels", {}).get("component") == component
            and not p["metadata"].get("deletionTimestamp")
            and p.get("status", {}).get("phase") == "Running"
        ]

    def jobmanager(self, pods):
        candidates = self.running(pods, "jobmanager")
        if len(candidates) != 1:
            return None
        # audit() has already checked the complete ownerReference graph.
        return candidates[0]

    def completed(self, rest, after):
        cp = rest.get("latest", {}).get("completed") or {}
        if (
            cp.get("status") != "COMPLETED"
            or cp.get("is_savepoint") is not False
            or not isinstance(cp.get("id"), int)
            or cp.get("trigger_timestamp", 0) / 1000 <= after
            or cp.get("latest_ack_timestamp", 0) < cp.get("trigger_timestamp", 0)
        ):
            return None
        path = cp.get("external_path", "")
        if not path.startswith(self.prefix("checkpoints")):
            raise Failure("Completed checkpoint is outside the approved state prefix")
        return cp

    def fresh_progress(self, phase):
        return [
            s
            for s in self.samples.values()
            if s["phase"] == phase and s["at"] > self.boundary
        ]

    def input_complete(self):
        return any(
            s["processed"] == self.records for s in self.fresh_progress("upgrade")
        )

    def recovery_proof(self, app, rest, pods):
        restored = rest.get("latest", {}).get("restored") or {}
        at = restored.get("restore_timestamp", 0) / 1000
        if at <= self.boundary:
            return None
        if not isinstance(restored.get("id"), int):
            raise Failure("Restored checkpoint identity is missing")
        progress = self.fresh_progress("upgrade")
        resumed = [
            s
            for s in progress
            if s["at"] >= at and s["processed"] > self.before["processed"]
        ]
        cp = self.completed(rest, min(s["at"] for s in resumed)) if resumed else None
        jm = self.jobmanager(pods)
        if not resumed or not cp or jm is None:
            return None
        replaced = self.stage == "failover" and self.failover == "taskmanager"
        if replaced:
            # Only the TaskManager was approved to go; the JobManager keeps the
            # job, and only the region on the lost TaskManager may restart.
            if jm["metadata"]["uid"] != self.before["jm_uid"]:
                raise Failure(
                    "JobManager was replaced during the TaskManager replacement"
                )
            if any(p["metadata"]["uid"] == self.before["tm_uid"] for p in pods):
                return None
        elif jm["metadata"]["uid"] == self.before["jm_uid"]:
            return None
        proof = {}
        if self.stage == "upgrade":
            status = app.get("status", {})
            savepoint = (
                status.get("jobStatus", {})
                .get("savepointInfo", {})
                .get("lastSavepoint")
                or {}
            )
            if (
                status.get("observedGeneration", 0) < self.generation
                or status.get("reconciliationStatus", {}).get("state") != "DEPLOYED"
            ):
                return None
            path = restored.get("external_path", "")
            if (
                restored.get("is_savepoint") is not True
                or savepoint.get("location") != path
                or savepoint.get("triggerType") != "UPGRADE"
                or savepoint.get("timeStamp", 0) / 1000 <= self.boundary
                or not path.startswith(self.prefix("savepoints"))
            ):
                raise Failure("Upgrade did not restore its newly completed savepoint")
            if self.rescale is not None:
                proof["rescale"] = self.rescaled(cp)
        else:
            if (
                app["status"]["jobStatus"]["jobId"] != self.before["job_id"]
                or restored.get("is_savepoint") is not False
                or restored["id"] < self.before["checkpoint"]["id"]
                or not restored.get("external_path", "").startswith(
                    self.prefix("checkpoints")
                )
            ):
                raise Failure(
                    "TaskManager replacement did not recover the checkpointed job"
                    if replaced
                    else "JobManager did not recover the checkpointed job"
                )
            if replaced:
                proof["replaced"] = {
                    "uid": self.before["tm_uid"],
                    "name": self.before["tm"],
                }
        return {
            "restored": restored,
            "checkpoint": cp,
            "progress": max(resumed, key=lambda s: s["processed"]),
            "jm_uid": jm["metadata"]["uid"],
            **proof,
        }

    def rescaled(self, cp):
        """The subtasks a rescale moved, once its checkpoint moved them as approved.

        The application may set its operators' parallelism from its arguments
        rather than from the manifest, so the new parallelism is read from what
        the job checkpointed: a checkpoint the restored job completed against
        the last one before the upgrade.
        """
        before = self.before["checkpoint"].get("num_subtasks")
        after = cp.get("num_subtasks")
        if type(before) is not int or type(after) is not int:
            raise Failure("Rescale checkpoints do not report their subtasks")
        if not (after > before if self.rescale == "out" else after < before):
            raise Failure("Rescale did not move the checkpointed subtasks as approved")
        return {
            "direction": self.rescale,
            "subtasks_before": before,
            "subtasks_after": after,
        }

    def begin_upgrade(self, app, cp, progress, pods):
        jm = self.jobmanager(pods)
        if jm is None:
            return
        operations = self.upgrade_operations(app)
        self.before = {
            "processed": max(s["processed"] for s in progress),
            "checkpoint": cp,
            "jm_uid": jm["metadata"]["uid"],
            "job_id": app["status"]["jobStatus"]["jobId"],
        }
        self.boundary = self.env.clock()
        self.deadline = min(
            self.boundary + self.timing["recovery_seconds"],
            self.env.schedule.cleanup_at,
        )
        self.generation = app["metadata"]["generation"] + 1
        self.persist("upgrade", before=self.before, generation=self.generation)
        self.check_open()
        self.patch_upgrade(app, operations)

    def upgrade_operations(self, app):
        """The patch to the approved upgrade: its arguments, and a rescale's sizes."""
        operations = [
            {
                "op": "replace",
                "path": "/spec/job/args",
                "value": self.upgrade["spec"]["job"]["args"],
            }
        ]
        for parent, field in RESCALE_SIZES:
            want = self.upgrade["spec"].get(parent, {}).get(field)
            have = app["spec"].get(parent, {}).get(field)
            if want == have:
                continue
            if self.rescale is None:
                raise Failure(
                    "Upgrade changes the parallelism of a trial without a rescale"
                )
            if not isinstance(app["spec"].get(parent), dict):
                raise Failure("Rescale sets a size in a section the application lacks")
            # bool is an int subclass, and True == 1. A size the initial
            # manifest leaves to Flink's default is added; none is removed.
            if (
                type(want) is not int
                or want < 1
                or (have is not None and type(have) is not int)
            ):
                raise Failure("Rescale sets a size that is not a positive count")
            operations.append(
                {
                    "op": "replace" if have is not None else "add",
                    "path": f"/spec/{parent}/{field}",
                    "value": want,
                }
            )
        return operations

    def patch_upgrade(self, app, operations):
        """Apply the approved upgrade once; a lost response must show it applied."""
        # Patch cannot recreate a deleted CR; the UID/version tests lose to deletion.
        try:
            self.env.kube.patch(app, operations)
        except (ApiError, TransportError) as error:
            if isinstance(error, ApiError) and error.status in (409, 422):
                raise Failure(
                    "Upgrade precondition rejected; trial stopped without retry"
                ) from error
            if isinstance(error, ApiError) and error.status not in (
                500,
                502,
                503,
                504,
            ):
                raise
            current = self.env.root("application")
            if (
                not current
                or current["metadata"].get("deletionTimestamp")
                or not contains(current["spec"], self.upgrade["spec"])
            ):
                raise Failure(
                    "Upgrade response lost without a verified applied outcome"
                ) from error

    def hold_upgrade(self, proof, pods):
        """Keep the proved upgrade running for its window before the failover.

        The job is in a steady state again, so the Pods running now are the
        stable set and a REST error is no longer a recovery's. The failover
        then has a recovery's time to begin once the window ends.
        """
        self.outcomes["upgrade"] = proof
        self.upgraded_at = self.env.clock()
        self.upgraded_until = self.upgraded_at + self.timing["post_upgrade_seconds"]
        self.deadline = min(
            self.upgraded_until + self.timing["recovery_seconds"],
            self.env.schedule.cleanup_at,
        )
        self.persist("upgraded", until=utc(self.upgraded_until))
        self.rebaseline(pods)

    def begin_failover(self, app, proof, pods):
        """Delete the approved Pod after the upgrade's proof.

        The failover must not restore behind the proof's checkpoint, or, after
        a held upgrade, behind ``failover_floor``.
        """
        jm, target = self.jobmanager(pods), self.failover_target(pods)
        if jm is None or target is None:
            return
        self.outcomes["upgrade"] = proof
        self.before = {
            "processed": max(s["processed"] for s in self.fresh_progress("upgrade")),
            "checkpoint": self.failover_floor or proof["checkpoint"],
            "jm_uid": jm["metadata"]["uid"],
            "job_id": app["status"]["jobStatus"]["jobId"],
        }
        if self.failover == "taskmanager":
            self.before.update(
                tm_uid=target["metadata"]["uid"], tm=target["metadata"]["name"]
            )
        self.boundary = self.env.clock()
        self.deadline = min(
            self.boundary + self.timing["recovery_seconds"],
            self.env.schedule.cleanup_at,
        )
        self.persist("failover", before=self.before)
        self.check_open()
        self.delete_pod(target, FAILOVERS[self.failover])

    def failover_target(self, pods):
        """The Pod the failover deletes, or None while it cannot be named yet.

        A subclass that does work before the deletion asks this first, since
        the failover does not start without it.
        """
        if self.failover == "jobmanager":
            return self.jobmanager(pods)
        return self.taskmanager(pods)

    def taskmanager(self, pods):
        """The running TaskManager the latest upgraded progress came from.

        Its loss stops the task that reports the input, so the job must restore
        at least that task's region; another TaskManager may hold no task, and
        losing it would restore nothing.
        """
        progress = self.fresh_progress("upgrade")
        if not progress:
            return None
        latest = max(progress, key=lambda s: s["at"])
        for pod in self.running(pods, "taskmanager"):
            if pod["metadata"]["uid"] == latest["pod_uid"]:
                return pod
        return None

    def delete_pod(self, pod, role):
        """Delete one planned Pod once; a lost response must show it going."""
        try:
            if self.env.kube.delete(pod) is False:
                raise Failure(role + " disappeared before its planned deletion")
        except (ApiError, TransportError) as error:
            if isinstance(error, ApiError) and error.status not in (500, 502, 503, 504):
                raise
            current = self.env.kube.get("Pod", self.namespace, pod["metadata"]["name"])
            if (
                current
                and current["metadata"]["uid"] == pod["metadata"]["uid"]
                and not current["metadata"].get("deletionTimestamp")
            ):
                raise Failure(
                    role + " delete response lost without a verified outcome"
                ) from error

    def attach_rest(self, service, job_id):
        """Hand over the Service the loop verified, for exercises that read more.

        The loop resolves it and proves its workload ownership once per poll;
        an exercise that resolved its own could disagree with that proof.
        """

    def observe(self, app, rest, pods):
        self.check_open()
        if not self.initialized:
            self.persist("baseline")
        if app["metadata"].get("deletionTimestamp"):
            raise Failure("Application deletion started during the exercise")
        status = app.get("status", {}).get("jobStatus", {}).get("state", "")
        if status in ("FAILED", "FAILING") or (
            status in ("CANCELED", "SUSPENDED") and self.stage != "upgrade"
        ):
            raise Failure("Unexpected Flink state: " + status)
        if app.get("status", {}).get("reconciliationStatus", {}).get("state") in (
            "ROLLING_BACK",
            "ROLLED_BACK",
        ):
            raise Failure("Operator rolled back the approved upgrade")
        if self.stage == "baseline" and status == "RUNNING":
            progress = self.fresh_progress("initial")
            cp = (
                self.completed(rest, min(s["at"] for s in progress))
                if progress
                else None
            )
            if cp:
                self.begin_upgrade(app, cp, progress, pods)
        elif self.recovering and status == "RUNNING":
            if self.stage == "upgrade" and not contains(
                app["spec"], self.upgrade["spec"]
            ):
                raise Failure("Application differs from the approved upgrade")
            proof = self.recovery_proof(app, rest, pods)
            if proof and self.stage == "upgrade":
                if self.timing.get("post_upgrade_seconds"):
                    self.hold_upgrade(proof, pods)
                else:
                    self.begin_failover(app, proof, pods)
            elif proof:
                self.outcomes["failover"] = proof
                self.deadline = self.env.schedule.cleanup_at
                self.persist("finishing")
                self.rebaseline(pods)
        elif (
            self.stage == "upgraded"
            and status == "RUNNING"
            and self.env.clock() >= self.upgraded_until
        ):
            self.failover_floor = self.completed(rest, self.upgraded_at)
            if self.failover_floor:
                self.begin_failover(app, self.outcomes["upgrade"], pods)
        if status == "FINISHED":
            # Operator 1.15.0 marks the old job FINISHED after stop-with-savepoint
            # and assigns the upgraded job's ID before that state clears, so
            # FINISHED under either ID is the transition. An upgraded job that
            # really finished never proves recovery, and the stage deadline
            # stops the run.
            if self.stage == "upgrade":
                return False
            if self.stage != "finishing" or not self.input_complete():
                raise Failure(
                    "Finished without both recovery proofs and the complete input"
                )
            self.persist("complete", processed=self.records)
            return True
        return False

    def tolerate_rest(self, error):
        self.check_open()
        return self.recovering and (
            isinstance(error, TransportError)
            or isinstance(error, ApiError)
            and error.status in (404, 502, 503, 504)
        )
