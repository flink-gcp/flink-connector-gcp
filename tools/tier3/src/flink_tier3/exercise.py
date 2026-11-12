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
one TaskManager, as the approved trial names them. A scenario that samples
the job also holds three measurement windows, which its timing sizes: a
baseline before the upgrade, a window after the upgrade before the
disruption, and one after the disruption.
"""

from __future__ import annotations

import copy
import re
import uuid
from dataclasses import dataclass

from .common import (
    UNPLANNED_EVENT,
    ApiError,
    Failure,
    TransportError,
    contains,
    timestamp,
    transient,
    utc,
)
from .metrics import FlinkRest

# Why a run stops when the platform preempts a workload Pod or loses its node,
# and when a Pod is replaced outside a planned recovery.
INTERRUPTED = "Unplanned workload interruption; trial is inconclusive"
REPLACED = "Unplanned workload replacement; trial is inconclusive"
# The stops of a run already admitted that a campaign repeats: only the
# platform's own. A replacement is not one of them, because a TaskManager
# whose JVM died is replaced too, its Pod never restarting a container.
PLATFORM_STOPS = (INTERRUPTED,)
# The `DisruptionTarget` reasons only the platform sets: the scheduler's
# preemption, a node's taint or loss, and an eviction through the API, as a
# node drain or upgrade makes.
PLATFORM_DISRUPTIONS = frozenset(
    {
        "PreemptionByScheduler",
        "DeletionByTaintManager",
        "DeletionByPodGC",
        "EvictionByEvictionAPI",
    }
)
# The message of the kubelet's `Shutdown` event for a shutdown it carries out.
SHUTDOWN_MESSAGE = "Shutdown manager detected shutdown event"
# How long before a node's `Shutdown` the cluster autoscaler's `ScaleDown` on it
# shows that it removed the node rather than the platform reclaiming it: 3 to 6
# seconds before every scale-down shutdown of the nodes from 2026-10-19 to
# 2026-10-22, and none before the reclamation in bt1855-ri-a2.
SCALE_DOWN_LEAD_SECONDS = 600
# The Pod the second recovery deletes, by the name the approved trial gives it,
# and the role its deletion messages name.
FAILOVERS = {"jobmanager": "JM", "taskmanager": "TM"}


def bound_at(pod):
    """When the scheduler bound a Pod to its node, or its creation if not reported."""
    for condition in pod.get("status", {}).get("conditions", []):
        if (
            condition.get("type") == "PodScheduled"
            and condition.get("status") == "True"
            and condition.get("lastTransitionTime")
        ):
            return timestamp(condition["lastTransitionTime"])
    return timestamp(pod["metadata"]["creationTimestamp"])


@dataclass(frozen=True)
class Fault:
    """One fault kind: the Pod it deletes, its rescale and its writers' sizes.

    ``initial`` and ``upgrade`` are the sink or writers' parallelism of each
    phase, one subtask per single-slot TaskManager. A rescale moves it between
    one and two, inside the three-Pod quota of the BigQuery, Cloud Tasks,
    Bigtable, Spanner and Firestore namespaces, and every other kind keeps two.
    """

    failover: str
    rescale: str | None
    initial: int
    upgrade: int

    def parallelism(self, phase):
        """The writers' parallelism, and TaskManager count, of a phase."""
        if phase not in ("initial", "upgrade"):
            raise ValueError("Unknown trial phase")
        return self.initial if phase == "initial" else self.upgrade


# The fault kinds every recovery scenario names its trials by: in approvals,
# dispatch, the lifecycle delivery's sizes and the analysis report. A BigQuery
# or Cloud Tasks trial runs the savepoint upgrade and then deletes the
# ``failover`` Pod, and a rescale kind makes that upgrade the rescale; a
# Pub/Sub trial runs only the kind's fault, deleting the Pod of a replacement
# or making a rescale its one upgrade. `kubernetes/lifecycle/faults.cue`
# renders the same sizes, and a test holds it to this table.
FAULTS = {
    "jm-replacement": Fault("jobmanager", None, 2, 2),
    "tm-replacement": Fault("taskmanager", None, 2, 2),
    "rescale-out": Fault("jobmanager", "out", 1, 2),
    "rescale-in": Fault("jobmanager", "in", 2, 1),
}
# The kind of a trial that names none. A BigQuery or Cloud Tasks trial of it
# runs the upgrade and then the JobManager failover, as those scenarios did
# before they had fault kinds.
DEFAULT_FAULT = "jm-replacement"
# The kinds that replace a Pod, by the Pod each deletes, and the rescales by
# the direction each moves the checkpointed subtasks in.
REPLACEMENT_KINDS = {
    fault.failover: kind for kind, fault in FAULTS.items() if fault.rescale is None
}
RESCALE_KINDS = {
    fault.rescale: kind for kind, fault in FAULTS.items() if fault.rescale is not None
}
# The timing a scenario that samples the job sizes its windows with.
WINDOW_TIMING = {
    "warmup_seconds",
    "baseline_seconds",
    "upgrade_wait_seconds",
    "post_upgrade_seconds",
    "post_recovery_seconds",
    "measure_seconds",
}
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

    A scenario that samples the job names the event each sample emits in
    ``measurement_event`` and the one its baseline window's span emits in
    ``baseline_event``, supplies its observer through ``discover`` and
    ``read``, its coverage fold as ``summarize`` and its input check as
    ``active_input``, and its timing names every key in ``WINDOW_TIMING``,
    which sizes the windows: the upgrade waits for ``warmup_seconds`` and
    ``baseline_seconds`` of steady input, and for a checkpoint triggered
    after them within ``upgrade_wait_seconds``; and the input must outlast
    ``post_recovery_seconds`` after the failover's proof. A reading counts
    toward the window it was taken in, and one taken during startup, a
    recovery or the rest of the input toward none.
    """

    namespace: str
    state_bucket: str
    timing: dict
    progress_event: str
    expected_pods: int
    records: int
    # A scenario that samples the job names these; one that does not keeps
    # its own measurement or none.
    measurement_event = None
    baseline_event = None
    # The failover regions a TaskManager's loss restarts, each restoring the
    # job's checkpoint once: one for a connected graph.
    failover_regions = 1
    # Whether `waits_out` may wait out a platform interruption after the last
    # window; only Firestore's opts in (#2140).
    waits_out_interruptions = False
    # Whether a progress line's restored flag must match its phase: an initial
    # line from a restored job is an unplanned restore. The fault sequence
    # waits restores out, so it does not check (ADR-0204).
    checks_restores = True

    def __init__(self, env, upgrade, *, failover="jobmanager", rescale=None):
        if self.measurement_event and (
            not self.baseline_event or not WINDOW_TIMING <= set(self.timing)
        ):
            raise Failure("A sampling exercise requires its windows' events and timing")
        if upgrade is None:
            raise Failure("Recovery requires the approved upgrade manifest")
        if not any(
            (failover, rescale) == (fault.failover, fault.rescale)
            for fault in FAULTS.values()
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
        # Each workload Pod's node and when it was bound there, kept after the
        # Pod is gone.
        self.placements = {}
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
        self.vertices = None
        self.baseline_until, self.post_until = None, None
        # The latest restore a planned recovery accounted for: its time, the
        # JobMaster's count, the stage whose recovery it was and the count the
        # recovery started from.
        self.last_restore = None

    @property
    def recovering(self):
        return self.stage in ("upgrade", "failover")

    def waits_out(self):
        """Whether an unplanned interruption now is waited out, not a stop.

        Only once both planned recoveries are proved and the last window has
        closed, with one measurement interval of margin for a poll that sees
        a cause late. A cause first seen later than that is waited out even
        if it began inside the window.
        """
        return (
            self.waits_out_interruptions
            and self.stage == "finishing"
            and self.post_until is not None
            and self.env.clock() >= self.post_until + self.timing["measure_seconds"]
        )

    def rest_may_vanish(self):
        """Whether the job's REST endpoint may be missing now without a stop.

        After the windows, only once a platform interruption is recorded: an
        endpoint lost without one could hide a restore the application made.
        """
        return self.recovering or (
            self.waits_out() and bool(self.outcomes.get("unplanned"))
        )

    def unplanned(self, message, platform=False):
        """Stop the run with `message`, unless the exercise waits it out.

        Only the platform's own interruption is waited out, and what follows
        it: a replacement, eviction, container restart or restore with no
        such interruption before it may be the application's failure, so it
        still stops the run. Each cause waited out is recorded once, in the
        evidence and in the record's outcomes.
        """
        causes = self.outcomes.get("unplanned", [])
        if not self.waits_out() or not (platform or causes):
            raise Failure(message)
        cause = message.split(";")[0]
        if cause not in causes:
            self.outcomes["unplanned"] = [*causes, cause]
            self.env.emit(UNPLANNED_EVENT, {"stage": self.stage, "cause": cause})

    def keeps_jobmaster(self):
        """Whether the failover under way is a TaskManager replacement.

        Only that recovery keeps the JobMaster, which counts its restore on
        from the upgrade's. A Pub/Sub trial has no ``failover`` stage, and its
        one recovery follows no restore, so its count is one whatever it
        replaces.
        """
        return self.stage == "failover" and self.failover == "taskmanager"

    def restarts(self, rest):
        """Stop the run on a restore that no planned recovery accounts for.

        Every restart that restores a checkpoint advances the restore the
        job's checkpoint statistics report, and each JobMaster counts its own
        restores. A planned recovery restores once: a new JobMaster's count is
        then one. A TaskManager replacement keeps the JobMaster, and Flink
        restarts each failover region the lost TaskManager held, so it adds
        one restore per region, `failover_regions`, which polls may see
        together or in turn. A restore outside a recovery, one more inside a
        recovery, or a count that shows a restore no poll saw is a restart the
        exercise did not plan, and its windows' readings or its proof's
        restore are no longer the planned fault's. Called with each poll's
        checkpoint statistics
        before the exercise observes the poll, and again just before a fault
        that replaces the JobMaster. A restart before the first completed
        checkpoint restores nothing and is not seen here.
        """
        restored = (rest.get("latest") or {}).get("restored")
        if not restored:
            return
        at = restored.get("restore_timestamp")
        if type(at) is not int:
            raise Failure("Restored checkpoint has no restore time")
        last = self.last_restore
        if last and at <= last["at"]:
            return
        count = (rest.get("counts") or {}).get("restored")
        if type(count) is not int:
            raise Failure("Checkpoint statistics have no restore count")
        first = self.recovering and not (last and last["stage"] == self.stage)
        if self.keeps_jobmaster():
            # The JobMaster's count before the replacement, which its own
            # restores raise by up to one per region.
            base = (last["count"] if last else 0) if first else last["base"]
            expected = base + self.failover_regions
            planned = (last["count"] if last else 0) < count <= expected
        else:
            base = 0
            # None where no restore is planned: outside a recovery, or after
            # the recovery's own.
            expected = 1 if first else None
            planned = count == expected
        if not planned:
            self.env.emit(
                "unplanned-restore",
                {
                    "stage": self.stage,
                    "restored": restored,
                    "count": count,
                    "expected": expected,
                },
            )
            self.unplanned("Unplanned job restart; trial is inconclusive")
        self.last_restore = {
            "at": at,
            "count": count,
            "stage": self.stage,
            "base": base,
        }

    def restarts_before_fault(self):
        """Read the restores once more just before the JobMaster is replaced.

        The new JobMaster counts its restores from nothing, so a restart
        between the poll's read and the fault would leave no count behind;
        this narrows that gap to the request. An exercise without a REST
        reader, or a read the service answers with a transient error, has
        only the poll's read: the fault goes ahead, as it did before the read
        was added, rather than lose the run.
        """
        if self.rest is None:
            return
        try:
            rest = self.rest.job("/checkpoints")
        except (ApiError, TransportError) as error:
            if not transient(error):
                raise
            self.env.emit("recovery-rest-unavailable", {"cause": str(error)})
            return
        self.restarts(rest)

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
        if stage == "finishing" and self.measurement_event:
            self.post_until = self.env.clock() + self.timing["post_recovery_seconds"]
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
            if self.checks_restores and (
                (phase == "initial" and restored != "false")
                or (phase == "upgrade" and restored != "true")
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
        # A Pod the platform took for another or lost with its node, an
        # evicted or disrupted one, which may be its own limits' doing, and a
        # restarted container, which may be the application's own failure.
        interrupted = evicted = restarted = False
        for event in self.env.kube.items("Event", self.namespace):
            if event.get("involvedObject", {}).get("uid") not in owned:
                continue
            meta = event["metadata"]
            if self.events.get(meta["uid"]) == meta["resourceVersion"]:
                continue
            self.events[meta["uid"]] = meta["resourceVersion"]
            new_events.append(event)
            if event.get("reason") in ("Preempted", "Shutdown", "NodeNotReady"):
                interrupted = True
            evicted |= event.get("reason") == "Evicted"
        if new_events:
            self.env.emit("scheduling-events", new_events)
        smoke = [p for p in pods if p["metadata"]["namespace"] == self.namespace]
        for pod in smoke:
            node = pod.get("spec", {}).get("nodeName")
            if node:
                self.placements[pod["metadata"]["uid"]] = (node, bound_at(pod))
        for pod in smoke:
            status = pod.get("status", {})
            disruptions = {
                c.get("reason")
                for c in status.get("conditions", [])
                if c.get("type") == "DisruptionTarget" and c.get("status") == "True"
            }
            # The kubelet's graceful node shutdown sets `Terminated` (once
            # `Shutdown`), and a resource-pressure eviction `Evicted`; both set
            # the same `TerminationByKubelet` disruption, so it decides nothing.
            interrupted |= status.get("reason") in (
                "Shutdown",
                "Terminated",
                "NodeLost",
            )
            interrupted |= bool(disruptions & PLATFORM_DISRUPTIONS)
            evicted |= status.get("reason") == "Evicted" or bool(disruptions)
            restarted |= any(
                c.get("restartCount", 0) for c in status.get("containerStatuses", [])
            )
        if interrupted:
            self.unplanned(INTERRUPTED, platform=True)
        if evicted:
            self.unplanned("Unplanned workload eviction; trial is inconclusive")
        if restarted:
            self.unplanned("Unplanned container restart; trial is inconclusive")
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
            if self.reclaimed(self.stable_pods - identities):
                self.unplanned(INTERRUPTED, platform=True)
            else:
                self.unplanned(REPLACED)

    def reclaimed(self, lost):
        """Whether a lost Pod's node shut down gracefully after the Pod was bound there.

        A Spot reclamation leaves nothing on the Pod to read: the kubelet stops
        it, and Flink deletes the TaskManager's Pod and asks for another within a
        second, before the next poll. The node's own `Shutdown` event is what is
        left. The autoscaler's removal of an empty node records the same event,
        just after its own `ScaleDown` on the node, so a shutdown that follows one
        is not counted; nor is one recorded before the Pod was bound, an earlier
        life of the same node name, or the kubelet's cancellation of a shutdown,
        which carries the same reason. It is read only for a Pod lost outside a
        planned recovery.
        """
        placed = [self.placements[uid] for uid in lost if uid in self.placements]
        if not placed:
            return False
        # The shutdowns first, so that the scale-down read sees every mark that
        # preceded one of them.
        candidates = [
            event
            for event in self.env.kube.node_events("Shutdown")
            if event.get("message") == SHUTDOWN_MESSAGE and event.get("lastTimestamp")
        ]
        if not candidates:
            return False
        scale_downs = [
            (event["involvedObject"]["name"], timestamp(event["lastTimestamp"]))
            for event in self.env.kube.node_events("ScaleDown")
            if event.get("lastTimestamp")
        ]
        shutdowns = []
        for event in candidates:
            node = event["involvedObject"]["name"]
            at = timestamp(event["lastTimestamp"])
            # Any recent mark on the node vetoes, even one from before the
            # binding, which an in-flight binding can follow: a removal read as a
            # reclamation would repeat a failure that may be the application's.
            if any(
                node == removed and 0 <= at - marked <= SCALE_DOWN_LEAD_SECONDS
                for removed, marked in scale_downs
            ):
                continue
            if any(node == placed_on and at >= bound for placed_on, bound in placed):
                shutdowns.append(event)
        if shutdowns:
            self.env.emit("scheduling-events", shutdowns)
        return bool(shutdowns)

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
        replaced = self.keeps_jobmaster()
        if replaced:
            # Only the TaskManager was approved to go; the JobManager keeps the
            # job, and only the regions on the lost TaskManager may restart.
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
        if jm is None or not self.advance_baseline(cp):
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
        self.restarts_before_fault()
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
            # A stop or the deadline reached while a repeat waits ends it.
            self.env.kube.patch(app, operations, before_repeat=self.check_open)
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

    def active_input(self, phase):
        """Whether the phase's input is still flowing; a scenario reads its own."""
        return True

    def advance_baseline(self, cp):
        """Open the baseline window when it can, and say whether it has passed.

        The window opens, with its deadline and its evidence, on the first
        poll that sees the initial input flowing on the whole set of Pods, and
        the upgrade waits for a checkpoint triggered after it closes, so the
        baseline is a steady state. A scenario that samples nothing has no
        baseline.
        """
        if not self.measurement_event:
            return True
        if not self.active_input("initial") or self.stable_pods is None:
            return False
        if self.baseline_until is None:
            self.baseline_until = (
                self.env.clock()
                + self.timing["warmup_seconds"]
                + self.timing["baseline_seconds"]
            )
            self.deadline = min(
                self.baseline_until + self.timing["upgrade_wait_seconds"],
                self.env.schedule.cleanup_at,
            )
            self.env.records.evidence(
                self.baseline_event,
                {
                    "started_at": utc(self.env.clock()),
                    "until": utc(self.baseline_until),
                },
            )
        return (
            self.env.clock() >= self.baseline_until
            and cp["trigger_timestamp"] / 1000 >= self.baseline_until
        )

    def window(self):
        """The measurement window the exercise is in now, or None outside one.

        The baseline starts after its warm-up and lasts until the upgrade
        begins; the window after the upgrade is its held stage; the one after
        the failover ends ``post_recovery_seconds`` after its proof.
        """
        now = self.env.clock()
        if self.stage == "baseline" and self.baseline_until is not None:
            if now >= self.baseline_until - self.timing["baseline_seconds"]:
                return "baseline"
        elif self.stage == "upgraded":
            return "upgraded"
        elif (
            self.stage == "finishing"
            and self.post_until is not None
            and now < self.post_until
        ):
            return "finishing"
        return None

    def discover(self, rest, known):
        """The scenario's vertex discovery, keeping what ``known`` found."""
        raise NotImplementedError

    def read(self, rest, state):
        """One sample of the scenario's families, each reading or its absence."""
        raise NotImplementedError

    def summarize(self, coverage, stage, reading):
        """The scenario's coverage with one more labelled reading in it."""
        raise NotImplementedError

    def measure(self):
        """Sample the job once a measurement interval, labelled with its window.

        Every sample is exported with the stage it was taken in; only one
        inside a window counts toward that window's coverage. Discovery runs
        at every sample and keeps the metric ids found before: a connector
        metric's id carries its operator name, so it cannot be predicted.
        """
        if not self.measurement_due():
            return
        self.vertices = self.discover(self.rest, self.vertices)
        reading = {
            "stage": self.stage,
            "window": self.window(),
            **self.read(self.rest, self.vertices),
        }
        self.coverage = self.summarize(self.coverage, self.stage, reading)
        self.env.emit(self.measurement_event, reading)

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
        if self.failover == "jobmanager":
            self.restarts_before_fault()
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

    def delete_pod(self, pod, role, force=False):
        """Delete one planned Pod once; a lost response must show it going.

        ``force`` deletes it without a grace period.
        """
        try:
            if self.env.kube.delete(pod, force=force) is False:
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
        an exercise that resolved its own could disagree with that proof. A
        new job, as after the upgrade, starts its discovery again.
        """
        if self.measurement_event and self.track_rest(service, job_id):
            self.vertices = None

    def observe(self, app, rest, pods):
        if self.measurement_event:
            self.measure()
        state = app.get("status", {}).get("jobStatus", {}).get("state")
        if (
            self.stage == "finishing"
            and state == "FINISHED"
            and self.post_until is not None
        ):
            completed = [
                sample["at"]
                for sample in self.fresh_progress("upgrade")
                if sample["processed"] == self.records
            ]
            if not completed or min(completed) < self.post_until:
                raise Failure("Input finished before the post-recovery window")
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
            # Operator 1.15.0 marked the old job FINISHED after stop-with-savepoint
            # and assigned the upgraded job's ID before that state cleared, so
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
        return self.rest_may_vanish() and (
            isinstance(error, TransportError)
            or isinstance(error, ApiError)
            and error.status in (404, 502, 503, 504)
        )


# The stage that completes a run, which carries its verdict.
COMPLETE_STAGE = "complete"
# The wait before repeating an oracle's read that failed transiently.
RETRY_SECONDS = 5
# What an oracle's read window leaves before the cleanup start, for writing the
# record's transition, which the cleanup start refuses.
RECORD_SECONDS = 60


class FiniteInputExercise(RecoveryExercise):
    """A recovery exercise over one finite, checkpointed input lineage.

    A subclass names `service` for its messages and `oracle_stage`, the stage
    a finished application waits in until the scenario's oracle has passed. It
    sets its own attributes before calling this constructor with the trial.
    The initial phase runs the JobManager and one single-slot TaskManager per
    subtask the trial's `parallelism` names.
    """

    service: str
    oracle_stage: str
    # Whether the first progress line's lineage is recorded and every later
    # one must match it; the fault sequence fixes it at its first fault.
    records_lineage = True

    def __init__(self, env, upgrade, trial):
        self.trial = trial
        self.records = trial.records
        self.expected_pods = 1 + trial.parallelism("initial")
        super().__init__(env, upgrade, failover=trial.failover, rescale=trial.rescale)
        self.deadline = min(
            env.schedule.started + self.timing["input_seconds"],
            env.schedule.cleanup_at,
        )

    def progress(self, pod, text):
        super().progress(pod, text)
        lineage = self.env.refresh().lineage
        for sample in self.samples.values():
            try:
                valid = str(uuid.UUID(sample["lineage"])) == sample["lineage"]
            except ValueError:
                valid = False
            if sample["at"] > self.env.clock():
                raise Failure(f"{self.service} progress is dated in the future")
            if not valid:
                raise Failure(f"{self.service} input lineage changed or is invalid")
            if not self.records_lineage:
                continue
            if lineage is not None and sample["lineage"] != lineage:
                raise Failure(f"{self.service} input lineage changed or is invalid")
            if lineage is None:
                self.env.records.record_lineage(sample["lineage"])
                lineage = sample["lineage"]

    def active_input(self, phase):
        """Whether the phase's input is still flowing, so a fault lands mid-input."""
        samples = self.fresh_progress(phase)
        if not samples:
            return False
        latest = max(samples, key=lambda sample: sample["at"])
        if latest["at"] > self.env.clock():
            raise Failure(f"{self.service} progress is dated in the future")
        return (
            latest["processed"] < self.records and self.env.clock() - latest["at"] <= 60
        )

    def begin_failover(self, app, proof, pods):
        if self.active_input("upgrade"):
            super().begin_failover(app, proof, pods)

    def persist(self, stage, **details):
        if stage == COMPLETE_STAGE:
            stage = self.oracle_stage
        super().persist(stage, **details)

    def oracle_window(self):
        """When the oracle's reads must end; the transition keeps the cleanup start."""
        until = min(
            self.env.clock() + self.timing["visibility_seconds"],
            self.env.schedule.cleanup_at - RECORD_SECONDS,
        )
        self.deadline = self.env.schedule.cleanup_at
        return until

    def retried(self, call, until, closed=None):
        """One oracle read, repeated while it fails without saying anything.

        Before `until`, a non-transient failure says something about the read
        and is raised. Once it has passed, any failure is raised, as `closed`
        when the caller names the window's closing apart.
        """
        while True:
            try:
                return call()
            except Failure as error:
                if self.env.clock() >= until:
                    if closed is None:
                        raise
                    raise closed(str(error)) from error
                if not transient(error):
                    raise
            self.env.sleep(RETRY_SECONDS)

    def complete(self, verdict):
        """Write the completing transition with the verdict decided over it.

        `verdict` decides from the record the transition is about to write,
        which the offline analyzer recomputes once the evidence is exported.
        """
        coverage = self._coverage()
        decided = verdict(
            {"stage": COMPLETE_STAGE, "outcomes": self.outcomes, "coverage": coverage}
        )
        # The stage the oracle waited in is complete only now.
        RecoveryExercise.persist(
            self, COMPLETE_STAGE, processed=self.records, coverage=coverage, **decided
        )


# The faults one run injects under ADR-0204, in order, and the parallelism the
# job runs at once each has been injected: the run starts at two, as a
# JobManager replacement trial does, and the two rescales move it to one and
# back.
SEQUENCE = ("upgrade", "jobmanager", "taskmanager", "rescale-in", "rescale-out")
SEQUENCE_PARALLELISM = {
    "upgrade": 2,
    "jobmanager": 2,
    "taskmanager": 2,
    "rescale-in": 1,
    "rescale-out": 2,
}
# The faults that change the application, which the Operator applies as a
# savepoint upgrade.
SAVEPOINT_FAULTS = ("upgrade", "rescale-in", "rescale-out")
# How long a run may take from its admission to its verdict (ADR-0204).
TIME_LIMIT_SECONDS = 3600
# The input finished while a fault was still to come or awaited, so the job
# could not be seen running again after it.
UNCONFIRMED = "Input finished before the fault sequence did; unconfirmed"


class FaultSequence:
    """One run that injects every fault kind and waits out everything else.

    Mixed in ahead of a scenario's `FiniteInputExercise` (ADR-0204). The run
    starts at parallelism 2, and once a completed checkpoint follows flowing
    input, and the scenario's own preparation is done, it injects each fault
    of `SEQUENCE` in turn: the savepoint upgrade, the JobManager's deletion,
    the deletion of the TaskManager reporting the input, and a rescale to one
    and back to two, each a patch of the approved upgrade manifest that
    changes only the parallelism.

    Before each fault, and before completion, it waits until the job runs
    again: `RUNNING`, reconciled to the last patch, with a JobManager and as
    many TaskManagers as the parallelism, and with progress logged after a
    restore that followed the fault. A line logged before that restore, by
    the job the fault stopped, is not the job running again.

    Anything else is recorded and waited out, however often it happens: an
    interruption, a replacement, an eviction, a container restart, a restore,
    a patch that conflicted, a Pod already gone, or a REST or API failure.
    Nothing judges restores, proves a recovery, samples metrics or opens a
    window; the base class's stage-machine state is unused. The run ends when
    the job has finished with its whole input after the last fault, and the
    scenario's `verdict` decides by the data; it stops abnormally when the job
    fails, the application is deleted or rolled back, or the time limit
    passes, and without a verdict when the input finishes first.
    """

    measurement_event = None
    checks_restores = False
    records_lineage = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.deadline = min(
            self.env.schedule.started + TIME_LIMIT_SECONDS,
            self.env.schedule.cleanup_at,
        )
        self.parallelism = SEQUENCE_PARALLELISM["upgrade"]
        self.step = 0
        # The fault the job is recovering from: its kind and when it was
        # injected, and for a patch the spec and the generation it waits for.
        self.awaiting = None
        # The latest restore the job's statistics showed, in seconds.
        self.restored_at = None
        # The input lineage the faults run on, fixed by the first fault.
        self.lineage = None

    @property
    def recovering(self):
        return self.awaiting is not None

    def rest_may_vanish(self):
        return True

    def check_open(self):
        """The base check, worded for the time limit that bounds the deadline."""
        self.env.require_running("Recovery exercise has been stopped")
        if self.env.clock() >= self.deadline:
            raise Failure("Run reached its time limit at stage " + self.stage)

    def unplanned(self, message, platform=False):
        """Record one cause the run waits out; nothing is a stop."""
        cause = message.split(";")[0]
        causes = self.outcomes.get("unplanned", [])
        if cause not in causes:
            self.outcomes["unplanned"] = [*causes, cause]
            self.env.emit(UNPLANNED_EVENT, {"stage": self.stage, "cause": cause})

    def restarts(self, rest):
        """Record each restore the job's statistics show; none is judged."""
        restored = (rest.get("latest") or {}).get("restored")
        at = restored.get("restore_timestamp") if restored else None
        if type(at) is not int or (
            self.restored_at is not None and at / 1000 <= self.restored_at
        ):
            return
        self.restored_at = at / 1000
        self.env.emit(
            "job-restore",
            {
                "stage": self.stage,
                "restored": restored,
                "count": (rest.get("counts") or {}).get("restored"),
            },
        )

    def scheduling(self, pods):
        """Record what the platform did, then take the Pods as they now are.

        Pods only going away, as an idle TaskManager does after a rescale in
        or once the input is done, or only arriving, as at startup, are not a
        replacement; the platform's own interruptions are recorded from their
        events and conditions either way.
        """
        if self.awaiting is None and self.stable_pods is not None:
            current = {
                p["metadata"]["uid"]
                for p in pods
                if p["metadata"]["namespace"] == self.namespace
            } - self.retiring_pods
            if current <= self.stable_pods or self.stable_pods <= current:
                self.rebaseline(pods)
        super().scheduling(pods)
        if self.awaiting is None:
            self.rebaseline(pods)

    def progress(self, pod, text):
        """Read progress; before the first fault a fresh start may replace the lineage.

        A job restarted before its first completed checkpoint starts its input
        again under a new lineage, which writes the same records; once the
        first fault lands, the lineage is fixed.
        """
        super().progress(pod, text)
        if self.lineage is None:
            if not self.samples:
                return
            latest = max(self.samples.values(), key=lambda s: s["at"])["lineage"]
            stale = [k for k, s in self.samples.items() if s["lineage"] != latest]
            if stale:
                self.env.emit("fresh-start", {"lineage": latest})
            for key in stale:
                del self.samples[key]
        elif any(s["lineage"] != self.lineage for s in self.samples.values()):
            raise Failure("Input lineage changed after the first fault")

    def input_complete(self):
        """Whether the latest progress read shows the whole input.

        A restore may go back to a checkpoint before a completion the stopped
        attempt logged, and the replay then logs a lower count after it. A
        restore that does not replay the input (another region's, or a
        finished input's) logs nothing, so the completion stands.
        """
        if not self.samples:
            return False
        latest = max(self.samples.values(), key=lambda s: (s["at"], s["processed"]))
        return latest["processed"] == self.records

    def ready_for_sequence(self):
        """Whether a scenario's own preparation lets the first fault go ahead."""
        return True

    def oracle_window(self):
        """The oracle's reads end within the time limit, before the record's write."""
        return min(
            self.env.clock() + self.timing["visibility_seconds"],
            self.deadline - RECORD_SECONDS,
        )

    def observe(self, app, rest, pods):
        self.check_open()
        if not self.initialized:
            self.persist("baseline")
        if app["metadata"].get("deletionTimestamp"):
            raise Failure("Application deletion started during the exercise")
        status = app.get("status", {}).get("jobStatus", {}).get("state", "")
        savepoint = self.awaiting and self.awaiting["kind"] in SAVEPOINT_FAULTS
        if status == "FAILED" or (
            status in ("CANCELED", "SUSPENDED") and not savepoint
        ):
            raise Failure("Unexpected Flink state: " + status)
        if app.get("status", {}).get("reconciliationStatus", {}).get("state") in (
            "ROLLING_BACK",
            "ROLLED_BACK",
        ):
            raise Failure("Operator rolled back the approved upgrade")
        complete = self.input_complete()
        if savepoint and self.awaiting["generation"] is None:
            # A patch not yet applied, so no upgrade is in flight and the
            # state is the running job's own.
            if status == "FINISHED" and complete:
                raise Failure(UNCONFIRMED)
            if status == "RUNNING" and not complete:
                self.patch(app)
            return False
        if self.awaiting is not None and (
            self.running_again(app, status, pods)
            # The input ran out after the restore; idle TaskManagers may go.
            or (complete and self.reconciled(app) and self.ran_after_restore())
        ):
            self.recovered(pods)
        if status == "FINISHED":
            # The stopped job's FINISHED may still be reported while the next
            # one starts (ADR-0165), and a restore may replay the input before
            # it logs again: only a finished current job with the whole input
            # read settles the run.
            if not (complete and self.finished()):
                return False
            if self.awaiting is not None or self.step < len(SEQUENCE):
                # No progress can follow, so the job could never be seen
                # running again after the remaining faults.
                raise Failure(UNCONFIRMED)
            self.persist(COMPLETE_STAGE, processed=self.records)
            return True
        if (
            status == "RUNNING"
            and self.awaiting is None
            and self.step < len(SEQUENCE)
            and not complete
            and self.ready(rest, pods)
        ):
            self.inject(SEQUENCE[self.step], app, pods)
        return False

    def ready(self, rest, pods):
        """Whether the next fault may land: the job runs with input flowing."""
        if self.jobmanager(pods) is None:
            return False
        if self.step == 0:
            progress = self.fresh_progress("initial")
            if not progress or not self.completed(rest, min(s["at"] for s in progress)):
                return False
            return self.ready_for_sequence()
        return True

    def attach_rest(self, service, job_id):
        """Keep a REST reader for the job the loop verified, measured or not."""
        if self.track_rest(service, job_id) and self.measurement_event:
            self.vertices = None

    def finished(self):
        """Whether the job the Operator names has finished, by its own REST status.

        Only a savepoint fault's upgrade leaves the stopped job's FINISHED in
        the Operator's status, also once the replacement was seen running
        again; after one, the replacement's own status decides, and a read
        that fails is waited out.
        """
        faults = self.outcomes.get("faults") or [{}]
        if faults[-1].get("kind") not in SAVEPOINT_FAULTS:
            return True
        if self.rest is None:
            return False
        try:
            return self.rest.job("").get("state") == "FINISHED"
        except (ApiError, TransportError):
            return False

    def ran_after_restore(self):
        """Whether the job restored after the awaited fault and then logged progress."""
        at = self.restored_at
        return (
            at is not None
            and at > self.awaiting["at"]
            and any(
                s["phase"] == "upgrade" and s["at"] > at for s in self.samples.values()
            )
        )

    def reconciled(self, app):
        """Whether a patch the job awaits is what the Operator has deployed."""
        awaited = self.awaiting
        if awaited["kind"] not in SAVEPOINT_FAULTS:
            return True
        status = app.get("status", {})
        return (
            status.get("observedGeneration", 0) >= awaited["generation"]
            and status.get("reconciliationStatus", {}).get("state") == "DEPLOYED"
        )

    def running_again(self, app, status, pods):
        """Whether the job runs again after the awaited fault, whatever restarted it."""
        return (
            status == "RUNNING"
            and self.reconciled(app)
            and self.jobmanager(pods) is not None
            and len(self.running(pods, "taskmanager")) >= self.parallelism
            and self.ran_after_restore()
        )

    def recovered(self, pods):
        """Record that the job runs again, and take its Pods as the stable set."""
        fault = self.awaiting
        self.outcomes["faults"][-1]["running_again_at"] = utc(self.env.clock())
        self.awaiting = None
        self.rebaseline(pods)
        self.persist("recovered-" + fault["kind"])

    def inject(self, kind, app, pods):
        """Inject one fault of the sequence and wait for the job to run again."""
        target = None
        if kind == "jobmanager":
            target = self.jobmanager(pods)
        elif kind == "taskmanager":
            target = self.taskmanager(pods)
            if target is None:
                return
        fault = {"kind": kind, "at": utc(self.env.clock())}
        if target is not None:
            fault["pod"] = {
                "name": target["metadata"]["name"],
                "uid": target["metadata"]["uid"],
            }
        awaiting = {"kind": kind, "at": self.env.clock()}
        if kind in SAVEPOINT_FAULTS:
            awaiting.update(
                spec=self.sequence_spec(SEQUENCE_PARALLELISM[kind]), generation=None
            )
        if self.lineage is None:
            latest = max(self.samples.values(), key=lambda s: s["at"])
            self.lineage = latest["lineage"]
            self.env.records.record_lineage(self.lineage)
        self.parallelism = SEQUENCE_PARALLELISM[kind]
        self.boundary = self.env.clock()
        self.outcomes.setdefault("faults", []).append(fault)
        self.awaiting = awaiting
        self.step += 1
        self.persist(kind)
        if kind in SAVEPOINT_FAULTS:
            self.patch(app)
        elif not self.delete(target):
            # Gone before its deletion, as the platform may take a Pod, or not
            # deleted: the fault is injected again once the job runs.
            self.outcomes["faults"].pop()
            self.awaiting = None
            self.step -= 1
            self.env.emit("fault-target-gone", {"kind": kind, **fault["pod"]})

    def sequence_spec(self, parallelism):
        """The approved upgrade spec at `parallelism`: its sizes and argument only."""
        spec = copy.deepcopy(self.upgrade["spec"])
        spec["job"]["parallelism"] = parallelism
        spec["taskManager"]["replicas"] = parallelism
        args = spec["job"]["args"]
        if args.count("--parallelism") != 1:
            raise Failure("Approved upgrade does not name its parallelism once")
        args[args.index("--parallelism") + 1] = str(parallelism)
        return spec

    def patch(self, app):
        """Patch the application to the awaited spec, and wait for its generation.

        A patch that conflicts with the Operator's own write of the
        application is sent again from the next poll's copy; one whose answer
        was lost counts only if the application now holds the spec.
        """
        spec = self.awaiting["spec"]
        operations = [
            {"op": "replace", "path": "/spec/job/args", "value": spec["job"]["args"]},
            {
                "op": "replace",
                "path": "/spec/job/parallelism",
                "value": spec["job"]["parallelism"],
            },
            {
                "op": "replace",
                "path": "/spec/taskManager/replicas",
                "value": spec["taskManager"]["replicas"],
            },
        ]
        try:
            updated = self.env.kube.patch(
                app, operations, before_repeat=self.check_open
            )
        except (ApiError, TransportError) as error:
            if isinstance(error, ApiError) and error.status in (409, 422):
                self.env.emit(
                    "patch-conflict",
                    {"kind": self.awaiting["kind"], "status": error.status},
                )
                return
            if isinstance(error, ApiError) and error.status not in (500, 502, 503, 504):
                raise
            updated = self.env.root("application")
            if not updated or updated["metadata"].get("deletionTimestamp"):
                raise Failure("Application gone while it was patched") from error
            if not contains(updated["spec"], spec):
                # Not applied: the same patch is sent again from the next poll.
                self.env.emit(
                    "patch-unconfirmed",
                    {"kind": self.awaiting["kind"], "cause": str(error)},
                )
                return
        self.awaiting["generation"] = updated["metadata"]["generation"]

    def delete(self, pod):
        """Delete one Pod of the sequence; False when it was not deleted.

        A Pod already gone, or one still running after an answer was lost, is
        injected again from a later poll.
        """
        try:
            return self.env.kube.delete(pod) is not False
        except (ApiError, TransportError) as error:
            if isinstance(error, ApiError) and error.status not in (500, 502, 503, 504):
                raise
            current = self.env.kube.get("Pod", self.namespace, pod["metadata"]["name"])
            return not (
                current
                and current["metadata"]["uid"] == pod["metadata"]["uid"]
                and not current["metadata"].get("deletionTimestamp")
            )
