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
"""The fault sequence every recovery trial runs, and the exercise it drives.

One run injects every fault kind in turn (ADR-0204): a savepoint upgrade, the
JobManager's and a TaskManager's deletion, and a rescale in and back out,
each once the job runs again after the one before. The scenario's own
exercise reads the job's progress and, once it has finished, its oracle.
"""

from __future__ import annotations

import copy
import re
import uuid

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
from .verdict import COMPLETE_STAGE, event_name, sequence_verdict

# The causes recorded when the platform preempts a workload Pod or loses its
# node, and when a Pod is replaced outside a fault.
INTERRUPTED = "Unplanned workload interruption; trial is inconclusive"
REPLACED = "Unplanned workload replacement; trial is inconclusive"
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
# The wait before repeating an oracle's read that failed transiently.
RETRY_SECONDS = 5
# What the fault sequence's oracle window leaves before the run's deadline, its
# time limit or the cleanup start, whichever is first, for writing the
# record's transition before the run stops.
RECORD_SECONDS = 60
# The faults one run injects under ADR-0204, in order, and the parallelism the
# job runs at once each has been injected: the run starts at two, as a
# JobManager replacement trial does, and the two rescales move it to one and
# back.
SEQUENCE = ("upgrade", "jobmanager", "taskmanager", "rescale-in", "rescale-out")
# The writers' parallelism, and TaskManager count, of both applications a
# trial renders, the initial one and the upgrade; the sequence patches only its
# rescales away from it. `kubernetes/lifecycle/parallelism.cue` renders the
# same, and a test holds it to this.
RENDERED_PARALLELISM = 2
SEQUENCE_PARALLELISM = {
    "upgrade": RENDERED_PARALLELISM,
    "jobmanager": RENDERED_PARALLELISM,
    "taskmanager": RENDERED_PARALLELISM,
    "rescale-in": 1,
    "rescale-out": RENDERED_PARALLELISM,
}
# The one trial kind every scenario dispatches: the fault sequence.
SEQUENCE_KIND = "sequence"
# The faults that change the application, which the Operator applies as a
# savepoint upgrade.
SAVEPOINT_FAULTS = ("upgrade", "rescale-in", "rescale-out")
# How long a run may take from its admission to its verdict (ADR-0204).
TIME_LIMIT_SECONDS = 3600
# The input finished while a fault was still to come or awaited, so the job
# could not be seen running again after it.
UNCONFIRMED = "Input finished before the fault sequence did; unconfirmed"


def rendered_parallelism(phase):
    """The rendered parallelism of the `initial` or `upgrade` application."""
    if phase not in ("initial", "upgrade"):
        raise ValueError("Unknown trial phase")
    return RENDERED_PARALLELISM


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


class RecoveryExercise:
    """One run that injects every fault kind and waits out everything else.

    A scenario's exercise names its namespace, state bucket and timing,
    reads its job's progress lines, and reads its oracle once the job has
    finished (ADR-0204).
    The run starts at parallelism 2, and once a completed checkpoint follows
    flowing input, and the scenario's own preparation is done, it injects each
    fault of `SEQUENCE` in turn: the savepoint upgrade, the JobManager's
    deletion, the deletion of the TaskManager reporting the input, and a
    rescale to one and back to two, each a patch of the approved upgrade
    manifest that changes only the parallelism.

    Before each fault, and before completion, it waits until the job runs
    again: `RUNNING`, reconciled to the last patch, with a JobManager and as
    many TaskManagers as the parallelism, and with progress logged after a
    restore that followed the fault. A line logged before that restore, by
    the job the fault stopped, is not the job running again.

    Anything else is recorded and waited out, however often it happens: an
    interruption, a replacement, an eviction, a container restart, a restore,
    a patch that conflicted, a Pod already gone, or a REST or API failure.
    Nothing judges a restore planned or unplanned. The run ends when the job
    has finished with its whole input after the last fault, and the
    scenario's verdict decides by the data; it stops abnormally when the job
    fails, the application is deleted or rolled back, or the time limit
    passes, and without a verdict when the input finishes first.
    """

    namespace: str
    state_bucket: str
    timing: dict
    records: int
    # Whether the verdict reads the latest restore, so that only a poll whose
    # checkpoint statistics answered may complete the run. Only a verdict
    # that needs it may wait: BigQuery's FILE_LOADS check refuses a load
    # still running when the job finished, which a later poll would miss.
    reads_final_restore = False

    def __init__(self, env, upgrade):
        if upgrade is None:
            raise Failure("Recovery requires the approved upgrade manifest")
        self.env, self.upgrade = env, upgrade
        self.stage = "baseline"
        self.boundary = env.schedule.started
        self.samples = {}
        self.events = {}
        # Each workload Pod's node and when it was bound there, kept after the
        # Pod is gone.
        self.placements = {}
        self.outcomes = {}
        self.initialized = False
        self.stable_pods = None
        self.retiring_pods = set()
        self.rest = None
        self.deadline = min(
            env.schedule.started + TIME_LIMIT_SECONDS,
            env.schedule.cleanup_at,
        )
        self.parallelism = SEQUENCE_PARALLELISM["upgrade"]
        self.step = 0
        # The fault the job is recovering from: its kind and when it was
        # injected, and for a patch the spec and the generation it waits for.
        self.awaiting = None
        # The latest restore the job's statistics showed, in seconds.
        self.restored_at = None

    @property
    def recovering(self):
        return self.awaiting is not None

    def prefix(self, kind):
        """The run's own location for ``checkpoints`` or ``savepoints``."""
        return f"gs://{self.state_bucket}/runs/{self.env.approval.run_id}/{kind}/"

    def track_rest(self, service, job_id):
        """Keep a REST reader for the job the loop verified.

        No service resolved means the job is between states; reading the last
        one would attribute an answer to a job that is not the one running.
        """
        if service is None:
            self.rest = None
            return
        if self.rest is None or self.rest.job_id != job_id:
            self.rest = FlinkRest(self.env, service, job_id)
        self.rest.service = service

    def rebaseline(self, pods):
        """Once the job runs again, the Pods running now are the stable set."""
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

    def persist(self, stage, **details):
        # For a run that stops before its verdict, the record is the only
        # account of what it read.
        self.check_open()
        value = {
            "stage": stage,
            "at": utc(self.env.clock()),
            "outcomes": copy.deepcopy(self.outcomes),
            **details,
        }
        self.env.records.evidence(event_name(stage), value)
        self.env.records.recovery_step(self.stage if self.initialized else None, value)
        self.stage, self.initialized = stage, True

    def tolerate_rest(self, error):
        """Whether a failed read of the job's REST API is waited out.

        The Service proxy sends the read once, so a failure that says nothing
        about the job is waited out here (ADR-0204): a transport failure, an
        API server's 429 or a 500 from it or the JobManager, and the other
        transient statuses, as well as a 404, which a fault that removes the
        endpoint leaves. Any other failure stops the run.
        """
        self.check_open()
        return transient(error) or (isinstance(error, ApiError) and error.status == 404)

    def check_open(self):
        """Stop the run once it is stopped, or once its time limit has passed."""
        self.env.require_running("Recovery exercise has been stopped")
        if self.env.clock() >= self.deadline:
            raise Failure("Run reached its time limit at stage " + self.stage)

    def unplanned(self, message):
        """Record one cause the run waits out; nothing is a stop."""
        cause = message.split(";")[0]
        causes = self.outcomes.get("unplanned", [])
        if cause not in causes:
            self.outcomes["unplanned"] = [*causes, cause]
            self.env.emit(UNPLANNED_EVENT, {"stage": self.stage, "cause": cause})

    def restarts(self, rest):
        """Record each restore the job's statistics show, judged neither planned nor unplanned."""
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

    def progress(self, pod, text):
        """Read one Pod's progress lines into the samples, as the scenario logs them.

        Each sample names when its line was logged, its phase, how many records
        it shows processed and the Pod that logged it, which is what the
        sequence reads.
        """
        raise NotImplementedError

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
            self.unplanned(INTERRUPTED)
        if evicted:
            self.unplanned("Unplanned workload eviction; trial is inconclusive")
        if restarted:
            self.unplanned("Unplanned container restart; trial is inconclusive")
        identities = {p["metadata"]["uid"] for p in smoke} - self.retiring_pods
        if (
            self.stable_pods is not None
            and identities != self.stable_pods
            and not self.recovering
        ):
            if self.reclaimed(self.stable_pods - identities):
                self.unplanned(INTERRUPTED)
            else:
                self.unplanned(REPLACED)
        if self.awaiting is None:
            self.rebaseline(pods)

    def reclaimed(self, lost):
        """Whether a lost Pod's node shut down gracefully after the Pod was bound there.

        A Spot reclamation leaves nothing on the Pod to read: the kubelet stops
        it, and Flink deletes the TaskManager's Pod and asks for another within a
        second, before the next poll. The node's own `Shutdown` event is what is
        left. The autoscaler's removal of an empty node records the same event,
        just after its own `ScaleDown` on the node, so a shutdown that follows one
        is not counted; nor is one recorded before the Pod was bound, an earlier
        life of the same node name, or the kubelet's cancellation of a shutdown,
        which carries the same reason. It is read only for a Pod lost while no
        fault is awaited.
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
            # read settles the run, on a poll whose checkpoint statistics
            # answered when the verdict reads the latest restore.
            if not (
                complete and (rest or not self.reads_final_restore) and self.finished()
            ):
                return False
            if self.awaiting is not None or self.step < len(SEQUENCE):
                # No progress can follow, so the job could never be seen
                # running again after the remaining faults. Unless the verdict
                # reads the restore, a failed read does not delay this, so a
                # run whose FILE_LOADS load outlived the job cannot reach a
                # verdict on a later poll.
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
        self.before_fault()
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

    def before_fault(self):
        """Fix what the faults run on, as a fault is about to be recorded.

        The shared sequence fixes nothing; an exercise over a finite input
        fixes the lineage its input runs under.
        """

    def sequence_spec(self, parallelism):
        """The approved upgrade spec at `parallelism`: its sizes and argument only."""
        spec = copy.deepcopy(self.upgrade["spec"])
        spec["job"]["parallelism"] = parallelism
        spec["taskManager"]["replicas"] = parallelism
        args = spec["job"]["args"]
        # A separate value, as most applications take it, or `--parallelism=N`,
        # as the Pub/Sub relay does.
        named = [i for i, arg in enumerate(args) if arg.startswith("--parallelism=")]
        if args.count("--parallelism") + len(named) != 1:
            raise Failure("Approved upgrade does not name its parallelism once")
        if named:
            args[named[0]] = f"--parallelism={parallelism}"
        else:
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


class FiniteInputExercise(RecoveryExercise):
    """A recovery exercise over one finite, checkpointed input lineage.

    A subclass names `service` for its messages, `progress_event` for the
    event its progress lines carry, and `oracle_stage`, the stage a finished
    application waits in until the scenario's oracle has passed. It sets its
    own attributes before calling this constructor with the trial.

    Each progress line names the input lineage it reads. A job restarted
    before its first completed checkpoint starts its input again under a new
    lineage, which writes the same records, so before the first fault the
    latest lineage replaces the others; the first fault fixes it.
    """

    service: str
    progress_event: str
    oracle_stage: str

    def __init__(self, env, upgrade, trial):
        self.trial = trial
        self.records = trial.records
        super().__init__(env, upgrade)
        # The input lineage the faults run on, fixed by the first fault.
        self.lineage = None

    def progress(self, pod, text):
        """Read progress; before the first fault a fresh start may replace the lineage.

        The lines themselves are read, and checked, by `read_progress`, which a
        scenario extends, before a fresh start prunes any of them.
        """
        self.read_progress(pod, text)
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

    def read_progress(self, pod, text):
        """Read one Pod's progress lines into the samples, and check every sample."""
        pattern = (
            rf"event={self.progress_event} run_id=(\S+) phase=(\S+) lineage=(\S+) "
            r"restored=(true|false) processed=([0-9]+) sequence=([0-9]+)"
        )
        for line in text.splitlines():
            match = re.search(pattern, line)
            if not match:
                continue
            run_id, phase, lineage, _restored, processed, sequence = match.groups()
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
        for sample in self.samples.values():
            try:
                valid = str(uuid.UUID(sample["lineage"])) == sample["lineage"]
            except ValueError:
                valid = False
            if sample["at"] > self.env.clock():
                raise Failure(f"{self.service} progress is dated in the future")
            if not valid:
                raise Failure(f"{self.service} input lineage changed or is invalid")

    def before_fault(self):
        """The first fault fixes the lineage the rest run on: its latest line's."""
        if self.lineage is None:
            latest = max(self.samples.values(), key=lambda s: s["at"])
            self.lineage = latest["lineage"]
            self.env.records.record_lineage(self.lineage)

    def input_finished_at(self):
        """When the latest progress line showed the whole input.

        A restore after an earlier completion may replay the input and finish
        it again, writing its rows again, so the latest line is the one a
        relay must have time for.
        """
        return max(sample["at"] for sample in self.samples.values())

    def restored_after_input(self):
        """Whether a restore followed the input's latest progress line.

        Such a restore may commit staged writes its state carried without
        logging a line, and its recorded time is when the JobManager assigned
        the state, before the tasks that commit were deployed, so it cannot
        bound when those writes landed. It is current only for a run that
        sets `reads_final_restore`, which completes on a poll whose checkpoint
        statistics answered.
        """
        return self.restored_at is not None and self.restored_at > (
            self.input_finished_at()
        )

    def oracle_window(self):
        """The oracle's reads end within the time limit, before the record's write."""
        return min(
            self.env.clock() + self.timing["visibility_seconds"],
            self.deadline - RECORD_SECONDS,
        )

    def persist(self, stage, **details):
        if stage == COMPLETE_STAGE:
            stage = self.oracle_stage
        super().persist(stage, **details)

    def retried(self, call, until, closed=None, said_nothing=transient):
        """One oracle read, repeated while it fails without saying anything.

        Before `until`, a failure that `said_nothing` does not excuse says
        something about the read and is raised. Once it has passed, any
        failure is raised, as `closed` when the caller names the window's
        closing apart.
        """
        while True:
            try:
                return call()
            except Failure as error:
                if self.env.clock() >= until:
                    if closed is None:
                        raise
                    raise closed(str(error)) from error
                if not said_nothing(error):
                    raise
            self.env.sleep(RETRY_SECONDS)

    def complete(self, data_reasons):
        """Write the completing transition with the verdict decided over it.

        The verdict is decided, by the scenario's `data_reasons`, from the
        record the transition is about to write, which the offline analyzer
        recomputes once the evidence is exported.
        """
        decided = sequence_verdict(
            {"stage": COMPLETE_STAGE, "outcomes": self.outcomes}, data_reasons
        )
        # The stage the oracle waited in is complete only now.
        RecoveryExercise.persist(
            self, COMPLETE_STAGE, processed=self.records, **decided
        )
