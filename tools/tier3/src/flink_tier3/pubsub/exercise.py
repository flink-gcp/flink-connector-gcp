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

from ..common import ApiError, Failure, TransportError, contains, utc
from ..exercise import FAILOVERS, FAULTS, RecoveryExercise
from ..metrics import metric_name, sample, subset, unavailable
from ..policy import PUBSUB, PUBSUB_EXERCISE, PUBSUB_STATE
from .messages import COHORTS, cohort_ranges
from .observe import DEDUPLICATION_METRICS, connector_metrics
from .oracle import reconcile
from .output import OutputCollector
from .verdict import (
    ADAPTIVE,
    COMPLETE_STAGE,
    MEASUREMENT_EVENT,
    REPLACEMENTS,
    started_by_fault,
    summarize,
    verdict,
)

FIRST, REPLAY, LAST = COHORTS
ATTEMPT = re.compile(
    r"event=pubsub-attempt run_id=(\S+) phase=(\S+) attempt=(\S+) "
    r"restored=(true|false)"
)
HOLD = re.compile(
    r"event=pubsub-hold run_id=(\S+) phase=(\S+) attempt=(\S+) "
    r"checkpoint_id=([0-9]+) millis=([0-9]+)"
)
# The cohorts the supervisor has requested by each stage; the first is
# published at admission.
REQUESTED = {
    "baseline": (FIRST,),
    "checkpoint": (FIRST,),
    "boundary": (FIRST, REPLAY),
    "recovering": (FIRST, REPLAY),
    "after": COHORTS,
}
# How much of a checkpoint's hold a TaskManager kill leaves unspent at the
# latest: the deletion request, the kubelet's reaction and the poll that saw
# the completion all come out of it.
KILL_MARGIN_MILLIS = 30000
# How many completions a kill passes over before it gives up.
KILL_PASSES = 3
# The Flink option an adaptive scale-in's recovery manifest adds.
PARALLELISM_OVERRIDES = "pipeline.jobvertex-parallelism-overrides"
# The fields a rescale's recovery manifest changes.
RESCALED = (("job", "args"), ("job", "parallelism"), ("taskManager", "replicas"))


def _identities(observations):
    return {(item["input_index"], item["sequence"]) for item in observations}


class PubSubExercise(RecoveryExercise):
    """Drive one trial through its cohorts, its fault and its recovery.

    The admission cohort is observed on the output subscription, then a
    checkpoint triggered after that observation is retained. The replay
    cohort is requested only after that checkpoint is seen completed, and
    the runner starts it only after reading the request, so no completed
    checkpoint taken before the request can cover it. Once the replay cohort
    is processed, the trial's fault is injected: the JobManager or an active
    TaskManager is deleted, or the application is patched to the recovery
    manifest for a savepoint rescale. Restoring from the retained checkpoint
    proves that no later checkpoint completed before the fault, and then what
    the fault displaced must reappear from new attempts. The last cohort is
    requested after recovery and must be processed on both inputs by the
    attempts serving after the fault, followed by another checkpoint.

    The exercise records what it observed and the expected replay population,
    and decides the trial's verdict when it completes, from the record it
    writes then: the output oracle over every collected line, the recovery
    outcomes and the observation coverage.
    """

    namespace = PUBSUB
    state_bucket = PUBSUB_STATE
    timing = PUBSUB_EXERCISE

    def __init__(self, env, upgrade, handoff):
        if env.approval.scenario != "pubsub-recovery":
            raise Failure("Pub/Sub exercise requires a Pub/Sub approval")
        super().__init__(env, upgrade)
        self.kind = env.approval.pubsub_trial["trial"]
        # An AdaptiveScheduler kind's sizes, fault and hold, or None.
        self.adaptive = ADAPTIVE.get(self.kind)
        # The Pod a replacement or a TaskManager kill deletes; a rescale
        # deletes none.
        self.replaced = (
            FAULTS[self.kind].failover
            if self.kind in REPLACEMENTS
            else "taskmanager"
            if self.adaptive and self.adaptive["fault"] == "kill"
            else None
        )
        # A TaskManager kill's mark: the highest checkpoint id the job had
        # reported once replay-cohort output was seen, so that a checkpoint
        # completing above it covers some of the cohort; `replay_seen` takes
        # the history as the mark only on the poll after, as `marked` does.
        self.kill_after = None
        self.replay_seen = False
        # Completions a kill saw too late to land inside their hold.
        self.kill_passes = 0
        # The checkpoints the relay logged a hold after, with how many holds.
        self.relay_holds = {}
        # The first restore after the fault, which is the case's; a kill may
        # restore again once the replacement TaskManager registers.
        self.first_restore = None
        # The highest suppressed-redelivery count a restored attempt
        # reported, and how many readings it was taken over: the counter is
        # per reader and starts again with every attempt.
        self.most_suppressed = 0
        self.suppressed_samples = 0
        self.handoff = handoff
        self.collector = OutputCollector(handoff)
        self.ranges = cohort_ranges(self.collector.records)
        # Attempt UUID to the Pod whose log announced it.
        self.attempts = {}
        # Whether a cohort was fully observed on an earlier poll, so that the
        # checkpoint history read on this poll postdates that observation.
        self.observed = False
        self.marker = None
        self.fault = None
        self.resized = None

    @property
    def recovering(self):
        return self.stage == "recovering"

    def admitted(self):
        """Start the first stage's own deadline from the application's admission."""
        app = self.env.root("application") or {}
        replicas = app.get("spec", {}).get("taskManager", {}).get("replicas")
        if type(replicas) is not int or replicas < 1:
            raise Failure("Pub/Sub application has no TaskManager replica count")
        # The JobManager and the initial parallelism's TaskManagers.
        self.expected_pods = 1 + replicas
        self.deadline = min(
            self.env.clock() + self.timing["input_seconds"],
            self.env.schedule.cleanup_at,
        )
        self.persist("baseline")

    def renew(self, start, seconds):
        """Replace the stage's deadline, only while the outgoing one still holds.

        Reads between a poll's own check and a transition can outlast the
        stage, which would otherwise continue under the next one's deadline.
        """
        self.check_open()
        self.deadline = min(start + seconds, self.env.schedule.cleanup_at)

    def progress(self, pod, text):
        """Map each attempt the application announces to the Pod announcing it."""
        for line in text.splitlines():
            match = ATTEMPT.search(line)
            if not match:
                continue
            run_id, phase, attempt, restored = match.groups()
            if run_id != self.env.approval.run_id or phase not in (
                "initial",
                "upgrade",
            ):
                raise Failure("Pub/Sub attempt belongs to another run or phase")
            known = self.attempts.get(attempt)
            if known and known["pod_uid"] != pod["metadata"]["uid"]:
                raise Failure("One Pub/Sub attempt was announced by two Pods")
            self.attempts[attempt] = {
                "pod_uid": pod["metadata"]["uid"],
                "pod": pod["metadata"]["name"],
                "phase": phase,
                "restored": restored == "true",
            }
        for line in text.splitlines():
            match = HOLD.search(line)
            if not match or match.group(1) != self.env.approval.run_id:
                continue
            # Logs are read again from their start, so a hold is counted once
            # per attempt and checkpoint.
            self.relay_holds.setdefault(match.group(4), set()).add(match.group(3))

    def attach_rest(self, service, job_id):
        self.track_rest(service, job_id)

    def restarts(self, rest):
        """Accept every restore of an in-place recovery; otherwise as the base does.

        The AdaptiveScheduler keeps its JobMaster and restarts the job from
        its latest checkpoint whenever its TaskManagers or its resource
        requirements change: once for a rescale, and twice for a kill, down
        when the Pod goes and up when
        its Deployment's replacement registers. A lost TaskManager may also
        fail the job before its slots are released, which would add one.
        Restores outside the recovery stay unplanned.
        """
        if self.adaptive is None or not self.recovering:
            super().restarts(rest)
            return
        restored = (rest.get("latest") or {}).get("restored")
        if not restored:
            return
        at = restored.get("restore_timestamp")
        if type(at) is not int:
            raise Failure("Restored checkpoint has no restore time")
        if self.last_restore and at <= self.last_restore["at"]:
            return
        count = (rest.get("counts") or {}).get("restored")
        if type(count) is not int:
            raise Failure("Checkpoint statistics have no restore count")
        self.last_restore = {"at": at, "count": count, "stage": self.stage, "base": 0}
        if self.first_restore is None:
            self.first_restore = dict(restored)

    def backlog(self):
        """Requested input not yet seen on the output subscription.

        Derived from the supervisor's own requests and observations, with no
        service read: it counts from the request rather than the publication,
        so it includes the runner's publication delay, and a message seen once
        is not counted again however often it is redelivered.
        """
        names = REQUESTED.get(self.stage, ())
        requested = 2 * sum(self.ranges[name]["count"] for name in names)
        observed = {
            (item["input_index"], item["sequence"])
            for item in self.collector.observations
            if any(self.in_cohort(name, item) for name in names)
        }
        return {
            "cohorts": list(names),
            "requested": requested,
            "observed": len(observed),
            "outstanding": requested - len(observed),
        }

    def measure(self):
        """Sample each vertex's backpressure and connector metrics, once per interval."""
        if not self.measurement_due():
            return
        plan = sample(lambda: self.rest.job(""))
        if unavailable(plan):
            readings = plan
        else:
            readings = [
                {
                    **subset(vertex, ("id", "name", "parallelism", "status")),
                    "backpressure": subset(
                        sample(
                            lambda vertex=vertex: self.rest.job(
                                f"/vertices/{vertex['id']}/backpressure"
                            )
                        ),
                        ("status", "backpressureLevel", "subtasks", "unavailable"),
                    ),
                    "metrics": connector_metrics(self.rest, vertex["id"]),
                }
                for vertex in plan.get("vertices") or []
                if vertex.get("id")
            ]
        reading = {"vertices": readings, "backlog": self.backlog()}
        self.coverage = summarize(self.coverage, self.stage, reading)
        self.env.emit(MEASUREMENT_EVENT, {"stage": self.stage, **reading})

    @staticmethod
    def known_id(rest):
        """The highest checkpoint id the job has reported in any state."""
        latest = rest.get("latest") or {}
        ids = [item.get("id") for item in rest.get("history") or []]
        ids += [(latest.get(key) or {}).get("id") for key in latest]
        return max((i for i in ids if type(i) is int), default=0)

    def completed_after(self, rest, after_id):
        """The latest completed checkpoint, when its id exceeds ``after_id``.

        Checkpoint ids grow with their trigger, so an id above every id the job
        reported after an observation belongs to a checkpoint triggered after
        it, on no clock but the job's own.
        """
        cp = (rest.get("latest") or {}).get("completed") or {}
        if (
            cp.get("status") != "COMPLETED"
            or cp.get("is_savepoint") is not False
            or type(cp.get("id")) is not int
            or cp["id"] <= after_id
        ):
            return None
        if not cp.get("external_path", "").startswith(self.prefix("checkpoints")):
            raise Failure("Completed checkpoint is outside the approved state prefix")
        return cp

    def summary(self):
        observations = self.collector.observations
        return {
            "observations": len(observations),
            "foreign": len(self.collector.foreign),
            "attempts": sorted({item["attempt"] for item in observations}),
            "last_output_at": utc(max(item["at"] for item in observations))
            if observations
            else None,
        }

    def in_cohort(self, name, item):
        cohort = self.ranges[name]
        return cohort["start"] <= item["sequence"] < cohort["start"] + cohort["count"]

    def observe(self, app, rest, pods):
        self.check_open()
        if app["metadata"].get("deletionTimestamp"):
            raise Failure("Application deletion started during the exercise")
        status = app.get("status", {}).get("jobStatus", {}).get("state", "")
        if status in ("FAILED", "FAILING") or (
            status in ("CANCELED", "SUSPENDED", "FINISHED") and not self.recovering
        ):
            raise Failure("Unexpected Flink state: " + status)
        if app.get("status", {}).get("reconciliationStatus", {}).get("state") in (
            "ROLLING_BACK",
            "ROLLED_BACK",
        ):
            raise Failure("Operator rolled back the approved upgrade")
        self.collector.drain(self.timing["drain_pulls"])
        self.measure()
        # The pulls and readings can outlast the stage; a transition below
        # replaces its deadline, so the expired one is enforced first.
        self.check_open()
        if status != "RUNNING":
            return False
        if self.adaptive and self.stage in ("recovering", "after"):
            self.track_suppression()
        if self.stage == "baseline":
            self.after_input(rest)
        elif self.stage == "checkpoint":
            self.after_checkpoint(rest)
        elif self.stage == "boundary":
            self.inject(pods, rest)
        elif self.stage == "recovering":
            self.recovered(app, rest, pods)
        elif self.stage == "after":
            return self.finished(rest)
        return False

    def marked(self, name, rest, where=None):
        """The checkpoint id to exceed, once a cohort was observed on a past poll.

        The loop reads this poll's checkpoint history before this poll's
        pulls, so the history is taken as the mark only on the poll after the
        one that saw the whole cohort.
        """
        if self.observed:
            self.observed = False
            return self.known_id(rest)
        self.observed = self.collector.complete(name, where=where)
        return None

    def after_input(self, rest):
        self.marker = self.marked(FIRST, rest)
        if self.marker is None:
            return
        self.renew(self.env.clock(), self.timing["checkpoint_seconds"])
        self.persist("checkpoint", observed=self.summary(), after_id=self.marker)

    def request(self, name, stage_seconds, stage, **details):
        """Record the next stage, then ask the runner for the cohort it waits on."""
        now = self.env.clock()
        self.renew(now, stage_seconds)
        self.persist(stage, **details)
        self.handoff.request_cohort(
            name, deadline=min(now + self.timing["cohort_seconds"], self.deadline)
        )

    def after_checkpoint(self, rest):
        cp = self.completed_after(rest, self.marker)
        if cp is None:
            return
        # Outcomes travel with every later record, so the run's last record
        # still names what the trial retained and what it did.
        self.outcomes["checkpoint"] = cp
        self.request(REPLAY, self.timing["boundary_seconds"], "boundary")

    def target(self, pods, replayed):
        """The Pod the fault removes, or None while it cannot yet be named."""
        if self.replaced == "jobmanager":
            return self.jobmanager(pods)
        return self.busiest(pods, replayed, required=True)

    def busiest(self, pods, replayed, *, required):
        """The running TaskManager whose attempts processed most of ``replayed``.

        A ``required`` count excludes a TaskManager that processed none; a
        kill takes the first by name when none did yet.
        """
        counts = {}
        for item in replayed:
            owner = self.attempts.get(item["attempt"])
            if owner:
                counts[owner["pod_uid"]] = counts.get(owner["pod_uid"], 0) + 1
        managers = sorted(
            (
                pod
                for pod in pods
                if pod["metadata"]["namespace"] == self.namespace
                and pod["metadata"].get("labels", {}).get("component") == "taskmanager"
                and not pod["metadata"].get("deletionTimestamp")
                and pod.get("status", {}).get("phase") == "Running"
                and (pod["metadata"]["uid"] in counts or not required)
            ),
            key=lambda pod: pod["metadata"]["name"],
        )
        if not managers:
            return None
        return max(managers, key=lambda pod: counts.get(pod["metadata"]["uid"], 0))

    def inject(self, pods, rest):
        if self.adaptive:
            self.inject_adaptive(pods, rest)
            return
        if not self.collector.complete(REPLAY):
            return
        cohort = self.handoff.cohorts().get(REPLAY)
        if not cohort or cohort["published_at"] is None:
            return
        replayed = [o for o in self.collector.observations if self.in_cohort(REPLAY, o)]
        before = {item["attempt"] for item in self.collector.observations}
        latest = ((rest.get("latest") or {}).get("completed") or {}).get("id")
        jm = self.jobmanager(pods)
        pod = None
        if self.kind in REPLACEMENTS:
            pod = self.target(pods, replayed)
            if pod is None or jm is None:
                return
            displaced = (
                before
                if self.replaced == "jobmanager"
                else {
                    attempt
                    for attempt, owner in self.attempts.items()
                    if owner["pod_uid"] == pod["metadata"]["uid"]
                }
            )
            expected = sorted(
                _identities(o for o in replayed if o["attempt"] in displaced)
            )
        else:
            # A savepoint completes before the job stops and acknowledges what
            # it covers, so nothing is expected again.
            displaced, expected = before, []
        # The application as it is now, because the patch tests its version.
        app = self.env.root("application")
        if not app:
            raise Failure("Application disappeared before completion")
        if self.kind not in REPLACEMENTS:
            self.generation = app["metadata"]["generation"] + 1
        self.fault = {
            "kind": self.kind,
            "at": self.env.clock(),
            "checkpoint": self.outcomes["checkpoint"]["id"],
            # Whether the retained checkpoint was still the latest completed
            # when the fault was decided; restoring it proves the same later.
            "latest_completed": latest,
            "cohort": cohort,
            "jm_uid": jm["metadata"]["uid"] if jm else None,
            "pod_uid": pod["metadata"]["uid"] if pod else None,
            "pod": pod["metadata"]["name"] if pod else None,
            "generation": self.generation,
            "before": sorted(before),
            "displaced": sorted(displaced),
            "expected_replay": [list(identity) for identity in expected],
        }
        self.renew(self.fault["at"], self.timing["recovery_seconds"])
        self.outcomes["fault"] = self.fault
        self.persist("recovering", observed=self.summary())
        self.check_open()
        if self.kind in REPLACEMENTS:
            self.delete_pod(pod, FAILOVERS[self.replaced])
        else:
            self.patch_upgrade(
                app,
                [
                    {
                        "op": "replace",
                        "path": f"/spec/{parent}/{field}",
                        "value": self.upgrade["spec"][parent][field],
                    }
                    for parent, field in RESCALED
                ],
            )

    def inject_adaptive(self, pods, rest):
        """Rescale as soon as the replay cohort is published, or kill on a held checkpoint.

        The relay holds the first replay-cohort record after each snapshot,
        so every checkpoint taken while the cohort flows completes with its
        notification queued behind that record for the kind's hold.

        A reactive scale-out patches the replica count at once, and an
        adaptive scale-in the job vertex's parallelism override. The new
        resources or requirements stabilize while the cohort is still
        flowing, and the scheduler restarts the job when its next checkpoint
        completes, inside that checkpoint's hold.

        A kill waits for replay-cohort output, then for a checkpoint
        triggered after it to complete, and deletes the TaskManager that
        processed the most replay-cohort input without a grace period while
        the hold still has a margin left; a completion seen too late to keep
        it is passed over for the next one.

        Every attempt restarts, so every one is displaced; what reappears is
        either redelivery of what the restored checkpoint had not covered,
        which the oracle's completeness counts, or what it had covered, which
        restore deduplication suppresses and the oracle would count as
        covered redeliveries.
        """
        cohort = self.handoff.cohorts().get(REPLAY)
        if not cohort or cohort["published_at"] is None:
            return
        jm = self.jobmanager(pods)
        if jm is None:
            return
        pod = None
        held = None
        if self.replaced:
            replayed = [
                o for o in self.collector.observations if self.in_cohort(REPLAY, o)
            ]
            if not replayed:
                return
            if not self.replay_seen:
                # This poll's history was read before this poll's pulls, so it
                # is taken as the mark only on the next poll, as `marked` does.
                self.replay_seen = True
                return
            if self.kill_after is None:
                self.kill_after = self.known_id(rest)
                self.renew(self.env.clock(), self.timing["checkpoint_seconds"])
                return
            held = self.completed_after(rest, self.kill_after)
            if held is None:
                return
            triggered = held.get("trigger_timestamp")
            if type(triggered) is not int:
                raise Failure("Completed checkpoint has no trigger timestamp")
            hold = self.adaptive["hold_millis"]
            if self.env.clock() * 1000 >= triggered + hold - KILL_MARGIN_MILLIS:
                # Too late to land inside this checkpoint's hold; the next
                # checkpoint, which the cohort still feeds, gets its own, but
                # only a few times: the cohort drains, and a checkpoint taken
                # after that has nothing to hold.
                self.kill_passes += 1
                if self.kill_passes > KILL_PASSES:
                    raise Failure(
                        "No completed checkpoint was seen early enough in its"
                        " hold; trial is inconclusive"
                    )
                self.kill_after = held["id"]
                self.renew(self.env.clock(), self.timing["checkpoint_seconds"])
                return
            pod = self.busiest(pods, replayed, required=False)
            if pod is None:
                return
        app = self.env.root("application")
        if not app:
            raise Failure("Application disappeared before completion")
        if not self.replaced:
            self.generation = app["metadata"]["generation"] + 1
        before = {item["attempt"] for item in self.collector.observations}
        latest = ((rest.get("latest") or {}).get("completed") or {}).get("id")
        self.fault = {
            "kind": self.kind,
            "at": self.env.clock(),
            "checkpoint": self.outcomes["checkpoint"]["id"],
            "latest_completed": latest,
            # The checkpoint a kill landed inside the hold of, and when it
            # was triggered; a scale-out cannot know its restart's yet.
            "held_checkpoint": held["id"] if held else None,
            "held_triggered_at": held["trigger_timestamp"] if held else None,
            "cohort": cohort,
            "jm_uid": jm["metadata"]["uid"],
            "pod_uid": pod["metadata"]["uid"] if pod else None,
            "pod": pod["metadata"]["name"] if pod else None,
            "generation": self.generation,
            "before": sorted(before),
            "displaced": sorted(set(self.attempts) | before),
            "expected_replay": [],
        }
        self.renew(self.fault["at"], self.timing["recovery_seconds"])
        self.outcomes["fault"] = self.fault
        self.persist("recovering", observed=self.summary())
        self.check_open()
        if pod is not None:
            # Without a grace period, so the TaskManager has no time to run
            # the held checkpoint's notification; the kubelet's shortest
            # termination still signals the container first, and whether the
            # dying reader nacks its leases decides only how soon Pub/Sub
            # redelivers them.
            self.delete_pod(pod, FAILOVERS["taskmanager"], force=True)
            # The deletion is the event the hold must cover, after the
            # decision and the record above; the outcome holds this dict.
            self.fault["deleted_at"] = self.env.clock()
        elif self.adaptive["fault"] == "override":
            # The only field the recovery manifest adds; the Operator applies
            # it in place through the resource-requirements API.
            self.patch_upgrade(
                app,
                [
                    {
                        "op": "add",
                        "path": "/spec/flinkConfiguration/" + PARALLELISM_OVERRIDES,
                        "value": self.upgrade["spec"]["flinkConfiguration"][
                            PARALLELISM_OVERRIDES
                        ],
                    }
                ],
            )
        else:
            self.patch_upgrade(
                app,
                [
                    {
                        "op": "replace",
                        "path": "/spec/taskManager/replicas",
                        "value": self.upgrade["spec"]["taskManager"]["replicas"],
                    }
                ],
            )

    def running_managers(self, pods):
        """The application's TaskManager Pods that are running and not going."""
        return [
            pod
            for pod in pods
            if pod["metadata"]["namespace"] == self.namespace
            and pod["metadata"].get("labels", {}).get("component") == "taskmanager"
            and not pod["metadata"].get("deletionTimestamp")
            and pod.get("status", {}).get("phase") == "Running"
        ]

    def hold_proof(self, rest, restored):
        """The case's checkpoint, its hold window, the event that must fall in it,
        and how many relay attempts logged a hold after that checkpoint.

        The case is the first restore after the fault; a kill may restore
        again once the replacement TaskManager registers. A kill's event is
        its deletion, inside the hold of the checkpoint it waited for. A
        rescale's is the first restore, of whichever checkpoint completed and
        triggered it, read from the job's checkpoint history; the
        cancellation came before the restore.
        """
        first = self.first_restore or restored
        hold = self.adaptive["hold_millis"]
        if self.replaced:
            checkpoint = self.fault["held_checkpoint"]
            triggered = self.fault["held_triggered_at"]
            deleted = self.fault.get("deleted_at")
            event = deleted * 1000 if deleted is not None else None
        else:
            checkpoint = first.get("id")
            entry = next(
                (
                    item
                    for item in rest.get("history") or []
                    if item.get("id") == checkpoint
                ),
                {},
            )
            triggered = entry.get("trigger_timestamp")
            event = first.get("restore_timestamp")
        return {
            "checkpoint": checkpoint,
            "triggered_at": triggered,
            "hold_millis": hold,
            "event_at": event,
            "relay_holds": len(self.relay_holds.get(str(checkpoint), ())),
        }

    def restored_adaptive(self, app, rest, pods):
        """The checkpoint the job last restored, once it runs at the recovery size.

        The AdaptiveScheduler restores the latest checkpoint each time it
        restarts, so the restore is the last one after the fault, and the
        recovery is complete when every vertex runs at the kind's recovery
        parallelism: after a kill, that is the restart that takes in the
        replacement, the first when it registers within the scheduler's
        resource stabilization. After an adaptive scale-in, the TaskManager
        it left idle must also be gone.
        """
        restored = (rest.get("latest") or {}).get("restored") or {}
        if restored.get("restore_timestamp", 0) / 1000 <= self.fault["at"]:
            return None
        if (
            type(restored.get("id")) is not int
            or restored.get("is_savepoint") is not False
            or restored["id"] < self.fault["checkpoint"]
            or not restored.get("external_path", "").startswith(
                self.prefix("checkpoints")
            )
        ):
            raise Failure("In-place restart did not restore a retained checkpoint")
        jm = self.jobmanager(pods)
        if jm is None:
            return None
        if self.fault["jm_uid"] != jm["metadata"]["uid"]:
            raise Failure("JobManager was replaced during an in-place recovery")
        if self.replaced:
            if any(pod["metadata"]["uid"] == self.fault["pod_uid"] for pod in pods):
                return None
        else:
            status = app.get("status", {})
            if (
                status.get("observedGeneration", 0) < self.generation
                or status.get("reconciliationStatus", {}).get("state") != "DEPLOYED"
            ):
                return None
            if not contains(app["spec"], self.upgrade["spec"]):
                raise Failure("Application differs from the approved recovery manifest")
            if self.adaptive["fault"] == "override" and (
                len(self.running_managers(pods)) != self.adaptive["recovery"]
            ):
                # The resource manager releases the TaskManager the scale-in
                # left idle once its idle timeout passes; recovery is proven
                # after that, so the Pods running then are the stable set.
                return None
        try:
            plan = self.rest.job("")
            # The sizes are read after the poll's checkpoint statistics, so a
            # restart between the two would pair this restore with the next
            # restart's sizes; the restore is read again to rule that out.
            again = (self.rest.job("/checkpoints").get("latest") or {}).get("restored")
        except (ApiError, TransportError) as error:
            if self.tolerate_rest(error):
                return None
            raise
        sizes = {vertex.get("parallelism") for vertex in plan.get("vertices") or []}
        if (
            sizes != {self.adaptive["recovery"]}
            or (again or {}).get("restore_timestamp") != restored["restore_timestamp"]
        ):
            return None
        self.resized = sizes.pop()
        return restored

    def restored(self, app, rest, pods):
        """What the job restored after the fault, or None while it has not."""
        if self.adaptive:
            return self.restored_adaptive(app, rest, pods)
        restored = (rest.get("latest") or {}).get("restored") or {}
        if restored.get("restore_timestamp", 0) / 1000 <= self.fault["at"]:
            return None
        if type(restored.get("id")) is not int:
            raise Failure("Restored checkpoint identity is missing")
        path = restored.get("external_path", "")
        if self.kind in REPLACEMENTS:
            if (
                restored.get("is_savepoint") is not False
                or restored["id"] < self.fault["checkpoint"]
                or not path.startswith(self.prefix("checkpoints"))
            ):
                raise Failure("Replacement did not restore a retained checkpoint")
            jm = self.jobmanager(pods)
            if jm is None:
                return None
            if self.replaced == "jobmanager" and (
                jm["metadata"]["uid"] == self.fault["jm_uid"]
            ):
                return None
            if self.replaced == "taskmanager" and any(
                pod["metadata"]["uid"] == self.fault["pod_uid"] for pod in pods
            ):
                return None
            return restored
        status = app.get("status", {})
        savepoint = (
            status.get("jobStatus", {}).get("savepointInfo", {}).get("lastSavepoint")
            or {}
        )
        if (
            status.get("observedGeneration", 0) < self.generation
            or status.get("reconciliationStatus", {}).get("state") != "DEPLOYED"
        ):
            return None
        if not contains(app["spec"], self.upgrade["spec"]):
            raise Failure("Application differs from the approved recovery manifest")
        if (
            restored.get("is_savepoint") is not True
            or savepoint.get("location") != path
            or savepoint.get("triggerType") != "UPGRADE"
            or savepoint.get("timeStamp", 0) / 1000 <= self.fault["at"]
            or not path.startswith(self.prefix("savepoints"))
        ):
            raise Failure("Rescale did not restore its newly completed savepoint")
        return restored

    def fresh(self, attempt, restored, phase):
        """An attempt the fault started, from restored state."""
        return started_by_fault(
            attempt, restored, phase, self.fault["before"], self.kind
        )

    def restarted(self, item):
        return self.fresh(item["attempt"], item["restored"], item["phase"])

    def serving(self, item):
        """An observation by an attempt the fault left running or started.

        A TaskManager's loss can restart only the region on it, so an attempt
        on the other TaskManager keeps processing without restoring anything.
        """
        return item["attempt"] not in self.fault["displaced"] and (
            item["attempt"] in self.fault["before"] or self.restarted(item)
        )

    def recovered(self, app, rest, pods):
        restored = self.restored(app, rest, pods)
        if restored is None:
            return
        # Restored attempts announce themselves in their Pods' logs. Output
        # is no proof of them yet: after a savepoint nothing is expected
        # again, and the last cohort is published only once recovery is proven.
        started = sorted(
            attempt
            for attempt, owner in self.attempts.items()
            if self.fresh(attempt, owner["restored"], owner["phase"])
        )
        if not started:
            return
        after = [o for o in self.collector.observations if self.restarted(o)]
        if self.adaptive:
            self.outcomes["recovery"] = {
                "restored": restored,
                "replay": "restored-checkpoint",
                # The size every vertex ran at when recovery was proven.
                "parallelism": self.resized,
                "restores": self.last_restore["count"] if self.last_restore else None,
                "first_restored": self.first_restore or restored,
                "hold": self.hold_proof(rest, restored),
                "first_output_after_fault": (
                    utc(min(o["at"] for o in after)) if after else None
                ),
                "attempts": started,
            }
            self.rebaseline(pods)
            self.marker = None
            self.request(LAST, self.timing["after_seconds"], "after")
            return
        held = self.kind in REPLACEMENTS and restored["id"] == self.fault["checkpoint"]
        expected = {tuple(identity) for identity in self.fault["expected_replay"]}
        replays = [o for o in after if self.in_cohort(REPLAY, o)]
        if held and not expected <= _identities(replays):
            return  # still waiting for the displaced population, to the deadline
        self.outcomes["recovery"] = {
            "restored": restored,
            # The boundary held when the job restored the retained checkpoint:
            # no checkpoint completed between the replay cohort's processing
            # and the fault.
            "boundary_held": held,
            "replay": (
                "observed"
                if held
                else "not-expected"
                if self.kind not in REPLACEMENTS
                else "unobserved"
            ),
            "expected_replay": len(expected),
            "replayed": len(_identities(replays) & expected),
            "extra_replay": len(_identities(replays) - expected),
            "replay_ids_preserved": self.ids_preserved(),
            "first_output_after_fault": (
                utc(min(o["at"] for o in after)) if after else None
            ),
            "attempts": started,
        }
        self.rebaseline(pods)
        self.marker = None
        self.request(LAST, self.timing["after_seconds"], "after")

    def deduplication_reading(self):
        """The source's restore-deduplication readings now, summed over its subtasks.

        A reading the service did not answer is left out, which the verdict
        does not credit.
        """
        reading = {}
        if self.rest is None:
            # The REST Service is gone while the job recovers, which the loop
            # tolerates; the next poll reads again.
            return reading
        try:
            plan = self.rest.job("")
        except (ApiError, TransportError) as error:
            if self.tolerate_rest(error):
                return reading
            raise
        for vertex in plan.get("vertices") or []:
            if not vertex.get("id"):
                continue
            metrics = connector_metrics(self.rest, vertex["id"])
            for item in metrics if isinstance(metrics, list) else ():
                name = metric_name(item.get("id", ""))
                if name in DEDUPLICATION_METRICS and type(item.get("sum")) in (
                    int,
                    float,
                ):
                    reading[name] = reading.get(name, 0) + item["sum"]
        return reading

    def track_suppression(self):
        """Keep the highest suppressed count any restored attempt reported.

        Read on every poll after the fault's first restore, because each
        attempt's counter starts at zero: a kill that restores twice would
        otherwise lose what the first restored attempts suppressed. The
        highest reading, not a sum, is kept: Flink answers metrics from a
        store that may still hold an earlier attempt's value, which a sum
        would count twice and a maximum only repeats, and the attempts before
        the fault restored no ledger, so their counter stays at zero. An
        attempt that ends between two polls can go unread, which errs towards
        inconclusive.
        """
        reading = self.deduplication_reading()
        count = reading.get("redeliveriesSuppressed")
        if self.first_restore is not None and count is not None:
            self.suppressed_samples += 1
            self.most_suppressed = max(self.most_suppressed, count)
        return reading

    def deduplication(self):
        """The restore-deduplication readings the verdict reads at completion.

        `redeliveriesSuppressed` is the highest count a restored attempt
        reported on any poll after the fault, including this one; the ledger
        readings are the serving attempts' at completion.
        """
        reading = self.track_suppression()
        reading["redeliveriesSuppressed"] = self.most_suppressed
        reading["suppressed_samples"] = self.suppressed_samples
        return reading

    def ids_preserved(self):
        """Whether each replay seen so far came under an input ID processed before.

        An input published twice can be processed under both IDs before the
        fault, and redelivery of either is a redelivery. Asked again at
        completion, because a replay collected after recovery was proven counts
        as much as one collected before.
        """
        expected = {tuple(identity) for identity in self.fault["expected_replay"]}
        original_ids = {}
        for o in self.collector.observations:
            # Collected by the fault, so processed before it: a surviving
            # attempt goes on processing, and a duplicate publication it takes
            # after the fault is not an ID the fault returned for redelivery.
            if (
                o["attempt"] in self.fault["before"]
                and o["at"] <= self.fault["at"]
                and self.in_cohort(REPLAY, o)
            ):
                original_ids.setdefault((o["input_index"], o["sequence"]), set()).add(
                    o["input_message_id"]
                )
        return all(
            o["input_message_id"]
            in original_ids.get((o["input_index"], o["sequence"]), ())
            for o in self.collector.observations
            if self.restarted(o)
            and self.in_cohort(REPLAY, o)
            and (o["input_index"], o["sequence"]) in expected
        )

    def finished(self, rest):
        if self.marker is None:
            self.marker = self.marked(LAST, rest, where=self.serving)
            return False
        cp = self.completed_after(rest, self.marker)
        if cp is None:
            return False
        self.outcomes["after"] = {
            "checkpoint": cp,
            "attempts": sorted(
                {
                    o["attempt"]
                    for o in self.collector.observations
                    if self.in_cohort(LAST, o)
                }
            ),
            # Every replay-cohort identity the attempts the fault started had
            # processed by the end, redelivered or republished alike; recovery
            # counted only the expected ones, at its first chance.
            "replay_by_new_attempts": len(
                _identities(
                    o
                    for o in self.collector.observations
                    if self.restarted(o) and self.in_cohort(REPLAY, o)
                )
            ),
            "replay_ids_preserved": self.ids_preserved(),
        }
        if self.adaptive:
            self.outcomes["after"]["deduplication"] = self.deduplication()
        # Decide the verdict over the record this transition writes, so it
        # reaches the `recovery-complete` evidence and not only the receipt,
        # and so the offline analysis can recompute it from that record.
        details = {
            "coverage": self._coverage(),
            "oracle": reconcile(
                self.collector.lines,
                self.env.approval.run_id,
                self.collector.records,
            ),
            # The Pod each attempt was announced by, which the Pod logs hold
            # too, kept beside the outcomes that name the attempts.
            "attempts": self.attempts,
        }
        decided = verdict(
            {"stage": COMPLETE_STAGE, "outcomes": self.outcomes, **details}
        )
        self.persist(COMPLETE_STAGE, observed=self.summary(), **details, **decided)
        return True
