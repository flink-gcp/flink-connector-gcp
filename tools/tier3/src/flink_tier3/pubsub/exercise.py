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

from ..common import Failure, contains, utc
from ..exercise import RecoveryExercise
from ..metrics import sample, subset, unavailable
from ..policy import PUBSUB, PUBSUB_EXERCISE, PUBSUB_STATE
from .messages import COHORTS, cohort_ranges
from .observe import connector_metrics
from .oracle import reconcile
from .output import OutputCollector
from .verdict import (
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
# The cohorts the supervisor has requested by each stage; the first is
# published at admission.
REQUESTED = {
    "baseline": (FIRST,),
    "checkpoint": (FIRST,),
    "boundary": (FIRST, REPLAY),
    "recovering": (FIRST, REPLAY),
    "after": COHORTS,
}
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
    records_coverage = True
    state_bucket = PUBSUB_STATE
    timing = PUBSUB_EXERCISE

    def __init__(self, env, upgrade, handoff):
        if env.approval.scenario != "pubsub-recovery":
            raise Failure("Pub/Sub exercise requires a Pub/Sub approval")
        super().__init__(env, upgrade)
        self.kind = env.approval.pubsub_trial["trial"]
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

    def attach_rest(self, service, job_id):
        self.track_rest(service, job_id)

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
        if self.kind == "jm-replacement":
            return self.jobmanager(pods)
        counts = {}
        for item in replayed:
            owner = self.attempts.get(item["attempt"])
            if owner:
                counts[owner["pod_uid"]] = counts.get(owner["pod_uid"], 0) + 1
        managers = [
            pod
            for pod in pods
            if pod["metadata"]["namespace"] == self.namespace
            and pod["metadata"].get("labels", {}).get("component") == "taskmanager"
            and not pod["metadata"].get("deletionTimestamp")
            and pod.get("status", {}).get("phase") == "Running"
            and pod["metadata"]["uid"] in counts
        ]
        if not managers:
            return None
        return max(managers, key=lambda pod: counts[pod["metadata"]["uid"]])

    def inject(self, pods, rest):
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
                if self.kind == "jm-replacement"
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
            self.delete_pod(pod, "JM" if self.kind == "jm-replacement" else "TM")
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

    def restored(self, app, rest, pods):
        """What the job restored after the fault, or None while it has not."""
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
            if self.kind == "jm-replacement" and (
                jm["metadata"]["uid"] == self.fault["jm_uid"]
            ):
                return None
            if self.kind == "tm-replacement" and any(
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
