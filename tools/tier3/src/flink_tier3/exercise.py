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
"""One approved savepoint upgrade and one UID-scoped JobManager disruption."""

from __future__ import annotations

import copy
import re

from .common import ApiError, Failure, TransportError, contains, digest, timestamp, utc
from .policy import RECOVERY, SMOKE, STATE


def validate_manifests(initial, upgrade):
    """An upgrade changes only the phase and the required-restoration flag."""
    expected = copy.deepcopy(initial)
    args = [
        "--run-id",
        initial["metadata"]["name"],
        "--phase",
        "initial",
        "--records",
        str(RECOVERY["records"]),
        "--records-per-second",
        str(RECOVERY["records_per_second"]),
        "--require-restored",
        "false",
    ]
    if initial["spec"]["job"]["args"] != args:
        raise Failure("Recovery input differs from the fixed payload")
    if (
        initial["spec"]["job"]["upgradeMode"] != "savepoint"
        or initial["spec"]["job"]["allowNonRestoredState"] is not False
        or initial["spec"]["flinkConfiguration"].get(
            "kubernetes.operator.job.upgrade.last-state-fallback.enabled"
        )
        != "false"
        or initial["spec"]["flinkConfiguration"].get(
            "kubernetes.operator.snapshot.resource.enabled"
        )
        != "false"
    ):
        raise Failure("Recovery requires savepoint upgrades without state fallback")
    args[3], args[-1] = "upgrade", "true"
    expected["spec"]["job"]["args"] = args
    if expected != upgrade:
        raise Failure("Recovery upgrade changes more than the approved job arguments")


class RecoveryExercise:
    """The supervisor advances this exercise once; recovery only cleans it up."""

    namespace = SMOKE
    state_bucket = STATE
    records = RECOVERY["records"]
    timing = RECOVERY
    progress_event = "smoke-progress"
    expected_pods = 2

    def __init__(self, env, upgrade):
        if (
            upgrade is None
            or digest(upgrade) != env.approval.upgrade_application_sha256
        ):
            raise Failure("Recovery requires the approved upgrade manifest")
        self.env, self.upgrade = env, upgrade
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

    @property
    def recovering(self):
        return self.stage in ("upgrade", "failover")

    def check_open(self):
        self.env.require_running("Recovery exercise has been stopped")
        if self.env.clock() >= min(self.deadline, self.env.schedule.cleanup_at):
            raise Failure("Recovery exercise deadline expired: " + self.stage)

    def persist(self, stage, **details):
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

    def jobmanager(self, pods):
        candidates = [
            p
            for p in pods
            if p["metadata"]["namespace"] == self.namespace
            and p["metadata"].get("labels", {}).get("component") == "jobmanager"
            and not p["metadata"].get("deletionTimestamp")
            and p.get("status", {}).get("phase") == "Running"
        ]
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
        if not path.startswith(
            f"gs://{self.state_bucket}/runs/{self.env.approval.run_id}/checkpoints/"
        ):
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
        if (
            not resumed
            or not cp
            or jm is None
            or jm["metadata"]["uid"] == self.before["jm_uid"]
        ):
            return None
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
                or not path.startswith(
                    f"gs://{self.state_bucket}/runs/{self.env.approval.run_id}/savepoints/"
                )
            ):
                raise Failure("Upgrade did not restore its newly completed savepoint")
        else:
            if (
                app["status"]["jobStatus"]["jobId"] != self.before["job_id"]
                or restored.get("is_savepoint") is not False
                or restored["id"] < self.before["checkpoint"]["id"]
                or not restored.get("external_path", "").startswith(
                    f"gs://{self.state_bucket}/runs/{self.env.approval.run_id}/checkpoints/"
                )
            ):
                raise Failure("JobManager did not recover the checkpointed job")
        return {
            "restored": restored,
            "checkpoint": cp,
            "progress": max(resumed, key=lambda s: s["processed"]),
            "jm_uid": jm["metadata"]["uid"],
        }

    def begin_upgrade(self, app, cp, progress, pods):
        jm = self.jobmanager(pods)
        if jm is None:
            return
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
        # Patch cannot recreate a deleted CR; the UID/version tests lose to deletion.
        try:
            self.env.kube.patch(
                app,
                [
                    {
                        "op": "replace",
                        "path": "/spec/job/args",
                        "value": self.upgrade["spec"]["job"]["args"],
                    }
                ],
            )
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

    def begin_failover(self, app, proof, pods):
        jm = self.jobmanager(pods)
        self.outcomes["upgrade"] = proof
        self.before = {
            "processed": max(s["processed"] for s in self.fresh_progress("upgrade")),
            "checkpoint": proof["checkpoint"],
            "jm_uid": jm["metadata"]["uid"],
            "job_id": app["status"]["jobStatus"]["jobId"],
        }
        self.boundary = self.env.clock()
        self.deadline = min(
            self.boundary + self.timing["recovery_seconds"],
            self.env.schedule.cleanup_at,
        )
        self.persist("failover", before=self.before)
        self.check_open()
        try:
            if self.env.kube.delete(jm) is False:
                raise Failure("JM disappeared before its planned deletion")
        except (ApiError, TransportError) as error:
            if isinstance(error, ApiError) and error.status not in (500, 502, 503, 504):
                raise
            current = self.env.kube.get("Pod", self.namespace, jm["metadata"]["name"])
            if (
                current
                and current["metadata"]["uid"] == jm["metadata"]["uid"]
                and not current["metadata"].get("deletionTimestamp")
            ):
                raise Failure(
                    "JM delete response lost without a verified outcome"
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
                self.begin_failover(app, proof, pods)
            elif proof:
                self.outcomes["failover"] = proof
                self.deadline = self.env.schedule.cleanup_at
                self.persist("finishing")
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
