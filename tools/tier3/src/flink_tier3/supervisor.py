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
"""Tier-3 lifecycle supervisor."""

from __future__ import annotations

import math
import re
import uuid

from .bigquery.exercise import BigQueryExercise, require_handoff
from .cleanup import Cleanup
from .common import ApiError, Failure, transient, utc
from .model import Phase
from .policy import MIB, POLL, SMOKE
from .pubsub.exercise import PubSubExercise
from .pubsub.guard import PubSubGuard, admission_deadline
from .pubsub.handoff import runner_released


class Supervisor:
    """Observe the workload, run an approved exercise, and return it to idle."""

    def __init__(
        self,
        env,
        upgrade=None,
        *,
        bigquery=None,
        pubsub=None,
        quiesce=None,
    ):
        self.env = env
        self.bigquery, self.pubsub = bigquery, pubsub
        self.cleanup = Cleanup(env, bigquery=bigquery, pubsub=pubsub, quiesce=quiesce)
        self.log_since = {}
        self.pod_read_failures = {}
        self.exercise = (
            BigQueryExercise(env, upgrade)
            if env.approval.scenario == "bigquery-recovery" and upgrade is not None
            else PubSubExercise(env, upgrade, pubsub)
            if env.approval.scenario == "pubsub-recovery"
            and upgrade is not None
            and pubsub is not None
            else None
        )

    def query(self, name, *, deadline):
        """Wait for runner-exported evidence while keeping the supervisor alive.

        The scenario chooses observation names, deadlines and result criteria;
        receiving a query result alone does not establish trial success.
        Repeating a request retains its original name and deadline. An expired
        observation aborts the trial rather than opening a new retry window.
        """
        if self.bigquery is None:
            raise Failure("BigQuery observation requires an attached handoff")
        if (
            type(deadline) not in (int, float)
            or not math.isfinite(deadline)
            or deadline > self.env.schedule.cleanup_at
        ):
            raise Failure("BigQuery observation exceeds the execution window")
        self.bigquery.request(name, deadline=deadline)
        result = None

        def collected():
            nonlocal result
            self.env.require_running("BigQuery observation has been stopped")
            self.env.records.heartbeat()
            self.cleanup.audit()
            result = self.bigquery.result(name)
            return result is not None

        self.env.wait(collected, deadline)
        return result

    def connector_jobs(self, *, deadline):
        """This run's connector-issued BigQuery jobs that are not DONE yet."""
        if self.bigquery is None:
            raise Failure("BigQuery observation requires an attached handoff")
        return self.bigquery.connector_jobs(deadline=deadline)

    def telemetry(self, items, pods):
        observation = [
            {
                "kind": obj["kind"],
                "metadata": {
                    k: obj["metadata"].get(k)
                    for k in ("namespace", "name", "uid", "ownerReferences")
                },
                "status": obj.get("status", {}),
            }
            for obj in items
        ]
        if self.exercise:
            for saved, obj in zip(observation, items, strict=True):
                if obj["kind"] == "Pod":
                    spec = obj.get("spec", {})
                    saved["scheduling"] = {
                        "node": spec.get("nodeName"),
                        "selector": spec.get("nodeSelector", {}),
                        "created_at": obj["metadata"].get("creationTimestamp"),
                        "deleting_at": obj["metadata"].get("deletionTimestamp"),
                        "containers": [
                            {k: c.get(k) for k in ("name", "image", "resources")}
                            for c in spec.get("containers", [])
                        ],
                    }
        self.env.emit("inventory", observation)
        for pod in pods:
            if pod.get("status", {}).get("phase") not in (
                "Running",
                "Succeeded",
                "Failed",
            ):
                continue
            uid = pod["metadata"]["uid"]
            before = utc(self.env.clock())
            since = self.log_since.get(uid)
            try:
                data = self.env.kube.logs(pod, since)
            except ApiError as error:
                # A Pod can be replaced between the inventory listing and this
                # read; a preempted Operator is the measured case. What makes
                # the 404 benign is the Pod being gone, or on its way out,
                # and not the scenario.
                if error.status != 404:
                    raise
                try:
                    current = self.env.kube.get(
                        "Pod", pod["metadata"]["namespace"], pod["metadata"]["name"]
                    )
                except Failure as unresolved:
                    # That re-read is the whole guard, so a re-read saying
                    # nothing about the Pod is not proof the Pod is alive. The
                    # next poll lists the namespace again and settles it. The
                    # allowance is this Pod's: another Pod answering cannot
                    # buy it a further poll, and cannot spend its own.
                    if (
                        not transient(unresolved)
                        or self.pod_read_failures.get(uid, 0) >= 2
                    ):
                        raise
                    self.pod_read_failures[uid] = self.pod_read_failures.get(uid, 0) + 1
                    self.env.emit("pod-presence-unavailable", {"uid": uid})
                    continue
                self.pod_read_failures.pop(uid, None)
                if (
                    current
                    and current["metadata"]["uid"] == uid
                    and not current["metadata"].get("deletionTimestamp")
                ):
                    raise
                self.env.emit(
                    "retired-pod-log-unavailable",
                    {"uid": uid, "namespace": pod["metadata"]["namespace"]},
                )
                continue
            self.pod_read_failures.pop(uid, None)
            if len(data) >= MIB:
                raise Failure(
                    "Log read would truncate; stop instead of discarding evidence"
                )
            decoded = data.decode(errors="replace")
            self.env.emit("pod-log", {"uid": uid, "text": decoded})
            if pod["metadata"]["namespace"] == SMOKE:
                self.inspect_progress(decoded)
            if (
                self.exercise
                and self.exercise.namespace != SMOKE
                and pod["metadata"]["namespace"] == self.exercise.namespace
            ):
                self.exercise.progress(pod, decoded)
            self.log_since[uid] = before
        listed = {pod["metadata"]["uid"] for pod in pods}
        self.pod_read_failures = {
            uid: count for uid, count in self.pod_read_failures.items() if uid in listed
        }
        if self.env.evidence_failed:
            raise Failure(
                "Durable evidence export failed: "
                + self.env.evidence_failure["operation"]
            )

    def inspect_progress(self, text):
        # SmokeVerifier emits these fields through the application's print sink.
        pattern = (
            r"event=smoke-progress run_id=(\S+) phase=(\S+) lineage=(\S+) "
            r"restored=(true|false) processed=([0-9]+) sequence=([0-9]+)"
        )
        previous = self.env.refresh().lineage
        for match in re.finditer(pattern, text):
            run_id, phase, lineage, _restored, _processed, _sequence = match.groups()
            if run_id != self.env.approval.run_id or phase != "initial":
                raise Failure("Smoke progress belongs to another run or phase")
            try:
                valid = str(uuid.UUID(lineage)) == lineage
            except ValueError:
                valid = False
            if not valid:
                raise Failure("Smoke progress contains an invalid lineage")
            if previous is not None and previous != lineage:
                raise Failure("Smoke restarted with an unexpected fresh lineage")
            if previous is None:
                self.env.records.record_lineage(lineage)
                previous = lineage

    def cancelled(self, control):
        """Raise when this run is being stopped, by a signal or its record."""
        if (
            self.env.stopping
            or self.env.evidence_failed
            or control.evidence_failed
            or control.stop_requested
            or control.phase in (Phase.CLEANING, Phase.CLEANED)
        ):
            raise Failure("Cancellation or recovery requested")

    def participate(self):
        """Join the Pub/Sub handoff once preparation completes, then probe access."""
        deadline = admission_deadline(self.env)

        def prepared():
            control = self.env.refresh()
            self.cancelled(control)
            self.env.records.heartbeat()
            state = control.pubsub or {}
            return (
                control.phase in (Phase.READY, Phase.RUNNING)
                and state.get("stage") == "prepared"
            )

        self.env.wait(prepared, deadline)
        self.pubsub.join()
        self.pubsub.access(deadline=deadline)

    def supervise(self, pod_uid):
        if self.env.approval.scenario == "pubsub-recovery" and (
            self.pubsub is None
            or self.exercise is None
            or self.cleanup.quiesce is None
            or not isinstance(self.pubsub.controller.before_operation, PubSubGuard)
            or self.pubsub.settled is None
        ):
            raise Failure(
                "Pub/Sub recovery supervision requires its authenticated handoff "
                "and its approved recovery manifest"
            )
        if self.env.approval.scenario == "bigquery-recovery":
            if self.exercise is None or self.bigquery is None:
                raise Failure(
                    "BigQuery recovery supervision requires its authenticated exercise and handoff"
                )
            require_handoff(self.env, self.bigquery)
        self.env.refresh()
        job = self.env.root("supervisor")
        if not job:
            raise Failure("Supervisor Job has no recorded UID")
        own = self.cleanup.owned(self.cleanup.inventory(), ["supervisor"])
        if pod_uid not in own:
            raise Failure("Supervisor Pod is not owned by the approved Job")
        reason, success = "interrupted", False
        try:
            if self.pubsub is not None:
                self.participate()

            def admitted():
                control = self.env.refresh()
                self.cancelled(control)
                self.env.records.heartbeat()
                return control.phase == Phase.RUNNING and bool(
                    control.roots.get("application")
                )

            if self.exercise:
                admitted_by = self.exercise.deadline
            else:
                admitted_by = self.env.schedule.cleanup_at
            self.env.wait(admitted, admitted_by)
            if self.exercise:
                self.exercise.admitted()
            while self.env.clock() < self.env.schedule.cleanup_at:
                control = self.env.refresh()
                if self.env.stopping or control.stop_requested:
                    raise Failure("Cancellation or recovery requested")
                self.env.records.heartbeat()
                items, pods = self.cleanup.audit()
                if self.exercise:
                    self.exercise.check_open()
                    self.exercise.scheduling(pods)
                self.telemetry(items, pods)
                app = self.env.root("application")
                if not app:
                    raise Failure("Application disappeared before completion")
                status = app.get("status", {}).get("jobStatus", {}).get("state", "")
                job_id = app.get("status", {}).get("jobStatus", {}).get("jobId", "")
                rest, service = {}, None
                if status in ("RUNNING", "FINISHED") and re.fullmatch(
                    r"[0-9a-f]{32}", job_id
                ):
                    service = self.env.kube.get(
                        "Service",
                        self.env.approval.application_namespace,
                        self.env.approval.run_id + "-rest",
                    )
                    if not service and self.exercise and self.exercise.recovering:
                        service = None
                    elif not service or service["metadata"][
                        "uid"
                    ] not in self.cleanup.owned(items, ["application"]):
                        raise Failure("REST service has no verified workload owner")
                    try:
                        if not service:
                            raise ApiError(404, "GET", "REST Service")
                        rest = self.env.kube.request(
                            "GET",
                            self.env.kube.path(
                                "Service",
                                self.env.approval.application_namespace,
                                service["metadata"]["name"] + ":8081",
                            )
                            + "/proxy/jobs/"
                            + job_id
                            + "/checkpoints",
                        )
                    except Failure as error:
                        if not self.exercise or not self.exercise.tolerate_rest(error):
                            raise
                        self.env.emit(
                            "recovery-rest-unavailable", {"cause": str(error)}
                        )
                    self.env.emit(
                        "checkpoints",
                        {"job_id": job_id, **rest} if self.exercise else rest,
                    )
                    if rest.get("counts", {}).get("completed", 0) > 0:
                        self.env.records.checkpoint()
                if self.exercise:
                    self.exercise.attach_rest(service, job_id)
                    if self.exercise.observe(app, rest, pods):
                        if isinstance(self.exercise, BigQueryExercise):
                            self.exercise.verify_rows(self)
                        success, reason = True, "recovery exercise finished"
                        break
                    self.env.sleep(POLL)
                    continue
                if status == "FINISHED":
                    if not self.env.refresh().checkpoint_observed:
                        raise Failure(
                            "Finished without an observed completed checkpoint"
                        )
                    if not self.env.records.cache.lineage:
                        raise Failure(
                            "Finished without observed smoke lineage evidence"
                        )
                    success, reason = True, "finished"
                    break
                if status in ("FAILED", "CANCELED", "SUSPENDED"):
                    raise Failure("Flink job entered " + status)
                self.env.sleep(POLL)
            else:
                reason = "admission/test deadline"
        except (Failure, OSError, ValueError) as error:
            reason = str(error)
        finally:
            self.conclude(reason, success)

    def runner_released(self):
        return runner_released(self.pubsub.actors())

    def conclude(self, reason, success):
        """Hand the run on, stop its admission, or return it to idle."""
        control = self.env.refresh()
        if control.phase in (Phase.APPROVED, Phase.READY):
            # Admission may still have an in-flight Kubernetes write. The
            # runner settles after start returns; completed-execution
            # recovery handles a runner that disappears before this handoff.
            if self.env.evidence_failed:
                self.env.records.mark_evidence_failed(self.env.evidence_failure)
            self.env.records.request_stop()
            # The reason otherwise survives only in this process, which
            # may end in the wait below without reporting it. The stop is
            # durable first, so this write cannot delay it.
            self.env.emit("admission-stopped", {"reason": reason})
            current = self.env.refresh()
            if self.env.approval.scenario == "pubsub-recovery" and current.pubsub:
                # Only this side deletes Pub/Sub resources, so it waits for the
                # runner to settle and release rather than leaving them.
                self.env.wait(self.runner_released, self.env.schedule.cleanup_at)
            elif (
                self.env.approval.scenario != "bigquery-recovery"
                or current.bigquery is None
            ):
                raise Failure(
                    "Admission unfinished; runner settlement required: " + reason
                )
            else:
                # The original runner releases only from settlement, after
                # start() has returned. Let admission record confirmed creation
                # before teardown; release does not settle an unknown outcome.
                self.env.wait(self.bigquery.released, self.env.schedule.cleanup_at)
        self.cleanup.run(reason, success)
