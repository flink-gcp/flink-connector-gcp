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

import re
import uuid

from .cleanup import Cleanup
from .common import ApiError, Failure, utc
from .exercise import RecoveryExercise
from .model import Phase
from .policy import CEILINGS, MIB, POLL, SMOKE


class Supervisor:
    """Observe the workload, run an approved exercise, and return it to idle."""

    def __init__(self, env, upgrade=None):
        self.env = env
        self.cleanup = Cleanup(env)
        self.log_bytes = 0
        self.log_since = {}
        self.exercise = (
            RecoveryExercise(env, upgrade)
            if env.approval.scenario == "generic-recovery"
            else None
        )

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
                if (
                    not self.exercise
                    or not self.exercise.recovering
                    or error.status != 404
                ):
                    raise
                current = self.env.kube.get(
                    "Pod", pod["metadata"]["namespace"], pod["metadata"]["name"]
                )
                if (
                    current
                    and current["metadata"]["uid"] == uid
                    and not current["metadata"].get("deletionTimestamp")
                ):
                    raise
                self.env.emit("retired-pod-log-unavailable", {"uid": uid})
                continue
            self.log_bytes += len(data)
            if (
                len(data) >= (65536 if since else MIB)
                or self.log_bytes > CEILINGS["log_bytes"]
            ):
                raise Failure(
                    "Log collection ceiling reached; stop instead of silently truncating"
                )
            decoded = data.decode(errors="replace")
            self.env.emit("pod-log", {"uid": uid, "text": decoded})
            if pod["metadata"]["namespace"] == SMOKE:
                self.inspect_progress(decoded)
                if self.exercise:
                    self.exercise.progress(pod, decoded)
            self.log_since[uid] = before
        if self.env.evidence_failed:
            raise Failure("Durable evidence export failed")

    def inspect_progress(self, text):
        # SmokeVerifier emits these fields through the application's print sink.
        pattern = (
            r"event=smoke-progress run_id=(\S+) phase=(\S+) lineage=(\S+) "
            r"restored=(true|false) processed=([0-9]+) sequence=([0-9]+)"
        )
        previous = self.env.refresh().lineage
        for match in re.finditer(pattern, text):
            run_id, phase, lineage, _restored, _processed, _sequence = match.groups()
            if run_id != self.env.approval.run_id or phase not in (
                ("initial", "upgrade") if self.exercise else ("initial",)
            ):
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

    def supervise(self, pod_uid):
        self.env.refresh()
        job = self.env.root("supervisor")
        if not job:
            raise Failure("Supervisor Job has no recorded UID")
        own = self.cleanup.owned(self.cleanup.inventory(), ["supervisor"])
        if pod_uid not in own:
            raise Failure("Supervisor Pod is not owned by the approved Job")
        reason, success = "interrupted", False
        try:

            def admitted():
                control = self.env.refresh()
                if (
                    self.env.stopping
                    or self.env.evidence_failed
                    or control.evidence_failed
                    or control.stop_requested
                    or control.phase in (Phase.CLEANING, Phase.CLEANED)
                ):
                    raise Failure("Cancellation or recovery requested")
                self.env.records.heartbeat()
                return control.phase == Phase.RUNNING and bool(
                    control.roots.get("application")
                )

            self.env.wait(
                admitted,
                self.exercise.deadline
                if self.exercise
                else self.env.schedule.cleanup_at,
            )
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
                rest = {}
                if status in ("RUNNING", "FINISHED") and re.fullmatch(
                    r"[0-9a-f]{32}", job_id
                ):
                    service = self.env.kube.get(
                        "Service", SMOKE, self.env.approval.run_id + "-rest"
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
                                "Service", SMOKE, service["metadata"]["name"] + ":8081"
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
                    if self.exercise.observe(app, rest, pods):
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
            control = self.env.refresh()
            if control.phase in (Phase.APPROVED, Phase.READY):
                # Admission may still have an in-flight Kubernetes write. The
                # runner settles after start returns; completed-execution
                # recovery handles a runner that disappears before this handoff.
                if self.env.evidence_failed:
                    self.env.records.mark_evidence_failed()
                self.env.records.request_stop()
                raise Failure(
                    "Admission unfinished; runner settlement required: " + reason
                )
            self.cleanup.run(reason, success)
