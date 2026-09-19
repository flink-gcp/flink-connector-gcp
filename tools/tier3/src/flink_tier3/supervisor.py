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
from .cloudtasks import transient, verify_queue
from .common import ApiError, Failure, TransportError, contains, ha_metadata, utc
from .exercise import RecoveryExercise
from .model import Phase, cell_budget_seconds
from .policy import CLOUDTASKS, CLOUDTASKS_POLICY, MIB, NONCE, POLL, SMOKE


class SessionHooks:
    """Seams for the evidence collector and observers; no-ops until they exist."""

    def poll(self, session, cell, app, pods):
        """Observe one cell once per poll while its job runs."""

    def after_cell(self, session, cell, outcome):
        """Collect a cell's evidence after its workload is gone."""

    def at_session_end(self, session, outcomes):
        """Collect whatever the per-cell pass left behind."""


class HookChain(SessionHooks):
    """Run several session hooks in order; a hook's failure stops the chain."""

    def __init__(self, *hooks):
        self.hooks = hooks

    def poll(self, session, cell, app, pods):
        for hook in self.hooks:
            hook.poll(session, cell, app, pods)

    def after_cell(self, session, cell, outcome):
        for hook in self.hooks:
            hook.after_cell(session, cell, outcome)

    def at_session_end(self, session, outcomes):
        for hook in self.hooks:
            hook.at_session_end(session, outcomes)


class Supervisor:
    """Observe the workload, run an approved exercise, and return it to idle."""

    def __init__(self, env, upgrade=None, cells=None, hooks=None):
        self.env = env
        self.cleanup = Cleanup(env)
        self.log_bytes = 0
        self.log_since = {}
        self.log_stopped = False
        self.exercise = (
            RecoveryExercise(env, upgrade)
            if env.approval.scenario == "generic-recovery"
            else None
        )
        self.session = (
            CellSession(self, cells, hooks)
            if env.approval.scenario == "cloudtasks"
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
            if self.log_stopped or pod.get("status", {}).get("phase") not in (
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
            truncated = len(data) >= (65536 if since else MIB)
            exhausted = self.log_bytes > self.cleanup.ceilings["log_bytes"]
            if (truncated or exhausted) and not self.session:
                raise Failure(
                    "Log collection ceiling reached; stop instead of silently truncating"
                )
            if truncated:
                # Cloud Tasks evidence rows travel through storage, not Pod
                # logs; a truncated Flink log is recorded, not fatal.
                self.env.emit("pod-log-truncated", {"uid": uid, "bytes": len(data)})
            if exhausted:
                self.env.emit("pod-log-collection-stopped", {"bytes": self.log_bytes})
                self.log_stopped = True
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
                if self.session:
                    return control.phase == Phase.RUNNING and bool(control.queue)
                return control.phase == Phase.RUNNING and bool(
                    control.roots.get("application")
                )

            self.env.wait(
                admitted,
                self.exercise.deadline
                if self.exercise
                else self.env.schedule.readiness_until(self.env.clock())
                if self.session
                else self.env.schedule.cleanup_at,
            )
            if self.session:
                success, reason = self.session.run()
            else:
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
                                    "Service",
                                    SMOKE,
                                    service["metadata"]["name"] + ":8081",
                                )
                                + "/proxy/jobs/"
                                + job_id
                                + "/checkpoints",
                            )
                        except Failure as error:
                            if not self.exercise or not self.exercise.tolerate_rest(
                                error
                            ):
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


class CellSession:
    """Run the approved cells one at a time and return each to an empty namespace."""

    def __init__(self, supervisor, manifests, hooks=None):
        self.supervisor, self.env = supervisor, supervisor.env
        self.cleanup = supervisor.cleanup
        self.approval = self.env.approval
        if manifests is None or len(manifests) != len(self.approval.cells):
            raise Failure("Cloud Tasks session requires one manifest per approved cell")
        self.manifests = manifests
        self.hooks = hooks or SessionHooks()
        self.meter = self.env.queues.meter
        self.outcomes = {}
        self.current = None
        self.job_id = None
        self.queue_read_failures = 0
        # Observed by the poll hook: the last successful queue readback and the
        # inventory and Service of the current poll.
        self.last_queue = None
        self.last_items = None
        self.last_service = None

    def check_open(self):
        self.env.require_running("Session has been stopped")

    def run(self):
        for index, (manifest, cell) in enumerate(
            zip(self.manifests, self.approval.cells, strict=True)
        ):
            now = self.env.clock()  # one reading for the credit and the check
            needed = cell_budget_seconds(cell)
            if index == 0:
                # Time since the approval's start (the runner's admission) was
                # taken from the first cell's startup allowance; credit it back,
                # never more than that allowance.
                spent = max(0, now - self.env.schedule.started)
                needed -= min(CLOUDTASKS_POLICY["cell_startup_seconds"], spent)
            if now + needed > self.env.schedule.cleanup_at:
                self.outcomes[cell["id"]] = "skipped"
                self.env.records.cell_done(
                    cell["id"], "skipped", "session window exhausted"
                )
                self.env.emit("cell-skipped", {"cell": cell["id"]})
                continue
            self.execute(cell, manifest)
        self.hooks.at_session_end(self, dict(self.outcomes))
        complete = len(self.outcomes) == len(self.approval.cells) and all(
            outcome == "completed" for outcome in self.outcomes.values()
        )
        return complete, "session finished" if complete else "session incomplete"

    def key(self, cell):
        return "cell:" + cell["id"]

    def execute(self, cell, manifest):
        self.check_open()
        self.recheck_queue()
        key = self.key(cell)
        if key in self.env.roots:
            raise Failure("Cell was already admitted in this session")
        self.env.ledger.claim(
            cell["id"], self.approval.run_id, self.approval.nonce, self.env.clock()
        )
        self.env.records.intend(
            "cell", {"cell": cell["id"], "manifest_sha256": cell["manifest_sha256"]}
        )
        if self.env.kube.get("FlinkDeployment", CLOUDTASKS, cell["id"]):
            raise Failure("Cell application name already exists")
        self.env.kube.create(manifest, dry_run=True)
        self.check_open()
        try:
            created = self.env.kube.create(manifest)
        except Failure:
            created = self.adopt(cell, manifest)
            if created is None:
                raise
        self.env.remember(key, created)
        self.cleanup.current_cell = cell["id"]
        self.current, self.job_id = cell, None
        self.env.emit("cell-start", {"cell": cell["id"], "arm": cell["arm"]})
        deadline = self.env.schedule.cell_deadline(self.env.clock(), cell)
        outcome, reason = self.observe(cell, key, deadline)
        self.teardown(cell, key)
        self.env.ledger.settle(
            cell["id"],
            self.approval.run_id,
            self.approval.nonce,
            outcome,
            reason,
            self.env.clock(),
        )
        self.env.records.cell_done(cell["id"], outcome, reason, self.meter.snapshot())
        self.hooks.after_cell(self, cell, outcome)
        self.outcomes[cell["id"]] = outcome
        self.current, self.job_id = None, None
        self.env.emit(
            "cell-finished",
            {"cell": cell["id"], "outcome": outcome, "reason": reason},
        )

    def adopt(self, cell, manifest):
        obj = self.env.kube.get("FlinkDeployment", CLOUDTASKS, cell["id"])
        if not obj:
            return None
        if obj["metadata"].get("annotations", {}).get(NONCE) != self.approval.nonce:
            raise Failure("Creation outcome is uncertain and cell nonce differs")
        if not contains(obj["spec"], manifest["spec"]):
            raise Failure("Observed cell differs from the delivered manifest")
        return obj

    def service(self, items, key, cell):
        """The cell's REST Service, proven to descend from the cell's root UID."""
        service = self.env.kube.get("Service", CLOUDTASKS, cell["id"] + "-rest")
        if not service or service["metadata"]["uid"] not in self.cleanup.owned(
            items, [key]
        ):
            raise Failure("REST service has no verified workload owner")
        return service

    def rest(self, service, path, limit=MIB):
        """Read one Flink REST path of the current job through its Service."""
        self.meter.tick("read_ops")
        return self.env.kube.request(
            "GET",
            self.env.kube.path(
                "Service", CLOUDTASKS, service["metadata"]["name"] + ":8081"
            )
            + "/proxy/jobs/"
            + self.job_id
            + path,
            limit=limit,
        )

    def recheck_queue(self):
        """Re-read the queue; a few transient read failures are recorded, not fatal."""
        try:
            readback = verify_queue(self.env)
        except Failure as error:
            self.last_queue = None
            if not transient(error) or self.queue_read_failures >= 2:
                raise
            self.queue_read_failures += 1
            self.env.emit("queue-read-unavailable", {"cause": str(error)})
            return
        self.queue_read_failures = 0
        self.last_queue = readback

    def observe(self, cell, key, deadline):
        own_deadline = deadline >= self.env.schedule.cleanup_at
        while self.env.clock() < deadline:
            self.check_open()
            self.env.records.heartbeat(self.meter.snapshot())
            items, pods = self.cleanup.audit()
            self.supervisor.telemetry(items, pods)
            self.recheck_queue()
            app = self.env.root(key)
            if not app:
                raise Failure("Cell application disappeared before completion")
            job = app.get("status", {}).get("jobStatus", {})
            status, job_id = job.get("state", ""), job.get("jobId", "")
            checkpoints, service = {}, None
            if status in ("RUNNING", "FINISHED") and re.fullmatch(
                r"[0-9a-f]{32}", job_id
            ):
                self.job_id = job_id
                service = self.service(items, key, cell)
                try:
                    checkpoints = self.rest(service, "/checkpoints")
                except (ApiError, TransportError) as error:
                    # A restarting JobManager answers late; the cell deadline,
                    # not one failed read, bounds the wait.
                    self.env.emit("cell-rest-unavailable", {"cause": str(error)})
            self.env.emit(
                "cell-status",
                {
                    "cell": cell["id"],
                    "state": status,
                    "job_id": job_id,
                    "counts": checkpoints.get("counts", {}),
                    "latest": checkpoints.get("latest", {}),
                },
            )
            self.last_items, self.last_service = items, service
            self.hooks.poll(self, cell, app, pods)
            if status == "FINISHED":
                return "completed", "finished"
            if status in ("FAILED", "CANCELED", "SUSPENDED"):
                return "failed", "Flink job entered " + status
            self.env.sleep(POLL)
        return "failed", "session window" if own_deadline else "cell deadline"

    def teardown(self, cell, key):
        app = self.env.root(key)
        if app:
            self.env.kube.delete(app)

        def gone():
            items = self.cleanup.inventory()
            owned = self.cleanup.owned(items, [key])
            return not any(
                obj["metadata"]["uid"] in owned
                or ha_metadata(obj, cell["id"], CLOUDTASKS)
                for obj in items
            )

        try:
            self.env.wait(
                gone, self.env.clock() + CLOUDTASKS_POLICY["cell_teardown_seconds"]
            )
        except Failure as error:
            raise Failure("Cell workload remains after its teardown window") from error
        self.cleanup.clean_state([cell["id"]])
        self.cleanup.current_cell = None
