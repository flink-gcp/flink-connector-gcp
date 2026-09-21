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
"""External admission, settlement and finalization for the Tier-3 lifecycle."""

import flink_tier3 as rt

from .bigquery_exercise import require_handoff
from .bigquery_handoff import require_bigquery_clean
from .cloudtasks import admission_budget_open, admit_queue
from .policy import BIGQUERY_OBSERVATIONS, RECOVERY
from .pubsub_lifecycle import require_pubsub_clean


class Runner:
    def __init__(self, env, bigquery=None, *, pubsub=None):
        if bigquery is not None and bigquery.env is not env:
            raise rt.Failure("BigQuery handoff belongs to another runner environment")
        if pubsub is not None and (
            bigquery is not None
            or pubsub.env is not env
            or env.actor != "runner"
            or env.approval.scenario != "pubsub-recovery"
        ):
            raise rt.Failure("Pub/Sub handoff requires its original runner environment")
        self.env = env
        self.env.actor = "runner"
        self.bigquery, self.pubsub = bigquery, pubsub
        self.cleanup = rt.Cleanup(env)

    @property
    def cloudtasks(self):
        return self.env.approval.scenario == "cloudtasks"

    @property
    def namespace(self):
        return self.env.approval.application_namespace

    def admission_open(self):
        self.env.admission_open()
        if (
            self.env.approval.scenario == "generic-recovery"
            and self.env.clock()
            >= self.env.schedule.started + RECOVERY["startup_seconds"]
        ):
            raise rt.Failure("Recovery scenario startup deadline expired")
        if (
            self.env.approval.scenario == "bigquery-recovery"
            and self.env.clock()
            >= self.env.schedule.started + BIGQUERY_OBSERVATIONS["startup_seconds"]
        ):
            raise rt.Failure("BigQuery startup deadline expired")
        # Admission may spend at most one cell's startup allowance, so the
        # first cell keeps the budget the session plan reserved for it.
        if self.cloudtasks:
            admission_budget_open(self.env)

    def create_application(self, application):
        if rt.digest(application) != self.env.approval.application_sha256:
            raise rt.Failure("Application differs from the approved manifest")
        self.admission_open()
        if self.env.kube.get(
            "FlinkDeployment", self.namespace, self.env.approval.run_id
        ):
            raise rt.Failure("Application name already exists")
        self.env.kube.create(application, dry_run=True)
        self.admission_open()
        # Persist the exact creation intent before the non-transactional API call.
        self.env.records.intend("application")
        try:
            created = self.env.kube.create(application)
        except rt.Failure:
            self.adopt_application(application)
            raise
        self.env.remember("application", created)

    def adopt_application(self, application):
        self.env.namespaces()
        if not self.env.refresh().application_intent:
            raise rt.Failure("Application creation intent was not persisted")
        obj = self.env.kube.get(
            "FlinkDeployment", self.namespace, self.env.approval.run_id
        )
        if not obj:
            return
        meta = obj["metadata"]
        if meta.get("annotations", {}).get(rt.NONCE) != self.env.approval.nonce:
            raise rt.Failure(
                "Creation outcome is uncertain and application nonce differs"
            )
        if rt.digest(application) != self.env.approval.application_sha256:
            raise rt.Failure("Creation intent does not match approval")

        # Compare every submitted field; the API may add CRD defaults.
        if not rt.contains(obj["spec"], application["spec"]):
            raise rt.Failure(
                "Observed application differs from the persisted creation intent"
            )
        self.env.remember("application", obj)

    def create_root(self, key, manifest):
        self.admission_open()
        if self.env.kube.get(manifest["kind"], rt.SYSTEM, manifest["metadata"]["name"]):
            raise rt.Failure("Temporary resource name already exists")
        intent = manifest
        if key == "config":
            # Keep repeated control CAS independent of the embedded source size.
            intent = {k: value for k, value in manifest.items() if k != "data"}
            intent["data_sha256"] = rt.digest(manifest.get("data", {}))
        self.env.records.intend(key, intent)
        try:
            self.env.remember(key, self.env.kube.create(manifest))
        except rt.Failure:
            self.adopt_root(key)
            raise

    def adopt_root(self, key):
        control = self.env.refresh()
        if key in control.roots:
            return
        manifest = getattr(control, key + "_intent")
        if not manifest:
            return
        self.env.namespaces()
        obj = self.env.kube.get(
            manifest["kind"], rt.SYSTEM, manifest["metadata"]["name"]
        )
        if not obj:
            return
        expected = dict(manifest)
        data_sha256 = expected.pop("data_sha256", None)
        if (
            data_sha256 is not None and rt.digest(obj.get("data", {})) != data_sha256
        ) or not rt.contains(obj, expected):
            raise rt.Failure("Temporary object differs from persisted creation intent")
        self.env.remember(key, obj)

    def start(self, config, job, application):
        if self.env.approval.scenario == "pubsub-recovery":
            raise rt.Failure("Pub/Sub execution admission is not implemented")
        if self.env.approval.scenario == "bigquery-recovery":
            if self.bigquery is None:
                raise rt.Failure(
                    "BigQuery execution admission is not implemented without an explicit handoff"
                )
            require_handoff(self.env, self.bigquery)
            if rt.digest(application) != self.env.approval.application_sha256:
                raise rt.Failure("Application differs from the approved manifest")
            self.admission_open()
        self.env.refresh()
        self.create_root("config", config)
        self.admission_open()
        self.cleanup.quota(rt.SYSTEM, "supervisor")
        self.create_root("supervisor", job)
        self.env.records.set_phase(rt.Phase.READY)

        def supervisor_ready():
            self.admission_open()
            current = self.env.root("supervisor")
            if not current or self.job_completed(current):
                raise rt.Failure("Supervisor stopped before application admission")
            items, pods = self.cleanup.audit()
            owned = self.cleanup.owned(items, ["supervisor"])
            return self.env.refresh().heartbeat and any(
                pod["metadata"]["uid"] in owned
                and pod.get("status", {}).get("phase") == "Running"
                for pod in pods
            )

        self.env.wait(
            supervisor_ready, self.env.schedule.readiness_until(self.env.clock())
        )
        self.admission_open()
        self.cleanup.quota(rt.SYSTEM, "run")
        self.admission_open()
        self.cleanup.scale_operator(1)

        def operator_ready():
            self.admission_open()
            self.cleanup.audit()
            job = self.env.root("supervisor")
            if not job or self.job_completed(job):
                raise rt.Failure("Supervisor stopped before application admission")
            operator = self.env.kube.get("Deployment", rt.SYSTEM, rt.OPERATOR)
            return operator.get("status", {}).get("readyReplicas", 0) == 1

        self.env.wait(
            operator_ready, self.env.schedule.readiness_until(self.env.clock())
        )
        self.admission_open()
        if self.cloudtasks:
            self.cleanup.quota(self.namespace, "session")
            self.admission_open()
            self.env.ledger.admit(self.env.approval.cell_ids)
            admit_queue(self.env)
            self.env.records.operations("runner", self.env.queues.meter.snapshot())
        else:
            if self.env.approval.scenario == "bigquery-recovery":
                self.bigquery.initialize()
                self.bigquery.provision()
                self.admission_open()
            self.cleanup.quota(self.namespace, "run")
            self.create_application(application)
        self.admission_open()
        self.env.records.set_phase(rt.Phase.RUNNING)

    @staticmethod
    def job_completed(job):
        status = job.get("status", {})
        return bool(
            status.get("succeeded", 0)
            or status.get("failed", 0)
            or any(
                c.get("status") == "True" and c.get("type") in ("Complete", "Failed")
                for c in status.get("conditions", [])
            )
        )

    def settle(self, request_stop=False):
        # The admitting runner calls this only after start() has returned.
        # Recovery must not reconstruct another process's submitting token.
        pubsub_released = False
        pubsub_failed = False
        last_pubsub_error = None

        def release_pubsub():
            nonlocal pubsub_released, pubsub_failed, last_pubsub_error
            if self.pubsub is None or pubsub_released:
                return
            try:
                if self.env.refresh().pubsub is not None:
                    self.pubsub.release()
                pubsub_released = True
            except (rt.Failure, OSError, ValueError) as error:
                self.env.stopping = True
                pubsub_failed = True
                cause = str(error)
                if cause != last_pubsub_error:
                    self.env.emit("pubsub-release-blocked", {"cause": cause})
                    last_pubsub_error = cause

        # Both actors have finished data operations before terminal settlement.
        # Release stops shared admission, including supervisor collection, before
        # waiting for the supervisor's cleanup to observe this actor's release.
        release_pubsub()
        self.env.refresh()
        if request_stop or self.env.stopping:
            self.env.records.request_stop()
        self.adopt_root("config")
        self.adopt_root("supervisor")

        query_failed = False
        released = False
        last_release_error = None

        def release_queries():
            nonlocal released, query_failed, last_release_error
            if self.bigquery is not None and not released:
                try:
                    self.bigquery.release()
                    released = True
                except (rt.Failure, OSError, ValueError) as error:
                    query_failed = True
                    cause = str(error)
                    if cause != last_release_error:
                        self.env.emit("bigquery-release-blocked", {"cause": cause})
                        last_release_error = cause

        def completed():
            nonlocal query_failed
            release_pubsub()
            control = self.env.refresh()
            if self.env.stopping:
                self.env.records.request_stop()
            job = self.env.root("supervisor")
            finished = (
                not job or self.job_completed(job) or control.phase == rt.Phase.CLEANED
            )
            if self.bigquery is not None and not released:
                if (
                    finished
                    or query_failed
                    or self.env.stopping
                    or self.env.evidence_failed
                    or control.stop_requested
                    or control.evidence_failed
                    or control.phase != rt.Phase.RUNNING
                    or self.env.clock() >= self.env.schedule.cleanup_at
                ):
                    release_queries()
                else:
                    try:
                        self.bigquery.poll()
                    except (rt.Failure, OSError, ValueError) as error:
                        query_failed = True
                        self.env.emit("bigquery-query-failed", {"cause": str(error)})
                        release_queries()
            return finished

        # Waiting preserves the Job's final log when possible. It is not a
        # prerequisite for cleanup. Exercise writes target existing UIDs; the
        # application patch also tests its version and cannot recreate a root.
        try:
            self.env.wait(
                completed,
                self.env.schedule.settle_until(self.env.clock()),
            )
        except rt.Failure:
            self.env.records.request_stop()
        finally:
            release_queries()
            release_pubsub()
        control = self.env.refresh()
        if control.application_intent and "application" not in self.env.roots:
            application, _ = self.env.store.read(
                f"runs/{self.env.approval.run_id}/application.json"
            )
            if not application:
                raise rt.Failure("Missing immutable application intent")
            self.adopt_application(application)
        if control.cell_intent and "cell:" + control.cell_intent["cell"] not in (
            self.env.roots
        ):
            self.cleanup.adopt_intended_cell(self.cleanup.inventory())
        # Reconcile actual state even after an earlier actor recorded cleanup.
        # A cleaned record alone is not a current idle observation.
        self.cleanup.run(
            "external settlement",
            control.success and not query_failed and not pubsub_failed,
        )

        job = self.env.root("supervisor")
        if (
            "supervisor" in self.env.roots
            and not self.env.refresh().final_log_attempted
        ):
            # A cleaned record can precede process exit. Give the Job a bounded
            # grace period before attempting its one final log export.
            if job and not self.job_completed(job):
                try:

                    def job_finished():
                        current = self.env.root("supervisor")
                        return not current or self.job_completed(current)

                    self.env.wait(
                        job_finished,
                        self.env.clock() + self.env.schedule.operator_grace,
                    )
                except rt.Failure:
                    pass
            job = self.env.root("supervisor")
            exported = bool(job and self.job_completed(job))
            if exported:
                try:
                    items = self.cleanup.inventory()
                    owned = self.cleanup.owned(items, ["supervisor"])
                    pods = [
                        obj
                        for obj in items
                        if obj["kind"] == "Pod" and obj["metadata"]["uid"] in owned
                    ]
                    exported = bool(pods)
                    for pod in pods:
                        data = self.env.kube.logs(pod)
                        if len(data) >= rt.MIB:
                            raise rt.Failure(
                                "Final supervisor log exceeds its read ceiling"
                            )
                        self.env.records.evidence(
                            "supervisor-final-log",
                            {
                                "uid": pod["metadata"]["uid"],
                                "text": data.decode(errors="replace"),
                            },
                            "runner",
                        )
                except (rt.Failure, ValueError, OSError):
                    exported = False
            self.env.records.final_log(exported)
        for key in ("supervisor", "config"):
            obj = self.env.root(key)
            if obj:
                self.env.kube.delete(obj)
        self.env.wait(
            self.temporary_gone, self.env.clock() + self.env.schedule.post_cleanup_grace
        )
        snapshot = None

        def idle_observed():
            nonlocal snapshot
            try:
                snapshot = rt.verify_idle(self.env)
            except rt.IdlePending:
                return False
            return True

        self.env.wait(
            idle_observed, self.env.clock() + self.env.schedule.post_cleanup_grace
        )
        if self.cleanup.remaining_state():
            raise rt.Failure("Run state remains after cleanup")
        self.env.emit("idle", snapshot)
        if self.cloudtasks:
            self.env.records.operations("runner", self.env.queues.meter.snapshot())
        self.env.records.settled(self.env.evidence_failed)

    def temporary_gone(self):
        items = self.cleanup.inventory()
        owned = self.cleanup.owned(items, ["supervisor", "config"])
        return not any(obj["metadata"]["uid"] in owned for obj in items)

    def finalize(self, plans):
        rt.EnvironmentLock(self.env.store).assert_owner(self.env.approval.lock_owner)
        control = self.env.refresh()
        require_pubsub_clean(control)
        require_bigquery_clean(control)
        if (
            plans.get("nonce") != self.env.approval.nonce
            or plans.get("roots") != ["flink-gcp", "tier3-bootstrap", "tier3-operator"]
            or not plans.get("empty")
        ):
            raise rt.Failure("All three refreshed empty plans are required")
        rt.verify_idle(self.env)
        if self.cleanup.remaining_state():
            raise rt.Failure("Run state reappeared")
        cells = control.cells
        result = {
            "nonce": self.env.approval.nonce,
            "sha": self.env.approval.sha,
            "idle": True,
            "plans": plans,
            "success": bool(
                control.success
                and not control.evidence_failed
                and not self.env.evidence_failed
                and (
                    self.env.approval.scenario == "smoke"
                    or (
                        self.cloudtasks
                        and set(cells) == set(self.env.approval.cell_ids)
                        and all(c.get("status") == "completed" for c in cells.values())
                    )
                    or (
                        self.env.approval.scenario == "generic-recovery"
                        and (control.recovery or {}).get("stage") == "complete"
                    )
                )
            ),
        }
        if self.env.approval.scenario == "generic-recovery":
            result.update(
                scenario=self.env.approval.scenario, recovery=control.recovery
            )
        if self.env.approval.scenario == "pubsub-recovery":
            result.update(
                scenario=self.env.approval.scenario,
                pubsub_trial=self.env.approval.pubsub_trial,
                recovery=control.recovery,
            )
        if self.env.approval.scenario == "bigquery-recovery":
            result.update(
                scenario=self.env.approval.scenario,
                bigquery_trial=self.env.approval.bigquery_trial,
                recovery=control.recovery,
            )
        if self.cloudtasks:
            result.update(
                scenario=self.env.approval.scenario,
                campaign=self.env.approval.campaign,
                flink_version=self.env.approval.flink_version,
                queue={"name": self.env.approval.queue, "admitted": control.queue},
                cells=cells,
                operations=control.operations,
                benchmark_evidence_retained=self.cleanup.retained_evidence(),
                # The session-level export summary, which the control record
                # carries and this receipt outlives: the control record is
                # deleted a few lines below.
                exported={
                    cell_id: {
                        key: summary.get(key)
                        for key in ("outcome", "objects", "evidence_bytes")
                    }
                    | {
                        "reconciliation": (summary.get("reconciliation") or {}).get(
                            "status"
                        ),
                        "manifest_sha256": summary.get("manifest_sha256"),
                    }
                    for cell_id, summary in sorted(control.exports.items())
                },
                evidence_bytes=control.evidence_bytes,
            )
        if control.pubsub is not None:
            result["pubsub"] = control.pubsub
        if control.bigquery is not None:
            result["bigquery"] = control.bigquery
        path = f"runs/{self.env.approval.run_id}/result.json"
        previous, _ = self.env.store.read(path)
        prior, expected = previous, result
        if previous is not None and (
            self.env.approval.scenario == "bigquery-recovery"
            or (
                self.env.approval.scenario == "pubsub-recovery"
                and self.env.approval.version == 5
            )
        ):
            # Refreshed empty plans have a new observation time on each retry.
            prior, expected = dict(previous), dict(result)
            for receipt in (prior, expected):
                if isinstance(receipt.get("plans"), dict):
                    receipt["plans"] = {
                        key: value
                        for key, value in receipt["plans"].items()
                        if key != "at"
                    }
        if previous is None:
            self.env.store.write(path, result)
        elif (
            previous.get("nonce") != result["nonce"]
            or not previous.get("idle")
            or (
                (
                    self.env.approval.scenario
                    in ("bigquery-recovery", "pubsub-recovery")
                    or control.pubsub is not None
                    or previous.get("pubsub") is not None
                    or control.bigquery is not None
                    or previous.get("bigquery") is not None
                )
                and rt.json_bytes(prior) != rt.json_bytes(expected)
            )
        ):
            raise rt.Failure("Final receipt conflicts with approval")
        if previous is not None:
            result = previous
        current, generation = self.env.records.read()
        if (
            self.env.approval.scenario in ("bigquery-recovery", "pubsub-recovery")
            or control.pubsub is not None
            or current.pubsub is not None
            or control.bigquery is not None
            or current.bigquery is not None
        ) and (rt.json_bytes(current.to_dict()) != rt.json_bytes(control.to_dict())):
            raise rt.Failure("Run control changed during service finalization")
        self.env.store.delete(self.env.records.path, generation)
        rt.EnvironmentLock(self.env.store).release(self.env.approval.lock_owner)
        return result["success"]
