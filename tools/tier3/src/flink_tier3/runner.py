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

from .bigquery.exercise import require_handoff
from .bigquery.handoff import require_bigquery_clean
from .policy import BIGQUERY_OBSERVATIONS
from .pubsub.admission import admit, require_admission
from .pubsub.guard import admission_deadline
from .pubsub.handoff import CohortUnstarted
from .pubsub.lifecycle import require_pubsub_clean
from .records import write_artifact

# Scenarios whose success is the verdict their exercise decided when it
# completed.
SERVICE_TRIALS = ("bigquery-recovery", "pubsub-recovery")


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
    def namespace(self):
        return self.env.approval.application_namespace

    def admission_open(self):
        self.env.admission_open()
        if (
            self.env.approval.scenario == "bigquery-recovery"
            and self.env.clock()
            >= self.env.schedule.started + BIGQUERY_OBSERVATIONS["startup_seconds"]
        ):
            raise rt.Failure("BigQuery startup deadline expired")
        if self.env.approval.scenario == "pubsub-recovery" and (
            self.env.clock() >= admission_deadline(self.env)
        ):
            raise rt.Failure("Pub/Sub admission deadline expired")

    def supervisor_ended(self):
        """Whether the supervisor Job is gone or has ended.

        The Job replaces no Pod, so a supervisor the infrastructure took
        away ends it, and the runner settles the run.
        """
        job = self.env.root("supervisor")
        return not job or rt.job_finished(job)

    def supervisor_open(self):
        """Refuse further admission once the supervisor Job has ended."""
        if self.supervisor_ended():
            raise rt.Failure("Supervisor stopped before application admission")

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
        # A rate-limited intent write backs off; admission may close meanwhile.
        self.admission_open()
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
        namespace = manifest["metadata"]["namespace"]
        if self.env.kube.get(manifest["kind"], namespace, manifest["metadata"]["name"]):
            raise rt.Failure("Temporary resource name already exists")
        intent = manifest
        if key == "config":
            # Keep repeated control CAS independent of the embedded source size.
            intent = {k: value for k, value in manifest.items() if k != "data"}
            intent["data_sha256"] = rt.digest(manifest.get("data", {}))
        self.env.records.intend(key, intent)
        self.admission_open()
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
            manifest["kind"],
            manifest["metadata"]["namespace"],
            manifest["metadata"]["name"],
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

    def start(self, config, job, application, probe=None):
        pubsub = self.env.approval.scenario == "pubsub-recovery"
        if pubsub:
            require_admission(self)
            self.admission_open()
        if self.env.approval.scenario == "bigquery-recovery":
            if self.bigquery is None:
                raise rt.Failure("BigQuery execution admission requires its handoff")
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
            self.supervisor_open()
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
            self.supervisor_open()
            operator = self.env.kube.get("Deployment", rt.SYSTEM, rt.OPERATOR)
            return operator.get("status", {}).get("readyReplicas", 0) == 1

        self.env.wait(
            operator_ready, self.env.schedule.readiness_until(self.env.clock())
        )
        self.admission_open()
        self.supervisor_open()
        if self.env.approval.scenario == "bigquery-recovery":
            self.bigquery.initialize()
            self.bigquery.provision()
            self.admission_open()
            self.supervisor_open()
        if pubsub:
            # Opens the application quota itself, for the probe Pod.
            admit(self, probe)
            self.admission_open()
            self.supervisor_open()
        else:
            self.cleanup.quota(self.namespace, "run")
        self.create_application(application)
        self.admission_open()
        self.env.records.set_phase(rt.Phase.RUNNING)

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

        # Release stops shared admission, including supervisor collection. A
        # run that is not stopping keeps the runner's authority while it waits,
        # because only the runner may publish the cohorts the supervisor
        # requests; a stopping one releases first, so the supervisor's cleanup
        # never waits on a runner that waits on it.
        pubsub_serving = (
            self.pubsub is not None and not request_stop and not self.env.stopping
        )
        if not pubsub_serving:
            release_pubsub()
        try:
            self.env.refresh()
            if request_stop or self.env.stopping:
                self.env.records.request_stop()
            self.adopt_root("config")
            self.adopt_root("supervisor")
            self.adopt_root("probe")
        except BaseException:
            # Nothing is served once settlement cannot reach its wait.
            release_pubsub()
            raise

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
            nonlocal query_failed, pubsub_serving, pubsub_failed
            if not pubsub_serving:
                release_pubsub()
            control = self.env.refresh()
            if self.env.stopping:
                self.env.records.request_stop()
            finished = self.supervisor_ended() or control.phase == rt.Phase.CLEANED
            # When neither handoff may admit another runner operation.
            closed = (
                finished
                or self.env.stopping
                or self.env.evidence_failed
                or control.stop_requested
                or control.evidence_failed
                or control.phase != rt.Phase.RUNNING
                or self.env.clock() >= self.env.schedule.cleanup_at
            )
            if pubsub_serving:
                if closed:
                    pubsub_serving = False
                    release_pubsub()
                else:
                    try:
                        self.pubsub.serve()
                    except CohortUnstarted:
                        # The run closed after this poll's read: nothing was
                        # started, so this is a close, not a failure.
                        pubsub_serving = False
                        release_pubsub()
                    except (rt.Failure, OSError, ValueError) as error:
                        pubsub_serving, pubsub_failed = False, True
                        self.env.emit(
                            "pubsub-publication-failed", {"cause": str(error)}
                        )
                        release_pubsub()
            if self.bigquery is not None and not released:
                if closed or query_failed:
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
        # Reconcile actual state even after an earlier actor recorded cleanup.
        # A cleaned record alone is not a current idle observation.
        self.cleanup.run(
            "external settlement",
            control.success and not query_failed and not pubsub_failed,
        )
        if (
            "supervisor" in self.env.roots
            and not self.env.refresh().final_log_attempted
        ):
            self.env.records.final_log(self.export_final_log())
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
        self.env.records.settled(self.env.evidence_failure)

    def export_final_log(self):
        """Export the ended supervisor Pod's log once.

        Returns the evidence failure to record, or None. A cleaned record can
        precede process exit, so the Job gets a bounded grace period first.
        Missing logs are an evidence failure, not a reason to keep paid
        resources: the caller tears down either way.
        """
        try:
            self.env.wait(
                self.supervisor_ended,
                self.env.clock() + self.env.schedule.operator_grace,
            )
        except rt.Failure:
            pass
        job = self.env.root("supervisor")
        if not job:
            failure = "The supervisor Job is gone"
        elif not rt.job_finished(job):
            failure = "The supervisor Job had not ended"
        else:
            try:
                items = self.cleanup.inventory()
                owned = self.cleanup.owned(items, ["supervisor"])
                pods = [
                    obj
                    for obj in items
                    if obj["kind"] == "Pod" and obj["metadata"]["uid"] in owned
                ]
                failure = None if pods else "The supervisor Job has no Pod"
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
            except (rt.Failure, ValueError, OSError) as error:
                failure = error
        if failure is None:
            return None
        self.env.evidence_lost("supervisor-final-log", failure)
        return self.env.evidence_failure

    def temporary_gone(self):
        items = self.cleanup.inventory()
        owned = self.cleanup.owned(items, ["supervisor", "config"])
        return not any(obj["metadata"]["uid"] in owned for obj in items)

    def receipt(self, control, plans):
        """The final receipt this run control record and plans produce."""
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
                        self.env.approval.scenario in SERVICE_TRIALS
                        # The exercise decided this when it completed, from the
                        # recoveries, the oracle and the observation coverage.
                        # A completed recovery alone is not the claim here.
                        # `recovery` is durable JSON that nothing types on the
                        # way in, so a malformed record must read as not-usable
                        # rather than raise while the lock is retained.
                        and isinstance(control.recovery, dict)
                        and control.recovery.get("verdict") == rt.USABLE
                    )
                )
            ),
        }
        failure = control.evidence_failure or self.env.evidence_failure
        if failure is not None:
            # Why `success` is false although every other input may say usable.
            result["evidence_failure"] = failure
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
        if control.pubsub is not None:
            result["pubsub"] = control.pubsub
        if control.bigquery is not None:
            result["bigquery"] = control.bigquery
        return result

    def finalize(self, plans):
        """Write the final receipt, delete the control record and release the lock.

        A retry after a failure past the receipt keeps the receipt it finds:
        that one was written from the record before the record was deleted.
        """
        rt.EnvironmentLock(self.env.store).assert_owner(self.env.approval.lock_owner)
        control, generation = self.env.records.read()
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
        path = f"runs/{self.env.approval.run_id}/result.json"
        result, _ = self.env.store.read(path)
        if result is None:
            result = self.receipt(control, plans)
            write_artifact(
                self.env.store, self.env.approval.run_id, "result.json", result
            )
        elif result.get("nonce") != self.env.approval.nonce or not result.get("idle"):
            raise rt.Failure("Final receipt conflicts with approval")
        # Conditional on the record the receipt was derived from.
        self.env.store.delete(self.env.records.path, generation)
        rt.EnvironmentLock(self.env.store).release(self.env.approval.lock_owner)
        return result["success"]

    def restore_control(self):
        """Write the run control record recovery found missing.

        Finalization deletes the record after writing the final receipt and
        before releasing the environment lock, and dispatch writes it after
        taking the lock and before admitting anything, so either way nothing
        in the cluster depends on it. A cleaned record lets recovery settle
        and finalize again; a receipt already written is kept.
        """
        record = rt.RunRecord(
            self.env.approval.nonce, phase=rt.Phase.CLEANED, idle=True
        )
        # Create-only: a record that reappeared since recovery read it wins.
        self.env.store.write(self.env.records.path, record.to_dict())
