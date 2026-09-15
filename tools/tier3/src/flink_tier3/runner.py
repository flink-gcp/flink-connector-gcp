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


class Runner:
    def __init__(self, env):
        self.env = env
        self.env.actor = "runner"
        self.cleanup = rt.Cleanup(env)

    def admission_open(self):
        self.env.admission_open()

    def create_application(self, application):
        if rt.digest(application) != self.env.approval.application_sha256:
            raise rt.Failure("Application differs from the approved manifest")
        self.admission_open()
        if self.env.kube.get("FlinkDeployment", rt.SMOKE, self.env.approval.run_id):
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
        obj = self.env.kube.get("FlinkDeployment", rt.SMOKE, self.env.approval.run_id)
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
        self.cleanup.quota(rt.SMOKE, "run")
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
        self.env.refresh()
        if request_stop or self.env.stopping:
            self.env.records.request_stop()
        self.adopt_root("config")
        self.adopt_root("supervisor")

        def completed():
            control = self.env.refresh()
            if self.env.stopping:
                self.env.records.request_stop()
            job = self.env.root("supervisor")
            return (
                not job or self.job_completed(job) or control.phase == rt.Phase.CLEANED
            )

        # Waiting preserves the Job's final log when possible. It is not a
        # prerequisite for cleanup: every supervisor write also moves to idle.
        try:
            self.env.wait(
                completed,
                self.env.schedule.settle_until(self.env.clock()),
            )
        except rt.Failure:
            self.env.records.request_stop()
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
        self.cleanup.run("external settlement", control.success)

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
        if self.env.store.objects(f"runs/{self.env.approval.run_id}/", rt.STATE):
            raise rt.Failure("Run state remains after cleanup")
        self.env.emit("idle", snapshot)
        self.env.records.settled(self.env.evidence_failed)

    def temporary_gone(self):
        items = self.cleanup.inventory()
        owned = self.cleanup.owned(items, ["supervisor", "config"])
        return not any(obj["metadata"]["uid"] in owned for obj in items)

    def finalize(self, plans):
        rt.EnvironmentLock(self.env.store).assert_owner(self.env.approval.lock_owner)
        control = self.env.refresh()
        if (
            plans.get("nonce") != self.env.approval.nonce
            or plans.get("roots") != ["flink-gcp", "tier3-bootstrap", "tier3-operator"]
            or not plans.get("empty")
        ):
            raise rt.Failure("All three refreshed empty plans are required")
        rt.verify_idle(self.env)
        if self.env.store.objects(f"runs/{self.env.approval.run_id}/", rt.STATE):
            raise rt.Failure("Run state reappeared")
        result = {
            "nonce": self.env.approval.nonce,
            "sha": self.env.approval.sha,
            "idle": True,
            "plans": plans,
            "success": bool(
                control.success
                and not control.evidence_failed
                and not self.env.evidence_failed
            ),
        }
        path = f"runs/{self.env.approval.run_id}/result.json"
        previous, _ = self.env.store.read(path)
        if previous is None:
            self.env.store.write(path, result)
        elif previous.get("nonce") != result["nonce"] or not previous.get("idle"):
            raise rt.Failure("Final receipt conflicts with approval")
        if previous is not None:
            result = previous
        _, generation = self.env.records.read()
        self.env.store.delete(self.env.records.path, generation)
        rt.EnvironmentLock(self.env.store).release(self.env.approval.lock_owner)
        return result["success"]
