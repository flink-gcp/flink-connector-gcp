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
"""Tier-3 lifecycle cleanup."""

from __future__ import annotations

import copy

from .common import (
    ApiError,
    Failure,
    IdlePending,
    ha_metadata,
    ownership,
    quantity,
    reference,
    verify_pod,
)
from .environment import retry_conflicts
from .model import Phase
from .policy import CEILINGS, OPERATOR, POD_RESOURCES, POLL, SMOKE, STATE, SYSTEM


class Cleanup:
    """Ownership-checked observation and convergence toward the idle foundation."""

    def __init__(self, env):
        self.env = env
        self.last_inventory = []

    @retry_conflicts
    def quota(self, namespace, admission):
        self.env.namespaces()
        saved = self.env.approval.namespaces[namespace]
        obj = self.env.kube.get("ResourceQuota", namespace, "tier3-idle")
        if not obj or obj["metadata"]["uid"] != saved["quota_uid"]:
            raise Failure("Quota identity changed")
        hard = copy.deepcopy(saved["hard"])
        if admission:
            pods = 1 if admission == "supervisor" else 2
            resources = (
                POD_RESOURCES["supervisor"]
                if admission == "supervisor"
                else {
                    "cpu": "1250m" if namespace == SYSTEM else "2",
                    "memory": "2560Mi" if namespace == SYSTEM else "4Gi",
                    "ephemeral-storage": "1152Mi" if namespace == SYSTEM else "2Gi",
                }
            )
            hard.update(
                {
                    "pods": str(pods),
                    "persistentvolumeclaims": "0",
                    "count/jobs.batch": "1" if namespace == SYSTEM else "0",
                    "count/cronjobs.batch": "0",
                    "count/statefulsets.apps": "0",
                }
            )
            for key, value in resources.items():
                hard["requests." + key] = value
                hard["limits." + key] = value
        if admission:
            self.env.admission_open()
        else:
            self.env.assert_owner()
        if obj["spec"]["hard"] != hard:
            self.env.kube.patch(
                obj, [{"op": "replace", "path": "/spec/hard", "value": hard}]
            )

    @retry_conflicts
    def scale_operator(self, replicas):
        self.env.namespaces()
        obj = self.env.kube.get("Deployment", SYSTEM, OPERATOR)
        if not obj or obj["metadata"]["uid"] != self.env.approval.operator_uid:
            raise Failure("Operator identity changed")
        # Lifecycle RBAC grants the scale subresource, not Deployment patch.
        scale = self.env.kube.request(
            "GET", self.env.kube.path("Deployment", SYSTEM, OPERATOR) + "/scale"
        )
        if scale["metadata"]["uid"] != obj["metadata"]["uid"]:
            raise Failure("Operator scale identity changed")
        if replicas:
            self.env.admission_open()
        else:
            self.env.assert_owner()
        # ScaleSpec omits zero replicas from its JSON representation.
        if scale["spec"].get("replicas", 0) != replicas:
            patch = [
                {
                    "op": "test",
                    "path": "/metadata/uid",
                    "value": obj["metadata"]["uid"],
                },
                {
                    "op": "test",
                    "path": "/metadata/resourceVersion",
                    "value": scale["metadata"]["resourceVersion"],
                },
                {"op": "add", "path": "/spec/replicas", "value": replicas},
            ]
            self.env.kube.request(
                "PATCH",
                self.env.kube.path("Deployment", SYSTEM, OPERATOR) + "/scale",
                patch,
                content_type="application/json-patch+json",
            )

    def inventory(self):
        self.env.namespaces()
        self.env.refresh()
        self.last_inventory = self.env.kube.inventory()
        known = ownership(
            self.last_inventory,
            set(self.env.observed) | {r["uid"] for r in self.env.roots.values()},
        )
        discovered = {
            obj["metadata"]["uid"]: reference(obj)
            for obj in self.last_inventory
            if obj["metadata"]["uid"] in known
            and obj["metadata"]["uid"] not in self.env.observed
        }
        if discovered:
            try:
                self.env.records.observe(discovered)
            except Failure:
                self.env.evidence_failed = True
        return self.last_inventory

    def owned(self, items, keys):
        return ownership(
            items, {self.env.roots[key]["uid"] for key in keys if key in self.env.roots}
        )

    def audit(self):
        items = self.inventory()
        groups = {
            "supervisor": self.owned(items, ["supervisor"]),
            "smoke": self.owned(items, ["application"]),
            "operator": ownership(items, {self.env.approval.operator_uid}),
        }
        known = (
            set(self.env.approval.baseline_uids)
            | set().union(*groups.values())
            | {r["uid"] for r in self.env.roots.values()}
        )
        for obj in items:
            if obj["metadata"]["uid"] not in known and not ha_metadata(
                obj, self.env.approval.run_id
            ):
                raise Failure(
                    "Unexpected object outside the baseline and run ownership graph"
                )
        pods = [obj for obj in items if obj["kind"] == "Pod"]
        if len(pods) > CEILINGS["pods"] or any(
            obj["kind"] == "PersistentVolumeClaim" for obj in items
        ):
            raise Failure("Pod/PVC count ceiling exceeded")
        for pod in pods:
            role = next(
                (k for k, uids in groups.items() if pod["metadata"]["uid"] in uids),
                None,
            )
            if not role:
                raise Failure("Pod lacks a verified owner UID")
            verify_pod(pod, role, self.env.approval.images[role])
        state = self.env.store.objects(
            f"runs/{self.env.approval.run_id}/", STATE, CEILINGS["state_objects"]
        )
        if sum(int(obj["size"]) for obj in state) > CEILINGS["state_bytes"]:
            raise Failure("State byte ceiling exceeded")
        return items, pods

    def clean_state(self):
        prefix = f"runs/{self.env.approval.run_id}/"
        objects = self.env.store.objects(prefix, STATE, CEILINGS["state_objects"] + 1)
        state_deadline = self.env.clock() + self.env.schedule.state_cleanup_seconds
        for obj in objects:
            if self.env.clock() >= state_deadline:
                raise Failure("State cleanup deadline exceeded")
            self.env.store.delete(obj["name"], obj["generation"], STATE)
        if self.env.store.objects(prefix, STATE):
            raise Failure("State prefix is not empty after generation-checked deletion")

    def run(self, reason, success=False):
        self.env.namespaces()
        self.env.assert_owner()
        self.env.refresh()
        try:
            self.env.records.begin_cleanup(reason)
        except Failure:
            self.env.evidence_failed = True
        self.env.emit("cleanup-start", {"reason": reason, "roots": self.env.roots})
        schedule = self.env.schedule.cleanup_window(self.env.clock())
        end, force_at = schedule.cleanup_end, schedule.force_at
        # Capture ownership before deleting any parent, and retain that graph
        # in mutable control storage for a later recovery process.
        before = self.inventory()
        known = ownership(
            before,
            {self.env.roots["application"]["uid"]}
            if "application" in self.env.roots
            else set(),
        )
        known |= {
            uid for uid, ref in self.env.observed.items() if ref["namespace"] == SMOKE
        }
        app = self.env.root("application")
        if app:
            try:
                self.env.kube.delete(app)
            except Failure:
                pass  # Retry during the bounded cleanup loop.
        # Keep the Operator running while it handles the FlinkDeployment's
        # finalizer. Observe descendants before GC can remove their parent.
        cleared = False
        while self.env.clock() < end:
            items = self.inventory()
            known = ownership(items, known)
            run_objects = [obj for obj in items if obj["metadata"]["uid"] in known]
            app = self.env.root("application")
            if not run_objects:
                cleared = True
                break
            if app:
                self.env.kube.delete(app)
            if self.env.clock() >= force_at:
                self.quota(SMOKE, None)
                # Stop workload controllers before removing their Pods. UID
                # preconditions reject any replacement with the same name.
                for kind in ("Deployment", "ReplicaSet", "Pod", "Service", "ConfigMap"):
                    for obj in run_objects:
                        if obj["kind"] == kind:
                            self.env.kube.delete(obj)
                app = self.env.root("application")
                if app and app["metadata"].get("finalizers"):
                    try:
                        self.env.kube.patch(
                            app,
                            [
                                {
                                    "op": "replace",
                                    "path": "/metadata/finalizers",
                                    "value": [],
                                }
                            ],
                        )
                    except ApiError as error:
                        if error.status not in (404, 409, 422):
                            raise
                        # The next inventory rechecks the root UID before retry.
            self.env.sleep(min(POLL, max(0, end - self.env.clock())))
        if not cleared:
            self.env.emit(
                "cleanup-blocked",
                {"reason": "owned workload remains", "uids": sorted(known)},
            )
            raise Failure(
                "Owned workload remains; Operator and environment lock retained for recovery"
            )
        # Evidence or storage failures must not keep paid resources alive.
        state_clean = False
        try:
            self.clean_state()
            state_clean = True
        except (Failure, ValueError, OSError) as error:
            self.env.emit("state-cleanup-failed", {"cause": str(error)})
        self.scale_operator(0)

        def operator_stopped():
            items = self.inventory()
            owned = ownership(items, {self.env.approval.operator_uid})
            return not any(
                obj["kind"] == "Pod" and obj["metadata"]["uid"] in owned
                for obj in items
            )

        self.env.wait(
            operator_stopped, max(self.env.clock() + schedule.operator_grace, end)
        )
        self.quota(SMOKE, None)
        self.quota(SYSTEM, None)
        self.env.emit(
            "cleanup-ready",
            {
                "success": success and state_clean and not self.env.evidence_failed,
                "state_clean": state_clean,
            },
        )
        try:
            self.env.records.set_phase(
                Phase.CLEANED,
                success=success and state_clean and not self.env.evidence_failed,
                state_clean=state_clean,
                evidence_failed=self.env.evidence_failed,
            )
        except Failure:
            self.env.evidence_failed = True


def verify_idle(env):
    env.namespaces()
    kube, approval = env.kube, env.approval
    items = kube.inventory()
    baseline = set(approval.baseline_uids)
    for obj in items:
        uid = obj["metadata"]["uid"]
        if obj["kind"] in (
            "Pod",
            "PersistentVolumeClaim",
            "Job",
            "CronJob",
            "StatefulSet",
        ) or obj["kind"].startswith("Flink"):
            raise Failure("Workload or run object remains after cleanup")
        # The idle Operator may leave a newly created, zero-replica RS.
        if uid not in baseline and (
            obj["kind"] != "ReplicaSet"
            or not any(
                r.get("uid") == approval.operator_uid
                for r in obj["metadata"].get("ownerReferences", [])
            )
            or obj.get("spec", {}).get("replicas", 0) != 0
        ):
            raise Failure("A non-baseline object remains after cleanup")
    operator = kube.get("Deployment", SYSTEM, OPERATOR)
    if (
        not operator
        or operator["metadata"]["uid"] != approval.operator_uid
        or operator["spec"].get("replicas") != 0
    ):
        raise Failure("Operator has not returned to zero replicas")
    pending = operator.get("status", {}).get("replicas", 0) != 0
    for ns, saved in approval.namespaces.items():
        quota = kube.get("ResourceQuota", ns, "tier3-idle")
        if (
            not quota
            or quota["metadata"]["uid"] != saved["quota_uid"]
            or quota["spec"]["hard"] != saved["hard"]
        ):
            raise Failure("Original idle quota was not restored")
        if (
            quota.get("status", {}).get("hard", {}) != saved["hard"]
            or quantity(quota.get("status", {}).get("used", {}).get("pods", "1")) != 0
        ):
            pending = True
    if pending:
        raise IdlePending("Controllers have not observed the idle state")
    return [{"kind": obj["kind"], "metadata": obj["metadata"]} for obj in items]
