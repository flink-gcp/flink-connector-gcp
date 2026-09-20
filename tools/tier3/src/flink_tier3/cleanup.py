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

from .cloudtasks import QUEUE_POLL_MASK, release_queue, transient
from .common import (
    ApiError,
    Failure,
    IdlePending,
    canonical_quantity,
    contains,
    digest,
    ha_metadata,
    ownership,
    quantity,
    reference,
    verify_pod,
)
from .environment import retry_conflicts
from .evidence import MARKER, authorized_release, cell_prefix, release_exported
from .model import Phase, session_shapes, taskmanager_class
from .policy import (
    BENCHMARK,
    CEILINGS,
    CLOUDTASKS_CEILINGS,
    NONCE,
    OPERATOR,
    POD_RESOURCES,
    POLL,
    STATE,
    SYSTEM,
)


def sum_resources(shapes):
    """The canonical quantities a quota needs to admit every shape given.

    Writing that sum by hand makes a Pod shape and its quota two facts, and a
    change to one silently starves the other. A canonical quantity rounds
    toward zero and these sums carry no slack, so a total that does not render
    exactly would produce a quota rejecting the Pods it was sized for; that is
    a policy the reviewer has to see rather than one to round away.
    """
    shapes = list(shapes)
    hard = {}
    for key in ("cpu", "memory", "ephemeral-storage"):
        total = sum(quantity(shape[key]) for shape in shapes)
        hard[key] = canonical_quantity(total, key)
        if quantity(hard[key]) < total:
            raise Failure(f"Quota {key} rounds below the shapes it must admit")
    return hard


def session_resources(approval):
    """Quota for one JobManager plus the largest approved TaskManager class."""
    return sum_resources(
        session_shapes(approval.cells, approval.cloudtasks_pod_resources)
    )


def system_resources():
    """The supervisor and the Operator, which share `tier3-system`."""
    return sum_resources([POD_RESOURCES["supervisor"], POD_RESOURCES["operator"]])


def application_resources():
    """The two Pods of a smoke or recovery application."""
    return sum_resources([POD_RESOURCES["smoke"]] * 2)


class Cleanup:
    """Ownership-checked observation and convergence toward the idle foundation."""

    def __init__(self, env):
        self.env = env
        self.last_inventory = []
        self.current_cell = None

    @property
    def cloudtasks(self):
        return self.env.approval.scenario == "cloudtasks"

    @property
    def ceilings(self):
        return CLOUDTASKS_CEILINGS if self.cloudtasks else CEILINGS

    def application_keys(self):
        return ["application"] + sorted(
            key for key in self.env.roots if key.startswith("cell:")
        )

    def state_prefixes(self, cell_ids=None):
        run_id = self.env.approval.run_id
        if not self.cloudtasks:
            return [(f"runs/{run_id}/", STATE)]
        if cell_ids is None:
            cell_ids = self.env.approval.cell_ids
        return [
            (f"runs/{run_id}/cells/{cell_id}/state/", BENCHMARK) for cell_id in cell_ids
        ]

    def remaining_state(self, cell_ids=None, maximum=None):
        maximum = self.ceilings["state_objects"] if maximum is None else maximum
        return [
            (obj, bucket)
            for prefix, bucket in self.state_prefixes(cell_ids)
            for obj in self.env.store.objects(prefix, bucket, maximum)
        ]

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
            if admission == "supervisor":
                resources = POD_RESOURCES["supervisor"]
            elif admission == "session":
                resources = session_resources(self.env.approval)
            else:
                resources = (
                    system_resources()
                    if namespace == SYSTEM
                    else application_resources()
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

    def cell_shape(self, pod):
        """Approved JobManager or TaskManager shape for a cell's Flink Pod."""
        labels = pod["metadata"].get("labels", {})
        cell = self.env.approval.cell(labels.get("app", ""))
        shapes = self.env.approval.cloudtasks_pod_resources
        component = labels.get("component")
        if component == "jobmanager":
            return shapes["jobmanager"]
        if component == "taskmanager":
            return shapes["taskmanager"][taskmanager_class(cell["parallelism"])]
        raise Failure("Flink Pod lacks a recognised component label")

    def audit(self):
        items = self.inventory()
        approval = self.env.approval
        groups = {
            "supervisor": self.owned(items, ["supervisor"]),
            "application": self.owned(items, self.application_keys()),
            "operator": ownership(items, {approval.operator_uid}),
        }
        images = {
            "supervisor": approval.images["supervisor"],
            "operator": approval.images["operator"],
            "application": approval.images[
                "application" if self.cloudtasks else "smoke"
            ],
        }
        names = approval.cell_ids if self.cloudtasks else approval.run_id
        known = (
            set(approval.baseline_uids)
            | set().union(*groups.values())
            | {r["uid"] for r in self.env.roots.values()}
        )
        for obj in items:
            if obj["metadata"]["uid"] not in known and not ha_metadata(
                obj, names, approval.application_namespace
            ):
                raise Failure(
                    "Unexpected object outside the baseline and run ownership graph"
                )
        pods = [obj for obj in items if obj["kind"] == "Pod"]
        if len(pods) > self.ceilings["pods"] or any(
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
            expected = None
            if role == "application":
                expected = (
                    self.cell_shape(pod) if self.cloudtasks else POD_RESOURCES["smoke"]
                )
            verify_pod(pod, role, images[role], expected)
        cells = [self.current_cell] if self.cloudtasks else None
        state = self.remaining_state([] if cells == [None] else cells)
        if sum(int(obj["size"]) for obj, _ in state) > self.ceilings["state_bytes"]:
            raise Failure("State byte ceiling exceeded")
        return items, pods

    def clean_state(self, cell_ids=None):
        objects = self.remaining_state(cell_ids, self.ceilings["state_objects"] + 1)
        state_deadline = self.env.clock() + self.env.schedule.state_cleanup_seconds
        for obj, bucket in objects:
            if self.env.clock() >= state_deadline:
                raise Failure("State cleanup deadline exceeded")
            self.env.store.delete(obj["name"], obj["generation"], bucket)
        if self.remaining_state(cell_ids):
            raise Failure("State prefix is not empty after generation-checked deletion")

    def retained_evidence(self):
        """Benchmark rows and receipts that remain for the later collector."""
        run_id = self.env.approval.run_id
        retained = {}
        for cell_id in self.env.approval.cell_ids:
            objects = self.env.store.objects(
                f"runs/{run_id}/cells/{cell_id}/", BENCHMARK, 100000
            )
            if objects:
                retained[cell_id] = {
                    "objects": len(objects),
                    "bytes": sum(int(obj["size"]) for obj in objects),
                }
        return retained

    def approved_manifest(self, cell_id):
        """The delivered manifest of a cell, from the immutable run evidence."""
        try:
            manifests, _ = self.env.store.read(
                f"runs/{self.env.approval.run_id}/application.json"
            )
        except Failure:
            return None
        return next(
            (m for m in manifests or [] if m["metadata"]["name"] == cell_id), None
        )

    def adopt_intended_cell(self, items, manifest=None):
        """Adopt a cell the supervisor intended but never recorded, if it exists.

        Uses the cached control record: intents cannot be written once a stop
        or cleanup is recorded, so the record read at entry is current. Returns
        the adopted object, or None. A same-named object with another nonce or
        a spec outside the approved manifest is left alone and reported.
        """
        approval = self.env.approval
        intent = self.env.records.cache.cell_intent
        if not intent or "cell:" + intent["cell"] in self.env.roots:
            return None
        obj = next(
            (
                o
                for o in items
                if o["kind"] == "FlinkDeployment"
                and o["metadata"]["namespace"] == approval.application_namespace
                and o["metadata"]["name"] == intent["cell"]
            ),
            None,
        )
        if obj is None:
            return None
        manifest = manifest or self.approved_manifest(intent["cell"])
        if (
            obj["metadata"].get("annotations", {}).get(NONCE) != approval.nonce
            or manifest is None
            or digest(manifest) != intent["manifest_sha256"]
            or not contains(obj["spec"], manifest["spec"])
        ):
            # Without the approved manifest the object cannot be verified, so
            # it is neither adopted nor deleted; the idle check reports it.
            self.env.emit(
                "cell-adoption-refused",
                {
                    "cell": intent["cell"],
                    "uid": obj["metadata"]["uid"],
                    "manifest": manifest is not None,
                },
            )
            return None
        try:
            self.env.remember("cell:" + intent["cell"], obj)
        except Failure:
            # The record could not be updated; the object is still ours to
            # delete in this pass, and the runner's settlement adopts again.
            self.env.evidence_failed = True
        return obj

    def intent_pending(self, items, intent, force_at):
        """Whether the runner must wait for an intended cell to materialize.

        The supervisor persisted a create intent, no matching object exists
        yet, and the supervisor Job is still active: its create may still be
        in flight, so the runner's cleanup does not declare the namespace
        clear before the force window. The supervisor's own cleanup knows its
        create outcome and never waits.
        """
        if (
            not intent
            or self.env.actor != "runner"
            or "cell:" + intent["cell"] in self.env.roots
            or self.env.clock() >= force_at
        ):
            return False
        job = self.env.root("supervisor")
        if not job:
            return False
        status = job.get("status", {})
        finished = bool(
            status.get("succeeded", 0)
            or status.get("failed", 0)
            or any(
                c.get("status") == "True" and c.get("type") in ("Complete", "Failed")
                for c in status.get("conditions", [])
            )
        )
        return not finished

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
        namespace = self.env.approval.application_namespace
        before = self.inventory()
        keys = [key for key in self.application_keys() if key in self.env.roots]
        known = ownership(before, {self.env.roots[key]["uid"] for key in keys})
        known |= {
            uid
            for uid, ref in self.env.observed.items()
            if ref["namespace"] == namespace
        }
        for key in keys:
            app = self.env.root(key)
            if app:
                try:
                    self.env.kube.delete(app)
                except Failure:
                    pass  # Retry during the bounded cleanup loop.
        # Keep the Operator running while it handles the FlinkDeployment's
        # finalizer. Observe descendants before GC can remove their parent.
        cleared = False
        apps_extra = []
        intent = self.env.records.cache.cell_intent if self.cloudtasks else None
        manifest = self.approved_manifest(intent["cell"]) if intent else None
        while self.env.clock() < end:
            items = self.inventory()
            if self.cloudtasks:
                # A cell create issued just before the stop can land after the
                # snapshot above; adopt it by intent, nonce and manifest.
                adopted = self.adopt_intended_cell(items, manifest)
                if adopted is not None:
                    known.add(adopted["metadata"]["uid"])
                    key = "cell:" + intent["cell"]
                    if key in self.env.roots and key not in keys:
                        keys.append(key)
                    else:
                        apps_extra = [adopted]
                for key in self.env.roots:
                    if key.startswith("cell:") and key not in keys:
                        keys.append(key)
                        known.add(self.env.roots[key]["uid"])
            known = ownership(items, known)
            run_objects = [obj for obj in items if obj["metadata"]["uid"] in known]
            apps = [app for key in keys if (app := self.env.root(key))]
            if not run_objects and not self.intent_pending(items, intent, force_at):
                cleared = True
                break
            for app in apps + apps_extra:
                self.env.kube.delete(app)
            apps_extra = []
            if self.env.clock() >= force_at:
                self.quota(namespace, None)
                # Stop workload controllers before removing their Pods. UID
                # preconditions reject any replacement with the same name.
                for kind in ("Deployment", "ReplicaSet", "Pod", "Service", "ConfigMap"):
                    for obj in run_objects:
                        if obj["kind"] == kind:
                            self.env.kube.delete(obj)
                for key in keys:
                    app = self.env.root(key)
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
        queue_clean = True
        if self.cloudtasks:
            self.current_cell = None
            try:
                release_queue(self.env)
            except (Failure, ValueError, OSError) as error:
                queue_clean = False
                self.env.emit("queue-cleanup-failed", {"cause": str(error)})
        state_clean = False
        try:
            self.clean_state()
            state_clean = True
        except (Failure, ValueError, OSError) as error:
            self.env.emit("state-cleanup-failed", {"cause": str(error)})
        if self.cloudtasks:
            # A session that stopped before its end hook leaves exported cells
            # unreleased; their markers prove the copies were verified.
            try:
                release_exported(self.env, self.env.store)
            except (Failure, ValueError, OSError) as error:
                self.env.emit("benchmark-release-failed", {"cause": str(error)})
            try:
                self.env.emit("benchmark-evidence-retained", self.retained_evidence())
                self.env.ledger.interrupt(self.env.approval.run_id, self.env.clock())
            except (Failure, ValueError, OSError) as error:
                self.env.emit("ledger-interrupt-failed", {"cause": str(error)})
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
        self.quota(namespace, None)
        self.quota(SYSTEM, None)
        clean = success and state_clean and queue_clean and not self.env.evidence_failed
        self.env.emit(
            "cleanup-ready",
            {
                "success": clean,
                "state_clean": state_clean,
                "queue_clean": queue_clean,
            },
        )
        try:
            self.env.records.set_phase(
                Phase.CLEANED,
                success=clean,
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
    if env.queues is not None and env.records.cache.queue_intent:
        try:
            remaining = env.queues.get(QUEUE_POLL_MASK)
        except Failure as error:
            if transient(error):
                raise IdlePending("Queue readback is unavailable") from error
            raise
        if remaining is not None:
            raise Failure("Benchmark queue remains after cleanup")
    if approval.scenario == "cloudtasks":
        # An exported cell must have released what its marker authorized; a
        # cell without a marker, and an object the export never owned, may
        # keep their objects until the bucket expires them. The release and
        # this check read the same authorization, or a refused object would
        # strand the environment lock.
        for cell_id in approval.cell_ids:
            prefix = cell_prefix(approval.run_id, cell_id)
            marker, _ = env.store.read(prefix + MARKER)
            if (
                marker is None
                or marker.get("version") != 1
                or marker.get("run_id") != approval.run_id
                or marker.get("cell_id") != cell_id
            ):
                continue
            objects = env.store.objects(prefix, BENCHMARK, 100000)
            unreleased, _retained = authorized_release(marker, objects, prefix)
            if unreleased:
                raise Failure(
                    "An exported cell's benchmark prefix was not released: " + prefix
                )
    return [{"kind": obj["kind"], "metadata": obj["metadata"]} for obj in items]
