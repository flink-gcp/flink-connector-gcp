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

from .bigquery.handoff import require_bigquery_clean
from .common import (
    ApiError,
    Failure,
    IdlePending,
    canonical_quantity,
    contains,
    ha_metadata,
    ownership,
    quantity,
    reference,
    transient,
    verify_pod,
)
from .environment import retry_conflicts
from .model import Phase
from .policy import (
    BIGQUERY,
    BIGQUERY_STATE,
    BIGTABLE,
    BIGTABLE_STATE,
    CLOUDTASKS,
    CLOUDTASKS_STATE,
    OPERATOR,
    POD_RESOURCES,
    POLL,
    PUBSUB,
    PUBSUB_STATE,
    SPANNER,
    SPANNER_STATE,
    STATE,
    STATE_OBJECTS,
    SYSTEM,
)
from .pubsub.lifecycle import require_pubsub_clean
from .records import require_services_clean
from .resource import RESOURCE_SCENARIOS, require_resources_clean


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


def system_resources():
    """The supervisor, the Operator, and the Operator's replacement.

    A preempted Pod holds its quota until it finishes terminating, so a quota
    sized for the steady state refuses the Deployment's own recovery. Measured
    on 2026-09-20: the Operator Pod was preempted mid-session, four
    replacements were refused with `exceeded quota: tier3-idle`, and the
    session died when the supervisor next read the retired Pod's log.
    """
    return sum_resources(
        [POD_RESOURCES["supervisor"]] + [POD_RESOURCES["operator"]] * 2
    )


def application_resources():
    """The two Pods of a smoke or recovery application."""
    return sum_resources([POD_RESOURCES["smoke"]] * 2)


class Cleanup:
    """Ownership-checked observation and convergence toward the idle foundation."""

    def __init__(self, env, *, bigquery=None, pubsub=None, resource=None, quiesce=None):
        if sum(item is not None for item in (bigquery, pubsub, resource)) > 1:
            raise Failure("Cleanup accepts only one service handoff")
        if resource is not None and (
            resource.env is not env
            or env.actor != "supervisor"
            or not callable(quiesce)
        ):
            raise Failure(
                resource.service
                + " cleanup needs its supervisor environment and a quiescence barrier"
            )
        if pubsub is not None and (
            pubsub.env is not env
            or env.actor != "supervisor"
            or env.approval.scenario != "pubsub-recovery"
            or not callable(quiesce)
        ):
            raise Failure(
                "Pub/Sub cleanup needs its supervisor environment and a quiescence barrier"
            )
        services = (bigquery, pubsub, resource)
        if all(item is None for item in services) and quiesce is not None:
            raise Failure("A quiescence barrier requires a service handoff")
        if bigquery is not None and (
            bigquery.env is not env
            or env.actor != "supervisor"
            or not callable(quiesce)
        ):
            raise Failure(
                "BigQuery cleanup needs its supervisor environment and a quiescence barrier"
            )
        self.env = env
        self.bigquery, self.pubsub, self.quiesce = bigquery, pubsub, quiesce
        self.resource = resource
        self.last_inventory = []

    def state_prefixes(self):
        run_id = self.env.approval.run_id
        if self.env.approval.scenario == "pubsub-recovery":
            return [(f"runs/{run_id}/", PUBSUB_STATE)]
        bucket = {
            "bigquery-recovery": BIGQUERY_STATE,
            "cloudtasks-recovery": CLOUDTASKS_STATE,
            "bigtable-recovery": BIGTABLE_STATE,
            "spanner-recovery": SPANNER_STATE,
        }.get(self.env.approval.scenario)
        if bucket is not None:
            # Flink's GCS writer stages uploads under .inprogress/<bucket>/.
            return [
                (f"runs/{run_id}/", bucket),
                (f".inprogress/{bucket}/runs/{run_id}/", bucket),
            ]
        return [(f"runs/{run_id}/", STATE)]

    def remaining_state(self, maximum=STATE_OBJECTS):
        remaining = []
        for prefix, bucket in self.state_prefixes():
            # Every prefix holds one run's state, and they share the ceiling.
            budget = maximum - len(remaining)
            remaining += [
                (obj, bucket) for obj in self.env.store.objects(prefix, bucket, budget)
            ]
        return remaining

    @retry_conflicts
    def quota(self, namespace, admission):
        self.env.namespaces()
        saved = self.env.approval.namespaces[namespace]
        obj = self.env.kube.get("ResourceQuota", namespace, "tier3-idle")
        if not obj or obj["metadata"]["uid"] != saved["quota_uid"]:
            raise Failure("Quota identity changed")
        hard = copy.deepcopy(saved["hard"])
        if admission:
            if admission == "supervisor":
                pods, resources = 1, POD_RESOURCES["supervisor"]
            elif namespace == PUBSUB:
                pods, resources = 4, sum_resources([POD_RESOURCES["smoke"]] * 4)
            elif namespace in (BIGQUERY, CLOUDTASKS, BIGTABLE, SPANNER):
                # One JobManager and two TaskManagers.
                pods, resources = 3, sum_resources([POD_RESOURCES["smoke"]] * 3)
            elif namespace == SYSTEM:
                # Two Pods run here; the third is the replacement slot.
                pods, resources = 3, system_resources()
            else:
                pods, resources = 2, application_resources()
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
            control = self.env.refresh()
            require_services_clean(control)
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
            except Failure as error:
                self.env.evidence_lost("observe", error)
        return self.last_inventory

    def owned(self, items, keys):
        return ownership(
            items, {self.env.roots[key]["uid"] for key in keys if key in self.env.roots}
        )

    def audit(self):
        items = self.inventory()
        approval = self.env.approval
        groups = {
            "supervisor": self.owned(items, ["supervisor"]),
            "application": self.owned(items, ["application"]),
            "probe": self.owned(items, ["probe"]),
            "operator": ownership(items, {approval.operator_uid}),
        }
        images = {
            "supervisor": approval.images["supervisor"],
            # The access probe runs the lifecycle tools image, as the
            # supervisor does, in the application namespace.
            "probe": approval.images["supervisor"],
            "operator": approval.images["operator"],
            "application": approval.images[
                "smoke" if approval.scenario == "smoke" else "application"
            ],
        }
        known = (
            set(approval.baseline_uids)
            | set().union(*groups.values())
            | {r["uid"] for r in self.env.roots.values()}
        )
        for obj in items:
            if obj["metadata"]["uid"] not in known and not ha_metadata(
                obj, approval.run_id, approval.application_namespace
            ):
                raise Failure(
                    "Unexpected object outside the baseline and run ownership graph"
                )
        # The ResourceQuotas bound how many Pods and PVCs there are; this
        # checks what each Pod is.
        pods = [obj for obj in items if obj["kind"] == "Pod"]
        for pod in pods:
            role = next(
                (k for k, uids in groups.items() if pod["metadata"]["uid"] in uids),
                None,
            )
            if not role:
                raise Failure("Pod lacks a verified owner UID")
            expected, spot = None, None
            if role == "application":
                expected = POD_RESOURCES["smoke"]
                # The capacity runs on Spot; the JobManager does not, because
                # losing it restarts the job rather than costing slots.
                spot = (
                    pod["metadata"].get("labels", {}).get("component") != "jobmanager"
                )
            elif role == "probe":
                # Shaped and placed as the supervisor is, Spot excluded.
                expected, spot = POD_RESOURCES["supervisor"], False
            verify_pod(
                pod,
                "supervisor" if role == "probe" else role,
                images[role],
                expected,
                spot=spot,
            )
        return items, pods

    def clean_state(self):
        objects = self.remaining_state(STATE_OBJECTS + 1)
        state_deadline = self.env.clock() + self.env.schedule.state_cleanup_seconds
        for obj, bucket in objects:
            if self.env.clock() >= state_deadline:
                raise Failure("State cleanup deadline exceeded")
            self.env.store.delete(obj["name"], obj["generation"], bucket)
        if self.remaining_state():
            raise Failure("State prefix is not empty after generation-checked deletion")

    def adopt_intended_probe(self, items):
        """Adopt a probe Pod the runner intended but never recorded, if it exists.

        The namespace barrier waits for every Pod there, so cleanup deletes
        this one even when only the runner's intent names it, and looks on
        every pass because a create whose response was lost can land after
        cleanup began. Like the runner's own adoption, the Pod must match the
        persisted intent; any other Pod of that name is left for the barrier
        to report. Uses the cached control record: an intent cannot be
        written once a stop or cleanup is recorded. Returns the adopted
        object, or None.
        """
        intent = self.env.records.cache.probe_intent
        if not intent or "probe" in self.env.roots:
            return None
        metadata = intent["metadata"]
        obj = next(
            (
                o
                for o in items
                if o["kind"] == "Pod"
                and o["metadata"]["namespace"] == metadata["namespace"]
                and o["metadata"]["name"] == metadata["name"]
            ),
            None,
        )
        if obj is None:
            return None
        if not contains(obj, intent):
            self.env.emit("probe-adoption-refused", {"uid": obj["metadata"]["uid"]})
            return None
        try:
            self.env.remember("probe", obj)
        except Failure as error:
            # The record could not be updated; the Pod is still ours to delete
            # in this pass.
            self.env.evidence_lost("remember-probe", error)
        return obj

    def run(self, reason, success=False):
        """Clean the run's resources and record the cleaned phase."""
        self.env.namespaces()
        self.env.assert_owner()
        self.env.refresh()
        if (
            self.env.records.cache.bigquery is not None
            or self.env.records.cache.pubsub is not None
            or any(
                getattr(self.env.records.cache, scenario.field) is not None
                for scenario in RESOURCE_SCENARIOS.values()
            )
        ):
            try:
                self.env.records.request_stop()
            except Failure as error:
                self.env.evidence_lost("request-stop", error)
        try:
            self.env.records.begin_cleanup(reason)
        except Failure as error:
            self.env.evidence_lost("begin-cleanup", error)
        self.env.emit("cleanup-start", {"reason": reason, "roots": self.env.roots})
        schedule = self.env.schedule.cleanup_window(self.env.clock())
        end, force_at = schedule.cleanup_end, schedule.force_at
        # Capture ownership before deleting any parent, and retain that graph
        # in mutable control storage for a later recovery process.
        namespace = self.env.approval.application_namespace
        before = self.inventory()
        keys = ["application"] if "application" in self.env.roots else []
        # A probe Pod the runner did not get to delete is removed with the
        # workload it preceded; one only the runner's intent names is adopted
        # in the loop below.
        if "probe" in self.env.roots:
            keys.append("probe")
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
        while self.env.clock() < end:
            items = self.inventory()
            probe = self.adopt_intended_probe(items)
            if probe is not None:
                known.add(probe["metadata"]["uid"])
                if "probe" in self.env.roots and "probe" not in keys:
                    keys.append("probe")
                else:
                    apps_extra.append(probe)
            known = ownership(items, known)
            run_objects = [obj for obj in items if obj["metadata"]["uid"] in known]
            apps = [app for key in keys if (app := self.env.root(key))]
            if not run_objects:
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
        self.finish_bigquery(end)
        self.finish_pubsub(end)
        self.finish_resource(end)
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
        self.quota(namespace, None)
        self.quota(SYSTEM, None)
        clean = success and state_clean and not self.env.evidence_failed
        self.env.emit("cleanup-ready", {"success": clean, "state_clean": state_clean})
        try:
            self.env.records.set_phase(
                Phase.CLEANED,
                success=clean,
                state_clean=state_clean,
                evidence_failure=self.env.evidence_failure,
            )
        except Failure as error:
            self.env.evidence_lost("record-cleaned", error)

    def finish_bigquery(self, deadline):
        """After workload teardown, wait for release and the writer fence."""
        if self.env.refresh().bigquery is None:
            return
        if self.bigquery is not None:
            self.bigquery.stop()
            self.env.wait(
                lambda: (
                    self.bigquery.released()
                    and self.quiesce() is True
                    and self.bigquery.cleanup(self.quiesce, deadline=deadline)
                ),
                deadline,
            )
        require_bigquery_clean(self.env.refresh())

    def finish_resource(self, deadline):
        """After workload teardown, wait for release and delete the run's resource."""
        record = self.env.refresh()
        if all(
            getattr(record, scenario.field) is None
            for scenario in RESOURCE_SCENARIOS.values()
        ):
            return
        if self.resource is not None:
            self.resource.stop()
            self.env.wait(
                lambda: (
                    self.resource.released()
                    and self.quiesce() is True
                    and self.resource.cleanup(self.quiesce, deadline=deadline)
                ),
                deadline,
            )
        require_resources_clean(self.env.refresh())

    def finish_pubsub(self, deadline):
        """After workload teardown, release this actor and require the external fence."""
        if self.env.refresh().pubsub is None:
            return
        if self.pubsub is not None:
            self.pubsub.release()
            self.env.wait(
                lambda: (
                    self.pubsub.released()
                    and self.reap_probe()
                    and self.quiesce() is True
                    and self.pubsub.cleanup(self.quiesce)
                ),
                deadline,
            )
        require_pubsub_clean(self.env.refresh())

    def reap_probe(self):
        """Delete an intended probe Pod that landed after the workload cleared.

        A create whose response was lost can still land once the deletion
        loop has found the namespace empty, and the namespace barrier would
        then wait on it for the rest of cleanup. Always True, so it can lead
        a barrier wait, which calls it again until the Pod is gone. It reads
        only the Pod the intent names, and a read that says nothing about the
        cluster is left to the next poll, as the barrier's own reads are.
        """
        intent = self.env.records.cache.probe_intent
        if not intent:
            return True
        metadata = intent["metadata"]
        try:
            # The namespace must still be the approved one, as every inventory
            # requires, or a replayed Pod in a replacement would be adopted.
            self.env.namespaces()
            obj = self.env.kube.get("Pod", metadata["namespace"], metadata["name"])
            probe = obj and (self.adopt_intended_probe([obj]) or self.env.root("probe"))
        except Failure as error:
            if not transient(error):
                raise
            self.env.emit("probe-unread", {"cause": type(error).__name__})
            return True
        if probe:
            try:
                self.env.kube.delete(probe)
            except Failure:
                pass  # Retried on the barrier's next poll.
        return True


def verify_idle(env):
    env.namespaces()
    control = env.refresh()
    require_services_clean(control)
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
