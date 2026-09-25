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
"""Controller-shaped observations and negative controls for the recovery trial."""

import copy

import pytest
from flink_tier3.common import TransportError
from flink_tier3.exercise import validate_manifests
from flink_tier3.policy import RECOVERY
from test_tier3_lifecycle import env as env  # noqa: PLC0414 - re-export pytest fixture
from test_tier3_lifecycle import obj, prepared_supervisor, rt


class World:
    """Advance a fake controller only after writes and synthetic elapsed time."""

    def __init__(self, env, monkeypatch, fault=None):
        self.kube, self.store, self.approval, self.clock = env
        _, self.supervisor_pod = prepared_supervisor(env)
        self.fault = fault
        self.phase = "initial"
        self.started = self.clock()
        self.changed = self.started
        self.upgrade_at = None
        self.failover_at = None
        self.history = {}
        self.cp = {}
        self.restore = {}
        self.calls = []
        self.original_patch = self.kube.patch
        self.original_delete = self.kube.delete
        self.original_request = self.kube.request
        original_inventory = self.kube.inventory
        monkeypatch.setattr(
            self.kube,
            "inventory",
            lambda *args: [
                o for o in original_inventory(*args) if o["kind"] != "Event"
            ],
        )
        original_items = self.kube.items
        monkeypatch.setattr(
            self.kube,
            "items",
            lambda kind, ns: (
                [
                    o
                    for o in original_inventory()
                    if o["kind"] == "Event" and o["metadata"]["namespace"] == ns
                ]
                if kind == "Event"
                else original_items(kind, ns)
            ),
        )
        self.original_sleep = self.clock.sleep
        app = obj("FlinkDeployment", self.approval["run_id"])
        app["metadata"]["generation"] = 1
        app["spec"] = {
            "image": self.approval["images"]["smoke"],
            "flinkConfiguration": {
                "kubernetes.operator.job.upgrade.last-state-fallback.enabled": "false",
                "kubernetes.operator.snapshot.resource.enabled": "false",
            },
            "job": {
                "upgradeMode": "savepoint",
                "allowNonRestoredState": False,
                "args": [
                    "--run-id",
                    self.approval["run_id"],
                    "--phase",
                    "initial",
                    "--records",
                    "12000",
                    "--records-per-second",
                    "10",
                    "--require-restored",
                    "false",
                ],
            },
        }
        upgrade = copy.deepcopy(app)
        upgrade["spec"]["job"]["args"][3] = "upgrade"
        upgrade["spec"]["job"]["args"][-1] = "true"
        validate_manifests(app, upgrade)
        self.approval.update(
            version=2,
            scenario="generic-recovery",
            recovery_policy=copy.deepcopy(RECOVERY),
            application_sha256=rt.digest(app),
            upgrade_application_sha256=rt.digest(upgrade),
        )
        app["status"] = {"jobStatus": {"state": "RUNNING", "jobId": "1" * 32}}
        self.kube.put(app)
        self.kube.put(
            obj(
                "Service",
                self.approval["run_id"] + "-rest",
                owner=app["metadata"]["uid"],
            )
        )
        monkeypatch.setattr(self.kube, "patch", self.patch)
        monkeypatch.setattr(self.kube, "delete", self.delete)
        monkeypatch.setattr(self.kube, "request", self.request)
        monkeypatch.setattr(self.kube, "logs", self.logs)
        monkeypatch.setattr(self.clock, "sleep", self.sleep)
        self.runner = rt.Supervisor(
            rt.Environment(
                self.kube, self.store, self.approval, self.clock, self.sleep
            ),
            upgrade,
        )
        self.runner.env.remember("application", app)
        self.runner.env.records.set_phase(rt.Phase.READY)
        self.runner.env.records.set_phase(rt.Phase.RUNNING)
        self.pod("jm-initial", "jobmanager")
        self.pod("tm-initial", "taskmanager")

    def application(self):
        return self.kube.get("FlinkDeployment", rt.SMOKE, self.approval["run_id"])

    def pod(self, name, component):
        pod = obj("Pod", name, owner=self.application()["metadata"]["uid"])
        pod["metadata"]["labels"] = {"component": component}
        pod["spec"] = {
            "nodeSelector": {
                "cloud.google.com/gke-spot": (
                    "false" if component == "jobmanager" else "true"
                )
            },
            "containers": [
                {
                    "name": "flink-main-container",
                    "image": self.approval["images"]["smoke"],
                    "resources": {
                        k: rt.POD_RESOURCES["smoke"] for k in ("requests", "limits")
                    },
                }
            ],
        }
        pod["status"] = {"phase": "Running", "containerStatuses": [{"restartCount": 0}]}
        self.kube.put(pod)
        return pod

    def patch(self, app, changes, subresource=""):
        if app["kind"] != "FlinkDeployment" or changes[0]["path"] != "/spec/job/args":
            return self.original_patch(app, changes, subresource)
        self.calls.append("upgrade")
        current = self.application()
        assert current["metadata"]["uid"] == app["metadata"]["uid"]
        assert (
            current["metadata"]["resourceVersion"] == app["metadata"]["resourceVersion"]
        )
        if self.fault == "no-upgrade":
            return current
        if self.fault in ("upgrade-409", "upgrade-422"):
            raise rt.ApiError(int(self.fault[-3:]), "PATCH", "application")
        current["spec"]["job"]["args"] = changes[0]["value"]
        current["metadata"]["generation"] += 1
        current["metadata"]["resourceVersion"] = "2"
        current["status"]["jobStatus"]["state"] = "FINISHED"
        current["status"]["reconciliationStatus"] = {"state": "UPGRADING"}
        self.kube.put(current)
        self.phase, self.changed = "upgrade", self.clock()
        self.upgrade_at = self.changed
        if self.fault == "lost-upgrade-response":
            raise TransportError("Lost patch response")
        return current

    def delete(self, pod, force=False):
        if (
            pod["kind"] != "Pod"
            or pod["metadata"].get("labels", {}).get("component") != "jobmanager"
        ):
            return self.original_delete(pod, force)
        self.calls.append("jm-delete")
        if self.fault == "no-delete":
            return True
        if self.fault == "retiring-jm":
            pod["metadata"]["deletionTimestamp"] = rt.utc(self.clock())
            self.kube.put(pod)
        else:
            self.original_delete(pod, force)
        if self.fault == "missing-jm":
            return False
        self.phase, self.changed = "failover", self.clock()
        self.failover_at = self.changed
        if self.fault == "lost-delete-response":
            raise TransportError("Lost delete response")
        return True

    def request(self, method, path, *args, **kwargs):
        if not path.endswith("/checkpoints"):
            return self.original_request(method, path, *args, **kwargs)
        if self.phase == "failover" and self.clock() - self.changed < 30:
            raise rt.ApiError(503, method, path)
        if self.fault == "rest-auth" and self.phase == "failover":
            raise rt.ApiError(403, method, path)
        return {
            "counts": {"completed": int(bool(self.cp))},
            "latest": {"completed": self.cp, "restored": self.restore},
        }

    def logs(self, pod, since=None):
        return "".join(
            line
            for at, line in self.history.get(pod["metadata"]["uid"], [])
            if since is None or at >= rt.timestamp(since)
        ).encode()

    def sleep(self, seconds):
        self.original_sleep(seconds)
        self.advance()

    def advance(self):
        app = self.application()
        if not app:
            return
        elapsed = self.clock() - self.changed
        if (
            self.fault == "override-before-running"
            and self.phase == "upgrade"
            and 15 <= elapsed < 30
        ):
            # bq1312-alo-50-a1: the Operator assigned the upgraded job's ID
            # while the stopped job's FINISHED state was still reported.
            app["status"]["jobStatus"].update(state="FINISHED", jobId="2" * 32)
            self.kube.put(app)
        if self.phase in ("upgrade", "failover") and elapsed >= 30:
            if self.phase == "upgrade":
                for key, pod in list(self.kube.data.items()):
                    if key[0] == "Pod" and key[1] == rt.SMOKE and "initial" in key[2]:
                        del self.kube.data[key]
                if not self.kube.get("Pod", rt.SMOKE, "jm-upgrade"):
                    self.pod("jm-upgrade", "jobmanager")
                    self.pod("tm-upgrade", "taskmanager")
            elif not self.kube.get("Pod", rt.SMOKE, "jm-failover"):
                self.pod("jm-failover", "jobmanager")
            if self.fault == "retiring-jm" and elapsed >= 90:
                self.kube.data.pop(("Pod", rt.SMOKE, "jm-upgrade"), None)
            app["status"].update(
                observedGeneration=2, reconciliationStatus={"state": "DEPLOYED"}
            )
            app["status"]["jobStatus"].update(state="RUNNING", jobId="2" * 32)
            if self.fault == "new-job-finished" and self.phase == "upgrade":
                app["status"]["jobStatus"]["state"] = "FINISHED"
                app["status"]["reconciliationStatus"]["state"] = "UPGRADING"
            if self.fault == "old-job-finished-deployed" and self.phase == "upgrade":
                app["status"]["jobStatus"].update(state="FINISHED", jobId="1" * 32)
            app["status"]["jobStatus"]["savepointInfo"] = {
                "lastSavepoint": {
                    "location": f"gs://{rt.STATE}/runs/{self.approval['run_id']}/savepoints/sp-1",
                    "timeStamp": int((self.upgrade_at + 10) * 1000),
                    "triggerType": "UPGRADE",
                }
            }
            self.restore = {
                "id": 42 if self.phase == "upgrade" else 43,
                "is_savepoint": self.phase == "upgrade",
                "restore_timestamp": int((self.changed + 30) * 1000),
                "external_path": f"gs://{rt.STATE}/runs/{self.approval['run_id']}/"
                + (
                    "savepoints/sp-1"
                    if self.phase == "upgrade"
                    else "checkpoints/chk-43"
                ),
            }
            if self.fault == "stale-restore" and self.phase == "failover":
                self.restore["restore_timestamp"] = int((self.upgrade_at + 30) * 1000)
            if self.fault == "wrong-savepoint" and self.phase == "upgrade":
                self.restore["external_path"] += "-other"
            if self.fault == "older-checkpoint" and self.phase == "failover":
                self.restore["id"] = 1
            if self.fault == "different-job" and self.phase == "failover":
                app["status"]["jobStatus"]["jobId"] = "3" * 32
        if self.phase == "initial" or elapsed >= 30:
            component = "tm-initial" if self.phase == "initial" else "tm-upgrade"
            task = self.kube.get("Pod", rt.SMOKE, component)
            processed = (
                100
                if self.phase == "initial"
                else (200 if self.phase == "upgrade" else 300)
            )
            if self.phase == "failover" and elapsed >= 120:
                app["status"]["jobStatus"]["state"] = "FINISHED"
                processed = 12000
                if self.fault == "incomplete-input":
                    processed = 300
            if self.fault != "no-progress":
                phase = "initial" if self.phase == "initial" else "upgrade"
                restored = (
                    "false"
                    if self.phase == "initial" or self.fault == "fresh-start"
                    else "true"
                )
                lineage = "11111111-1111-4111-8111-111111111111"
                if self.fault == "new-lineage" and self.phase != "initial":
                    lineage = "22222222-2222-4222-8222-222222222222"
                at = self.clock()
                if self.fault == "stale-progress" and self.phase == "failover":
                    at = self.upgrade_at + 30
                line = (
                    f"{rt.utc(at)} event=smoke-progress run_id={self.approval['run_id']} phase={phase} "
                    f"lineage={lineage} restored={restored} processed={processed} sequence={processed - 1}\n"
                )
                self.history.setdefault(task["metadata"]["uid"], []).append((at, line))
        if elapsed >= (45 if self.phase == "initial" else 75):
            cp_id = {"initial": 1, "upgrade": 43, "failover": 44}[self.phase]
            trigger = self.changed + (30 if self.phase == "initial" else 60)
            self.cp = {
                "id": cp_id,
                "status": "COMPLETED",
                "is_savepoint": False,
                "trigger_timestamp": int(trigger * 1000),
                "latest_ack_timestamp": int((trigger + 1) * 1000),
                "external_path": f"gs://{rt.STATE}/runs/{self.approval['run_id']}/checkpoints/chk-{cp_id}",
            }
            if self.fault == "no-new-checkpoint" and self.phase == "failover":
                self.cp["trigger_timestamp"] = int((self.changed - 5) * 1000)
        if self.fault == "spot-interruption" and self.phase == "upgrade":
            event = obj("Event", "eviction")
            event.update(involvedObject={"uid": "tm-initial-uid"}, reason="Preempted")
            self.kube.put(event)
        self.kube.put(app)

    def run(self):
        self.runner.supervise(self.supervisor_pod["metadata"]["uid"])
        return self.runner.env.refresh()


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "lost-upgrade-response",
        "lost-delete-response",
        "retiring-jm",
        "override-before-running",
    ],
)
def test_integrated_trial_proves_each_recovery_then_cleans(env, monkeypatch, fault):
    world = World(env, monkeypatch, fault)
    result = world.run()
    assert result.success, result.reason
    assert result.recovery["stage"] == "complete"
    assert set(result.recovery["outcomes"]) == {"upgrade", "failover"}
    assert world.calls == ["upgrade", "jm-delete"]
    assert result.phase == rt.Phase.CLEANED
    assert world.application() is None
    assert world.kube.get("Deployment", rt.SYSTEM, rt.OPERATOR)["spec"]["replicas"] == 0
    assert world.store.read(rt.ENVIRONMENT)[0] is not None


@pytest.mark.parametrize(
    "fault",
    [
        "no-upgrade",
        "no-delete",
        "fresh-start",
        "new-lineage",
        "no-progress",
        "stale-progress",
        "stale-restore",
        "no-new-checkpoint",
        "rest-auth",
        "spot-interruption",
        "wrong-savepoint",
        "older-checkpoint",
        "different-job",
        "incomplete-input",
        "missing-jm",
        "new-job-finished",
        "old-job-finished-deployed",
        "upgrade-409",
        "upgrade-422",
    ],
)
def test_missing_phase_evidence_cannot_pass_and_still_cleans(env, monkeypatch, fault):
    world = World(env, monkeypatch, fault)
    result = world.run()
    assert not result.success, fault
    assert result.phase == rt.Phase.CLEANED
    assert world.application() is None
    assert world.calls.count("upgrade") <= 1
    assert world.calls.count("jm-delete") <= 1
    if fault == "spot-interruption":
        assert "Unplanned workload interruption" in result.reason
    if fault == "rest-auth":
        assert "HTTP 403" in result.reason
    if fault == "missing-jm":
        assert "JM disappeared before its planned deletion" in result.reason
    if fault in ("upgrade-409", "upgrade-422"):
        assert "Upgrade precondition rejected" in result.reason
        assert world.calls == ["upgrade"]
    if fault in ("new-job-finished", "old-job-finished-deployed"):
        # FINISHED is the upgrade's transition; only its deadline ends it.
        assert "deadline expired: upgrade" in result.reason
        assert world.clock() >= world.upgrade_at + RECOVERY["recovery_seconds"]
        assert world.calls == ["upgrade"]


@pytest.mark.parametrize(
    "change", ["idle-release", "early-release", "replacement", "deleting-replacement"]
)
def test_completion_distinguishes_idle_release_from_interruption(
    env, monkeypatch, change
):
    world = World(env, monkeypatch)
    advance = world.advance

    def finish_later():
        elapsed = world.clock() - world.changed
        if world.phase != "failover" or elapsed <= 120:
            advance()
        if world.phase != "failover":
            return
        app = world.application()
        if elapsed >= 120:
            # The final progress is observable before idle release; status lags behind it.
            app["status"]["jobStatus"]["state"] = (
                "FINISHED" if elapsed >= 180 else "RUNNING"
            )
            world.kube.put(app)
        remove_at = 105 if change == "early-release" else 135
        if elapsed == remove_at:
            world.kube.data.pop(("Pod", rt.SMOKE, "tm-upgrade"))
            if change in ("replacement", "deleting-replacement"):
                pod = world.pod("tm-unplanned", "taskmanager")
                if change == "deleting-replacement":
                    pod["metadata"]["deletionTimestamp"] = rt.utc(world.clock())
                    world.kube.put(pod)

    monkeypatch.setattr(world, "advance", finish_later)
    result = world.run()
    assert result.success == (change == "idle-release"), result.reason
    assert result.phase == rt.Phase.CLEANED
    assert world.calls == ["upgrade", "jm-delete"]
    if change != "idle-release":
        assert "Unplanned workload replacement" in result.reason


@pytest.mark.parametrize(
    "stop", ["cancellation", "evidence", "cleanup", "lock", "deadline"]
)
@pytest.mark.parametrize("operation", ["upgrade", "failover"])
def test_stop_between_intent_and_write_prevents_disruption(
    env, monkeypatch, stop, operation
):
    world = World(env, monkeypatch)
    persist = world.runner.exercise.persist

    def interrupted(stage, **details):
        persist(stage, **details)
        if stage != operation:
            return
        if stop == "cancellation":
            world.runner.env.records.request_stop()
        elif stop == "evidence":
            world.runner.env.records.mark_evidence_failed()
        elif stop == "cleanup":
            world.runner.env.records.begin_cleanup("concurrent recovery")
        elif stop == "lock":
            world.runner.env.assert_owner = lambda: (_ for _ in ()).throw(
                rt.Failure("lock changed")
            )
        else:
            world.clock.now = world.runner.exercise.deadline

    monkeypatch.setattr(world.runner.exercise, "persist", interrupted)
    if stop == "lock":
        with pytest.raises(rt.Failure, match="lock changed"):
            world.run()
    else:
        assert not world.run().success
    assert world.calls == ([] if operation == "upgrade" else ["upgrade"])


def test_recovery_approval_is_explicit_and_legacy_approval_still_loads(
    env, monkeypatch
):
    assert rt.Approval.from_dict(env[2]).scenario == "smoke"
    world = World(env, monkeypatch)
    for field, value in [
        ("version", 1),
        ("recovery_policy", {}),
        ("upgrade_application_sha256", ""),
        ("scenario", "other"),
    ]:
        approval = {**world.approval, field: value}
        with pytest.raises(rt.Failure):
            rt.Approval.from_dict(approval)


def test_recovery_does_not_resume_an_existing_exercise(env, monkeypatch):
    world = World(env, monkeypatch)
    world.runner.env.records.recovery_step(None, {"stage": "upgrade"})
    result = world.run()
    assert not result.success
    assert world.calls == []


def test_final_receipt_requires_completed_exercise(env, monkeypatch):
    world = World(env, monkeypatch)
    result = world.run()
    assert result.success
    # Finalization independently checks the exercise, even if a stale success flag survived.
    runner = rt.lifecycle.runner_api.Runner(world.runner.env)
    job = world.kube.get("Job", rt.SYSTEM, "supervisor")
    job["status"]["succeeded"] = 1
    world.kube.put(job)
    runner.settle()
    assert runner.env.refresh().success
    assert not runner.env.refresh().evidence_failed
    world.runner.env.records._change(
        lambda record: setattr(record, "recovery", {"stage": "failover"})
    )
    assert not runner.finalize(
        {
            "nonce": world.approval["nonce"],
            "empty": True,
            "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
        }
    )
