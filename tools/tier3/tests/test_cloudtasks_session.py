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
"""Fault injection for Cloud Tasks session admission, queue ownership and cells."""

import copy
import json
from decimal import Decimal
from pathlib import Path

import pytest
from flink_tier3 import cloudtasks as ct
from flink_tier3 import lifecycle as cli
from test_tier3_lifecycle import env as env  # noqa: PLC0414 - re-export pytest fixture
from test_tier3_lifecycle import obj, prepared_supervisor, rt, supervisor_pod

ROOT = Path(__file__).resolve().parents[3]
DIGEST = "sha256:" + "e" * 64
IMAGE = rt.GAR + "cloudtasks-measurement@" + DIGEST

CELL_A = {
    "id": "example-wiring-a",
    "arm": "UNNAMED",
    "body_bytes": 1024,
    "parallelism": 1,
    "concurrency": 1,
    "checkpoint_seconds": 1,
    "channel_pool_size": 1,
    "distribution": "even",
    "offered_rate": 10,
    "warmup_seconds": 60,
    "observation_seconds": 180,
    "record_limit": 2430,
    "attempt_limit": 3645,
    "control_delay_millis": 0,
    "emit_attempts": True,
}
CELL_B = {
    **CELL_A,
    "id": "example-wiring-b",
    "arm": "STAGED_HASH",
    "parallelism": 4,
    "concurrency": 4,
    "checkpoint_seconds": 10,
    "offered_rate": 100,
    "record_limit": 26100,
    "attempt_limit": 9788,
}


def shape(cell, component):
    shapes = rt.CLOUDTASKS_POD_RESOURCES
    if component == "jobmanager":
        return shapes["jobmanager"]
    return shapes["taskmanager"]["p" + str(cell["parallelism"])]


def manifest(cell, run_id, nonce, image=IMAGE, line="2.2.1"):
    """A FlinkDeployment shaped like kubernetes/pkg/cloudtasks/application.cue."""
    storage = ct.state_prefix(run_id, cell["id"])

    def component(name):
        resources = shape(cell, name)
        return {
            "replicas": 1,
            "resource": {"cpu": int(resources["cpu"]), "memory": resources["memory"]},
            "podTemplate": {
                "spec": {
                    "nodeSelector": {"cloud.google.com/gke-spot": "true"},
                    "containers": [
                        {
                            "name": "flink-main-container",
                            "resources": {"requests": resources, "limits": resources},
                        }
                    ],
                }
            },
        }

    return {
        "apiVersion": "flink.apache.org/v1beta1",
        "kind": "FlinkDeployment",
        "metadata": {
            "namespace": rt.CLOUDTASKS,
            "name": cell["id"],
            "labels": {rt.LABEL: run_id},
            "annotations": {
                rt.NONCE: nonce,
                ct.SCENARIO: "cloudtasks",
                ct.CELL: cell["id"],
            },
        },
        "spec": {
            "image": image,
            "flinkVersion": rt.FLINK_LINES[line][1],
            "serviceAccount": ct.SERVICE_ACCOUNT,
            "mode": "native",
            "flinkConfiguration": {
                "taskmanager.numberOfTaskSlots": str(cell["parallelism"]),
                "restart-strategy.type": "fixed-delay",
                "restart-strategy.fixed-delay.attempts": "3",
                "execution.checkpointing.interval": f"{cell['checkpoint_seconds']} s",
                "execution.checkpointing.dir": storage + "/checkpoints",
                "execution.checkpointing.savepoint-dir": storage + "/savepoints",
                "high-availability.storageDir": storage + "/ha",
            },
            "jobManager": component("jobmanager"),
            "taskManager": component("taskmanager"),
            "job": {
                "jarURI": ct.JAR,
                "entryClass": ct.ENTRY_CLASS,
                "parallelism": cell["parallelism"],
                "upgradeMode": "stateless",
                "allowNonRestoredState": False,
                "args": ct.cell_arguments(
                    cell,
                    run_id,
                    rt.queue_name(run_id),
                    rt.CLOUDTASKS_POLICY["target"],
                ),
            },
        },
    }


class FakeQueues:
    """The approved queue as the service would report it, with injectable faults.

    Each actor holds its own client and meter, as in production; ``actor()``
    returns a sibling bound to the same service-side queue state.
    """

    def __init__(self, name, meter=None, shared=None):
        if name != rt.queue_name(name.rsplit("ct1246-", 1)[-1]):
            raise rt.Failure("Queue is outside this run's ownership")
        self.name, self.meter = name, meter or rt.Meter()
        self.shared = shared or {
            "state": None,
            "calls": [],
            "exists_before_create": False,
            "pause_leaves_running": False,
            "executed_last_minute": 0,
            "fail_delete": False,
            "tombstoned": False,
            "on_pause": None,
            "read_errors": [],
        }

    def actor(self):
        return FakeQueues(self.name, shared=self.shared)

    def __getattr__(self, name):
        shared = self.__dict__.get("shared", {})
        if name in shared:
            return shared[name]
        raise AttributeError(name)

    def __setattr__(self, name, value):
        if name in self.__dict__.get("shared", {}):
            self.shared[name] = value
        else:
            super().__setattr__(name, value)

    def get(self, read_mask=ct.QUEUE_READ_MASK):
        self.meter.tick("read_ops")
        self.calls.append(("get", read_mask))
        if self.read_errors:
            raise self.read_errors.pop(0)
        if self.state is None and not self.exists_before_create:
            return None
        return {
            "name": self.name,
            "state": self.state or "RUNNING",
            **copy.deepcopy(ct.QUEUE_CONFIGURATION),
            "stats": {
                "tasksCount": "0",
                "executedLastMinuteCount": str(self.executed_last_minute),
                "concurrentDispatchesCount": "0",
            },
        }

    def create(self):
        self.meter.tick("admin_write_ops")
        self.calls.append(("create",))
        if self.tombstoned:
            raise rt.ApiError(400, "POST", "queues")
        if self.state is not None or self.exists_before_create:
            raise rt.ApiError(409, "POST", "queues")
        self.state = "RUNNING"
        return {"name": self.name, "state": self.state}

    def pause(self):
        self.meter.tick("admin_write_ops")
        self.calls.append(("pause",))
        if self.state is None:
            raise rt.ApiError(404, "POST", self.name + ":pause")
        if not self.pause_leaves_running:
            self.state = "PAUSED"
        if self.on_pause:
            self.on_pause()
        return {"name": self.name, "state": self.state}

    def delete(self):
        self.meter.tick("admin_write_ops")
        self.calls.append(("delete",))
        if self.fail_delete:
            raise rt.ApiError(500, "DELETE", self.name)
        existed = self.state is not None
        self.state, self.exists_before_create = None, False
        return existed


def session_approval(env, cells=(CELL_A, CELL_B), line="2.2.1", image=IMAGE):
    """Rewrite the smoke fixture's approval into a Cloud Tasks session approval."""
    kube, _store, approval, clock = env
    run_id = approval["run_id"]
    manifests = [
        manifest(cell, run_id, approval["nonce"], image, line) for cell in cells
    ]
    plan = rt.session_plan(list(cells)) + rt.CLOUDTASKS_CEILINGS["cleanup_seconds"]
    quota = obj("ResourceQuota", "tier3-idle", rt.CLOUDTASKS)
    quota["metadata"]["uid"] = rt.CLOUDTASKS + "-quota"
    quota["spec"]["hard"] = {"pods": "0", "persistentvolumeclaims": "0"}
    quota["status"] = {
        "hard": copy.deepcopy(quota["spec"]["hard"]),
        "used": {"pods": "0"},
    }
    kube.put(quota)
    kube.data.pop(("ResourceQuota", rt.SMOKE, "tier3-idle"))
    namespaces = {
        rt.CLOUDTASKS: {
            "uid": rt.CLOUDTASKS + "-uid",
            "quota_uid": quota["metadata"]["uid"],
            "hard": copy.deepcopy(quota["spec"]["hard"]),
        },
        rt.SYSTEM: approval["namespaces"][rt.SYSTEM],
    }
    images = {k: v for k, v in approval["images"].items() if k != "smoke"}
    images["application"] = image
    approval.update(
        version=3,
        scenario="cloudtasks",
        expires_at=rt.utc(clock() + plan),
        cleanup_at=rt.utc(clock() + plan - 900),
        namespaces=namespaces,
        images=images,
        campaign="example",
        flink_version=line,
        queue=rt.queue_name(run_id),
        target=rt.CLOUDTASKS_POLICY["target"],
        cells=[
            {**cell, "manifest_sha256": rt.digest(m)}
            for cell, m in zip(cells, manifests, strict=True)
        ],
        cloudtasks_ceilings=copy.deepcopy(rt.CLOUDTASKS_CEILINGS),
        cloudtasks_pod_resources=copy.deepcopy(rt.CLOUDTASKS_POD_RESOURCES),
        application_sha256=rt.digest(manifests),
    )
    return manifests


def session_environment(env, queues=None, actor="supervisor"):
    kube, store, approval, clock = env
    queues = queues or FakeQueues(approval["queue"])
    return rt.Environment(
        kube,
        store,
        approval,
        clock,
        clock.sleep,
        actor=actor,
        queues=queues,
        ledger=rt.Ledger(store, approval["campaign"]),
    )


def runner_for(env, queues=None):
    return cli.runner_api.Runner(session_environment(env, queues, actor="runner"))


class CellWorld:
    """A fake Operator: creating a cell's FlinkDeployment brings up JM/TM Pods."""

    def __init__(self, env, manifests, monkeypatch, polls_to_finish=2):
        self.kube, self.store, self.approval, self.clock = env
        self.manifests = manifests
        self.polls_to_finish = polls_to_finish
        self.polls = {}
        self.deployments_seen = []
        self.concurrent_max = 0
        self.fail_state = None
        self.create = self.kube.create
        monkeypatch.setattr(self.kube, "create", self.created)
        monkeypatch.setattr(self.kube, "request", self.request)
        monkeypatch.setattr(self.clock, "sleep", self.sleep)
        self.sleep_original = type(self.clock).sleep

    def cell(self, name):
        return next(c for c in self.approval["cells"] if c["id"] == name)

    def created(self, value, dry_run=False):
        result = self.create(value, dry_run)
        if value["kind"] != "FlinkDeployment" or dry_run:
            return result
        live = [o for o in self.kube.inventory() if o["kind"] == "FlinkDeployment"]
        self.concurrent_max = max(self.concurrent_max, len(live))
        self.deployments_seen.append(value["metadata"]["name"])
        name, uid = value["metadata"]["name"], result["metadata"]["uid"]
        cell = self.cell(name)
        current = self.kube.get("FlinkDeployment", rt.CLOUDTASKS, name)
        current["status"] = {"jobStatus": {"state": "RUNNING", "jobId": "1" * 32}}
        current["metadata"]["finalizers"] = ["flink.apache.org"]
        self.kube.put(current)
        for component in ("jobmanager", "taskmanager"):
            pod = obj("Pod", f"{name}-{component}", rt.CLOUDTASKS, uid)
            pod["metadata"]["labels"] = {"app": name, "component": component}
            pod["spec"] = {
                "nodeSelector": {"cloud.google.com/gke-spot": "true"},
                "containers": [
                    {
                        "name": "flink-main-container",
                        "image": self.approval["images"]["application"],
                        "resources": {
                            k: copy.deepcopy(shape(cell, component))
                            for k in ("requests", "limits")
                        },
                    }
                ],
            }
            pod["status"]["phase"] = "Running"
            self.kube.put(pod)
        self.kube.put(obj("Service", name + "-rest", rt.CLOUDTASKS, uid))
        # Checkpoint state the cell leaves behind in the benchmark bucket.
        self.store.write(
            f"runs/{self.approval['run_id']}/cells/{name}/state/chk-1/_metadata",
            {"chk": 1},
            bucket=rt.BENCHMARK,
        )
        self.store.write(
            f"runs/{self.approval['run_id']}/cells/{name}/receipts/creator-x-start",
            {"role": "creator"},
            bucket=rt.BENCHMARK,
        )
        return result

    def request(self, method, path, body=None, **kwargs):
        if "/proxy/jobs/" in path:
            assert method == "GET" and kwargs.get("limit")
            return {"counts": {"completed": 3, "in_progress": 0}, "latest": {}}
        assert path.endswith("/scale")
        return type(self.kube).request(self.kube, method, path, body, **kwargs)

    def sleep(self, seconds):
        self.sleep_original(self.clock, seconds)
        for app in [o for o in self.kube.inventory() if o["kind"] == "FlinkDeployment"]:
            name = app["metadata"]["name"]
            self.polls[name] = self.polls.get(name, 0) + 1
            if self.polls[name] >= self.polls_to_finish:
                current = self.kube.get("FlinkDeployment", rt.CLOUDTASKS, name)
                current["status"]["jobStatus"]["state"] = self.fail_state or "FINISHED"
                self.kube.put(current)


def prepared_session(env, manifests, queues=None):
    """A supervisor whose Job and Pod already exist, before admission."""
    environment = session_environment(env, queues)
    supervisor = rt.Supervisor(environment, cells=manifests)
    job = env[0].put(obj("Job", "supervisor", rt.SYSTEM))
    environment.remember("supervisor", job)
    return supervisor, supervisor_pod(env, job)


def admitted_session(env, manifests, queues=None):
    """Run the runner's admission so the supervisor finds a paused queue."""
    supervisor, pod = prepared_session(env, manifests, queues)
    runner = runner_for(env, supervisor.env.queues.actor())
    runner.cleanup.scale_operator(1)
    runner.cleanup.quota(rt.CLOUDTASKS, "session")
    runner.env.ledger.admit(runner.env.approval.cell_ids)
    ct.admit_queue(runner.env)
    runner.env.records.operations("runner", runner.env.queues.meter.snapshot())
    runner.env.records.set_phase(rt.Phase.READY)
    runner.env.records.set_phase(rt.Phase.RUNNING)
    return supervisor, pod, runner


# --- approval and policy contract ---------------------------------------------


def test_session_approval_is_accepted_and_smoke_fields_are_exclusive(env):
    smoke = rt.Approval.from_dict(env[2], env[3]())
    assert "cells" not in smoke.to_dict() and "queue" not in smoke.to_dict()
    session_approval(env)
    approval = rt.Approval.from_dict(env[2], env[3]())
    assert approval.application_namespace == rt.CLOUDTASKS
    assert approval.cell_ids == [CELL_A["id"], CELL_B["id"]]
    assert approval.to_dict()["cells"] == env[2]["cells"]
    assert rt.validate_approval(approval.to_dict(), env[3]()) is None
    downgraded = copy.deepcopy(env[2])
    downgraded.update(scenario="smoke", version=1)
    with pytest.raises(rt.Failure, match="cannot carry"):
        rt.validate_approval(downgraded)


@pytest.mark.parametrize(
    "change, message",
    [
        (lambda a: a.update(queue=a["queue"] + "x"), "own ct1246 queue"),
        (lambda a: a.update(target="https://example.com/"), "target differs"),
        (
            lambda a: a["images"].update(
                application=rt.GAR + "cloudtasks-measurement-flink120@" + DIGEST
            ),
            "GAR digest",
        ),
        (
            lambda a: a["images"].update(application=IMAGE.replace("@sha256", "@md5")),
            "GAR digest",
        ),
        (lambda a: a["cells"][0].update(extra=1), "Cell fields"),
        (lambda a: a["cells"].append(copy.deepcopy(a["cells"][0])), "unique"),
        (lambda a: a["cells"][1].update(attempt_limit=10**7), "3 [*] records"),
        (lambda a: a["cells"][1].update(record_limit=1), "record_limit"),
        (
            lambda a: a["cells"][0].update(control_delay_millis=100, arm="STAGED_HASH"),
            "Delay control",
        ),
        (lambda a: a["cells"][0].update(offered_rate=10.0), "integer"),
        (
            lambda a: a["cloudtasks_ceilings"].update(task_creations=10**9),
            "fixed session",
        ),
        (
            lambda a: a.update(expires_at=rt.utc(rt.timestamp(a["expires_at"]) + 601)),
            "over-reservation",
        ),
        (lambda a: a.update(cleanup_at=a["expires_at"]), "over-reservation"),
        (lambda a: a.update(flink_version="1.19.0"), "Unsupported Flink"),
        (lambda a: a.update(campaign="Bad Campaign"), "campaign"),
        (lambda a: a.update(recovery_policy=rt.RunRecord("x").to_dict()), "recovery"),
        (
            lambda a: a["namespaces"].update({rt.SMOKE: a["namespaces"][rt.SYSTEM]}),
            "namespaces",
        ),
    ],
)
def test_session_approval_refuses_unapproved_inputs(env, change, message):
    session_approval(env)
    change(env[2])
    with pytest.raises(rt.Failure, match=message):
        rt.validate_approval(env[2], env[3]())
    assert env[0].calls == []


def test_session_ceilings_refuse_too_many_cells_or_creations():
    cells = [{**CELL_A, "id": f"c{i}", "manifest_sha256": "a" * 64} for i in range(21)]
    with pytest.raises(rt.Failure, match="cell ceiling"):
        rt.validate_cells(cells)
    cells = [
        {
            **CELL_B,
            "id": f"c{i}",
            "parallelism": 16,
            "concurrency": 16,
            "checkpoint_seconds": 60,
            "offered_rate": 10000,
            "warmup_seconds": 120,
            "observation_seconds": 600,
            "record_limit": 10000000,
            "attempt_limit": 600000,
            "manifest_sha256": "a" * 64,
        }
        for i in range(2)
    ]
    # 600,000 attempts x 16 subtasks x 4 incarnations x 2 cells = 76,800,000.
    with pytest.raises(rt.Failure, match="task creation ceiling"):
        rt.validate_cells(cells)
    from flink_tier3.model import validate_cell

    too_few = dict(cells[0], attempt_limit=525624)  # one below the creator share
    with pytest.raises(rt.Failure, match="record share"):
        validate_cell(too_few)
    skewed = dict(cells[0], distribution="skew", attempt_limit=7568999)
    with pytest.raises(rt.Failure, match="record share"):
        validate_cell(skewed)
    validate_cell(dict(skewed, attempt_limit=7569000))
    # One subtask receives every record whatever the distribution says.
    single = {**CELL_A, "distribution": "skew", "attempt_limit": 2429}
    with pytest.raises(rt.Failure, match="record share"):
        validate_cell(single, manifest=False)
    validate_cell({**single, "attempt_limit": 2430}, manifest=False)


def test_session_cost_and_quota_shapes_stay_within_policy():
    cells = [
        {
            **CELL_B,
            "id": f"c{i}",
            "parallelism": 16,
            "concurrency": 16,
            "offered_rate": 1000,
            "record_limit": 261000,
            "attempt_limit": 16313,  # one creator's share of 261,000 records
            "manifest_sha256": "a" * 64,
        }
        for i in range(9)
    ]
    plan = rt.validate_cells(cells)
    # Planning bound: 16,313 attempts x 16 subtasks x 4 incarnations per cell.
    assert plan["task_creations"] == 9 * 16313 * 16 * 4 == 9396288
    assert plan["task_creations"] == sum(rt.cell_creations(c) for c in cells)
    cost = rt.estimated_session_cost(rt.CLOUDTASKS_CEILINGS["session_seconds"], cells)
    assert Decimal(9) < cost <= Decimal(rt.CLOUDTASKS_CEILINGS["additional_cost_usd"])
    too_many = [dict(c, id=f"c{i}") for i, c in enumerate(cells * 2)][:12]
    with pytest.raises(rt.Failure, match="task creation ceiling"):
        rt.validate_cells(too_many)
    # Eleven such cells stay under the creation ceiling but not under USD 10.
    expensive = [dict(c, id=f"c{i}") for i, c in enumerate(cells * 2)][:11]
    assert rt.validate_cells(expensive)["task_creations"] == 11484352
    assert rt.estimated_session_cost(18000, expensive) > Decimal("10.00")
    approval = rt.Approval(
        **{
            k: None
            for k in (
                "version",
                "run_id",
                "nonce",
                "sha",
                "started_at",
                "expires_at",
                "cleanup_at",
                "ceilings",
                "namespaces",
                "images",
                "operator_uid",
                "baseline_uids",
                "lock_owner",
                "runtime_sha256",
                "application_sha256",
            )
        },
        cells=cells,
        cloudtasks_pod_resources=rt.CLOUDTASKS_POD_RESOURCES,
    )
    from flink_tier3.cleanup import session_resources

    assert session_resources(approval) == {
        "cpu": "5",
        "memory": "18Gi",
        "ephemeral-storage": "5Gi",
    }
    approval = rt.Approval(
        **{**approval.__dict__, "cells": [cells[0] | {"parallelism": 1}]}
    )
    assert session_resources(approval) == {
        "cpu": "2",
        "memory": "6Gi",
        "ephemeral-storage": "2Gi",
    }


def test_reviewed_session_policy_preserves_the_fixed_contract():
    assert rt.CLOUDTASKS_CEILINGS == {
        "session_seconds": 18000,
        "cleanup_seconds": 900,
        "pods": 4,
        "pvcs": 0,
        "cells": 20,
        "task_creations": 12000000,
        "admin_write_ops": 6,
        "read_ops": 60000,
        "dispatches": 0,
        "state_bytes": 1073741824,
        "state_objects": 20000,
        "log_bytes": 104857600,
        "evidence_bytes": 4294967296,
        "receipt_bytes_supervisor": 268435456,
        "receipt_bytes_runner": 33554432,
        "additional_cost_usd": "10.00",
    }
    assert rt.CLOUDTASKS_POD_RESOURCES == {
        "jobmanager": {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "1Gi"},
        "taskmanager": {
            "p1": {"cpu": "1", "memory": "4Gi", "ephemeral-storage": "1Gi"},
            "p4": {"cpu": "2", "memory": "8Gi", "ephemeral-storage": "2Gi"},
            "p16": {"cpu": "4", "memory": "16Gi", "ephemeral-storage": "4Gi"},
        },
    }
    assert rt.CLOUDTASKS_POLICY["target"] == "https://ct1246.invalid/task"
    assert (
        cli.CLOUDTASKS_APPROVAL
        == "APPROVE ONE CLOUD TASKS SESSION: 4 PODS, 300 MINUTES, USD 10, 0 DISPATCHES"
    )
    assert rt.CEILINGS["evidence_bytes"] == 104857600  # smoke unchanged
    session = rt.load_session(
        ROOT / "kubernetes/lifecycle/sessions/example-wiring.toml"
    )
    assert session["campaign"] == "example"
    assert session["cells"] == [CELL_A, CELL_B]


# --- queue client ---------------------------------------------------------------


class Response:
    def __init__(self, status, body=None):
        self.status_code = status
        self.body = body
        self.content = b"" if body is None else json.dumps(body).encode()

    def json(self):
        return self.body


class Http:
    def __init__(self, replies):
        self.replies, self.calls = list(replies), []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.replies.pop(0)


def test_queue_client_binds_one_name_and_meters_every_operation():
    name = rt.queue_name("test-1310")
    with pytest.raises(rt.Failure, match="ownership"):
        rt.Queues(Http([]), name.replace("ct1246-", "other-"))
    with pytest.raises(rt.Failure, match="ownership"):
        rt.Queues(Http([]), name + "/tasks/x")
    http = Http(
        [
            Response(404),
            Response(200, {"name": name, "state": "RUNNING"}),
            Response(200, {"name": name, "state": "PAUSED"}),
            Response(200, {"name": name, "state": "PAUSED", "stats": {}}),
            Response(200),
            Response(404),
            Response(503),
        ]
    )
    queues = rt.Queues(http, name)
    assert queues.get() is None
    assert queues.create()["state"] == "RUNNING"
    assert queues.pause()["state"] == "PAUSED"
    assert queues.get(ct.QUEUE_POLL_MASK)["state"] == "PAUSED"
    assert queues.delete() is True
    assert queues.delete() is False
    with pytest.raises(rt.ApiError) as error:
        queues.get()
    assert error.value.status == 503
    assert queues.meter.snapshot() == {"read_ops": 3, "admin_write_ops": 4}
    methods = [(c[0], c[1].removeprefix(ct.BASE)) for c in http.calls]
    assert methods[0] == ("GET", name + "?readMask=" + ct.QUEUE_READ_MASK)
    assert methods[1] == ("POST", ct.PARENT + "/queues")
    assert http.calls[1][2]["json"] == {"name": name, **ct.QUEUE_CONFIGURATION}
    assert methods[2] == ("POST", name + ":pause")
    assert methods[4] == ("DELETE", name)
    assert all(
        c[2]["timeout"] == rt.HTTP_TIMEOUT and c[2]["allow_redirects"] is False
        for c in http.calls
    )


def test_meter_stops_at_the_session_ceiling():
    meter = rt.Meter({"read_ops": 2, "admin_write_ops": 1})
    meter.tick("read_ops")
    meter.tick("read_ops")
    with pytest.raises(rt.Failure, match="read_ops ceiling"):
        meter.tick("read_ops")
    meter.tick("admin_write_ops")
    with pytest.raises(rt.Failure, match="admin_write_ops ceiling"):
        meter.tick("admin_write_ops")


# --- queue admission and ownership --------------------------------------------


def test_existing_queue_is_refused_before_any_write(env):
    manifests = session_approval(env)
    queues = FakeQueues(env[2]["queue"])
    queues.exists_before_create = True
    runner = runner_for(env, queues)
    with pytest.raises(rt.Failure, match="does not own"):
        ct.admit_queue(runner.env)
    assert queues.calls == [("get", ct.QUEUE_POLL_MASK)]
    assert manifests and env[0].calls == []


def test_readback_that_is_not_paused_stops_admission_and_deletes_the_queue(env):
    manifests = session_approval(env)
    queues = FakeQueues(env[2]["queue"])
    queues.pause_leaves_running = True
    _supervisor, _pod = prepared_session(env, manifests, queues)
    runner = runner_for(env, queues)
    runner.cleanup.scale_operator(1)
    with pytest.raises(rt.Failure, match="paused approved configuration"):
        ct.admit_queue(runner.env)
    assert runner.env.refresh().queue is None
    runner.settle(request_stop=True)
    assert ("delete",) in queues.calls
    assert queues.get() is None
    assert runner.env.refresh().phase == rt.Phase.CLEANED
    rt.verify_idle(runner.env)


def test_tombstoned_name_is_refused_without_renaming(env):
    session_approval(env)
    queues = FakeQueues(env[2]["queue"])
    queues.tombstoned = True
    runner = runner_for(env, queues)
    with pytest.raises(rt.ApiError):
        ct.admit_queue(runner.env)
    assert [c[0] for c in queues.calls] == ["get", "create"]


def test_verify_queue_stops_on_dispatch_or_state_change(env):
    session_approval(env)
    queues = FakeQueues(env[2]["queue"])
    environment = session_environment(env, queues)
    queues.create()
    queues.pause()
    ct.verify_queue(environment)
    queues.executed_last_minute = 1
    with pytest.raises(rt.Failure, match="deviated"):
        ct.verify_queue(environment)
    queues.executed_last_minute = 0
    queues.state = "RUNNING"
    with pytest.raises(rt.Failure, match="deviated"):
        ct.verify_queue(environment)
    events = [
        d["event"]
        for (b, n), (d, _g) in env[1].data.items()
        if n.startswith("runs/test-1310/supervisor/")
    ]
    assert events.count("queue-deviation") == 2


# --- campaign ledger ------------------------------------------------------------


def test_ledger_refuses_completed_or_running_cells_and_settles_once(env):
    store, clock = env[1], env[3]
    ledger = rt.Ledger(store, "example")
    ledger.admit(["a", "b"])
    ledger.claim("a", "run-1", "n" * 32, clock())
    with pytest.raises(rt.Failure, match="already running"):
        ledger.admit(["a"])
    with pytest.raises(rt.Failure, match="claimed twice"):
        ledger.claim("a", "run-2", "m" * 32, clock())
    with pytest.raises(rt.Failure, match="not this run"):
        ledger.settle("a", "run-2", "m" * 32, "completed", "x", clock())
    ledger.settle("a", "run-1", "n" * 32, "completed", "finished", clock())
    with pytest.raises(rt.Failure, match="already completed"):
        ledger.admit(["a"])
    ledger.claim("b", "run-1", "n" * 32, clock())
    ledger.interrupt("run-1", clock())
    value, _ = ledger.read()
    assert value["cells"]["a"]["status"] == "completed"
    assert value["cells"]["b"] == {
        "status": "interrupted",
        "run_id": "run-1",
        "nonce": "n" * 32,
        "at": rt.utc(clock()),
        "attempts": 1,
        "reason": "session stopped",
    }
    ledger.claim("b", "run-3", "k" * 32, clock())
    assert ledger.read()[0]["cells"]["b"]["attempts"] == 2
    with pytest.raises(rt.Failure, match="Unknown ledger outcome"):
        ledger.settle("b", "run-3", "k" * 32, "skipped", "x", clock())
    with pytest.raises(rt.Failure, match="campaign"):
        rt.Ledger(store, "Bad")


def test_ledger_retries_a_concurrent_write_once(env):
    store, clock = env[1], env[3]
    ledger = rt.Ledger(store, "example")
    other = rt.Ledger(store, "example")
    store.before_write = lambda: other.claim("z", "run-9", "z" * 32, clock())
    ledger.claim("a", "run-1", "n" * 32, clock())
    cells = ledger.read()[0]["cells"]
    assert set(cells) == {"a", "z"}
    assert store.conflicts == 1


# --- cell manifests ---------------------------------------------------------------


def test_cell_manifest_contract_holds_and_each_deviation_is_refused(env):
    manifests = session_approval(env)
    approval = rt.Approval.from_dict(env[2])
    assert (
        rt.validate_cell_manifest(manifests[0], approval.cells[0], approval)
        == "cloudtasks-measurement"
    )
    # The literal list pins the order the CUE package renders; a change in
    # either place must fail here, not only in the CUE contract test.
    assert manifests[0]["spec"]["job"]["args"] == [
        "--run-id",
        "test-1310",
        "--cell-id",
        "example-wiring-a",
        "--queue",
        "projects/flink-gcp/locations/us-central1/queues/ct1246-test-1310",
        "--target",
        "https://ct1246.invalid/task",
        "--arm",
        "UNNAMED",
        "--body-bytes",
        "1024",
        "--parallelism",
        "1",
        "--concurrency",
        "1",
        "--checkpoint-seconds",
        "1",
        "--channel-pool-size",
        "1",
        "--distribution",
        "even",
        "--offered-rate",
        "10",
        "--warmup-seconds",
        "60",
        "--observation-seconds",
        "180",
        "--record-limit",
        "2430",
        "--attempt-limit",
        "3645",
        "--control-delay-millis",
        "0",
        "--emit-attempts",
        "true",
    ]
    deviations = [
        ("job differs", lambda m: m["spec"]["job"]["args"].reverse()),
        (
            "runtime differs",
            lambda m: m["spec"].update(image=IMAGE.replace("e" * 64, "f" * 64)),
        ),
        ("runtime differs", lambda m: m["spec"].update(flinkVersion="v1_20")),
        (
            "Flink configuration",
            lambda m: m["spec"]["flinkConfiguration"].update(
                {"taskmanager.numberOfTaskSlots": "2"}
            ),
        ),
        (
            "Flink configuration",
            lambda m: m["spec"]["flinkConfiguration"].update(
                {"high-availability.storageDir": "gs://elsewhere/ha"}
            ),
        ),
        ("identity differs", lambda m: m["metadata"].update(namespace=rt.SMOKE)),
        (
            "identity differs",
            lambda m: m["metadata"]["annotations"].update({rt.NONCE: "f" * 32}),
        ),
        ("job differs", lambda m: m["spec"]["job"].update(upgradeMode="savepoint")),
        (
            "container resources",
            lambda m: m["spec"]["taskManager"]["podTemplate"]["spec"]["containers"][0][
                "resources"
            ]["limits"].update(memory="32Gi"),
        ),
        ("exactly one", lambda m: m["spec"]["taskManager"].update(replicas=2)),
        ("runtime differs", lambda m: m["spec"].update(serviceAccount="smoke")),
        (
            "Flink configuration",
            lambda m: m["spec"]["flinkConfiguration"].update(
                {"restart-strategy.fixed-delay.attempts": "5"}
            ),
        ),
        (
            "Flink configuration",
            lambda m: m["spec"]["flinkConfiguration"].update(
                {"restart-strategy.type": "exponential-delay"}
            ),
        ),
    ]
    for message, change in deviations:
        broken = copy.deepcopy(manifests[1])
        change(broken)
        with pytest.raises(rt.Failure, match=message):
            rt.validate_cell_manifest(broken, approval.cells[1], approval)


def test_delivered_cells_must_match_every_pin_before_cloud_access(env, tmp_path):
    manifests = session_approval(env)
    approval = rt.Approval.from_dict(env[2])
    (tmp_path / "application.json").write_text(json.dumps(manifests))
    assert ct.load_cells(tmp_path, approval) == manifests
    (tmp_path / "application.json").write_text(json.dumps(manifests[:1]))
    with pytest.raises(rt.Failure, match="differ from approval"):
        ct.load_cells(tmp_path, approval)
    swapped = [manifests[1], manifests[0]]
    (tmp_path / "application.json").write_text(json.dumps(swapped))
    with pytest.raises(rt.Failure, match="differ from approval"):
        ct.load_cells(tmp_path, approval)
    (tmp_path / "application.json").write_text(json.dumps(manifests))
    env[2]["cells"][0]["manifest_sha256"] = "0" * 64
    with pytest.raises(rt.Failure, match="approved pin"):
        ct.load_cells(tmp_path, rt.Approval.from_dict(env[2]))


def test_session_file_rejects_unknown_shapes(tmp_path):
    path = tmp_path / "s.toml"
    path.write_text('campaign = "x"\n[[cells]]\nid = "a"\n')
    with pytest.raises(rt.Failure, match="Cell fields"):
        rt.load_session(path)
    path.write_text('campaign = "x"\ncells = []\nextra = 1\n')
    with pytest.raises(rt.Failure, match="exactly campaign and cells"):
        rt.load_session(path)
    path.write_text("not toml [[")
    with pytest.raises(rt.Failure, match="Unreadable"):
        rt.load_session(path)


# --- supervisor session -------------------------------------------------------------


def evidence_events(env, actor="supervisor"):
    return [
        d["event"]
        for (b, n), (d, _g) in env[1].data.items()
        if b == rt.EVIDENCE and n.startswith(f"runs/test-1310/{actor}/")
    ]


def test_session_runs_cells_one_at_a_time_and_reaches_a_verified_idle(env, monkeypatch):
    manifests = session_approval(env)
    world = CellWorld(env, manifests, monkeypatch)
    supervisor, pod, runner = admitted_session(env, manifests)
    supervisor.supervise(pod["metadata"]["uid"])
    assert world.deployments_seen == [CELL_A["id"], CELL_B["id"]]
    assert world.concurrent_max == 1
    control = supervisor.env.refresh()
    assert control.phase == rt.Phase.CLEANED and control.success
    assert {k: v["status"] for k, v in control.cells.items()} == {
        CELL_A["id"]: "completed",
        CELL_B["id"]: "completed",
    }
    ledger = supervisor.env.ledger.read()[0]["cells"]
    assert all(entry["status"] == "completed" for entry in ledger.values())
    assert supervisor.env.queues.get() is None
    benchmark = [n for (b, n) in env[1].data if b == rt.BENCHMARK]
    assert not any("/state/" in n for n in benchmark)
    assert len([n for n in benchmark if "/receipts/" in n]) == 2
    events = evidence_events(env)
    assert events.count("cell-start") == 2 and events.count("cell-finished") == 2
    assert "queue-admitted" in evidence_events(env, "runner")
    assert "queue-deleted" in events and "benchmark-evidence-retained" in events
    job = env[0].get("Job", rt.SYSTEM, "supervisor")
    job["status"]["succeeded"] = 1
    env[0].put(job)
    runner.settle()
    assert runner.env.refresh().idle
    plans = {"nonce": env[2]["nonce"], "roots": cli.wf.ROOTS, "empty": True}
    # The collector records each verified export on the control record, which
    # finalize deletes; the final receipt is what outlives it.
    runner.env.records.record_export(
        CELL_A["id"],
        {
            "outcome": "completed",
            "objects": 3,
            "evidence_bytes": 4096,
            "reconciliation": {"status": "complete", "reasons": []},
            "manifest_sha256": "a" * 64,
        },
    )
    assert runner.finalize(plans) is True
    result = env[1].read("runs/test-1310/result.json")[0]
    assert result["success"]
    assert result["exported"] == {
        CELL_A["id"]: {
            "outcome": "completed",
            "objects": 3,
            "evidence_bytes": 4096,
            "reconciliation": "complete",
            "manifest_sha256": "a" * 64,
        }
    }
    assert result["evidence_bytes"] == 4096
    assert result["scenario"] == "cloudtasks" and result["campaign"] == "example"
    assert set(result["benchmark_evidence_retained"]) == {CELL_A["id"], CELL_B["id"]}
    assert result["queue"]["admitted"]["state"] == "PAUSED"
    # Each actor records its own meter: the runner's create and pause, the
    # supervisor's zero writes before its last cell record.
    assert result["operations"]["runner"]["admin_write_ops"] == 2
    assert result["operations"]["supervisor"]["admin_write_ops"] == 0
    assert result["operations"]["supervisor"]["read_ops"] > 2
    assert supervisor.env.queues.meter.snapshot()["admin_write_ops"] == 2  # cleanup


def test_dispatch_during_a_cell_stops_the_session_and_interrupts_the_ledger(
    env, monkeypatch
):
    manifests = session_approval(env)
    world = CellWorld(env, manifests, monkeypatch, polls_to_finish=99)
    supervisor, pod, _runner = admitted_session(env, manifests)
    queues = supervisor.env.queues
    sleep = world.sleep

    def dispatch_then_sleep(seconds):
        queues.executed_last_minute = 1
        sleep(seconds)

    monkeypatch.setattr(supervisor.env, "sleep", dispatch_then_sleep)
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.phase == rt.Phase.CLEANED and not control.success
    assert "deviated" in control.reason
    assert world.deployments_seen == [CELL_A["id"]]
    assert ("delete", "FlinkDeployment", CELL_A["id"] + "-uid") in env[0].calls
    assert queues.get() is None
    ledger = supervisor.env.ledger.read()[0]["cells"]
    assert ledger[CELL_A["id"]]["status"] == "interrupted"
    assert CELL_B["id"] not in ledger


def test_stop_request_mid_cell_tears_down_with_uid_preconditions(env, monkeypatch):
    manifests = session_approval(env)
    world = CellWorld(env, manifests, monkeypatch, polls_to_finish=99)
    supervisor, pod, _runner = admitted_session(env, manifests)
    sleep = world.sleep

    def stop_then_sleep(seconds):
        supervisor.env.records.request_stop()
        sleep(seconds)

    monkeypatch.setattr(supervisor.env, "sleep", stop_then_sleep)
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.phase == rt.Phase.CLEANED and not control.success
    deletes = [c for c in env[0].calls if c[0] == "delete"]
    assert ("delete", "FlinkDeployment", CELL_A["id"] + "-uid") in deletes
    assert supervisor.env.queues.get() is None
    assert supervisor.env.ledger.read()[0]["cells"][CELL_A["id"]]["status"] == (
        "interrupted"
    )
    assert control.cell_intent is not None  # the interrupted cell keeps its intent
    job = env[0].get("Job", rt.SYSTEM, "supervisor")
    job["status"]["succeeded"] = 1
    env[0].put(job)
    _runner.settle(request_stop=True)
    rt.verify_idle(_runner.env)
    assert _runner.env.refresh().idle


def test_failed_cell_is_recorded_and_the_session_continues(env, monkeypatch):
    manifests = session_approval(env)
    world = CellWorld(env, manifests, monkeypatch)
    world.fail_state = "FAILED"
    supervisor, pod, _runner = admitted_session(env, manifests)
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.phase == rt.Phase.CLEANED and not control.success
    assert world.deployments_seen == [CELL_A["id"], CELL_B["id"]]
    assert {k: v["status"] for k, v in control.cells.items()} == {
        CELL_A["id"]: "failed",
        CELL_B["id"]: "failed",
    }
    ledger = supervisor.env.ledger.read()[0]["cells"]
    assert all(e["status"] == "failed" for e in ledger.values())
    assert all(e["reason"] == "Flink job entered FAILED" for e in ledger.values())


def test_evidence_failure_stops_before_the_next_cell(env, monkeypatch):
    manifests = session_approval(env)
    world = CellWorld(env, manifests, monkeypatch)
    supervisor, pod, _runner = admitted_session(env, manifests)
    sleep = world.sleep

    def fail_evidence_then_sleep(seconds):
        env[1].fail_evidence = True
        sleep(seconds)

    monkeypatch.setattr(supervisor.env, "sleep", fail_evidence_then_sleep)
    supervisor.supervise(pod["metadata"]["uid"])
    assert world.deployments_seen == [CELL_A["id"]]
    control = supervisor.env.refresh()
    assert control.phase == rt.Phase.CLEANED and control.evidence_failed
    assert not control.success


def test_cell_exhausting_the_window_is_skipped_not_started(env, monkeypatch):
    manifests = session_approval(env)
    # Cell A must end after the point where cell B's budget no longer fits
    # before cleanup, yet before its own deadline; derive that from policy.
    from flink_tier3.model import cell_budget_seconds, cell_nominal_seconds

    cleanup_at = rt.timestamp(env[2]["cleanup_at"])
    latest_start_b = cleanup_at - cell_budget_seconds(CELL_B) - env[3]()
    deadline_a = (
        rt.CLOUDTASKS_POLICY["cell_startup_seconds"]
        + 4 * cell_nominal_seconds(CELL_A)
        + rt.CLOUDTASKS_POLICY["cell_teardown_seconds"]
    )
    polls = int(latest_start_b // rt.POLL) + 2
    assert latest_start_b < polls * rt.POLL < deadline_a
    world = CellWorld(env, manifests, monkeypatch, polls_to_finish=polls)
    supervisor, pod, _runner = admitted_session(env, manifests)
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert world.deployments_seen == [CELL_A["id"]]
    assert control.cells[CELL_A["id"]]["status"] == "completed"
    assert control.cells[CELL_B["id"]] == {
        "status": "skipped",
        "reason": "session window exhausted",
    }
    assert CELL_B["id"] not in supervisor.env.ledger.read()[0]["cells"]
    assert control.phase == rt.Phase.CLEANED and not control.success


def test_cell_deadline_fails_the_cell_and_the_rest_is_skipped(env, monkeypatch):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch, polls_to_finish=10**6)
    supervisor, pod, _runner = admitted_session(env, manifests)
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.cells[CELL_A["id"]] == {
        "status": "failed",
        "reason": "cell deadline",
    }
    assert control.cells[CELL_B["id"]]["status"] == "skipped"
    ledger = supervisor.env.ledger.read()[0]["cells"]
    assert ledger[CELL_A["id"]]["status"] == "failed"
    assert control.phase == rt.Phase.CLEANED and not control.success


def test_cell_cut_by_the_session_window_says_so(env, monkeypatch):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch, polls_to_finish=10**6)
    env[2]["cells"] = env[2]["cells"][:1]
    env[2]["application_sha256"] = rt.digest(manifests[:1])
    plan = rt.session_plan(env[2]["cells"]) + 900
    env[2]["expires_at"] = rt.utc(env[3]() + plan + 500)
    env[2]["cleanup_at"] = rt.utc(env[3]() + plan + 500 - 900)
    supervisor, pod, _runner = admitted_session(env, manifests[:1])
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.cells[CELL_A["id"]]["reason"] == "session window"
    assert control.phase == rt.Phase.CLEANED


def test_lost_cell_create_response_is_adopted_by_nonce(env, monkeypatch):
    manifests = session_approval(env)
    world = CellWorld(env, manifests, monkeypatch)
    supervisor, pod, _runner = admitted_session(env, manifests)
    created = world.created

    def lose_first(value, dry_run=False):
        result = created(value, dry_run)
        if value["kind"] == "FlinkDeployment" and not dry_run and not world.lost:
            world.lost = True
            raise rt.Failure("Lost cell response")
        return result

    world.lost = False
    monkeypatch.setattr(env[0], "create", lose_first)
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.success
    assert control.roots["cell:" + CELL_A["id"]]["uid"] == CELL_A["id"] + "-uid"


def test_pod_log_truncation_is_recorded_not_fatal_for_sessions(env, monkeypatch):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch)
    supervisor, pod, _runner = admitted_session(env, manifests)
    monkeypatch.setattr(env[0], "logs", lambda _pod, since=None: b"x" * rt.MIB)
    supervisor.log_bytes = rt.CEILINGS["log_bytes"]
    supervisor.supervise(pod["metadata"]["uid"])
    assert supervisor.env.refresh().success
    events = evidence_events(env)
    assert "pod-log-truncated" in events
    assert "pod-log-collection-stopped" in events


def test_runner_admits_session_only_after_a_ready_supervisor(env, monkeypatch):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch)
    queues = FakeQueues(env[2]["queue"])
    runner = runner_for(env, queues)
    config = obj("ConfigMap", "source", rt.SYSTEM)
    job = obj("Job", "supervisor", rt.SYSTEM)
    job["spec"]["template"] = {
        "spec": {"containers": [{"image": env[2]["images"]["supervisor"]}]}
    }
    sleep = env[3].sleep
    calls_before_supervisor = []

    def publish(seconds):
        current = env[0].get("Job", rt.SYSTEM, "supervisor")
        if current and not env[0].items("Pod", rt.SYSTEM):
            calls_before_supervisor.extend(queues.calls)
            supervisor_pod(env, current)
            runner.env.records.heartbeat()
        sleep(seconds)

    monkeypatch.setattr(runner.env, "sleep", publish)
    runner.start(config, job, None)
    control = runner.env.refresh()
    assert control.phase == rt.Phase.RUNNING
    assert control.queue["state"] == "PAUSED"
    assert control.operations["runner"] == {"read_ops": 2, "admin_write_ops": 2}
    assert calls_before_supervisor == []  # no queue call before the supervisor ran
    assert [c[0] for c in queues.calls] == ["get", "create", "pause", "get"]
    quota = env[0].get("ResourceQuota", rt.CLOUDTASKS, "tier3-idle")["spec"]["hard"]
    assert quota["pods"] == "2" and quota["requests.cpu"] == "3"
    assert quota["requests.memory"] == "10Gi"
    assert not any(o["kind"] == "FlinkDeployment" for o in env[0].inventory())


def test_session_admission_deadline_blocks_a_late_queue_admission(env):
    manifests = session_approval(env)
    runner = runner_for(env)
    env[3].now += rt.CLOUDTASKS_POLICY["cell_startup_seconds"]
    with pytest.raises(rt.Failure, match="Session admission deadline"):
        runner.admission_open()
    assert manifests and env[0].calls == []


def test_application_pods_must_match_their_cell_shape_and_select_spot(env, monkeypatch):
    manifests = session_approval(env)
    world = CellWorld(env, manifests, monkeypatch, polls_to_finish=99)
    supervisor, pod, _runner = admitted_session(env, manifests)
    created = world.created

    def wrong_shape(value, dry_run=False):
        result = created(value, dry_run)
        if value["kind"] == "FlinkDeployment" and not dry_run:
            tm = env[0].get(
                "Pod", rt.CLOUDTASKS, value["metadata"]["name"] + "-taskmanager"
            )
            tm["spec"]["containers"][0]["resources"]["limits"]["memory"] = "32Gi"
            env[0].put(tm)
        return result

    monkeypatch.setattr(env[0], "create", wrong_shape)
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert "Effective Pod resources" in control.reason
    assert control.phase == rt.Phase.CLEANED and not control.success
    spot = manifests[0]["spec"]["jobManager"]["podTemplate"]["spec"]
    assert spot["nodeSelector"] == {"cloud.google.com/gke-spot": "true"}
    naked = copy.deepcopy(
        env[0].get("Pod", rt.CLOUDTASKS, CELL_A["id"] + "-jobmanager")
        or obj("Pod", "x", rt.CLOUDTASKS)
    )
    naked["spec"] = {
        "containers": [
            {
                "name": "flink-main-container",
                "image": IMAGE,
                "resources": {
                    k: shape(CELL_A, "jobmanager") for k in ("requests", "limits")
                },
            }
        ]
    }
    with pytest.raises(rt.Failure, match="Spot"):
        rt.verify_pod(naked, "application", IMAGE, shape(CELL_A, "jobmanager"))


def test_smoke_supervisor_still_fails_on_a_truncated_log(env, monkeypatch):
    from test_tier3_lifecycle import prepared_supervisor

    supervisor, _pod = prepared_supervisor(env)
    monkeypatch.setattr(env[0], "logs", lambda _pod, since=None: b"x" * rt.MIB)
    with pytest.raises(rt.Failure, match="ceiling"):
        supervisor.telemetry([], [_pod])


# --- negative controls the first review round found missing ---------------------


def test_cell_intent_is_refused_outside_the_running_phase(env):
    session_approval(env)
    environment = session_environment(env)
    intent = {"cell": "x", "manifest_sha256": "a" * 64}
    with pytest.raises(rt.Failure, match="stopped"):
        environment.records.intend("cell", intent)
    environment.records.set_phase(rt.Phase.READY)
    with pytest.raises(rt.Failure, match="stopped"):
        environment.records.intend("cell", intent)
    environment.records.set_phase(rt.Phase.RUNNING)
    environment.records.intend("cell", intent)
    assert environment.refresh().cell_intent == intent


def test_verify_idle_fails_while_the_queue_exists_and_waits_on_a_transient_read(env):
    session_approval(env)
    environment = session_environment(env)
    environment.queues.create()
    # A queue this run never intended to create is not this run's to judge.
    rt.verify_idle(environment)
    environment.records.set_phase(rt.Phase.READY)
    environment.records.intend("queue")
    environment.queues.pause()
    with pytest.raises(rt.Failure, match="queue remains"):
        rt.verify_idle(environment)
    environment.queues.delete()
    environment.queues.read_errors.append(rt.ApiError(503, "GET", "q"))
    with pytest.raises(rt.IdlePending):
        rt.verify_idle(environment)
    environment.queues.read_errors.append(rt.ApiError(403, "GET", "q"))
    with pytest.raises(rt.ApiError):
        rt.verify_idle(environment)
    rt.verify_idle(environment)


def test_verify_queue_stops_when_the_queue_vanished(env):
    session_approval(env)
    environment = session_environment(env)
    with pytest.raises(rt.Failure, match="deviated"):
        ct.verify_queue(environment)


def test_transient_queue_reads_are_tolerated_twice_then_stop_the_session(
    env, monkeypatch
):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch, polls_to_finish=6)
    supervisor, pod, _runner = admitted_session(env, manifests)
    queues = supervisor.env.queues
    queues.read_errors.extend(
        [rt.ApiError(503, "GET", "q"), rt.ApiError(429, "GET", "q")]
    )
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.success
    assert evidence_events(env).count("queue-read-unavailable") == 2


def test_three_consecutive_transient_queue_reads_stop_the_session(env, monkeypatch):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch, polls_to_finish=99)
    supervisor, pod, _runner = admitted_session(env, manifests)
    supervisor.env.queues.read_errors.extend([rt.ApiError(503, "GET", "q")] * 3)
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert not control.success and "HTTP 503" in control.reason
    assert evidence_events(env).count("queue-read-unavailable") == 2
    assert supervisor.env.queues.get() is None


def test_permission_failure_on_a_queue_read_stops_immediately(env, monkeypatch):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch, polls_to_finish=99)
    supervisor, pod, _runner = admitted_session(env, manifests)
    supervisor.env.queues.read_errors.append(rt.ApiError(403, "GET", "q"))
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert not control.success and "HTTP 403" in control.reason
    assert "queue-read-unavailable" not in evidence_events(env)


def test_queue_client_translates_transport_and_credential_errors():
    class Broken:
        def __init__(self, error):
            self.error = error

        def request(self, *_args, **_kwargs):
            raise self.error

    import google.auth.exceptions
    import requests

    name = rt.queue_name("test-1310")
    for error in (
        requests.exceptions.ConnectionError("reset"),
        google.auth.exceptions.RefreshError("metadata"),
    ):
        with pytest.raises(rt.Failure, match="Cloud Tasks request failed") as caught:
            rt.Queues(Broken(error), name).get()
        assert ct.transient(caught.value)
    assert ct.transient(rt.ApiError(503, "GET", "q"))
    assert not ct.transient(rt.ApiError(403, "GET", "q"))
    assert not ct.transient(rt.Failure("x"))


def test_runner_refuses_a_session_whose_cell_is_already_running_before_any_queue_write(
    env, monkeypatch
):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch)
    queues = FakeQueues(env[2]["queue"])
    runner = runner_for(env, queues)
    runner.env.ledger.claim(CELL_B["id"], "other-run", "f" * 32, env[3]())
    config = obj("ConfigMap", "source", rt.SYSTEM)
    job = obj("Job", "supervisor", rt.SYSTEM)
    job["spec"]["template"] = {
        "spec": {"containers": [{"image": env[2]["images"]["supervisor"]}]}
    }
    sleep = env[3].sleep

    def publish(seconds):
        current = env[0].get("Job", rt.SYSTEM, "supervisor")
        if current and not env[0].items("Pod", rt.SYSTEM):
            supervisor_pod(env, current)
            runner.env.records.heartbeat()
        sleep(seconds)

    monkeypatch.setattr(runner.env, "sleep", publish)
    with pytest.raises(rt.Failure, match="already running"):
        runner.start(config, job, None)
    assert queues.calls == []
    assert runner.env.refresh().phase == rt.Phase.READY


def test_workload_that_survives_its_teardown_window_stops_the_session(env, monkeypatch):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch)
    supervisor, pod, _runner = admitted_session(env, manifests)
    env[0].normal_cleanup = False  # the Operator never removes the deployment
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert "remains after its teardown window" in control.reason
    assert control.phase == rt.Phase.CLEANED and not control.success
    assert supervisor.env.ledger.read()[0]["cells"][CELL_A["id"]]["status"] == (
        "interrupted"
    )
    assert not any(o["kind"] == "FlinkDeployment" for o in env[0].inventory())
    assert any(
        c[0] == "patch" and c[3][0]["path"] == "/metadata/finalizers"
        for c in env[0].calls
    )


def test_lost_cell_response_with_a_foreign_nonce_is_not_adopted(env, monkeypatch):
    manifests = session_approval(env)
    world = CellWorld(env, manifests, monkeypatch)
    supervisor, pod, _runner = admitted_session(env, manifests)
    created = world.created

    def foreign_then_lose(value, dry_run=False):
        result = created(value, dry_run)
        if value["kind"] == "FlinkDeployment" and not dry_run:
            current = env[0].get(
                "FlinkDeployment", rt.CLOUDTASKS, value["metadata"]["name"]
            )
            current["metadata"]["annotations"][rt.NONCE] = "f" * 32
            env[0].put(current)
            raise rt.Failure("Lost cell response")
        return result

    monkeypatch.setattr(env[0], "create", foreign_then_lose)
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert "nonce differs" in control.reason
    assert "cell:" + CELL_A["id"] not in control.roots


def test_settlement_adopts_a_lost_cell_only_with_the_approved_nonce_and_spec(env):
    manifests = session_approval(env)
    runner = runner_for(env)
    env[1].write("runs/test-1310/application.json", manifests)
    runner.env.records.set_phase(rt.Phase.READY)
    runner.env.records.set_phase(rt.Phase.RUNNING)
    intent = {
        "cell": CELL_A["id"],
        "manifest_sha256": env[2]["cells"][0]["manifest_sha256"],
    }
    runner.env.records.intend("cell", intent)
    inventory = runner.cleanup.inventory
    assert runner.cleanup.adopt_intended_cell(inventory()) is None
    foreign = copy.deepcopy(manifests[0])
    foreign["metadata"]["annotations"][rt.NONCE] = "f" * 32
    foreign["metadata"]["uid"] = "foreign-uid"
    env[0].put(foreign)
    assert runner.cleanup.adopt_intended_cell(inventory()) is None
    env[0].data.pop(("FlinkDeployment", rt.CLOUDTASKS, CELL_A["id"]))
    drifted = env[0].create(copy.deepcopy(manifests[0]))
    drifted["spec"]["job"]["parallelism"] = 4
    env[0].put(drifted)
    assert runner.cleanup.adopt_intended_cell(inventory()) is None
    assert evidence_events(env, "runner").count("cell-adoption-refused") == 2
    assert "cell:" + CELL_A["id"] not in runner.env.roots
    env[0].data.pop(("FlinkDeployment", rt.CLOUDTASKS, CELL_A["id"]))
    env[0].create(copy.deepcopy(manifests[0]))
    # Without the approved manifest nothing can be verified: refuse, not adopt.
    _, generation = env[1].read("runs/test-1310/application.json")
    env[1].delete("runs/test-1310/application.json", generation)
    assert runner.cleanup.adopt_intended_cell(inventory()) is None
    assert evidence_events(env, "runner").count("cell-adoption-refused") == 3
    env[1].write("runs/test-1310/application.json", manifests)
    adopted = runner.cleanup.adopt_intended_cell(inventory())
    assert adopted["metadata"]["uid"] == CELL_A["id"] + "-uid"
    assert runner.env.roots["cell:" + CELL_A["id"]]["uid"] == CELL_A["id"] + "-uid"


def test_a_cell_root_cannot_be_admitted_twice_in_one_session(env, monkeypatch):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch)
    supervisor, _pod, _runner = admitted_session(env, manifests)
    supervisor.env.remember(
        "cell:" + CELL_A["id"], obj("FlinkDeployment", CELL_A["id"], rt.CLOUDTASKS)
    )
    with pytest.raises(rt.Failure, match="already admitted"):
        supervisor.session.execute(supervisor.env.approval.cells[0], manifests[0])


def cli_session(env, monkeypatch, tmp_path, expires_in, complete_job=True):
    """Drive ``cli.start`` for the cloudtasks scenario against the fakes."""
    from types import SimpleNamespace

    manifests = session_approval(env)
    env[1].data.clear()
    owner = env[2]["lock_owner"]
    for key, value in {
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_SHA": owner["sha"],
        "GITHUB_ACTOR": "fixture",
        "GITHUB_OUTPUT": str(tmp_path / "output"),
    }.items():
        monkeypatch.setenv(key, value)
    new_uuid = cli.uuid.uuid4
    uuids = iter([cli.uuid.UUID(env[2]["nonce"])])
    monkeypatch.setattr(cli.uuid, "uuid4", lambda: next(uuids, None) or new_uuid())
    monkeypatch.setattr(cli.time, "time", env[3])
    monkeypatch.setattr(cli, "ROOT", ROOT)
    monkeypatch.setattr(cli.wf, "execution", lambda _kind, _nonce: owner)
    monkeypatch.setattr(cli.wf, "external", lambda _path, **kwargs: env[0])
    monkeypatch.setattr(
        cli.bootstrap,
        "Cluster",
        lambda _path: SimpleNamespace(can_i=lambda *args: None),
    )
    monkeypatch.setattr(
        cli.wf,
        "snapshot",
        lambda _kube, *_a: (
            env[2]["namespaces"],
            env[2]["operator_uid"],
            env[2]["images"]["operator"],
            env[2]["baseline_uids"],
        ),
    )
    monkeypatch.setattr(rt, "GoogleToken", lambda: None)
    monkeypatch.setattr(rt, "authorized_session", lambda _token: None)
    monkeypatch.setattr(cli.wf, "image_receipts", lambda *_args: {})
    queues = FakeQueues(env[2]["queue"])
    monkeypatch.setattr(
        cli, "Queues", lambda _http, name: FakeQueues(name, shared=queues.shared)
    )
    config, job = (
        obj("ConfigMap", "source", rt.SYSTEM),
        obj("Job", "supervisor", rt.SYSTEM),
    )
    job["spec"]["template"] = {
        "spec": {"containers": [{"image": env[2]["images"]["supervisor"]}]}
    }
    renders = []

    def render(*args, **kwargs):
        renders.append(kwargs)
        if kwargs.get("expression") == "cellManifests":
            return copy.deepcopy(manifests)
        return {"config": config, "supervisor": job}

    monkeypatch.setattr(cli.wf, "render", render)
    environment = rt.Environment

    def publishing_environment(kube, store, approval, **kwargs):
        made = environment(kube, store, approval, env[3], env[3].sleep, **kwargs)
        sleep = made.sleep

        def publish(seconds):
            current = env[0].get("Job", rt.SYSTEM, "supervisor")
            if current and not env[0].items("Pod", rt.SYSTEM):
                supervisor_pod(env, current)
                made.records.heartbeat()
            sleep(seconds)

        made.sleep = publish
        return made

    monkeypatch.setattr(rt, "Environment", publishing_environment)
    if complete_job:

        def finish_job():
            current = env[0].get("Job", rt.SYSTEM, "supervisor")
            current["status"]["succeeded"] = 1
            env[0].put(current)

        queues.on_pause = finish_job
    args = SimpleNamespace(
        approve=cli.CLOUDTASKS_APPROVAL,
        sha=owner["sha"],
        run_id=env[2]["run_id"],
        expires_at=rt.utc(env[3]() + expires_in),
        directory=tmp_path,
        kubeconfig=tmp_path / "kubeconfig",
        scenario="cloudtasks",
        session="example-wiring",
        flink_version="2.2.1",
        application_digest=DIGEST,
    )
    return args, queues, renders, manifests


def test_cli_start_admits_a_session_and_settles_to_idle(env, monkeypatch, tmp_path):
    plan = rt.session_plan([CELL_A, CELL_B]) + 900
    args, queues, renders, manifests = cli_session(
        env, monkeypatch, tmp_path, plan + 600
    )
    cli.start(args, env[1])
    assert (tmp_path / "output").read_text() == "idle=true\n"
    approval = env[1].read("runs/test-1310/approval.json")[0]
    assert approval["version"] == 3 and approval["scenario"] == "cloudtasks"
    assert approval["campaign"] == "example"
    assert approval["images"]["application"] == IMAGE
    assert [c["id"] for c in approval["cells"]] == [CELL_A["id"], CELL_B["id"]]
    assert approval["cells"][0]["manifest_sha256"] == rt.digest(manifests[0])
    assert env[1].read("runs/test-1310/application.json")[0] == manifests
    assert env[1].read("runs/test-1310/session.json")[0]["campaign"] == "example"
    tags = renders[0]
    assert tags["expression"] == "cellManifests" and tags["scenario"] == "cloudtasks"
    assert json.loads(tags["cells"]) == [CELL_A, CELL_B]
    assert tags["flink_version"] == "2.2.1" and tags["application_image"] == IMAGE
    assert tags["target"] == rt.CLOUDTASKS_POLICY["target"]
    assert [c[0] for c in queues.calls][:4] == ["get", "get", "create", "pause"]
    assert queues.get() is None  # settled and deleted
    assert env[1].read(rt.ENVIRONMENT)[0] is not None  # lock retained for plans


@pytest.mark.parametrize("extra", [601, -1])
def test_cli_start_refuses_a_session_window_outside_plan_plus_ten_minutes(
    env, monkeypatch, tmp_path, extra
):
    plan = rt.session_plan([CELL_A, CELL_B]) + 900
    args, queues, _renders, _m = cli_session(env, monkeypatch, tmp_path, plan + extra)
    with pytest.raises(rt.Failure, match="Dispatch expiry"):
        cli.start(args, env[1])
    assert queues.calls == [] and env[1].read(rt.ENVIRONMENT)[0] is None


def test_cli_start_refuses_before_the_lock_when_the_queue_or_ledger_is_taken(
    env, monkeypatch, tmp_path
):
    plan = rt.session_plan([CELL_A, CELL_B]) + 900
    args, queues, _renders, _m = cli_session(env, monkeypatch, tmp_path, plan + 60)
    queues.exists_before_create = True
    with pytest.raises(rt.Failure, match="does not own"):
        cli.start(args, env[1])
    queues.exists_before_create = False
    rt.Ledger(env[1], "example").claim(CELL_A["id"], "other", "f" * 32, env[3]())
    with pytest.raises(rt.Failure, match="already running"):
        cli.start(args, env[1])
    assert env[1].read(rt.ENVIRONMENT)[0] is None
    assert not [n for (b, n) in env[1].data if n.startswith("runs/")]


def test_smoke_prepared_supervisor_and_session_share_one_pod_shape(env):
    _runner, pod = prepared_supervisor(env)
    assert pod["spec"]["containers"][0]["image"] == env[2]["images"]["supervisor"]


# --- independent review findings ---------------------------------------------------


def test_refused_foreign_queue_is_never_deleted_by_settlement(env, monkeypatch):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch)
    queues = FakeQueues(env[2]["queue"])
    queues.exists_before_create = True
    _supervisor, _pod = prepared_session(env, manifests, queues)
    runner = runner_for(env, queues.actor())
    runner.cleanup.scale_operator(1)
    with pytest.raises(rt.Failure, match="does not own"):
        ct.admit_queue(runner.env)
    runner.settle(request_stop=True)
    assert ("delete",) not in queues.calls and ("pause",) not in queues.calls
    assert queues.get() is not None  # the foreign queue survives
    assert "queue-retained-unowned" in evidence_events(env, "runner")
    assert runner.env.refresh().idle
    rt.verify_idle(runner.env)


def test_cleanup_adopts_a_cell_whose_create_landed_after_the_stop(env, monkeypatch):
    manifests = session_approval(env)
    world = CellWorld(env, manifests, monkeypatch)
    supervisor, _pod, runner = admitted_session(env, manifests)
    env[1].write("runs/test-1310/application.json", manifests)
    # The supervisor persisted its intent; its create is still in flight.
    supervisor.env.records.intend(
        "cell",
        {
            "cell": CELL_A["id"],
            "manifest_sha256": env[2]["cells"][0]["manifest_sha256"],
        },
    )
    inventory = env[0].inventory
    landed = {"done": False}

    def late_landing():
        items = inventory()
        # Land only once cleanup has begun, after settlement's own adoption
        # read: the snapshot that starts cleanup must not contain the cell.
        if not landed["done"] and runner.env.records.cache.phase == rt.Phase.CLEANING:
            landed["done"] = True
            world.created(copy.deepcopy(manifests[0]))
            return items
        return inventory()

    monkeypatch.setattr(env[0], "inventory", late_landing)
    runner.settle(request_stop=True)
    assert ("delete", "FlinkDeployment", CELL_A["id"] + "-uid") in env[0].calls
    assert not any(o["kind"] == "FlinkDeployment" for o in inventory())
    assert runner.env.refresh().roots["cell:" + CELL_A["id"]]["uid"] == (
        CELL_A["id"] + "-uid"
    )
    rt.verify_idle(runner.env)


def test_first_cell_keeps_the_startup_admission_already_spent(env, monkeypatch):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch)
    env[2]["cells"] = env[2]["cells"][:1]
    env[2]["application_sha256"] = rt.digest(manifests[:1])
    plan = rt.session_plan(env[2]["cells"]) + 900
    env[2]["expires_at"] = rt.utc(env[3]() + plan)  # the minimum window
    env[2]["cleanup_at"] = rt.utc(env[3]() + plan - 900)
    supervisor, pod, _runner = admitted_session(env, manifests[:1])
    env[3].now += 120  # admission took two minutes of the startup allowance
    # A live clock advances between reads; the skip rule must read it once.
    original = env[3].__call__

    def ticking():
        env[3].now += 1
        return original()

    monkeypatch.setattr(supervisor.env, "clock", ticking)
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.cells[CELL_A["id"]]["status"] == "completed"
    assert control.success


def test_first_cell_credit_never_exceeds_the_startup_allowance(env, monkeypatch):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch)
    env[2]["cells"] = env[2]["cells"][:1]
    env[2]["application_sha256"] = rt.digest(manifests[:1])
    plan = rt.session_plan(env[2]["cells"]) + 900
    # With 50 s of slack, crediting the full 700 s would admit the cell
    # (1023 - 700 = 323 <= 373) while the capped credit refuses it (423 > 373).
    env[2]["expires_at"] = rt.utc(env[3]() + plan + 50)
    env[2]["cleanup_at"] = rt.utc(env[3]() + plan + 50 - 900)
    supervisor, pod, _runner = admitted_session(env, manifests[:1])
    env[3].now += 700
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.cells[CELL_A["id"]]["status"] == "skipped"


def test_session_cost_gate_refuses_a_session_under_the_creation_ceiling(env):
    session_approval(env)
    base = env[2]["cells"][1]
    big = {
        **base,
        "parallelism": 16,
        "concurrency": 16,
        "checkpoint_seconds": 60,
        "warmup_seconds": 120,
        "observation_seconds": 360,
        "offered_rate": 900,
        "record_limit": 540900,
        "attempt_limit": 37000,  # above the 33,807 creator share
    }
    small = {
        **base,
        "arm": "UNNAMED",
        "parallelism": 1,
        "concurrency": 1,
        "checkpoint_seconds": 60,
        "warmup_seconds": 120,
        "observation_seconds": 360,
        "offered_rate": 1,
        "record_limit": 601,
        "attempt_limit": 601,
    }
    cells = [dict(big, id=f"big-{i}") for i in range(5)] + [
        dict(small, id=f"small-{i}") for i in range(7)
    ]
    plan = rt.session_plan(cells)
    window = rt.CLOUDTASKS_CEILINGS["session_seconds"]
    assert plan + 900 <= window <= plan + 900 + 600
    env[2]["cells"] = cells
    env[2]["expires_at"] = rt.utc(env[3]() + window)
    env[2]["cleanup_at"] = rt.utc(env[3]() + window - 900)
    assert rt.validate_cells(cells)["task_creations"] == 11840000 + 7 * 601 * 4
    assert rt.estimated_session_cost(window, cells) > Decimal("10.00")
    with pytest.raises(rt.Failure, match="session cost exceeds"):
        rt.validate_approval(env[2], env[3]())


def test_a_failed_export_leaves_the_cell_claimable_in_the_campaign(env, monkeypatch):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch)
    supervisor, pod, _runner = admitted_session(env, manifests)

    class FailingExport(rt.SessionHooks):
        def after_cell(self, session, cell, outcome):
            raise rt.Failure("Exported object differs from its source")

    supervisor.session.hooks = FailingExport()
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert not control.success and "differs from its source" in control.reason
    # The ledger never settled the cell, so cleanup marks it interrupted and a
    # later reviewed session may claim it again.
    ledger = supervisor.env.ledger.read()[0]["cells"]
    assert ledger[CELL_A["id"]]["status"] == "interrupted"
    assert CELL_A["id"] not in control.cells
    supervisor.env.ledger.claim(
        CELL_A["id"], "later-run", "f" * 32, supervisor.env.clock()
    )
    assert supervisor.env.ledger.read()[0]["cells"][CELL_A["id"]]["attempts"] == 2


def test_a_queue_deviation_stops_a_poll_before_it_observes_anything(env, monkeypatch):
    manifests = session_approval(env)
    CellWorld(env, manifests, monkeypatch, polls_to_finish=9)
    supervisor, pod, _runner = admitted_session(env, manifests)
    queues = supervisor.env.queues
    get, reads, seen = queues.get, [0], []

    def deviating_get(read_mask=ct.QUEUE_READ_MASK):
        readback = get(read_mask)
        reads[0] += 1
        if reads[0] >= 3 and readback is not None:
            readback["state"] = "RUNNING"  # the queue resumed between polls
        return readback

    class Recording(rt.SessionHooks):
        def poll(self, session, cell, app, pods):
            seen.append(reads[0])

    monkeypatch.setattr(queues, "get", deviating_get)
    supervisor.session.hooks = Recording()
    supervisor.supervise(pod["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert not control.success and "deviated" in control.reason
    # The deviating readback is taken before that poll's observers run, so no
    # observation is recorded for a queue that had already resumed.
    assert seen == [2]


# --- admission tolerates a queue that is still initializing ---------------------


def initializing(queues, answers):
    """Make the readback answer ``answers`` before the queue settles paused.

    The service does not answer for a queue it has just created, so the fake
    injects those answers at the moment of the pause, where production meets
    them: a transient status, or the state the queue held before the pause.
    """

    def at_pause():
        queues.read_errors = [a for a in answers if isinstance(a, Exception)]
        queues.pause_leaves_running = any(a == "RUNNING" for a in answers)

    queues.on_pause = at_pause
    return queues


def test_admission_waits_for_a_queue_that_is_still_readable_only_later(env):
    manifests = session_approval(env)
    queues = FakeQueues(env[2]["queue"])
    initializing(
        queues, [rt.ApiError(503, "GET", "queues"), rt.ApiError(503, "GET", "queues")]
    )
    _supervisor, _pod = prepared_session(env, manifests, queues)
    runner = runner_for(env, queues)
    runner.cleanup.scale_operator(1)
    readback = ct.admit_queue(runner.env)
    assert readback["state"] == "PAUSED"
    assert runner.env.refresh().queue["state"] == "PAUSED"
    # The two unreadable answers are recorded, not treated as a deviation.
    assert "queue-initializing" in evidence_events(env, "runner")
    # One read before the create, two that could not answer, one that did.
    assert [c[0] for c in queues.calls].count("get") == 4


def test_admission_waits_for_a_queue_that_still_reports_running(env):
    manifests = session_approval(env)
    queues = FakeQueues(env[2]["queue"])
    # The pause is accepted, but the queue still reports the state it held.
    queues.pause_leaves_running = True
    _supervisor, _pod = prepared_session(env, manifests, queues)
    runner = runner_for(env, queues)
    runner.cleanup.scale_operator(1)
    reads, original = [], queues.get

    def get(read_mask=ct.QUEUE_READ_MASK):
        answer = original(read_mask)
        reads.append(read_mask)
        if len(reads) >= 2:  # the service reports the pause from now on
            queues.pause_leaves_running = False
            queues.state = "PAUSED"
        return answer

    queues.get = get
    readback = ct.admit_queue(runner.env)
    # The first readback still said RUNNING. Admission waited for the state to
    # settle rather than calling a queue it had just paused a deviation.
    assert readback["state"] == "PAUSED"
    assert len(reads) == 3


def test_a_dispatching_queue_is_refused_without_waiting(env):
    manifests = session_approval(env)
    queues = FakeQueues(env[2]["queue"])
    queues.executed_last_minute = 1
    _supervisor, _pod = prepared_session(env, manifests, queues)
    runner = runner_for(env, queues)
    runner.cleanup.scale_operator(1)
    before = env[3].now
    with pytest.raises(rt.Failure, match="paused approved configuration"):
        ct.admit_queue(runner.env)
    # A dispatch is never an initialization delay, so admission does not wait.
    assert env[3].now == before


def test_a_non_transient_read_failure_is_not_retried(env):
    manifests = session_approval(env)
    queues = FakeQueues(env[2]["queue"])
    initializing(queues, [rt.ApiError(403, "GET", "queues")])
    _supervisor, _pod = prepared_session(env, manifests, queues)
    runner = runner_for(env, queues)
    runner.cleanup.scale_operator(1)
    with pytest.raises(rt.ApiError):
        ct.admit_queue(runner.env)


def test_the_pre_create_read_survives_a_transient_failure(env):
    session_approval(env)
    queues = FakeQueues(env[2]["queue"])
    queues.read_errors = [rt.ApiError(503, "GET", "queues")]
    runner = runner_for(env, queues)
    # The name is free; one unreadable answer must not cost the session.
    assert ct.absent_before_create(runner.env) is True
    assert [c[0] for c in queues.calls] == ["get", "get"]


def test_a_settling_window_never_outlives_the_admission_budget(env):
    manifests = session_approval(env)
    queues = FakeQueues(env[2]["queue"])
    _supervisor, _pod = prepared_session(env, manifests, queues)
    runner = runner_for(env, queues)
    runner.cleanup.scale_operator(1)
    # Admission has already spent all but twenty seconds of its allowance.
    clock = env[3]
    clock.now = (
        runner.env.schedule.started + rt.CLOUDTASKS_POLICY["cell_startup_seconds"] - 20
    )
    queues.read_errors = [rt.ApiError(503, "GET", "queues") for _ in range(20)]
    with pytest.raises(rt.Failure):
        ct.admit_queue(runner.env)
    # The window stopped at the budget rather than running its own two minutes.
    assert clock.now <= (
        runner.env.schedule.started + rt.CLOUDTASKS_POLICY["cell_startup_seconds"]
    )
    assert ("create",) not in queues.calls


def test_an_unexpected_queue_state_is_a_deviation_not_a_delay(env):
    manifests = session_approval(env)
    queues = FakeQueues(env[2]["queue"])
    _supervisor, _pod = prepared_session(env, manifests, queues)
    runner = runner_for(env, queues)
    runner.cleanup.scale_operator(1)
    original, before = queues.get, env[3].now

    def get(read_mask=ct.QUEUE_READ_MASK):
        answer = original(read_mask)
        if answer is not None:
            answer.pop("state", None)  # neither paused nor the settling state
        return answer

    queues.get = get
    with pytest.raises(rt.Failure, match="paused approved configuration"):
        ct.admit_queue(runner.env)
    # A state outside the settling one is refused at once, like a dispatch.
    assert env[3].now == before


def test_a_queue_is_never_created_after_the_admission_deadline(env):
    """The read that answers at the deadline must not still create the queue."""
    manifests = session_approval(env)
    queues = FakeQueues(env[2]["queue"])
    _supervisor, _pod = prepared_session(env, manifests, queues)
    runner = runner_for(env, queues)
    runner.cleanup.scale_operator(1)
    clock = env[3]
    clock.now = ct.admission_deadline(runner.env) - 1
    # One unreadable answer, then the name reads free exactly at the deadline.
    queues.read_errors = [rt.ApiError(503, "GET", "queues")]
    with pytest.raises(rt.Failure, match="admission deadline"):
        ct.admit_queue(runner.env)
    assert ("create",) not in queues.calls
    assert runner.env.refresh().queue is None
