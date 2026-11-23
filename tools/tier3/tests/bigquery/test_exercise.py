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
"""Drive the internal BigQuery loops with synthetic controller and service actors."""

import copy
import json
from dataclasses import replace

import pytest
import requests
from flink_tier3 import analyze
from flink_tier3.bigquery import plan
from flink_tier3.bigquery.exercise import (
    BigQueryExercise,
    BigQuerySequenceExercise,
    require_handoff,
)
from flink_tier3.bigquery.handoff import BigQueryHandoff
from flink_tier3.bigquery.lifecycle import BigQueryLifecycle
from flink_tier3.bigquery.oracle import assess
from flink_tier3.bigquery.verdict import MEASUREMENT_EVENT
from flink_tier3.common import (
    INCONCLUSIVE,
    Failure,
    StorageRequestFailed,
    TransportError,
)
from flink_tier3.environment import Environment
from flink_tier3.exercise import RECORD_SECONDS, TIME_LIMIT_SECONDS
from flink_tier3.model import Phase
from flink_tier3.policy import BIGQUERY, BIGQUERY_OBSERVATIONS, BIGQUERY_STATE, MIB
from flink_tier3.runner import Runner
from flink_tier3.supervisor import Supervisor

from ..test_lifecycle import env as env  # noqa: PLC0414
from ..test_lifecycle import heartbeat_as_supervisor, obj, rt, supervisor_pod
from .test_lifecycle import Resources
from .test_plan import inputs as inputs  # noqa: PLC0414
from .test_plan import renderer as renderer  # noqa: PLC0414
from .test_plan import trial as trial  # noqa: PLC0414
from .test_runtime_approval import prepared as prepared  # noqa: PLC0414


def handoff(environment, resources, application):
    return BigQueryHandoff(
        BigQueryLifecycle(environment, resources, application),
        runner_token="a" * 32,
        evidence_bytes=10 * MIB,
        query_until=environment.schedule.cleanup_at,
    )


def rows(trial, *, missing=False, duplicate=False, invalid=False):
    return [
        {
            "run_id": trial.run_id,
            "mode": trial.mode,
            "expected_records": trial.records,
            "destinations": trial.destinations,
            "destination": i,
            "total_rows": trial.expected(i)
            - int(missing)
            + int(duplicate)
            + int(invalid),
            "valid_rows": trial.expected(i) - int(missing) + int(duplicate),
            "distinct_sequences": trial.expected(i) - int(missing),
        }
        for i in range(trial.destinations)
    ]


BILLED_PER_QUERY = 3 * 1024 * 1024
LOAD = {
    "id": "flink-bq-load-" + "2" * 32 + "-c43-" + "0" * 16,
    "kind": "load",
    "table": "bq_proposal_1312_d0",
    "state": "RUNNING",
    "failed": False,
}


class QueryResources(Resources):
    def job(self, slot):
        return copy.deepcopy(self.jobs.get(slot))


class World:
    def __init__(self, prepared, monkeypatch, fault=None, withheld=None):
        self.runner_env, initial, upgrade, _ = prepared
        self.kube, self.store, self.clock = (
            self.runner_env.kube,
            self.runner_env.store,
            self.runner_env.clock,
        )
        self.approval = self.runner_env.approval
        self.plan = self.approval.bigquery_plan
        self.start = self.clock()
        self.changed = self.start
        self.fault = fault
        self.withheld = withheld
        self.phase = "initial"
        self.upgrade_at = None
        self.failover_at = None
        self.calls = []
        self.query_calls = []
        self.history = {}
        self.cp, self.restore = {}, {}
        # The restores the job's current JobMaster has counted.
        self.restores = 0
        self.resources = QueryResources(self.plan)
        self.runner = handoff(self.runner_env, self.resources, initial)
        self.runner.initialize()
        self.runner.provision()
        self.env = Environment(
            self.kube, self.store, self.approval.to_dict(), self.clock, self.sleep
        )
        self.observer = handoff(self.env, self.resources, initial)
        self.supervisor = Supervisor(
            self.env, upgrade, bigquery=self.observer, quiesce=self.quiesce
        )
        listing = self.supervisor.exercise.connector_jobs

        def recorded(*, deadline):
            self.listings.append((self.clock(), deadline))
            return listing(deadline=deadline)

        self.supervisor.exercise.connector_jobs = recorded
        job = self.kube.put(obj("Job", "supervisor", rt.SYSTEM))
        self.env.remember("supervisor", job)
        self.supervisor_pod = supervisor_pod(
            (self.kube, self.store, self.approval.to_dict(), self.clock), job
        )
        app = copy.deepcopy(initial)
        app["metadata"].update(uid="app-uid", resourceVersion="1", generation=1)
        # A freshly created deployment is not running yet, which is the first
        # poll the supervisor takes and the one where no Service exists.
        app["status"] = {"jobStatus": {"state": "CREATED", "jobId": ""}}
        self.restarting = True
        self.kube.put(app)
        self.env.remember("application", app)
        self.env.records.set_phase(Phase.READY)
        self.env.records.set_phase(Phase.RUNNING)
        self.service = obj(
            "Service", self.approval.run_id + "-rest", BIGQUERY, "app-uid"
        )
        self.kube.put(self.service)
        self.pod("jm-initial", "jobmanager")
        # The initial phase's TaskManagers; the first runs the task that
        # reports the input, until a TaskManager replacement deletes it.
        self.taskmanagers = 0
        for _ in range(self.plan.trial.parallelism("initial")):
            self.add_taskmanager()
        self.reporting = "tm-1"
        self.failover_kind = None
        self.original_patch, self.original_delete = self.kube.patch, self.kube.delete
        self.original_request = self.kube.request
        monkeypatch.setattr(self.kube, "patch", self.patch)
        monkeypatch.setattr(self.kube, "delete", self.delete)
        monkeypatch.setattr(self.kube, "request", self.request)
        monkeypatch.setattr(self.kube, "logs", self.logs)
        monkeypatch.setattr(self.resources, "results", self.results)
        self.connector_listings = []
        # Listings the failover took while waiting, and after its delete,
        # with the clock and the deadline each one was asked under.
        self.armed_listings, self.relisted = 0, False
        self.listings = []
        self.listing_error = None
        monkeypatch.setattr(
            self.resources, "unfinished_connector_jobs", self.connector_jobs
        )

    def app(self):
        return self.kube.get("FlinkDeployment", BIGQUERY, self.approval.run_id)

    def add_taskmanager(self):
        self.taskmanagers += 1
        name = f"tm-{self.taskmanagers}"
        self.pod(name, "taskmanager")
        return name

    def subtasks(self, app):
        """The subtasks Flink 2.2.1 checkpoints for the application's graph.

        The writers' parallelism is the `--parallelism` argument, not the
        manifest's. The source and input identity are one vertex of one
        subtask; the EO committer is chained to its writer, while the
        FILE_LOADS committer is a vertex of its own at the writers'
        parallelism.
        """
        args = app["spec"]["job"]["args"]
        writers = int(args[args.index("--parallelism") + 1])
        return 1 + writers * (2 if self.plan.trial.mode == "FILE_LOADS" else 1)

    def running_taskmanagers(self):
        return sorted(
            name
            for (kind, namespace, name) in self.kube.data
            if kind == "Pod" and namespace == BIGQUERY and name.startswith("tm-")
        )

    def pod(self, name, component):
        pod = obj("Pod", name, BIGQUERY, "app-uid")
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
                    "image": self.approval.images["application"],
                    "resources": {
                        k: rt.POD_RESOURCES["smoke"] for k in ("requests", "limits")
                    },
                }
            ],
        }
        pod["status"] = {"phase": "Running", "containerStatuses": [{"restartCount": 0}]}
        return self.kube.put(pod)

    def patch(self, app, changes, subresource="", before_repeat=None):
        if app["kind"] != "FlinkDeployment":
            return self.original_patch(
                app, changes, subresource, before_repeat=before_repeat
            )
        self.calls.append(("upgrade", self.clock()))
        current = self.app()
        assert app["metadata"]["uid"] == current["metadata"]["uid"]
        assert (
            app["metadata"]["resourceVersion"] == current["metadata"]["resourceVersion"]
        )
        if self.fault == "upgrade-conflict":
            raise rt.ApiError(409, "PATCH", "app")
        # The arguments, and a rescale's parallelism and TaskManager count.
        for change in changes:
            _, _, parent, field = change["path"].split("/")
            current["spec"][parent][field] = change["value"]
        current["metadata"].update(generation=2, resourceVersion="2")
        self.patched = copy.deepcopy(current["spec"])
        current["status"]["jobStatus"]["state"] = "FINISHED"
        current["status"]["reconciliationStatus"] = {"state": "UPGRADING"}
        self.kube.put(current)
        self.phase, self.changed = "upgrade", self.clock()
        self.upgrade_at = self.clock()
        return current

    def delete(self, value):
        component = value["metadata"].get("labels", {}).get("component")
        if value["kind"] == "Pod" and component in ("jobmanager", "taskmanager"):
            self.calls.append(("failover", self.clock()))
            self.phase, self.changed = "failover", self.clock()
            self.failover_at = self.clock()
            self.failover_kind = component
            self.deleted = value["metadata"]["name"]
        return self.original_delete(value)

    def request(self, method, path, *args, **kwargs):
        # The measurement collector reads the job plan, the sink's metric ids
        # and the TaskManagers; the real service answers all three through the
        # same proxy, so the double does too.
        file_loads = self.plan.trial.mode == "FILE_LOADS"
        if "/proxy/jobs/" in path and "/" not in path.split("/proxy/jobs/")[1]:
            if file_loads:
                # The plan Flink 2.2.1 builds for the application's FILE_LOADS
                # sink: the committer is a vertex of its own.
                return {
                    "vertices": [
                        {"id": "source-vertex", "name": "Source: datagen"},
                        {
                            "id": "sink-vertex",
                            "name": "BigQuery FILE_LOADS: Writer -> "
                            "BigQuery FILE_LOADS: Stamp checkpoint ids",
                        },
                        {
                            "id": "committer-vertex",
                            "name": "BigQuery FILE_LOADS: Committer",
                        },
                    ]
                }
            return {
                "vertices": [
                    {"id": "source-vertex", "name": "Source: datagen"},
                    {"id": "sink-vertex", "name": "Sink: bigquery"},
                ]
            }
        if path.endswith("/vertices/committer-vertex/subtasks/metrics"):
            return [
                {"id": "BigQuery_FILE_LOADS__Committer.loadJobsSubmitted"},
                {"id": "BigQuery_FILE_LOADS__Committer.lastCommitDurationMillis"},
            ]
        if "/checkpoints/details/" in path and "/subtasks/" in path:
            return {
                "subtasks": [
                    {
                        "index": index,
                        "status": "completed",
                        "checkpoint": {"sync": 2, "async": 30},
                        "start_delay": 1,
                        "end_to_end_duration": 40,
                    }
                    for index in range(2)
                ]
            }
        if path.endswith("/subtasks/metrics"):
            return [
                {"id": "Sink__Writer.openDestinations"},
                {"id": "busyTimeMsPerSecond"},
                {"id": "inPoolUsage"},
            ]
        if "/subtasks/metrics?" in path:
            # The service answers with an entry per requested id it holds, so
            # the double echoes the query rather than a fixed single metric:
            # the verdict credits a family from what came back, not from what
            # discovery listed. `withheld` is the id the job stopped
            # publishing after discovery listed it, which the real service
            # answers by simply leaving out.
            requested = path.split("?get=")[1].split("&")[0]
            return [
                {"id": name, "sum": "7"}
                for name in requested.split(",")
                if name != self.withheld
            ]
        if path.endswith("/taskmanagers"):
            return {"taskmanagers": [{"id": "tm-0"}]}
        if "/taskmanagers/" in path and "/metrics?" in path:
            return [{"id": "Status.JVM.Memory.Direct.MemoryUsed", "value": "1024"}]
        if not path.endswith("/checkpoints"):
            return self.original_request(method, path, *args, **kwargs)
        assert BIGQUERY in path and rt.SMOKE not in path
        if self.phase != "initial" and self.clock() - self.changed < 30:
            raise rt.ApiError(503, method, path)
        return {
            "counts": {"completed": int(bool(self.cp)), "restored": self.restores},
            "latest": {"completed": self.cp, "restored": self.restore},
        }

    def logs(self, pod, since=None):
        return "".join(
            line
            for at, line in self.history.get(pod["metadata"]["uid"], [])
            if since is None or at >= rt.timestamp(since)
        ).encode()

    def sleep(self, seconds):
        self.clock.sleep(seconds)
        record = self.env.refresh()
        if record.stop_requested:
            self.runner.release()
        else:
            for job in self.resources.jobs.values():
                job["status"]["state"] = "DONE"
            self.runner.poll()
        self.advance()

    def advance(self):
        app = self.app()
        if not app:
            return
        if app["status"]["jobStatus"]["state"] == "CREATED":
            app["status"]["jobStatus"].update(state="RUNNING", jobId="1" * 32)
            self.kube.put(app)
            return
        now, elapsed = self.clock(), self.clock() - self.changed
        if self.phase != "initial" and elapsed >= 30:
            if self.phase == "upgrade" or self.failover_kind == "jobmanager":
                old_name = "jm-initial" if self.phase == "upgrade" else "jm-upgrade"
                self.kube.data.pop(("Pod", BIGQUERY, old_name), None)
                name = "jm-" + self.phase
                if not self.kube.get("Pod", BIGQUERY, name):
                    self.pod(name, "jobmanager")
            else:
                # Flink requests a replacement for the lost TaskManager, and
                # the restored task reports from it.
                self.kube.data.pop(("Pod", BIGQUERY, self.deleted), None)
                if self.reporting == self.deleted:
                    self.reporting = self.add_taskmanager()
            # The upgraded deployment runs as many TaskManagers as it asks for.
            replicas = app["spec"]["taskManager"]["replicas"]
            while len(self.running_taskmanagers()) < replicas:
                self.add_taskmanager()
            for name in self.running_taskmanagers()[replicas:]:
                self.kube.data.pop(("Pod", BIGQUERY, name), None)
            app["status"].update(
                observedGeneration=2, reconciliationStatus={"state": "DEPLOYED"}
            )
            if self.restarting:
                # The Operator has replaced the deployment but the job has not
                # come back yet: the loop resolves no Service on this poll.
                self.restarting = False
                app["status"]["jobStatus"].update(state="RECONCILING", jobId="")
                self.kube.put(app)
                return
            app["status"]["jobStatus"].update(state="RUNNING", jobId="2" * 32)
            path = f"gs://{BIGQUERY_STATE}/runs/{self.approval.run_id}/"
            app["status"]["jobStatus"]["savepointInfo"] = {
                "lastSavepoint": {
                    "location": path + "savepoints/sp",
                    "timeStamp": (self.upgrade_at + 10) * 1000,
                    "triggerType": "UPGRADE",
                }
            }
            at = int((self.changed + 30) * 1000)
            if self.restore.get("restore_timestamp") != at:
                # A new JobMaster counts its restores from one; the one a
                # TaskManager replacement keeps adds this one to its count.
                kept = self.phase != "upgrade" and self.failover_kind != "jobmanager"
                self.restores = self.restores + 1 if kept else 1
            self.restore = {
                "id": 43,
                "is_savepoint": self.phase == "upgrade",
                # Flink reports epoch milliseconds as a JSON integer.
                "restore_timestamp": at,
                "external_path": path
                + ("savepoints/sp" if self.phase == "upgrade" else "checkpoints/cp"),
            }
            if self.fault == "wrong-state":
                self.restore["external_path"] = self.restore["external_path"].replace(
                    BIGQUERY_STATE, rt.STATE
                )
        if self.phase != "initial" and elapsed < 30:
            self.kube.put(app)
            return
        end = 1100 if self.fault == "early-finish" else 1800
        processed = min(
            self.plan.trial.records,
            int((now - self.start) / end * self.plan.trial.records),
        )
        if processed == self.plan.trial.records:
            app["status"]["jobStatus"]["state"] = "FINISHED"
        if self.fault != "no-progress":
            lineage = "11111111-1111-4111-8111-111111111111"
            if self.fault == "fresh-lineage" and self.phase != "initial":
                lineage = "22222222-2222-4222-8222-222222222222"
            phase = "initial" if self.phase == "initial" else "upgrade"
            line = (
                f"{rt.utc(now)} event=bigquery-progress run_id={self.approval.run_id} "
                f"phase={phase} lineage={lineage} restored={str(self.phase != 'initial').lower()} "
                f"processed={processed} sequence={processed - 1}\n"
            )
            self.history.setdefault(self.reporting + "-uid", []).append((now, line))
        if elapsed >= 45:
            self.cp = {
                "id": 1 if self.phase == "initial" else 43,
                "status": "COMPLETED",
                "is_savepoint": False,
                "trigger_timestamp": (now - 1) * 1000,
                "latest_ack_timestamp": int(now * 1000),
                "num_subtasks": self.subtasks(app),
                "external_path": f"gs://{BIGQUERY_STATE}/runs/{self.approval.run_id}/checkpoints/cp",
            }
        if self.fault in ("restart", "shutdown", "evicted") and now - self.start > 300:
            pod = self.kube.get("Pod", BIGQUERY, "tm-1")
            pod["status"]["containerStatuses"][0]["restartCount"] = 1
            # A node shutdown is the platform's, whatever else the Pod shows;
            # a disruption alone may be an eviction under the Pod's own limits.
            disruption = {"type": "DisruptionTarget", "status": "True"}
            if self.fault == "shutdown":
                # As the kubelet's graceful node shutdown leaves the Pod.
                pod["status"]["reason"] = "Terminated"
                disruption["reason"] = "TerminationByKubelet"
            if self.fault == "evicted":
                pod["status"]["reason"] = "Evicted"
                disruption["reason"] = "TerminationByKubelet"
            if self.fault != "restart":
                pod["status"].setdefault("conditions", []).append(disruption)
            self.kube.put(pod)
        if self.fault == "job-restart" and now - self.start > 300 and not self.restore:
            # The job restarts from its checkpoint on the same Pods, which
            # leaves nothing in Kubernetes to see.
            self.restores += 1
            self.restore = {
                "id": self.cp["id"],
                "is_savepoint": False,
                "restore_timestamp": int(now * 1000),
                "external_path": self.cp["external_path"],
            }
        self.kube.put(app)

    def results(self, slot):
        self.query_calls.append((slot, self.clock()))
        data = rows(
            self.plan.trial,
            missing=self.fault == "invisible"
            or (self.fault in ("late-visibility", "load-between-reads") and slot == 0),
            duplicate=self.fault == "duplicates",
            invalid=self.fault == "invalid",
        )
        # The real adapter returns the job it read, and refuses one whose
        # billing statistics are absent, so the double carries them too.
        return {
            "job": {
                "status": {"state": "DONE"},
                "statistics": {"query": {"totalBytesBilled": str(BILLED_PER_QUERY)}},
            },
            "rows": data,
            "report": assess(self.plan.trial, data),
        }

    def measurements(self):
        return [
            value
            for value in self.evidence().values()
            if isinstance(value, dict) and value.get("event") == MEASUREMENT_EVENT
        ]

    def evidence(self):
        return {
            name: value
            for (bucket, name), (value, _generation) in self.store.data.items()
            if bucket == rt.EVIDENCE
        }

    def export(self, root, record):
        """The run directory as `gcloud storage cp --recursive` leaves it.

        These loops drive the supervisor, not the runner's admission and
        finalization, so the two run documents are written here from the run's
        own approval and its own control record rather than by `Runner`. What
        `Runner` writes into them is held by `test_runtime_approval`; what is
        proved here is that the evidence the exercise emitted recomputes to
        the verdict it recorded.
        """
        prefix = f"runs/{self.approval.run_id}/"
        for name, value in self.evidence().items():
            if not name.startswith(prefix):
                continue
            path = root / name[len("runs/") :]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value))
        run_dir = root / self.approval.run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "approval.json").write_text(json.dumps(self.approval.to_dict()))
        (run_dir / "result.json").write_text(
            json.dumps(
                {
                    "scenario": self.approval.scenario,
                    # The runner copies the trial into the receipt, which is
                    # where the analyzer reads a run's kind.
                    "bigquery_trial": self.approval.bigquery_trial,
                    "success": record.success
                    and record.recovery.get("verdict") == rt.USABLE,
                    "recovery": record.recovery,
                }
            )
        )
        return root

    def quiesce(self):
        return self.fault != "barrier" and self.app() is None

    def connector_jobs(self, since):
        """A load the committer left: before the oracle, or after the Pods.

        `load-outlives` is a load still RUNNING when the workload is gone,
        which cleanup must wait out; `load-after-finished` is one still
        running when the oracle reads, which it must refuse.
        """
        assert since == self.start
        if (
            self.phase == "upgrade"
            and self.failover_at is None
            and not self.env.refresh().stop_requested
        ):
            # The failover's wait, each listing taking 0.4 s: nothing, then a
            # load still PENDING beside a running copy, neither of which is a
            # load in flight, then a commit's load runs.
            self.clock.sleep(0.4)
            self.armed_listings += 1
            if self.fault == "listing-error" and self.armed_listings == 1:
                raise self.listing_error()
            if self.fault == "listing-refused" and self.armed_listings == 1:
                raise Failure("A listed BigQuery job has no readable id")
            if self.fault == "stop-while-waiting" and self.armed_listings == 2:
                self.env.records.request_stop()
            if self.armed_listings == 2:
                return [
                    {**LOAD, "state": "PENDING"},
                    {**LOAD, "id": LOAD["id"] + "-copy", "kind": "copy"},
                ]
            return (
                [dict(LOAD)]
                if self.armed_listings > 2
                and self.fault not in ("no-load-in-flight", "stop-while-waiting")
                else []
            )
        if self.failover_at is not None and not self.relisted:
            # The relisting just after the delete: the load is still running
            # unless it finished in between, and another may have started.
            self.clock.sleep(0.4)
            self.relisted = True
            if self.fault == "listing-error":
                raise self.listing_error()
            if self.fault == "load-finished-before-delete":
                return []
            if self.fault == "another-load-after-delete":
                return [dict(LOAD), {**LOAD, "id": LOAD["id"][:-1] + "1"}]
            return [dict(LOAD)]
        self.connector_listings.append(self.app() is None)
        if self.fault == "load-outlives" and self.app() is None:
            pending = self.connector_listings.count(True) <= 2
        elif self.fault == "load-after-finished":
            pending = self.app() is not None
        elif self.fault == "load-between-reads":
            # Absent before the first read, running before the second.
            pending = self.app() is not None and len(self.query_calls) == 1
        else:
            pending = False
        return [dict(LOAD)] if pending else []

    def run(self):
        self.supervisor.supervise(self.supervisor_pod["metadata"]["uid"])
        return self.env.refresh()


def test_unresolved_writer_barrier_retains_control_and_tables(prepared, monkeypatch):
    world = World(prepared, monkeypatch, "barrier")
    with pytest.raises(Failure):
        world.run()
    assert world.resources.tables
    assert world.env.refresh().phase == Phase.CLEANING
    assert world.store.read(rt.ENVIRONMENT)[0] is not None


@pytest.mark.parametrize("fault", [None, "stop", "slow", "ambiguous"])
def test_runner_provisions_before_application_admission(prepared, monkeypatch, fault):
    environment, application, *_ = prepared
    resources = QueryResources(environment.approval.bigquery_plan)
    actor = handoff(environment, resources, application)
    runner = Runner(environment, bigquery=actor)
    kube = environment.kube
    create = kube.create
    sequence = []

    def created(manifest, dry_run=False, before_repeat=None):
        if manifest["kind"] == "FlinkDeployment":
            sequence.append("application")
            assert len(resources.tables) == resources.plan.trial.destinations
        result = create(manifest, dry_run, before_repeat=before_repeat)
        if manifest["kind"] == "Job" and not dry_run:
            pod = supervisor_pod(
                (
                    kube,
                    environment.store,
                    environment.approval.to_dict(),
                    environment.clock,
                ),
                result,
            )
            heartbeat_as_supervisor(
                (
                    kube,
                    environment.store,
                    environment.approval.to_dict(),
                    environment.clock,
                ),
                pod,
            )
        return result

    def after_create(_destination):
        sequence.append("table")
        if fault == "stop":
            environment.records.request_stop()
        if fault == "slow":
            environment.clock.now += 600
        if fault == "ambiguous":
            raise Failure("Creation response was lost")

    resources.after_create = after_create
    monkeypatch.setattr(kube, "create", created)
    args = (
        obj("ConfigMap", "source", rt.SYSTEM),
        obj("Job", "supervisor", rt.SYSTEM),
        application,
    )
    if fault:
        with pytest.raises(Failure):
            runner.start(*args)
        assert "application" not in sequence
        assert environment.refresh().phase == Phase.READY
        if fault == "ambiguous":
            with pytest.raises(Failure, match="unresolved"):
                actor.release()
            assert environment.refresh().bigquery["handoff"]["inflight"] is not None
    else:
        runner.start(*args)
        assert sequence[0] == "table"
        assert sequence[-1] == "application"
        assert environment.refresh().phase == Phase.RUNNING
        assert environment.refresh().roots["application"]["namespace"] == BIGQUERY


@pytest.mark.parametrize(
    "fault", ["budget", "tiny-budget", "deadline", "plan", "manifest", "started"]
)
def test_runner_refuses_binding_drift_before_mutation(prepared, fault):
    environment, application, *_ = prepared
    resources = QueryResources(environment.approval.bigquery_plan)
    actor = handoff(environment, resources, application)
    if fault == "budget":
        actor.binding["evidence_bytes"] = 10 * MIB + 1
    elif fault == "tiny-budget":
        actor = BigQueryHandoff(
            actor.controller,
            runner_token="a" * 32,
            evidence_bytes=resources.plan.query_slots,
            query_until=environment.schedule.cleanup_at,
        )
    elif fault == "deadline":
        actor.binding["query_until"] -= 1
    elif fault == "plan":
        actor.controller.plan = replace(actor.controller.plan, query_slots=1)
    elif fault == "manifest":
        application["spec"]["job"]["args"][3] = "upgrade"
    else:
        environment.clock.now += 600
    before = copy.deepcopy(environment.store.data)
    with pytest.raises(Failure):
        Runner(environment, bigquery=actor).start({}, {}, application)
    assert resources.calls == []
    assert environment.kube.calls == []
    assert environment.store.data == before


def test_input_may_start_after_the_admission_budget(prepared):
    """A cold cluster provisions Pods past admission's budget; input may follow."""
    environment, _, upgrade, _ = prepared
    environment.records.set_phase(Phase.READY)
    environment.records.set_phase(Phase.RUNNING)
    exercise = BigQueryExercise(environment, upgrade, None)
    started = environment.schedule.started
    environment.clock.now = started + BIGQUERY_OBSERVATIONS["startup_seconds"]
    exercise.check_open()
    environment.clock.now = started + BIGQUERY_OBSERVATIONS["input_seconds"]
    with pytest.raises(Failure, match="deadline expired: baseline"):
        exercise.check_open()


def test_handoff_requires_same_environment(prepared):
    environment, application, *_ = prepared
    actor = handoff(
        environment, QueryResources(environment.approval.bigquery_plan), application
    )
    actor.env = object()
    with pytest.raises(Failure, match="bound handoff"):
        require_handoff(environment, actor)


@pytest.mark.parametrize(
    "fault", ["missing-rows", "forged-report", "late", "stop", "slots"]
)
def test_visibility_rejects_unusable_results_and_cancellation(prepared, fault):
    environment, _, upgrade, _ = prepared
    environment.records.set_phase(Phase.READY)
    environment.records.set_phase(Phase.RUNNING)
    exercise = BigQueryExercise(environment, upgrade, None)
    exercise.stage = "visibility"
    observations = []

    class Queries:
        def query(self, name, *, deadline):
            observations.append((name, deadline))
            data = rows(exercise.trial, missing=fault == "slots")
            result = {"rows": data, "report": assess(exercise.trial, data)}
            if fault == "missing-rows":
                return {}
            if fault == "forged-report":
                result["report"]["verdict"] = "fail"
            if fault == "late":
                environment.clock.now = deadline
            if fault == "stop":
                environment.records.request_stop()
            return result

    with pytest.raises(Failure):
        exercise.verify_rows(Queries())
    assert exercise.stage == "visibility"
    assert "query" not in exercise.outcomes
    assert len({deadline for _, deadline in observations}) == 1


def test_visibility_refuses_before_recovery_and_input_completion(prepared):
    environment, _, upgrade, _ = prepared
    with pytest.raises(Failure, match="both recoveries"):
        BigQueryExercise(environment, upgrade, None).verify_rows(None)


@pytest.mark.parametrize("fault", ["future", "invalid-lineage"])
def test_invalid_progress_is_not_recovery_evidence(prepared, fault):
    environment, _, upgrade, _ = prepared
    exercise = BigQueryExercise(environment, upgrade, None)
    at = environment.clock() + (1 if fault == "future" else 0)
    lineage = (
        "bad" if fault == "invalid-lineage" else "11111111-1111-4111-8111-111111111111"
    )
    text = (
        f"{rt.utc(at)} event=bigquery-progress run_id={environment.approval.run_id} "
        f"phase=initial lineage={lineage} restored=false processed=10 sequence=9"
    )
    with pytest.raises(Failure):
        exercise.progress(obj("Pod", "tm", BIGQUERY), text)
    assert environment.refresh().lineage is None


@pytest.mark.parametrize("failure", ["dry-run", "late-created"])
def test_failed_admission_keeps_supervisor_for_released_cleanup(
    prepared, monkeypatch, failure
):
    environment, application, upgrade, _ = prepared
    resources = QueryResources(environment.approval.bigquery_plan)
    actor = handoff(environment, resources, application)
    runner = Runner(environment, bigquery=actor)
    observer_env = Environment(
        environment.kube,
        environment.store,
        environment.approval,
        environment.clock,
        environment.clock.sleep,
    )
    observer = handoff(observer_env, resources, application)
    supervisor = Supervisor(
        observer_env,
        upgrade,
        bigquery=observer,
        quiesce=lambda: (
            environment.kube.get(
                "FlinkDeployment", BIGQUERY, environment.approval.run_id
            )
            is None
        ),
    )
    create = environment.kube.create
    pods = []

    def created(value, dry_run=False, before_repeat=None):
        if value["kind"] == "FlinkDeployment" and dry_run and failure == "dry-run":
            raise Failure("Application dry-run rejected")
        actual = create(value, dry_run, before_repeat=before_repeat)
        if value["kind"] == "Job" and not dry_run:
            pods.append(
                supervisor_pod(
                    (
                        environment.kube,
                        environment.store,
                        environment.approval.to_dict(),
                        environment.clock,
                    ),
                    actual,
                )
            )
            observer_env.records.heartbeat()
        if (
            value["kind"] == "FlinkDeployment"
            and not dry_run
            and failure == "late-created"
        ):
            environment.clock.now += 600
        return actual

    monkeypatch.setattr(environment.kube, "create", created)
    with pytest.raises(Failure):
        runner.start(
            obj("ConfigMap", "source", rt.SYSTEM),
            obj("Job", "supervisor", rt.SYSTEM),
            application,
        )
    assert resources.tables
    assert environment.refresh().phase == Phase.READY
    original_sleep = environment.sleep
    supervised = []

    def schedule_supervisor(seconds):
        original_sleep(seconds)
        if not supervised:
            supervised.append(True)
            assert actor.released(), (
                "Only settle after start returned may release admission"
            )
            supervisor.supervise(pods[0]["metadata"]["uid"])

    environment.sleep = schedule_supervisor
    runner.settle(request_stop=True)
    assert supervised
    assert resources.tables == {}
    assert environment.refresh().phase == Phase.CLEANED
    assert not environment.refresh().success
    assert (
        environment.kube.get("FlinkDeployment", BIGQUERY, environment.approval.run_id)
        is None
    )
    assert (
        environment.kube.get("Deployment", rt.SYSTEM, rt.OPERATOR)["spec"]["replicas"]
        == 0
    )


def test_failed_config_creation_does_not_leave_resource_intent(prepared, monkeypatch):
    environment, application, *_ = prepared
    resources = QueryResources(environment.approval.bigquery_plan)
    runner = Runner(environment, bigquery=handoff(environment, resources, application))

    def fail(_value, dry_run=False, before_repeat=None):
        raise Failure("Config admission rejected")

    monkeypatch.setattr(environment.kube, "create", fail)
    with pytest.raises(Failure, match="Config admission"):
        runner.start(obj("ConfigMap", "source", rt.SYSTEM), {}, application)
    assert environment.refresh().bigquery is None
    runner.settle(request_stop=True)
    assert environment.refresh().phase == Phase.CLEANED
    assert resources.calls == []


@pytest.mark.parametrize("released", [True, False])
def test_unfinished_admission_waits_before_any_workload_teardown(
    prepared, monkeypatch, released
):
    world = World(prepared, monkeypatch)
    application = world.app()

    def admitting(control):
        control.phase = Phase.READY
        control.roots.pop("application")

    world.env.records._change(admitting)
    # The runner stops the run while its admission is still in flight. A
    # signal would instead leave the run to the Job's replacement.
    world.runner_env.records.request_stop()
    slept = []

    def admission_returns(seconds):
        assert not any(call[0] == "delete" for call in world.kube.calls)
        assert not any(call[0] == "delete" for call in world.resources.calls)
        world.clock.sleep(seconds)
        slept.append(seconds)
        if released:
            world.runner_env.remember("application", application)
            world.runner.release()

    world.env.sleep = admission_returns
    if not released:
        with pytest.raises(Failure, match="wait expired"):
            world.run()
        assert world.resources.tables
        assert world.app() is not None
        assert world.env.refresh().phase == Phase.READY
    else:
        record = world.run()
        assert record.phase == Phase.CLEANED
        assert not record.success
        assert world.app() is None
        assert world.resources.tables == {}
    assert slept
    assert world.store.read(rt.ENVIRONMENT)[0] is not None


def test_stop_racing_resource_initialization_keeps_supervisor_cleanup(
    prepared, monkeypatch
):
    environment, application, upgrade, _ = prepared
    resources = QueryResources(environment.approval.bigquery_plan)
    runner = handoff(environment, resources, application)
    observer_env = Environment(
        environment.kube,
        environment.store,
        environment.approval,
        environment.clock,
        environment.clock.sleep,
    )
    observer = handoff(observer_env, resources, application)
    supervisor = Supervisor(
        observer_env, upgrade, bigquery=observer, quiesce=lambda: True
    )
    job = environment.kube.put(obj("Job", "supervisor", rt.SYSTEM))
    observer_env.remember("supervisor", job)
    pod = supervisor_pod(
        (
            environment.kube,
            environment.store,
            environment.approval.to_dict(),
            environment.clock,
        ),
        job,
    )
    observer_env.records.set_phase(Phase.READY)
    # The supervisor's own admission wait expires. A signal would instead
    # leave the run to the Job's replacement, which is covered elsewhere.
    wait = observer_env.wait
    waits = []

    def admission_expires(predicate, deadline):
        waits.append(deadline)
        if len(waits) == 1:
            raise Failure("Bounded wait expired")
        return wait(predicate, deadline)

    monkeypatch.setattr(observer_env, "wait", admission_expires)
    stop = observer_env.records.request_stop
    raced = []

    def concurrent_start():
        if not raced:
            raced.append(True)
            runner.initialize()
            runner.provision()
            stop()
            runner.release()
        else:
            stop()

    monkeypatch.setattr(observer_env.records, "request_stop", concurrent_start)
    supervisor.supervise(pod["metadata"]["uid"])
    assert raced
    assert resources.tables == {}
    assert observer_env.refresh().phase == Phase.CLEANED
    assert not observer_env.refresh().success


def supervised(prepared):
    """The exercise the supervisor builds for the approved trial."""
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    environment.records.set_phase(Phase.READY)
    environment.records.set_phase(Phase.RUNNING)
    value = Supervisor(environment, upgrade).exercise
    value.persist("baseline")
    return value


# Every delivery method's fault sequence, at its own input.
SEQUENCE_TRIALS = [plan.trial(name) for name in plan.TRIALS]


@pytest.mark.parametrize("trial", SEQUENCE_TRIALS)
def test_the_sequence_trial_builds_the_sequence_exercise(prepared, trial):
    value = supervised(prepared)
    assert isinstance(value, BigQuerySequenceExercise)
    assert value.measurement_event is None
    assert value.records == trial["records"]
    assert value.deadline == value.env.schedule.started + TIME_LIMIT_SECONDS


def finishing(world, monkeypatch):
    """The sequence as finished on the supervisor's first poll that observes it.

    The fault sequence itself is driven in `test_fault_sequence.py`; this
    world only needs the loop to reach the read.
    """
    exercise = world.supervisor.exercise

    def finished(app, rest, pods):
        exercise.persist("complete")
        return True

    monkeypatch.setattr(exercise, "observe", finished)


@pytest.mark.parametrize("trial", SEQUENCE_TRIALS)
def test_the_supervisor_reads_a_finished_sequence_through_its_handoff(
    prepared, monkeypatch, trial
):
    """The supervisor's loop reads the rows through its own handoff, after
    listing FILE_LOADS's loads through it, and the run cleans up."""
    world = World(prepared, monkeypatch)
    finishing(world, monkeypatch)
    record = world.run()
    assert record.recovery["verdict"] == rt.USABLE, record.reason
    assert record.recovery["outcomes"]["query"]["observation"] == "final-0"
    assert [slot for slot, _ in world.query_calls] == [0]
    # The service saw one listing while the application still existed: the
    # supervisor's, which cleanup's later ones follow.
    listed = world.connector_listings.count(False)
    assert listed == (1 if trial["mode"] == "FILE_LOADS" else 0)
    assert record.phase == Phase.CLEANED
    assert world.resources.tables == {}


@pytest.mark.parametrize("trial", SEQUENCE_TRIALS)
def test_the_exported_evidence_recomputes_to_the_verdict_the_run_wrote(
    prepared, monkeypatch, tmp_path, trial
):
    """The test that crosses the producer/consumer seam.

    The event names and the record's shape are proved against evidence the
    run itself wrote, rather than against a fixture that spells them a second
    time; the receipt is the one `World.export` writes, and the runner's own
    is held by `test_runtime_approval.py`. The run injects no fault, since
    `finishing` skips the sequence, so its fault list is empty here; the
    faults' order is held by `test_fault_sequence.py` and the analyzer's
    report of it by `test_analyze.py`.
    """
    world = World(prepared, monkeypatch)
    finishing(world, monkeypatch)
    record = world.run()
    exported = world.export(tmp_path, record)
    report = analyze.analyze(exported)
    [item] = report["bigquery"]
    assert item["run_id"] == world.approval.run_id
    assert (item["verdict"], item["reasons"], item["problems"]) == (rt.USABLE, [], [])
    assert item["faults"] == []
    rendered = analyze.render_markdown(report)
    assert f"| {world.approval.run_id} | usable | usable |" in rendered


@pytest.mark.parametrize("trial", [plan.trial("fl-10-uploader-sequence")])
def test_the_supervisor_refuses_a_load_its_listing_finds_running(
    prepared, monkeypatch, trial
):
    """The listing goes through the supervisor's handoff to the service."""
    world = World(prepared, monkeypatch, "load-after-finished")
    finishing(world, monkeypatch)
    record = world.run()
    assert not record.success
    assert "outlived the finished application" in record.reason
    assert world.query_calls == []
    events = [
        value["payload"]
        for value in world.evidence().values()
        if isinstance(value, dict)
        and value.get("event") == "bigquery-unfinished-connector-jobs"
    ]
    assert events == [{"jobs": [LOAD]}]
    assert record.phase == Phase.CLEANED


class Rows:
    """A query handoff answering one aggregate per destination.

    Its connector-job listing shows `running`, the jobs not yet DONE.
    """

    def __init__(self, exercise, running=(), errors=(), **damage):
        self.exercise, self.damage, self.observations = exercise, damage, []
        self.running, self.listings = list(running), []
        # Failures the listings raise first, one each, before they answer.
        self.errors = list(errors)

    def connector_jobs(self, *, deadline):
        self.listings.append(deadline)
        if self.errors:
            raise self.errors.pop(0)
        return self.running

    def query(self, name, *, deadline):
        self.observations.append((name, deadline))
        data = rows(self.exercise.trial, **self.damage)
        return {"rows": data, "report": assess(self.exercise.trial, data)}


NOT_PASSED = (INCONCLUSIVE, ["oracle-not-passed"])


@pytest.mark.parametrize("trial", SEQUENCE_TRIALS)
@pytest.mark.parametrize(
    "damage,expected",
    [
        ({}, (rt.USABLE, [])),
        # Reported at least once, failing exactly once.
        (
            {"duplicate": True},
            {"ALO": (rt.USABLE, []), "EO": NOT_PASSED, "FILE_LOADS": NOT_PASSED},
        ),
        ({"missing": True}, NOT_PASSED),
        ({"invalid": True}, NOT_PASSED),
    ],
)
def test_a_sequence_run_is_judged_by_one_query_alone(prepared, trial, damage, expected):
    """Lost or invalid rows are the verdict, not a stop, and are not read again."""
    verdict, reasons = (
        expected[trial["mode"]] if isinstance(expected, dict) else expected
    )
    value = supervised(prepared)
    value.persist("complete")
    # Late in the run, so the read window is the time limit's, not the
    # visibility allowance's.
    limit = value.env.schedule.started + TIME_LIMIT_SECONDS
    value.env.clock.now = limit - 300
    queries = Rows(value, **damage)
    value.verify_rows(queries)
    record = value.env.refresh().recovery
    assert (record["stage"], record["verdict"], record["reasons"]) == (
        "complete",
        verdict,
        reasons,
    )
    report = record["outcomes"]["query"]["report"]
    assert report["duplicate_rows"] == (10 if damage.get("duplicate") else 0)
    # One read, which ends before the run's time limit leaves its record.
    [(name, deadline)] = queries.observations
    assert name == "final-0"
    assert value.deadline == limit
    assert deadline == limit - RECORD_SECONDS
    # FILE_LOADS listed its loads first, within the same read window.
    assert queries.listings == ([deadline] if trial["mode"] == "FILE_LOADS" else [])


@pytest.mark.parametrize("trial", [plan.trial("fl-10-uploader-sequence")])
def test_a_file_loads_sequence_refuses_to_read_while_a_load_still_runs(prepared, trial):
    """FINISHED says the committer is done; a running load says otherwise."""
    value = supervised(prepared)
    value.persist("complete")
    queries = Rows(value, running=[LOAD])
    with pytest.raises(Failure, match="outlived the finished application"):
        value.verify_rows(queries)
    assert queries.observations == []
    assert "query" not in value.outcomes


@pytest.mark.parametrize("trial", [plan.trial("fl-10-uploader-sequence")])
@pytest.mark.parametrize(
    "error",
    [
        lambda: TransportError("GET BigQuery request failed: ReadTimeout"),
        lambda: StorageRequestFailed("GET storage request failed: ConnectionError"),
    ],
    ids=["bigquery", "storage"],
)
def test_a_file_loads_sequence_lists_again_after_a_listing_without_an_answer(
    prepared, trial, error
):
    """A listing that got no answer does not end the run without a verdict."""
    value = supervised(prepared)
    value.persist("complete")
    queries = Rows(value, errors=[error()])
    value.verify_rows(queries)
    assert len(queries.listings) == 2
    assert value.env.refresh().recovery["verdict"] == rt.USABLE


def invalid_url():
    """A storage failure the adapter does not read as a lost answer."""
    try:
        raise requests.exceptions.InvalidURL("bad URL")
    except requests.exceptions.InvalidURL as error:
        try:
            raise Failure("GET storage request failed: InvalidURL") from error
        except Failure as wrapped:
            return wrapped


@pytest.mark.parametrize("trial", [plan.trial("fl-10-uploader-sequence")])
@pytest.mark.parametrize(
    "error",
    [lambda: Failure("Environment lock belongs to another run"), invalid_url],
    ids=["lock", "invalid-url"],
)
def test_a_listing_failure_that_says_something_still_stops_the_run(
    prepared, trial, error
):
    value = supervised(prepared)
    value.persist("complete")
    queries = Rows(value, errors=[error()])
    with pytest.raises(Failure, match="lock|InvalidURL"):
        value.verify_rows(queries)
    assert len(queries.listings) == 1
    assert queries.observations == []


@pytest.mark.parametrize("trial", [plan.trial("fl-10-uploader-sequence")])
def test_a_found_job_stops_the_run_even_when_its_record_fails(
    prepared, monkeypatch, trial
):
    """The listing that found it is not repeated, so the job cannot slip away."""
    value = supervised(prepared)
    value.persist("complete")
    records = value.env.records
    evidence = records.evidence

    def unrecorded(event, payload):
        if event == "bigquery-unfinished-connector-jobs":
            raise StorageRequestFailed("PUT storage request failed: ConnectionError")
        return evidence(event, payload)

    monkeypatch.setattr(records, "evidence", unrecorded)
    queries = Rows(value, running=[LOAD])
    with pytest.raises(StorageRequestFailed):
        value.verify_rows(queries)
    assert len(queries.listings) == 1
    assert queries.observations == []


@pytest.mark.parametrize("trial", [plan.trial("fl-10-uploader-sequence")])
def test_no_listing_begins_after_the_read_window(prepared, trial):
    """A retry that wakes past the window reads nothing more."""
    value = supervised(prepared)
    value.persist("complete")
    value.env.clock.now = value.deadline - RECORD_SECONDS - 1
    checks = []
    check_open = value.check_open

    def counted():
        checks.append(value.env.clock())
        check_open()

    value.check_open = counted
    queries = Rows(value, errors=[TransportError("GET BigQuery: reset")])
    with pytest.raises(Failure, match="read window closed"):
        value.verify_rows(queries)
    # The first listing ran in the window; the retry, five seconds on, did not.
    assert len(queries.listings) == 1
    assert len(checks) == 2


@pytest.mark.parametrize("trial", [plan.trial("fl-10-uploader-sequence")])
def test_a_stop_ends_the_repeated_listing(prepared, trial):
    value = supervised(prepared)
    value.persist("complete")

    class Stopped(Rows):
        def connector_jobs(self, *, deadline):
            value.env.records.request_stop()
            return super().connector_jobs(deadline=deadline)

    queries = Stopped(value, errors=[TransportError("GET BigQuery: reset")] * 3)
    with pytest.raises(Failure, match="stopped"):
        value.verify_rows(queries)
    assert len(queries.listings) == 1


@pytest.mark.parametrize("trial", SEQUENCE_TRIALS)
def test_a_sequence_run_reads_only_once_the_input_is_done(prepared, trial):
    value = supervised(prepared)
    with pytest.raises(Failure, match="fault sequence and input"):
        value.verify_rows(Rows(value))


@pytest.mark.parametrize("trial", SEQUENCE_TRIALS)
def test_a_job_that_finished_in_the_last_minute_stops_at_the_time_limit(
    prepared, trial
):
    """No read can end before the record's write, so none is asked for."""
    value = supervised(prepared)
    value.persist("complete")
    value.env.clock.now = value.deadline - RECORD_SECONDS + 1
    queries = Rows(value)
    with pytest.raises(Failure, match="time limit"):
        value.verify_rows(queries)
    assert queries.observations == []


@pytest.mark.parametrize("trial", SEQUENCE_TRIALS)
@pytest.mark.parametrize("fault", ["before", "during", "no-rows"])
def test_a_stop_or_an_empty_answer_leaves_the_sequence_without_a_verdict(
    prepared, trial, fault
):
    value = supervised(prepared)
    value.persist("complete")

    class Answer(Rows):
        def query(self, name, *, deadline):
            result = super().query(name, deadline=deadline)
            if fault == "during":
                value.env.records.request_stop()
            return {} if fault == "no-rows" else result

    if fault == "before":
        value.env.records.request_stop()
    queries = Answer(value)
    with pytest.raises(Failure):
        value.verify_rows(queries)
    assert "query" not in value.outcomes
    assert len(queries.observations) == (0 if fault == "before" else 1)


@pytest.mark.parametrize("trial", SEQUENCE_TRIALS)
def test_a_forged_sequence_report_still_stops_the_run(prepared, trial):
    """The archived report must be the one its rows give."""
    value = supervised(prepared)
    value.persist("complete")

    class Forged(Rows):
        def query(self, name, *, deadline):
            result = super().query(name, deadline=deadline)
            result["report"]["verdict"] = "fail"
            return result

    with pytest.raises(Failure, match="archived oracle differs"):
        value.verify_rows(Forged(value))
    assert "query" not in value.outcomes
