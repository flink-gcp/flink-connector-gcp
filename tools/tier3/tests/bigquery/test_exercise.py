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

import google.auth.exceptions
import pytest
import requests
from flink_tier3 import analyze
from flink_tier3.bigquery.exercise import BigQueryExercise, require_handoff
from flink_tier3.bigquery.handoff import BigQueryHandoff
from flink_tier3.bigquery.lifecycle import BigQueryLifecycle
from flink_tier3.bigquery.oracle import assess
from flink_tier3.bigquery.verdict import MEASUREMENT_EVENT
from flink_tier3.common import INCONCLUSIVE, Failure, TransportError
from flink_tier3.environment import Environment
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

    def patch(self, app, changes, subresource=""):
        if app["kind"] != "FlinkDeployment":
            return self.original_patch(app, changes, subresource)
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

    def delete(self, value, force=False):
        component = value["metadata"].get("labels", {}).get("component")
        if value["kind"] == "Pod" and component in ("jobmanager", "taskmanager"):
            self.calls.append(("failover", self.clock()))
            self.phase, self.changed = "failover", self.clock()
            self.failover_at = self.clock()
            self.failover_kind = component
            self.deleted = value["metadata"]["name"]
        return self.original_delete(value, force)

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
            self.restore = {
                "id": 43,
                "is_savepoint": self.phase == "upgrade",
                # Flink reports epoch milliseconds as a JSON integer.
                "restore_timestamp": int((self.changed + 30) * 1000),
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
        end = 1250 if self.fault == "early-finish" else 1800
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
        if self.fault == "restart" and now - self.start > 300:
            pod = self.kube.get("Pod", BIGQUERY, "tm-1")
            pod["status"]["containerStatuses"][0]["restartCount"] = 1
            self.kube.put(pod)
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


@pytest.mark.parametrize(
    "mode,destinations",
    [("ALO", 10), ("ALO", 50), ("EO", 10), ("EO", 50), ("FILE_LOADS", 10)],
)
def test_bigquery_recovery_and_query_then_cleanup(
    prepared, monkeypatch, mode, destinations
):
    # The parameterized input is supplied by the trial fixture before rendering below.
    world = World(prepared, monkeypatch)
    assert world.plan.trial.mode == mode
    assert world.plan.trial.destinations == destinations
    record = world.run()
    assert record.success, record.reason
    assert record.recovery["stage"] == "complete"
    assert set(record.recovery["outcomes"]) == {"upgrade", "failover", "query"} | (
        {"failover_in_flight"} if mode == "FILE_LOADS" else set()
    )
    # The verdict the production path produced, not one typed into a fixture:
    # a run that proves both recoveries, passes the oracle and reads every
    # family in both sampled windows is the usable measurement.
    assert record.recovery["verdict"] == rt.USABLE
    assert record.recovery["reasons"] == []
    for stage in ("baseline", "finishing"):
        window = record.recovery["coverage"][stage]
        assert window["observed"] == (
            ["checkpoint", "committer", "connector", "memory", "network", "task"]
            if mode == "FILE_LOADS"
            else ["connector", "memory", "network", "task"]
        )
        assert window["attempts"] >= 1
    if mode == "FILE_LOADS":
        # The JobManager went only once a load was RUNNING, and the load was
        # still running when the listing after the delete looked again.
        assert world.armed_listings == 3
        armed = world.listings[0][0]
        waits = world.listings[:3]
        # Gapless: one listing a second, each under the wait's own deadline.
        assert [at - armed for at, _ in waits] == [0, 1, 2]
        assert {d for _, d in waits} == {
            armed + BIGQUERY_OBSERVATIONS["in_flight_seconds"]
        }
        failover_at = world.failover_at
        assert world.listings[3] == (
            failover_at,
            failover_at + BIGQUERY_OBSERVATIONS["recovery_seconds"],
        )
        in_flight = record.recovery["outcomes"]["failover_in_flight"]
        assert in_flight == {
            "listed_at": rt.utc(armed + 2),
            "running_before": [LOAD["id"]],
            "listing_errors": 0,
            "failover_at": rt.utc(failover_at),
            "relisted_at": rt.utc(failover_at + 0.4),
            "running_after": [LOAD["id"]],
            "running_through": [LOAD["id"]],
        }
        assert failover_at == armed + 2.4
        events = world.evidence()
        assert [
            v["payload"]
            for v in events.values()
            if isinstance(v, dict) and v.get("event") == "bigquery-failover-in-flight"
        ] == [in_flight]
        # The failover record, written before the delete, already holds what
        # the wait saw.
        [failover] = [
            v["payload"]
            for v in events.values()
            if isinstance(v, dict) and v.get("event") == "recovery-failover"
        ]
        assert failover["outcomes"]["failover_in_flight"] == {
            "listed_at": rt.utc(armed + 2),
            "running_before": [LOAD["id"]],
            "listing_errors": 0,
        }
        # The oracle asked before each read, and cleanup before any delete.
        assert False in world.connector_listings and True in world.connector_listings
        assert record.bigquery["file_loads"]["staging"] == {
            "objects": 0,
            "bytes": 0,
            "names": [],
        }
    else:
        assert world.connector_listings == []
    assert [name for name, _ in world.calls] == ["upgrade", "failover"]
    # The deployed measurements the issue asks for: sampled through the run's
    # own windows, with every reading named even when it is absent.
    samples = world.measurements()
    assert len(samples) > 1
    assert {sample["payload"]["stage"] for sample in samples} >= {"baseline"}
    readings = [s["payload"]["sink"] for s in samples if "sink" in s["payload"]]
    assert readings, "no sample carried a sink reading"
    # A sample taken when the plan could not be read says so rather than
    # reporting an empty measurement.
    for sample in samples:
        assert "sink" in sample["payload"] or "vertices" in sample["payload"]
    assert any(s["payload"].get("taskmanagers") for s in samples)
    assert world.upgrade_at - world.start >= 180 + 600
    assert world.query_calls[0][1] - world.failover_at >= 600
    assert record.phase == Phase.CLEANED
    assert world.resources.tables == {}
    assert record.bigquery["handoff"]["released"]
    assert world.store.read(rt.ENVIRONMENT)[0] is not None
    # Every sample the run emitted is accounted for by the record it wrote.
    assert sum(w["attempts"] for w in record.recovery["coverage"].values()) == len(
        world.measurements()
    )
    # Every transition carries the coverage so far, so a run that stops before
    # its verdict still records what it read.
    transitions = [
        value["payload"]
        for value in world.evidence().values()
        if isinstance(value, dict)
        and str(value.get("event", "")).startswith("recovery-")
        and isinstance(value.get("payload"), dict)
        and "stage" in value["payload"]
    ]
    assert transitions
    assert all("coverage" in payload for payload in transitions)


def test_the_exported_evidence_recomputes_to_the_verdict_the_run_wrote(
    prepared, monkeypatch, tmp_path
):
    """The one test that crosses the producer/consumer seam.

    Every constant coupling the exercise to the offline analyzer -- the event
    names, the record's shape, the receipt's copy of it and the per-stage
    sample counts -- is proved here against evidence the run itself wrote,
    rather than against a fixture that spells them a second time.
    """
    world = World(prepared, monkeypatch)
    record = world.run()
    exported = world.export(tmp_path, record)
    assert sorted(path.name for path in exported.iterdir()) == [world.approval.run_id]
    report = analyze.analyze(exported)
    assert [item["verdict"] for item in report["bigquery"]] == [rt.USABLE]
    item = report["bigquery"][0]
    assert item["run_id"] == world.approval.run_id
    assert item["verdict"] == rt.USABLE
    assert item["reasons"] == []
    assert item["problems"] == []
    assert item["samples"] == len(world.measurements())
    # Each recovery's timing is measured from the start its own stage record
    # holds, which precedes the fault the World saw, to the restore and the
    # proving checkpoint the record's proof holds.
    events = world.evidence().values()
    faults = {"upgrade": world.upgrade_at, "failover": world.failover_at}
    defaults = {"upgrade": "savepoint-upgrade", "failover": "jm-replacement"}
    for name, fault in defaults.items():
        [began] = [
            v["payload"]["at"]
            for v in events
            if isinstance(v, dict) and v.get("event") == "recovery-" + name
        ]
        assert rt.timestamp(began) <= faults[name]
        proof = record.recovery["outcomes"][name]
        start = round(rt.timestamp(began) * 1000)
        assert item["recoveries"][name] == {
            "fault": fault,
            "began_at": began,
            "restored_from": "savepoint" if name == "upgrade" else "checkpoint",
            "restored_id": proof["restored"]["id"],
            "restored_ms": proof["restored"]["restore_timestamp"] - start,
            "checkpoint_id": proof["checkpoint"]["id"],
            "checkpoint_ms": proof["checkpoint"]["latest_ack_timestamp"] - start,
        }
        # The World restores 30 seconds after the fault.
        assert item["recoveries"][name]["restored_ms"] >= 30_000
    rendered = analyze.render_markdown(report)
    assert rendered.startswith("# Tier-3 evidence analysis")
    assert "0162-cloudtasks-assessment-1246.md" not in rendered
    assert f"| {world.approval.run_id} | usable | usable |" in rendered
    assert (
        "| run | recovery | fault | began | restored from | restored after " in rendered
    )
    assert f"| {world.approval.run_id} | upgrade | savepoint-upgrade | " in rendered
    out = analyze.write_reports(report, tmp_path / "analysis")
    assert "Deployed BigQuery runs" in (out / "report.md").read_text()
    assert (
        json.loads((out / "report.json").read_text())["bigquery"] == report["bigquery"]
    )
    assert "bigquery runs: usable 1" in analyze.summary_line(report, out)


def test_a_listed_metric_the_job_stopped_publishing_is_not_observed(
    prepared, monkeypatch, tmp_path
):
    """Discovery keeps the id; the answer omits it; the family is not read."""
    world = World(prepared, monkeypatch, withheld="inPoolUsage")
    record = world.run()
    assert record.recovery["verdict"] == INCONCLUSIVE
    assert record.recovery["reasons"] == [
        f"unobserved-network-in-{stage}" for stage in ("baseline", "finishing")
    ]
    report = analyze.analyze(world.export(tmp_path, record))
    item = report["bigquery"][0]
    assert item["verdict"] == INCONCLUSIVE
    assert item["problems"] == []
    assert item["reasons"] == record.recovery["reasons"]


@pytest.fixture(autouse=True)
def select_trial(request, trial):
    for key in ("mode", "destinations"):
        if key in request.fixturenames:
            trial[key] = request.getfixturevalue(key)
    if "trial_fault" in request.fixturenames:
        trial["fault"] = request.getfixturevalue("trial_fault")


@pytest.mark.parametrize(
    "mode,trial_fault,subtasks,replaced",
    [
        ("EO", "tm-replacement", None, "tm-1"),
        ("EO", "rescale-out", (2, 3), None),
        ("EO", "rescale-in", (3, 2), None),
        ("FILE_LOADS", "tm-replacement", None, "tm-1"),
        ("FILE_LOADS", "rescale-out", (3, 5), None),
        ("FILE_LOADS", "rescale-in", (5, 3), None),
    ],
)
def test_each_fault_replaces_one_recovery_and_proves_it(
    prepared, monkeypatch, tmp_path, mode, trial_fault, subtasks, replaced
):
    """The trial's fault reaches the exercise, which proves it in its place."""
    world = World(prepared, monkeypatch)
    assert world.plan.trial.fault == trial_fault
    upgrade = world.plan.trial.parallelism("upgrade")
    record = world.run()
    assert record.success, record.reason
    assert record.recovery["verdict"] == rt.USABLE
    outcomes = record.recovery["outcomes"]
    if subtasks is None:
        assert "rescale" not in outcomes["upgrade"]
    else:
        assert outcomes["upgrade"]["rescale"] == {
            "direction": trial_fault.removeprefix("rescale-"),
            "subtasks_before": subtasks[0],
            "subtasks_after": subtasks[1],
        }
    if replaced is None:
        assert "replaced" not in outcomes["failover"]
        assert world.failover_kind == "jobmanager"
    else:
        # The TaskManager that reported the input went, and the JobManager
        # that held the job stayed.
        assert outcomes["failover"]["replaced"]["name"] == replaced
        assert world.failover_kind == "taskmanager"
        assert outcomes["failover"]["jm_uid"] == outcomes["upgrade"]["jm_uid"]
    assert [name for name, _ in world.calls] == ["upgrade", "failover"]
    # The upgrade asked for the upgrade phase's sizes, and only a rescale's differ.
    assert world.patched["job"]["parallelism"] == upgrade
    assert world.patched["taskManager"]["replicas"] == upgrade
    # The offline analysis names the fault from the proof the run exported.
    report = analyze.analyze(world.export(tmp_path, record))
    recoveries = report["bigquery"][0]["recoveries"]
    assert {name: timing["fault"] for name, timing in recoveries.items()} == {
        "upgrade": trial_fault if subtasks else "savepoint-upgrade",
        "failover": trial_fault if replaced else "jm-replacement",
    }


@pytest.mark.parametrize("mode,trial_fault", [("FILE_LOADS", "tm-replacement")])
def test_a_file_loads_failover_without_a_pod_to_delete_waits_for_no_load(
    prepared, monkeypatch, mode, trial_fault
):
    """The in-flight wait and its record precede the deletion, so they wait for
    a Pod to delete as the failover itself does."""
    world = World(prepared, monkeypatch)
    exercise = world.supervisor.exercise
    assert exercise.trial.mode == "FILE_LOADS"
    assert exercise.failover == "taskmanager"
    waited = []
    monkeypatch.setattr(exercise, "active_input", lambda phase: True)
    monkeypatch.setattr(exercise, "_await_running_load", lambda: waited.append(1))
    # No TaskManager reported the upgraded input yet, so none can be named.
    assert exercise.failover_target([]) is None
    exercise.begin_failover(world.app(), {}, [])
    assert waited == []
    assert "failover_in_flight" not in exercise.outcomes
    assert exercise.stage == "baseline"


@pytest.mark.parametrize(
    "fault,reason",
    [
        ("no-progress", "deadline"),
        ("fresh-lineage", "lineage"),
        ("wrong-state", "savepoint"),
        ("early-finish", "post-recovery"),
        ("restart", "interruption"),
        ("upgrade-conflict", "precondition"),
        ("invisible", "query slots"),
        ("duplicates", "uniqueness"),
        ("invalid", "routing"),
    ],
)
def test_incomplete_trials_fail_and_cleanup(prepared, monkeypatch, fault, reason):
    world = World(prepared, monkeypatch, fault)
    record = world.run()
    assert not record.success
    assert reason in record.reason
    assert record.phase == Phase.CLEANED
    assert world.resources.tables == {}


@pytest.mark.parametrize("mode,destinations", [("FILE_LOADS", 10)])
def test_cleanup_waits_for_a_load_job_that_outlived_the_workload(
    prepared, monkeypatch, mode, destinations
):
    """A load the committer submitted just before its Pods stopped."""
    world = World(prepared, monkeypatch, "load-outlives")
    deleted = []
    delete_table = world.resources.delete_table

    def delete(destination):
        deleted.append(world.connector_listings.count(True))
        return delete_table(destination)

    monkeypatch.setattr(world.resources, "delete_table", delete)
    record = world.run()
    assert record.success, record.reason
    # Two listings after the Pods were gone still showed it; no table was
    # deleted until a third showed it DONE.
    assert world.connector_listings.count(True) >= 3
    assert deleted and min(deleted) >= 3
    assert record.bigquery["file_loads"]["unfinished_jobs"] == [LOAD["id"]]
    assert world.resources.tables == {}


@pytest.mark.parametrize("mode,destinations", [("FILE_LOADS", 10)])
def test_the_oracle_refuses_to_read_while_a_load_job_still_runs(
    prepared, monkeypatch, mode, destinations
):
    """FINISHED says the committer is done; a running load says otherwise."""
    world = World(prepared, monkeypatch, "load-after-finished")
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
    assert world.resources.tables == {}


@pytest.mark.parametrize("mode,destinations", [("FILE_LOADS", 10)])
def test_the_oracle_asks_again_before_every_read(
    prepared, monkeypatch, mode, destinations
):
    """A visibility retry is a later read; a load that started since is refused."""
    world = World(prepared, monkeypatch, "load-between-reads")
    record = world.run()
    assert not record.success
    assert "outlived the finished application" in record.reason
    assert [slot for slot, _ in world.query_calls] == [0]


@pytest.mark.parametrize("mode,destinations", [("FILE_LOADS", 10)])
def test_file_loads_duplicates_fail_the_oracle(
    prepared, monkeypatch, mode, destinations
):
    world = World(prepared, monkeypatch, "duplicates")
    record = world.run()
    assert not record.success
    assert "uniqueness" in record.reason


def test_visibility_retries_use_distinct_slots(prepared, monkeypatch):
    world = World(prepared, monkeypatch, "late-visibility")
    record = world.run()
    assert record.success, record.reason
    assert [slot for slot, _ in world.query_calls] == [0, 1]
    requests = record.bigquery["handoff"]["requests"]
    assert requests["final-0"] == requests["final-1"]


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

    def created(manifest, dry_run=False):
        if manifest["kind"] == "FlinkDeployment":
            sequence.append("application")
            assert len(resources.tables) == resources.plan.trial.destinations
        result = create(manifest, dry_run)
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


@pytest.mark.parametrize("mode", ["ALO"])
def test_alo_duplicate_rows_are_reported_without_failing(prepared, mode):
    environment, _, upgrade, _ = prepared
    assert environment.approval.bigquery_plan.trial.mode == mode
    exercise = BigQueryExercise(environment, upgrade, None)
    exercise.stage, exercise.initialized = "visibility", True
    environment.records.set_phase(Phase.READY)
    environment.records.set_phase(Phase.RUNNING)
    environment.records.recovery_step(None, {"stage": "visibility"})

    class Queries:
        def query(self, _name, *, deadline):
            assert deadline <= environment.schedule.cleanup_at
            data = rows(exercise.trial, duplicate=True)
            return {"rows": data, "report": assess(exercise.trial, data)}

    exercise.verify_rows(Queries())
    assert (
        exercise.outcomes["query"]["report"]["duplicate_rows"]
        == exercise.trial.destinations
    )
    assert exercise.stage == "complete"


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

    def created(value, dry_run=False):
        if value["kind"] == "FlinkDeployment" and dry_run and failure == "dry-run":
            raise Failure("Application dry-run rejected")
        actual = create(value, dry_run)
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

    def fail(_value, dry_run=False):
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


@pytest.mark.parametrize("mode,destinations", [("FILE_LOADS", 10)])
@pytest.mark.parametrize(
    "fault,after,through",
    [
        # The load finished between the listing and the delete.
        ("load-finished-before-delete", [], []),
        # Another load started after the delete: it was not running through it.
        (
            "another-load-after-delete",
            sorted([LOAD["id"], LOAD["id"][:-1] + "1"]),
            [LOAD["id"]],
        ),
    ],
)
def test_only_a_load_running_on_both_sides_of_the_delete_is_in_flight(
    prepared, monkeypatch, mode, destinations, fault, after, through
):
    world = World(prepared, monkeypatch, fault)
    record = world.run()
    assert record.success, record.reason
    in_flight = record.recovery["outcomes"]["failover_in_flight"]
    assert in_flight["running_before"] == [LOAD["id"]]
    assert (in_flight["running_after"], in_flight["running_through"]) == (
        after,
        through,
    )


def storage_timeout():
    """What the storage adapter raises for a control read that timed out."""
    try:
        raise requests.exceptions.ReadTimeout("read timed out")
    except requests.exceptions.ReadTimeout as error:
        try:
            raise Failure("GET storage request failed: ReadTimeout") from error
        except Failure as wrapped:
            return wrapped


def refresh_unreachable():
    """A control read whose token refresh found the metadata server down.

    google-auth raises a RefreshError from its own TransportError, and the
    storage adapter wraps that as a plain failure.
    """
    try:
        try:
            raise google.auth.exceptions.TransportError("metadata server down")
        except google.auth.exceptions.TransportError as transport:
            raise google.auth.exceptions.RefreshError("refresh failed") from transport
    except google.auth.exceptions.RefreshError as error:
        try:
            raise Failure("GET storage request failed: RefreshError") from error
        except Failure as wrapped:
            return wrapped


@pytest.mark.parametrize("mode,destinations", [("FILE_LOADS", 10)])
@pytest.mark.parametrize(
    "error",
    [
        lambda: TransportError("GET BigQuery request failed: ReadTimeout"),
        storage_timeout,
        refresh_unreachable,
    ],
    ids=["bigquery", "storage", "refresh"],
)
def test_transient_listing_errors_neither_stop_the_wait_nor_lose_the_record(
    prepared, monkeypatch, mode, destinations, error
):
    world = World(prepared, monkeypatch, "listing-error")
    world.listing_error = error
    record = world.run()
    assert record.success, record.reason
    in_flight = record.recovery["outcomes"]["failover_in_flight"]
    assert in_flight["listing_errors"] == 1
    assert in_flight["running_before"] == [LOAD["id"]]
    assert in_flight["running_after"] is in_flight["running_through"] is None
    assert in_flight["relisting_error"] == str(error())


@pytest.mark.parametrize("mode,destinations", [("FILE_LOADS", 10)])
def test_a_listing_that_refuses_its_answer_stops_the_wait(
    prepared, monkeypatch, mode, destinations
):
    """An invariant, unlike a transport error, is not a listing that saw nothing."""
    world = World(prepared, monkeypatch, "listing-refused")
    record = world.run()
    assert not record.success
    assert "no readable id" in record.reason
    assert [name for name, _ in world.calls] == ["upgrade"]


@pytest.mark.parametrize("mode,destinations", [("FILE_LOADS", 10)])
@pytest.mark.parametrize(
    "fault,reason",
    [
        ("no-load-in-flight", "No load job of the run ran within the in-flight wait"),
        ("stop-while-waiting", "stopped"),
    ],
)
def test_a_file_loads_failover_waits_for_a_running_load_and_no_longer(
    prepared, monkeypatch, mode, destinations, fault, reason
):
    """No load runs, or the run stops: the JobManager is never deleted."""
    world = World(prepared, monkeypatch, fault)
    record = world.run()
    assert not record.success
    assert reason in record.reason
    assert [name for name, _ in world.calls] == ["upgrade"]
    armed = world.listings[0][0]
    if fault == "no-load-in-flight":
        # One listing a second, without a gap, for the whole wait and no longer.
        wait = BIGQUERY_OBSERVATIONS["in_flight_seconds"]
        # One checkpoint interval and a margin, as the runbook states it.
        assert wait == 130
        assert [at - armed for at, _ in world.listings] == list(range(wait))
    else:
        # A stop is checked once a poll, not on every listing.
        assert len(world.listings) == rt.POLL
