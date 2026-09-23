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
from flink_tier3 import analyze
from flink_tier3.bigquery import assess
from flink_tier3.bigquery_exercise import BigQueryExercise, require_handoff
from flink_tier3.bigquery_handoff import BigQueryHandoff
from flink_tier3.bigquery_lifecycle import BigQueryLifecycle
from flink_tier3.bigquery_verdict import MEASUREMENT_EVENT
from flink_tier3.common import INCONCLUSIVE, Failure
from flink_tier3.environment import Environment
from flink_tier3.model import Phase
from flink_tier3.policy import BIGQUERY, BIGQUERY_STATE, MIB
from flink_tier3.runner import Runner
from flink_tier3.supervisor import Supervisor
from test_bigquery_approval import prepared as prepared  # noqa: PLC0414
from test_bigquery_lifecycle import Resources
from test_bigquery_plan import inputs as inputs  # noqa: PLC0414
from test_bigquery_plan import renderer as renderer  # noqa: PLC0414
from test_bigquery_plan import trial as trial  # noqa: PLC0414
from test_tier3_lifecycle import env as env  # noqa: PLC0414
from test_tier3_lifecycle import obj, rt, supervisor_pod


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
        for name, component in (
            ("jm-initial", "jobmanager"),
            ("tm-1", "taskmanager"),
            ("tm-2", "taskmanager"),
        ):
            self.pod(name, component)
        self.original_patch, self.original_delete = self.kube.patch, self.kube.delete
        self.original_request = self.kube.request
        monkeypatch.setattr(self.kube, "patch", self.patch)
        monkeypatch.setattr(self.kube, "delete", self.delete)
        monkeypatch.setattr(self.kube, "request", self.request)
        monkeypatch.setattr(self.kube, "logs", self.logs)
        monkeypatch.setattr(self.resources, "results", self.results)

    def app(self):
        return self.kube.get("FlinkDeployment", BIGQUERY, self.approval.run_id)

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
        current["spec"]["job"]["args"] = changes[0]["value"]
        current["metadata"].update(generation=2, resourceVersion="2")
        current["status"]["jobStatus"]["state"] = "FINISHED"
        current["status"]["reconciliationStatus"] = {"state": "UPGRADING"}
        self.kube.put(current)
        self.phase, self.changed = "upgrade", self.clock()
        self.upgrade_at = self.clock()
        return current

    def delete(self, value, force=False):
        if (
            value["kind"] == "Pod"
            and value["metadata"].get("labels", {}).get("component") == "jobmanager"
        ):
            self.calls.append(("failover", self.clock()))
            self.phase, self.changed = "failover", self.clock()
            self.failover_at = self.clock()
        return self.original_delete(value, force)

    def request(self, method, path, *args, **kwargs):
        # The measurement collector reads the job plan, the sink's metric ids
        # and the TaskManagers; the real service answers all three through the
        # same proxy, so the double does too.
        if "/proxy/jobs/" in path and "/" not in path.split("/proxy/jobs/")[1]:
            return {
                "vertices": [
                    {"id": "source-vertex", "name": "Source: datagen"},
                    {"id": "sink-vertex", "name": "Sink: bigquery"},
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
            old_name = "jm-initial" if self.phase == "upgrade" else "jm-upgrade"
            self.kube.data.pop(("Pod", BIGQUERY, old_name), None)
            name = "jm-" + self.phase
            if not self.kube.get("Pod", BIGQUERY, name):
                self.pod(name, "jobmanager")
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
                "restore_timestamp": (self.changed + 30) * 1000,
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
            self.history.setdefault("tm-1-uid", []).append((now, line))
        if elapsed >= 45:
            self.cp = {
                "id": 1 if self.phase == "initial" else 43,
                "status": "COMPLETED",
                "is_savepoint": False,
                "trigger_timestamp": (now - 1) * 1000,
                "latest_ack_timestamp": now * 1000,
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
            or (self.fault == "late-visibility" and slot == 0),
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
        `Runner` writes into them is held by `test_bigquery_approval`; what is
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

    def run(self):
        self.supervisor.supervise(self.supervisor_pod["metadata"]["uid"])
        return self.env.refresh()


@pytest.mark.parametrize("mode", ["ALO", "EO"])
@pytest.mark.parametrize("destinations", [10, 50])
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
    assert set(record.recovery["outcomes"]) == {"upgrade", "failover", "query"}
    # The verdict the production path produced, not one typed into a fixture:
    # a run that proves both recoveries, passes the oracle and reads every
    # family in both sampled windows is the usable measurement.
    assert record.recovery["verdict"] == rt.USABLE
    assert record.recovery["reasons"] == []
    for stage in ("baseline", "finishing"):
        window = record.recovery["coverage"][stage]
        assert window["observed"] == ["connector", "memory", "network", "task"]
        assert window["attempts"] >= 1
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
    assert [item["status"] for item in report["bigquery"]] == [rt.USABLE]
    item = report["bigquery"][0]
    assert item["run_id"] == world.approval.run_id
    assert item["verdict"] == rt.USABLE
    assert item["reasons"] == []
    assert item["problems"] == []
    assert item["samples"] == len(world.measurements())
    rendered = analyze.render_markdown(report)
    assert rendered.startswith("# Tier-3 evidence analysis")
    assert "0162-cloudtasks-assessment-1246.md" not in rendered
    assert f"| {world.approval.run_id} | usable | usable |" in rendered
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
    assert item["status"] == INCONCLUSIVE
    assert item["problems"] == []
    assert item["reasons"] == record.recovery["reasons"]


@pytest.fixture(autouse=True)
def select_trial(request, trial):
    for key in ("mode", "destinations"):
        if key in request.fixturenames:
            trial[key] = request.getfixturevalue(key)


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
            supervisor_pod(
                (
                    kube,
                    environment.store,
                    environment.approval.to_dict(),
                    environment.clock,
                ),
                result,
            )
            environment.records.heartbeat()
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


@pytest.mark.parametrize("actor,limit", [("runner", 8), ("supervisor", 80)])
def test_query_reservation_reduces_actor_receipt_budget(
    prepared, monkeypatch, actor, limit
):
    environment, *_ = prepared
    monkeypatch.setattr(
        environment.store, "objects", lambda _prefix: [{"size": limit * MIB}]
    )
    with pytest.raises(Failure, match="evidence ceiling"):
        environment.records.evidence("test", {}, actor)


def test_wrong_upgrade_hash_refused_at_construction(prepared):
    environment, _, upgrade, _ = prepared
    upgrade["spec"]["job"]["args"][3] = "wrong"
    with pytest.raises(Failure, match="approved upgrade"):
        BigQueryExercise(environment, upgrade)


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
    exercise = BigQueryExercise(environment, upgrade)
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
        BigQueryExercise(environment, upgrade).verify_rows(None)


@pytest.mark.parametrize("mode", ["ALO"])
def test_alo_duplicate_rows_are_reported_without_failing(prepared, mode):
    environment, _, upgrade, _ = prepared
    assert environment.approval.bigquery_plan.trial.mode == mode
    exercise = BigQueryExercise(environment, upgrade)
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
    exercise = BigQueryExercise(environment, upgrade)
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
    world.env.stopping = True
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
    observer_env.stopping = True
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
