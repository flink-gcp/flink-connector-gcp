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
"""The supervised Pub/Sub recovery exercise over a simulated relay."""

import base64
import itertools
import json
import uuid

import pytest
from flink_tier3.policy import PUBSUB, PUBSUB_STATE
from flink_tier3.pubsub_exercise import MEASUREMENT_EVENT
from flink_tier3.pubsub_plan import COUNTER_CEILINGS
from test_pubsub_actors import prepared as prepared  # noqa: PLC0414
from test_pubsub_admission import admitting as admitting  # noqa: PLC0414
from test_pubsub_admission import supervisor_uid
from test_pubsub_bundle import approval as approval  # noqa: PLC0414
from test_pubsub_messages import Response
from test_pubsub_plan import inputs as inputs  # noqa: PLC0414
from test_pubsub_plan import renderer as renderer  # noqa: PLC0414
from test_tier3_lifecycle import env as env  # noqa: PLC0414
from test_tier3_lifecycle import obj, rt

RECORDS = 30
INTERVAL = 120


@pytest.fixture(
    params=["jm-replacement", "tm-replacement", "rescale-out", "rescale-in"]
)
def trial(request):
    return {
        "version": 3,
        "trial": request.param,
        "entry_point": "datastream",
        "records_per_subscription": RECORDS,
        "traffic_limits": dict(COUNTER_CEILINGS),
        "total_request_limit": 100000,
    }


def encode(value):
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


class Relay:
    """The application, the Operator and the Pub/Sub service, simulated.

    Each input index is processed by one attempt; output is published at
    once. A checkpoint is taken every ``interval`` seconds and acknowledges
    what it covers. A fault redelivers what the attempts it ends processed
    since the checkpoint the job restores: a JobManager loss restarts every
    attempt, a TaskManager loss only the attempt on that Pod, and a savepoint
    upgrade acknowledges everything before the job stops.
    """

    def __init__(
        self,
        a,
        monkeypatch,
        *,
        interval=INTERVAL,
        redeliver=True,
        tm_full=False,
        new_ids=False,
        restored_flag=True,
        ignore_delete=False,
    ):
        self.a, self.kube, self.clock = a, a.environment.kube, a.environment.clock
        self.approval = a.environment.approval
        self.run_id = self.approval.run_id
        self.kind = self.approval.pubsub_trial["trial"]
        self.interval, self.redeliver = interval, redeliver
        # A TaskManager's loss restarts every attempt rather than its region.
        self.tm_full = tm_full
        # Redelivery carries new input message IDs, as a duplicate publication.
        self.new_ids = new_ids
        # Attempts after a fault announce they restored state.
        self.restored_flag = restored_flag
        # The deleted Pod stays, and the job restarts in place anyway.
        self.ignore_delete = ignore_delete
        # A checkpoint triggered and not yet completed, with what it covers.
        self.inflight = None
        # Deleted Pods still terminating, and when they go.
        self.terminating = []
        self.jm_down = False
        self.parallelism = 1 if self.kind == "rescale-out" else 2
        self.phase = "initial"
        self.pending = {0: [], 1: []}
        # Processed and not yet acknowledged: (message, attempt).
        self.unacked = []
        self.output = []
        self.attempts = {}
        # Attempts that initialized from restored state.
        self.restored_attempts = set()
        self.logs = {}
        self.pods = 0
        self.history, self.next_id = [], 1
        self.completed, self.restored = {}, {}
        self.restart_at = None
        self.restart = None
        self.started = None
        self.next_trigger = None
        self.events = []
        # Called at each step, after the simulated job has advanced.
        self.hooks = []
        self.original_request = self.kube.request
        self.original_delete = self.kube.delete
        self.original_patch = self.kube.patch
        self.original_logs = self.kube.logs
        monkeypatch.setattr(self.kube, "request", self.request)
        monkeypatch.setattr(self.kube, "logs", self.read_logs)
        monkeypatch.setattr(self.kube, "delete", self.delete)
        monkeypatch.setattr(self.kube, "patch", self.patch)
        sender = a.sender.controller.http
        receiver = a.receiver.controller.http
        publish, pull = sender.request, receiver.request
        monkeypatch.setattr(
            sender, "request", lambda m, u, **k: self.publish(publish, m, u, **k)
        )
        monkeypatch.setattr(
            receiver, "request", lambda m, u, **k: self.pull(pull, m, u, **k)
        )

    # The service.

    def publish(self, original, method, url, **kwargs):
        response = original(method, url, **kwargs)
        if url.endswith(":publish"):
            ids = json_ids(response)
            for message, message_id in zip(
                kwargs["json"]["messages"], ids, strict=True
            ):
                payload = base64.b64decode(message["data"]).decode()
                index = int(payload.split("|")[2])
                self.pending[index].append(
                    {"id": message_id, "payload": payload, "index": index}
                )
        return response

    def pull(self, original, method, url, **kwargs):
        if url.endswith(":acknowledge"):
            return Response({})
        if not url.endswith(":pull") or kwargs["json"].get("returnImmediately"):
            return original(method, url, **kwargs)
        batch, self.output = (
            self.output[: kwargs["json"]["maxMessages"]],
            self.output[kwargs["json"]["maxMessages"] :],
        )
        return Response(
            {
                "receivedMessages": [
                    {
                        "ackId": "ack-" + item["messageId"],
                        "message": {
                            "messageId": item["messageId"],
                            "data": base64.b64encode(item["data"].encode()).decode(),
                        },
                    }
                    for item in batch
                ]
            }
        )

    # The cluster.

    def app(self):
        return self.kube.get("FlinkDeployment", PUBSUB, self.run_id)

    def pod(self, component):
        app = self.app()
        name = f"{component}-{self.pods}"
        self.pods += 1
        pod = obj("Pod", name, PUBSUB, app["metadata"]["uid"])
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

    def managers(self):
        return [
            p
            for p in self.kube.items("Pod", PUBSUB)
            if p["metadata"]["labels"].get("component") == "taskmanager"
        ]

    def deploy(self):
        """The Operator's first deployment, once admission created it."""
        app = self.app()
        app["metadata"].update(generation=1, resourceVersion="1")
        app["status"] = {"jobStatus": {"state": "CREATED", "jobId": ""}}
        self.kube.put(app)
        self.kube.put(
            obj("Service", self.run_id + "-rest", PUBSUB, app["metadata"]["uid"])
        )
        self.pod("jobmanager")
        for _ in range(self.parallelism):
            self.pod("taskmanager")

    def start_attempts(self, indices, *, restored, managers=None):
        managers = managers or [
            p for p in self.managers() if not p["metadata"].get("deletionTimestamp")
        ]
        for index in indices:
            attempt = str(uuid.uuid4())
            manager = managers[index % len(managers)]
            self.attempts[index] = attempt
            if restored:
                self.restored_attempts.add(attempt)
            line = (
                f"{rt.utc(self.clock())} INFO event=pubsub-attempt run_id={self.run_id} "
                f"phase={self.phase} attempt={attempt} "
                f"restored={str(restored).lower()}\n"
            )
            self.logs.setdefault(manager["metadata"]["uid"], []).append(
                (self.clock(), line)
            )

    def read_logs(self, pod, since=None):
        if pod["metadata"]["uid"] not in self.logs:
            return self.original_logs(pod, since)
        return "".join(
            line
            for at, line in self.logs.get(pod["metadata"]["uid"], [])
            if since is None or at >= rt.timestamp(since)
        ).encode()

    def checkpoint_path(self, kind="checkpoints"):
        return f"gs://{PUBSUB_STATE}/runs/{self.run_id}/{kind}/chk-{self.next_id}"

    def trigger_checkpoint(self):
        cp = {
            "id": self.next_id,
            "status": "IN_PROGRESS",
            "is_savepoint": False,
            "trigger_timestamp": self.clock() * 1000,
            "external_path": self.checkpoint_path(),
        }
        self.next_id += 1
        self.history.append(cp)
        # The barrier covers what was processed before it.
        self.inflight = (cp, list(self.unacked))

    def complete_checkpoint(self):
        cp, covered = self.inflight
        self.inflight = None
        cp.update(
            status="COMPLETED",
            latest_ack_timestamp=self.clock() * 1000,
            end_to_end_duration=900,
        )
        self.completed = dict(cp)
        # Completion acknowledges what the checkpoint covers, nothing later.
        self.unacked = [item for item in self.unacked if item not in covered]

    def process(self):
        for index in (0, 1):
            attempt = self.attempts.get(index if self.parallelism == 2 else 0)
            for message in self.pending[index]:
                fields = message["payload"].split("|")
                data = "|".join(
                    [
                        *fields,
                        encode(message["id"]),
                        attempt,
                        str(uuid.uuid4()),
                        self.phase,
                        "true" if attempt in self.restored_attempts else "false",
                    ]
                )
                self.output.append({"messageId": uuid.uuid4().hex, "data": data})
                self.unacked.append((message, attempt))
            self.pending[index] = []

    def advance(self):
        app = self.app()
        if not app:
            return
        now = self.clock()
        for pod, until in list(self.terminating):
            if now >= until:
                self.terminating.remove((pod, until))
                self.original_delete(pod)
        state = app["status"].get("jobStatus", {}).get("state")
        if state == "CREATED":
            app["status"]["jobStatus"].update(state="RUNNING", jobId="1" * 32)
            self.kube.put(app)
            self.started = now
            self.next_trigger = now + self.interval
            self.start_attempts(range(self.parallelism), restored=False)
            return
        if self.restart_at is not None:
            if now < self.restart_at:
                return
            self.restart()
            self.restart_at = self.restart = None
            return
        if state != "RUNNING":
            return
        self.process()
        if self.inflight:
            self.complete_checkpoint()
        if now >= self.next_trigger:
            self.trigger_checkpoint()
            self.next_trigger += self.interval

    # Faults.

    def requeue(self, attempts):
        if not self.redeliver:
            self.unacked = [item for item in self.unacked if item[1] not in attempts]
            return
        kept = []
        for message, attempt in self.unacked:
            if attempt in attempts:
                if self.new_ids:
                    message = {**message, "id": uuid.uuid4().hex}
                self.pending[message["index"]].append(message)
            else:
                kept.append((message, attempt))
        self.unacked = kept

    def restore(self, *, savepoint=None):
        # An in-progress checkpoint is abandoned by the failover.
        self.inflight = None
        cp = savepoint or self.completed
        self.restored = {
            "id": cp["id"],
            "is_savepoint": savepoint is not None,
            "restore_timestamp": self.clock() * 1000,
            "external_path": cp["external_path"],
        }

    def delete(self, value, force=False):
        component = value["metadata"].get("labels", {}).get("component")
        if value["kind"] == "Pod" and component in ("jobmanager", "taskmanager"):
            self.events.append(("delete", component, value["metadata"]["name"]))
            current = self.kube.get("Pod", PUBSUB, value["metadata"]["name"])
            if not self.ignore_delete:
                # The Pod terminates until a step after the restart, as a
                # graceful shutdown outlasting the replacement would.
                current["metadata"]["deletionTimestamp"] = rt.utc(self.clock())
                self.kube.put(current)
                self.terminating.append((current, self.clock() + 45))
            restored = self.restored_flag
            if component == "jobmanager":
                # The Operator has not noticed yet, and the REST API is gone
                # with the JobManager.
                self.jm_down = True
                lost = set(self.attempts.values())

                def restart():
                    self.jm_down = False
                    if not self.ignore_delete:
                        self.pod("jobmanager")
                    self.requeue(lost)
                    self.restore()
                    self.start_attempts(range(self.parallelism), restored=restored)

            else:
                index = next(
                    i
                    for i, attempt in self.attempts.items()
                    if self.attempt_pod(attempt) == value["metadata"]["uid"]
                )
                indices = range(self.parallelism) if self.tm_full else [index]
                lost = {self.attempts[i] for i in indices}

                def restart():
                    replacement = (
                        current if self.ignore_delete else self.pod("taskmanager")
                    )
                    self.requeue(lost)
                    self.restore()
                    for i in indices:
                        # The replaced attempt starts on the new Pod, the
                        # others where they ran.
                        survivor = self.attempt_pod(self.attempts[i])
                        pod = (
                            replacement
                            if i == index
                            else next(
                                p
                                for p in self.managers()
                                if p["metadata"]["uid"] == survivor
                            )
                        )
                        self.start_attempts([i], restored=restored, managers=[pod])

            self.restart_at, self.restart = self.clock() + 30, restart
            return True
        return self.original_delete(value, force)

    def attempt_pod(self, attempt):
        for uid, lines in self.logs.items():
            if any(attempt in line for _, line in lines):
                return uid
        return None

    def running(self):
        app = self.app()
        app["status"]["jobStatus"]["state"] = "RUNNING"
        self.kube.put(app)

    def patch(self, value, changes, subresource=""):
        if value["kind"] != "FlinkDeployment":
            return self.original_patch(value, changes, subresource)
        self.events.append(("patch", [c["path"] for c in changes]))
        app = self.app()
        for change in changes:
            target = app
            *parents, leaf = change["path"].strip("/").split("/")
            for key in parents:
                target = target[key]
            target[leaf] = change["value"]
        app["metadata"].update(generation=2, resourceVersion="2")
        # Stop with a savepoint: it completes and acknowledges what it covers.
        savepoint = {
            "id": self.next_id,
            "external_path": self.checkpoint_path("savepoints"),
        }
        self.next_id += 1
        self.unacked = []
        app["status"]["jobStatus"]["state"] = "FINISHED"
        app["status"]["reconciliationStatus"] = {"state": "UPGRADING"}
        self.kube.put(app)

        def restart():
            for pod in self.managers():
                self.original_delete(pod)
            self.parallelism = app["spec"]["job"]["parallelism"]
            self.phase = "upgrade"
            for _ in range(self.parallelism):
                self.pod("taskmanager")
            current = self.app()
            current["status"].update(
                observedGeneration=2,
                reconciliationStatus={"state": "DEPLOYED"},
            )
            current["status"]["jobStatus"].update(
                state="RUNNING",
                jobId="2" * 32,
                savepointInfo={
                    "lastSavepoint": {
                        "location": savepoint["external_path"],
                        "triggerType": "UPGRADE",
                        "timeStamp": self.clock() * 1000,
                    }
                },
            )
            self.kube.put(current)
            self.restore(savepoint=savepoint)
            self.attempts = {}
            self.start_attempts(range(self.parallelism), restored=True)

        self.restart_at, self.restart = self.clock() + 30, restart
        return app

    # The REST API.

    def request(self, method, path, *args, **kwargs):
        if "/proxy/jobs/" not in path:
            return self.original_request(method, path, *args, **kwargs)
        if self.jm_down:
            raise rt.ApiError(503, method, path)
        suffix = path.split("/proxy/jobs/")[1]
        if suffix.endswith("/checkpoints"):
            return {
                "counts": {"completed": len(self.history)},
                "latest": {"completed": self.completed, "restored": self.restored},
                "history": list(reversed(self.history[-10:])),
            }
        if suffix.endswith("/backpressure"):
            return {"status": "ok", "backpressureLevel": "ok", "subtasks": []}
        if "/" not in suffix:
            return {"vertices": [{"id": "relay", "name": "Source: Pub/Sub input"}]}
        raise AssertionError(path)


def json_ids(response):
    return json.loads(response.data)["messageIds"]


class World:
    def __init__(self, a, monkeypatch, **relay):
        self.a = a
        self.relay = Relay(a, monkeypatch, **relay)
        a.runner.start(*a.args)
        self.relay.deploy()
        original = a.observer.sleep

        def sleep(seconds):
            original(seconds)
            self.runner()
            self.relay.advance()
            for hook in self.relay.hooks:
                hook()

        monkeypatch.setattr(a.observer, "sleep", sleep)

    def runner(self):
        """The runner's settlement poll: serve while running, else release."""
        control = self.a.environment.refresh()
        actor = control.pubsub["handoff"]["actors"]["runner"]
        if actor["released"]:
            return
        if control.stop_requested or control.phase != rt.Phase.RUNNING:
            self.a.sender.release()
        else:
            self.a.sender.serve()

    def run(self):
        self.a.supervisor.supervise(supervisor_uid(self.a))
        return self.a.environment.refresh()

    def evidence_events(self):
        """The run's evidence documents, in the order they were written."""
        store = self.a.environment.store
        values = [
            value
            for (bucket, _), (value, _) in store.data.items()
            if bucket == rt.EVIDENCE and isinstance(value, dict) and "event" in value
        ]
        return sorted(values, key=lambda value: value["at"])

    def evidence(self, event):
        return [value for value in self.evidence_events() if value["event"] == event]


def test_each_trial_runs_to_completion_and_cleans(admitting, monkeypatch):
    world = World(admitting, monkeypatch)
    control = world.run()
    kind = world.relay.kind
    assert control.recovery["stage"] == "complete", control.reason
    assert control.pubsub["stage"] == "cleaned"
    recovery = control.recovery["outcomes"]["recovery"]
    third = RECORDS // 3
    if kind in ("jm-replacement", "tm-replacement"):
        assert recovery["boundary_held"] is True
        assert recovery["replay"] == "observed"
        # The JobManager's loss displaces both inputs' attempts; one
        # TaskManager's, the attempt on that Pod only.
        expected = 2 * third if kind == "jm-replacement" else third
        assert recovery["expected_replay"] == recovery["replayed"] == expected
        assert recovery["replay_ids_preserved"] is True
    else:
        assert recovery["replay"] == "not-expected"
        assert recovery["expected_replay"] == 0
        assert (
            "patch",
            ["/spec/job/args", "/spec/job/parallelism", "/spec/taskManager/replicas"],
        ) in world.relay.events
    outcomes = control.recovery["outcomes"]
    fault = outcomes["fault"]
    # The retained checkpoint and the fault travel to the last record.
    assert outcomes["checkpoint"]["id"] == fault["checkpoint"]
    assert outcomes["checkpoint"]["external_path"].startswith(
        f"gs://{PUBSUB_STATE}/runs/{world.relay.run_id}/checkpoints/"
    )
    assert fault["latest_completed"] == fault["checkpoint"]
    assert fault["cohort"]["started_at"] <= fault["cohort"]["published_at"]
    assert outcomes["after"]["attempts"]
    assert outcomes["after"]["checkpoint"]["id"] > fault["checkpoint"]
    if kind in ("jm-replacement", "tm-replacement"):
        assert recovery["first_output_after_fault"] is not None
        assert recovery["extra_replay"] == 0
    # Each cohort request leaves the runner 90 seconds to start publishing.
    for name in ("after_checkpoint", "after_recovery"):
        cohort = control.pubsub["cohorts"][name]
        assert cohort["deadline"] - cohort["requested_at"] == 90
    measurements = world.evidence(MEASUREMENT_EVENT)
    assert measurements and all(m["payload"]["vertices"] for m in measurements)
    times = [rt.timestamp(m["at"]) for m in measurements]
    assert all(later - earlier >= 60 for earlier, later in itertools.pairwise(times))
    stages = [
        value["payload"]["stage"]
        for value in world.evidence_events()
        if value["event"].startswith("recovery-") and "stage" in value["payload"]
    ]
    assert stages == [
        "baseline",
        "checkpoint",
        "boundary",
        "recovering",
        "after",
        "complete",
    ]


def one(kind):
    return pytest.mark.parametrize("trial", [kind], indirect=True)


def stage(world):
    return (world.a.environment.refresh().recovery or {}).get("stage")


@one("tm-replacement")
def test_a_taskmanager_fault_displaces_only_the_attempt_on_its_pod(
    admitting, monkeypatch
):
    world = World(admitting, monkeypatch)
    control = world.run()
    fault = control.recovery["outcomes"]["fault"]
    assert fault["pod"].startswith("taskmanager-") and len(fault["displaced"]) == 1
    # The other TaskManager's attempt keeps running unrestored and still
    # serves its input's last cohort.
    survivor = set(fault["before"]) - set(fault["displaced"])
    assert survivor & set(control.recovery["outcomes"]["after"]["attempts"])


@pytest.mark.parametrize("trial", ["jm-replacement", "tm-replacement"], indirect=True)
def test_a_checkpoint_completing_before_the_fault_loses_the_boundary(
    admitting, monkeypatch
):
    """The run goes on; the replay population is recorded as unobserved."""
    world = World(admitting, monkeypatch, interval=15)
    control = world.run()
    assert control.recovery["stage"] == "complete", control.reason
    recovery = control.recovery["outcomes"]["recovery"]
    assert recovery["boundary_held"] is False
    assert recovery["replay"] == "unobserved"
    assert (
        recovery["restored"]["id"] > control.recovery["outcomes"]["fault"]["checkpoint"]
    )


@one("jm-replacement")
def test_a_displaced_population_that_never_returns_fails_the_trial(
    admitting, monkeypatch
):
    world = World(admitting, monkeypatch, redeliver=False)
    control = world.run()
    assert "deadline expired: recovering" in control.reason
    assert control.pubsub["stage"] == "cleaned"
    assert control.recovery["stage"] == "recovering"


@one("jm-replacement")
def test_a_checkpoint_outside_the_run_s_state_is_refused(admitting, monkeypatch):
    world = World(admitting, monkeypatch)
    path = world.relay.checkpoint_path
    monkeypatch.setattr(
        world.relay,
        "checkpoint_path",
        lambda kind="checkpoints": path(kind).replace(PUBSUB_STATE, rt.STATE),
    )
    control = world.run()
    assert "outside the approved state prefix" in control.reason
    assert control.pubsub["stage"] == "cleaned"


def test_an_unplanned_pod_replacement_is_inconclusive(admitting, monkeypatch):
    world = World(admitting, monkeypatch)
    relay, replaced = world.relay, []

    def replace():
        if stage(world) == "checkpoint" and not replaced:
            replaced.append(relay.managers()[0])
            relay.original_delete(replaced[0])
            relay.pod("taskmanager")

    relay.hooks.append(replace)
    control = world.run()
    assert replaced and "Unplanned workload replacement" in control.reason
    assert control.pubsub["stage"] == "cleaned"


@one("rescale-in")
def test_a_rescale_that_restores_another_savepoint_is_refused(admitting, monkeypatch):
    world = World(admitting, monkeypatch)
    relay = world.relay

    def elsewhere():
        if relay.restored.get("is_savepoint"):
            relay.restored["external_path"] += "-other"

    relay.hooks.append(elsewhere)
    control = world.run()
    assert "did not restore its newly completed savepoint" in control.reason


@one("jm-replacement")
def test_a_completed_exercise_leaves_the_verdict_to_the_offline_analysis(
    admitting, monkeypatch
):
    world = World(admitting, monkeypatch)
    control = world.run()
    assert control.recovery["stage"] == "complete" and control.success
    receipt = world.a.runner.receipt(
        control,
        {
            "nonce": world.a.environment.approval.nonce,
            "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
            "empty": True,
        },
    )
    assert receipt["success"] is False
    assert receipt["recovery"] == control.recovery


@one("jm-replacement")
def test_an_attempt_belongs_to_one_pod_and_this_run(admitting, monkeypatch):
    world = World(admitting, monkeypatch)
    exercise = world.a.supervisor.exercise
    pods = world.relay.managers()
    run_id = world.a.environment.approval.run_id
    line = (
        f"event=pubsub-attempt run_id={run_id} phase=initial attempt=a1 restored=false"
    )
    exercise.progress(pods[0], line)
    with pytest.raises(rt.Failure, match="two Pods"):
        exercise.progress(pods[1], line)
    with pytest.raises(rt.Failure, match="another run"):
        exercise.progress(pods[0], line.replace(run_id, "other-run"))
    with pytest.raises(rt.Failure, match="another run or phase"):
        exercise.progress(pods[0], line.replace("phase=initial", "phase=final"))


@one("jm-replacement")
def test_only_a_checkpoint_unknown_at_the_observation_is_retained(
    admitting, monkeypatch
):
    """Ids order checkpoints against an observation, with no clock involved."""
    world = World(admitting, monkeypatch)
    exercise = world.a.supervisor.exercise
    path = world.relay.checkpoint_path()
    known = {
        "latest": {"completed": {"id": 3}},
        "history": [{"id": 4, "status": "IN_PROGRESS"}, {"id": 3}],
    }
    assert exercise.known_id(known) == 4

    def rest(cp_id):
        return {
            "latest": {
                "completed": {
                    "id": cp_id,
                    "status": "COMPLETED",
                    "is_savepoint": False,
                    "external_path": path,
                }
            }
        }

    # Triggered before the observation, even if completed after it.
    assert exercise.completed_after(rest(4), 4) is None
    assert exercise.completed_after(rest(5), 4)["id"] == 5


@one("jm-replacement")
def test_a_restore_older_than_the_retained_checkpoint_is_refused(
    admitting, monkeypatch
):
    world = World(admitting, monkeypatch)
    relay = world.relay

    def older():
        if relay.restored and not relay.restored.get("is_savepoint"):
            relay.restored["id"] = 0

    relay.hooks.append(older)
    control = world.run()
    assert "did not restore a retained checkpoint" in control.reason


@pytest.mark.parametrize("trial", ["jm-replacement", "rescale-out"], indirect=True)
def test_recovery_waits_for_restored_attempts_to_announce_themselves(
    admitting, monkeypatch
):
    world = World(admitting, monkeypatch)
    relay, held = world.relay, []
    start = relay.start_attempts

    def late(indices, *, restored):
        before = {uid: len(lines) for uid, lines in relay.logs.items()}
        start(indices, restored=restored)
        if not restored:
            return
        # The restored attempts run at once, but their log lines arrive two
        # minutes late.
        for uid, lines in relay.logs.items():
            fresh = lines[before.get(uid, 0) :]
            del lines[before.get(uid, 0) :]
            held.extend((relay.clock() + 120, uid, line) for _, line in fresh)

    def release():
        for item in [item for item in held if relay.clock() >= item[0]]:
            held.remove(item)
            relay.logs[item[1]].append((relay.clock(), item[2]))

    monkeypatch.setattr(relay, "start_attempts", late)
    relay.hooks.append(release)
    announced = []
    original = relay.read_logs

    def logs(pod, since=None):
        data = original(pod, since)
        if b"restored=true" in data and not announced:
            announced.append(relay.clock())
        return data

    monkeypatch.setattr(relay.kube, "logs", logs)
    control = world.run()
    assert control.recovery["stage"] == "complete", control.reason
    after = next(
        value for value in world.evidence_events() if value["event"] == "recovery-after"
    )
    assert announced and rt.timestamp(after["at"]) >= announced[0]


@one("jm-replacement")
def test_the_trial_waits_for_a_checkpoint_after_the_last_cohort(admitting, monkeypatch):
    world = World(admitting, monkeypatch)
    relay = world.relay

    def no_more():
        if stage(world) == "after":
            relay.next_trigger = float("inf")

    relay.hooks.append(no_more)
    control = world.run()
    assert "deadline expired: after" in control.reason


@pytest.mark.parametrize("state", ["FINISHED", "CANCELED", "SUSPENDED", "FAILED"])
@one("jm-replacement")
def test_a_job_leaving_running_outside_recovery_stops_the_trial(
    admitting, monkeypatch, state
):
    world = World(admitting, monkeypatch)
    relay = world.relay

    def leave():
        app = relay.app()
        if app and stage(world) == "checkpoint":
            app["status"]["jobStatus"]["state"] = state
            relay.kube.put(app)

    relay.hooks.append(leave)
    control = world.run()
    assert "Unexpected Flink state: " + state in control.reason


@one("jm-replacement")
def test_the_jobmanager_s_absence_is_tolerated_while_recovering(admitting, monkeypatch):
    world = World(admitting, monkeypatch)
    control = world.run()
    assert control.recovery["stage"] == "complete", control.reason
    assert world.evidence("recovery-rest-unavailable")


@pytest.mark.parametrize("trial", ["jm-replacement", "tm-replacement"], indirect=True)
@pytest.mark.parametrize(
    "change,reason",
    [
        ({"is_savepoint": True}, "did not restore a retained checkpoint"),
        (
            {"external_path": f"gs://{PUBSUB_STATE}/runs/other/checkpoints/chk-1"},
            "did not restore a retained checkpoint",
        ),
    ],
)
def test_a_replacement_restoring_something_else_is_refused(
    admitting, monkeypatch, change, reason
):
    world = World(admitting, monkeypatch)
    relay = world.relay

    def alter():
        if relay.restored and not relay.restored.get("altered"):
            relay.restored.update(change, altered=True)

    relay.hooks.append(alter)
    control = world.run()
    assert reason in control.reason


@one("rescale-out")
@pytest.mark.parametrize(
    "alter,reason",
    [
        (
            lambda app: app["status"]["jobStatus"]["savepointInfo"][
                "lastSavepoint"
            ].update(triggerType="PERIODIC"),
            "did not restore its newly completed savepoint",
        ),
        (
            lambda app: app["spec"]["job"].update(parallelism=1),
            "differs from the approved recovery manifest",
        ),
        (
            lambda app: app["status"].update(observedGeneration=1),
            "deadline expired: recovering",
        ),
    ],
)
def test_a_rescale_that_did_not_deploy_the_approved_upgrade_is_refused(
    admitting, monkeypatch, alter, reason
):
    world = World(admitting, monkeypatch)
    relay = world.relay

    def change():
        app = relay.app()
        if app and app["status"].get("observedGeneration") == 2:
            alter(app)
            relay.kube.put(app)

    relay.hooks.append(change)
    control = world.run()
    assert reason in control.reason


@one("tm-replacement")
def test_a_wider_restart_than_the_fault_s_is_counted_as_extra_replay(
    admitting, monkeypatch
):
    world = World(admitting, monkeypatch, tm_full=True)
    control = world.run()
    assert control.recovery["stage"] == "complete", control.reason
    recovery = control.recovery["outcomes"]["recovery"]
    third = RECORDS // 3
    assert recovery["expected_replay"] == recovery["replayed"] == third
    assert recovery["extra_replay"] == third


@one("jm-replacement")
def test_a_replay_under_new_message_ids_is_recorded(admitting, monkeypatch):
    world = World(admitting, monkeypatch, new_ids=True)
    control = world.run()
    recovery = control.recovery["outcomes"]["recovery"]
    assert recovery["replay"] == "observed"
    assert recovery["replay_ids_preserved"] is False


@one("jm-replacement")
def test_attempts_that_restored_nothing_do_not_prove_recovery(admitting, monkeypatch):
    world = World(admitting, monkeypatch, restored_flag=False)
    control = world.run()
    assert "deadline expired: recovering" in control.reason


@one("jm-replacement")
@pytest.mark.parametrize(
    "alter,reason",
    [
        (
            lambda app: app["status"].update(
                reconciliationStatus={"state": "ROLLED_BACK"}
            ),
            "rolled back",
        ),
        (
            lambda app: app["metadata"].update(
                deletionTimestamp="2026-01-01T00:00:00Z"
            ),
            "Application deletion started",
        ),
    ],
)
def test_the_operator_undoing_or_deleting_the_application_stops_the_trial(
    admitting, monkeypatch, alter, reason
):
    world = World(admitting, monkeypatch)
    relay = world.relay

    def change():
        app = relay.app()
        if app and stage(world) == "checkpoint":
            alter(app)
            relay.kube.put(app)

    relay.hooks.append(change)
    control = world.run()
    assert reason in control.reason


@one("jm-replacement")
def test_the_mark_is_read_on_the_poll_after_the_cohort_was_observed(
    admitting, monkeypatch
):
    """The history read before a poll's pulls is no mark for what they saw."""
    world = World(admitting, monkeypatch)
    exercise = world.a.supervisor.exercise
    exercise.env.refresh()
    exercise.admitted()
    monkeypatch.setattr(exercise.collector, "complete", lambda name, where=None: True)
    rest = {
        "latest": {"completed": {"id": 3}},
        "history": [{"id": 4, "status": "IN_PROGRESS"}, {"id": 3}],
    }
    exercise.after_input({"latest": {"completed": {"id": 1}}})
    assert exercise.stage == "baseline" and exercise.marker is None
    exercise.after_input(rest)
    assert exercise.stage == "checkpoint" and exercise.marker == 4


@pytest.mark.parametrize("trial", ["jm-replacement", "tm-replacement"], indirect=True)
def test_a_restart_without_the_planned_replacement_is_not_recovery(
    admitting, monkeypatch
):
    """The deleted Pod stayed; the job restored in place, which proves nothing."""
    world = World(admitting, monkeypatch, ignore_delete=True)
    control = world.run()
    assert "deadline expired: recovering" in control.reason


@one("rescale-in")
def test_a_rescale_restoring_a_checkpoint_is_refused(admitting, monkeypatch):
    world = World(admitting, monkeypatch)
    relay = world.relay

    def checkpoint():
        if relay.restored.get("is_savepoint"):
            relay.restored["is_savepoint"] = False

    relay.hooks.append(checkpoint)
    control = world.run()
    assert "did not restore its newly completed savepoint" in control.reason
