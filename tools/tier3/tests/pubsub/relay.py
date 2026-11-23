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
"""The Pub/Sub relay, its Operator and its service, simulated for the sequence tests."""

import base64
import json
import uuid

from flink_tier3.policy import PUBSUB, PUBSUB_STATE

from ..test_lifecycle import obj, rt
from .test_admission import supervisor_uid
from .test_messages import Response

INTERVAL = 120


def encode(value):
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


class Relay:
    """The application, the Operator and the Pub/Sub service, simulated.

    Each input index is processed by one attempt; output is published at
    once. A checkpoint is taken every `INTERVAL` seconds and acknowledges
    what it covers. A fault redelivers what the attempts it ends processed
    since the checkpoint the job restores: a JobManager loss restarts every
    attempt, a TaskManager loss only the attempt on that Pod, and a savepoint
    upgrade acknowledges everything before the job stops.
    """

    def __init__(self, a, monkeypatch, *, pace=None):
        self.a, self.kube, self.clock = a, a.environment.kube, a.environment.clock
        self.approval = a.environment.approval
        self.run_id = self.approval.run_id
        # How many waiting inputs each subtask processes a step, as the
        # relay's delay per record paces them; all of them without one.
        self.pace = pace
        # How many inputs each attempt processed, which its progress lines say.
        self.processed = {}
        # How many more pulls the service refuses with a 429, and how many
        # acknowledgements it answers with a 503.
        self.pull_failures = 0
        self.ack_failures = 0
        # Each attempt's restored checkpoint, and the latest checkpoint
        # barrier the job has passed.
        self.origins = {}
        self.epoch = 0
        # Each attempt's subtask and attempt number, and each subtask's last.
        self.numbers = {}
        self.subtask_attempts = {}
        # Output messages the collector has been given, for redelivering one.
        self.delivered = []
        # A checkpoint triggered and not yet completed, with what it covers.
        self.inflight = None
        # Deleted Pods still terminating, and when they go.
        self.terminating = []
        self.jm_down = False
        self.parallelism = 2
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
        # The restores the job's current JobMaster has counted.
        self.restores = 0
        self.restart_at = None
        self.restart = None
        self.started = None
        self.next_trigger = None
        self.events = []
        # Called at each step, after the simulated job has advanced.
        self.hooks = []
        # The checkpoint statistics answer these statuses, one read each.
        self.checkpoint_failures = []
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
            if self.ack_failures:
                self.ack_failures -= 1
                return Response({}, status=503)
            return Response({})
        # The supervisor pulls only the output subscription: its access probe
        # at admission, while nothing is relayed, and then its collection.
        if not url.endswith(":pull"):
            return original(method, url, **kwargs)
        # The service may hold a pull that does not return immediately until
        # messages arrive, past the transport timeout.
        assert kwargs["json"].get("returnImmediately") is True
        if self.pull_failures:
            self.pull_failures -= 1
            return Response({}, status=429)
        size = min(kwargs["json"]["maxMessages"], len(self.output))
        batch, self.output = self.output[:size], self.output[size:]
        self.delivered.extend(batch)
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
            self.origins[attempt] = self.restored.get("id", 0) if restored else 0
            # Flink numbers each subtask's attempts upwards across restarts.
            self.subtask_attempts[index] = self.subtask_attempts.get(index, -1) + 1
            self.numbers[attempt] = (str(index), str(self.subtask_attempts[index]))
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
            # Flink reports epoch milliseconds as a JSON integer.
            "trigger_timestamp": int(self.clock() * 1000),
            "external_path": self.checkpoint_path(),
        }
        self.next_id += 1
        self.history.append(cp)
        self.epoch = cp["id"]
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
            attempt = self.attempts.get(index if self.parallelism >= 2 else 0)
            origin = self.origins.get(attempt, 0)
            epoch = max(origin, self.epoch)
            taken = self.pending[index][: self.pace]
            for message in taken:
                fields = message["payload"].split("|")
                data = "|".join(
                    [
                        *fields,
                        encode(message["id"]),
                        attempt,
                        str(uuid.uuid4()),
                        self.phase,
                        "true" if attempt in self.restored_attempts else "false",
                        str(origin),
                        str(epoch),
                        *self.numbers[attempt],
                    ]
                )
                self.output.append({"messageId": uuid.uuid4().hex, "data": data})
                self.unacked.append((message, attempt))
            self.pending[index] = self.pending[index][len(taken) :]
            if taken:
                self.progress(attempt, origin, len(taken))

    def progress(self, attempt, origin, count):
        """The relay's progress line for ``attempt``, on the Pod that runs it."""
        self.processed[attempt] = self.processed.get(attempt, 0) + count
        pod = self.attempt_pod(attempt)
        if pod is None:
            return
        restored = "true" if attempt in self.restored_attempts else "false"
        line = (
            f"{rt.utc(self.clock())} INFO event=pubsub-progress "
            f"run_id={self.run_id} phase={self.phase} attempt={attempt} "
            f"restored={restored} origin={origin} "
            f"processed={self.processed[attempt]}\n"
        )
        self.logs.setdefault(pod, []).append((self.clock(), line))

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
            self.next_trigger = now + INTERVAL
            self.start_attempts(range(self.parallelism), restored=False)
            return
        if self.restart_at is not None:
            if now < self.restart_at:
                return
            # A restart may schedule the next one.
            restart, self.restart_at, self.restart = self.restart, None, None
            restart()
            return
        if state != "RUNNING":
            return
        self.process()
        if self.inflight:
            self.complete_checkpoint()
        if now >= self.next_trigger:
            self.trigger_checkpoint()
            self.next_trigger += INTERVAL

    # Faults.

    def requeue(self, attempts):
        kept = []
        for message, attempt in self.unacked:
            if attempt in attempts:
                self.pending[message["index"]].append(message)
            else:
                kept.append((message, attempt))
        self.unacked = kept

    def restore(self, *, savepoint=None, kept=False):
        # An in-progress checkpoint is abandoned by the failover.
        self.inflight = None
        # A new JobMaster counts its restores from one; one that `kept` the
        # job adds this restore to its count.
        self.restores = self.restores + 1 if kept else 1
        cp = savepoint or self.completed
        self.epoch = cp["id"]
        self.restored = {
            "id": cp["id"],
            "is_savepoint": savepoint is not None,
            # Flink reports epoch milliseconds as a JSON integer.
            "restore_timestamp": int(self.clock() * 1000),
            "external_path": cp["external_path"],
        }

    def delete(self, value):
        component = value["metadata"].get("labels", {}).get("component")
        if value["kind"] == "Pod" and component in ("jobmanager", "taskmanager"):
            self.events.append(("delete", component, value["metadata"]["name"]))
            current = self.kube.get("Pod", PUBSUB, value["metadata"]["name"])
            # The Pod terminates until a step after the restart, as a
            # graceful shutdown outlasting the replacement would.
            current["metadata"]["deletionTimestamp"] = rt.utc(self.clock())
            self.kube.put(current)
            self.terminating.append((current, self.clock() + 45))
            if component == "jobmanager":
                # The Operator has not noticed yet, and the REST API is gone
                # with the JobManager.
                self.jm_down = True
                lost = set(self.attempts.values())

                def restart():
                    self.jm_down = False
                    self.pod("jobmanager")
                    self.requeue(lost)
                    self.restore()
                    self.start_attempts(range(self.parallelism), restored=True)

            else:
                index = next(
                    i
                    for i, attempt in self.attempts.items()
                    if self.attempt_pod(attempt) == value["metadata"]["uid"]
                )
                lost = {self.attempts[index]}

                def restart():
                    replacement = self.pod("taskmanager")
                    self.requeue(lost)
                    self.restore(kept=True)
                    # Only the replaced attempt restarts, on the new Pod.
                    self.start_attempts([index], restored=True, managers=[replacement])

            self.restart_at, self.restart = self.clock() + 30, restart
            return True
        return self.original_delete(value)

    def attempt_pod(self, attempt):
        for uid, lines in self.logs.items():
            if any(attempt in line for _, line in lines):
                return uid
        return None

    def patch(self, value, changes, subresource="", before_repeat=None):
        if value["kind"] != "FlinkDeployment":
            return self.original_patch(
                value, changes, subresource, before_repeat=before_repeat
            )
        self.events.append(("patch", [c["path"] for c in changes]))
        app = self.app()
        for change in changes:
            target = app
            *parents, leaf = change["path"].strip("/").split("/")
            for key in parents:
                target = target[key]
            target[leaf] = change["value"]
        generation = app["metadata"]["generation"] + 1
        app["metadata"].update(generation=generation, resourceVersion=str(generation))
        # Stop with a savepoint: it completes and acknowledges what it covers.
        savepoint = {
            "id": self.next_id,
            "external_path": self.checkpoint_path("savepoints"),
        }
        self.next_id += 1
        lost = set(self.attempts.values())
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
                observedGeneration=generation,
                reconciliationStatus={"state": "DEPLOYED"},
            )
            current["status"]["jobStatus"].update(
                state="RUNNING",
                jobId=str(generation % 10) * 32,
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
            self.requeue(lost)
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
        if suffix.endswith("/checkpoints") and self.checkpoint_failures:
            raise rt.ApiError(self.checkpoint_failures.pop(0), method, path)
        if suffix.endswith("/checkpoints"):
            return {
                "counts": {"completed": len(self.history), "restored": self.restores},
                "latest": {"completed": self.completed, "restored": self.restored},
                "history": list(reversed(self.history[-10:])),
            }
        # Nothing else of the job is read.
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


def receipt(world, control):
    return world.a.runner.receipt(control)
