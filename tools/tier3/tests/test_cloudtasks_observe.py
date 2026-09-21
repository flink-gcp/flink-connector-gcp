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
"""Per-poll cell observations and the K11 interrupt control."""

import copy
import urllib.parse

import pytest
from flink_tier3 import cloudtasks as ct
from flink_tier3 import observe
from flink_tier3.common import TransportError
from flink_tier3.observe import CellObserver
from flink_tier3.supervisor import CellSession
from test_cloudtasks_session import (
    CELL_A,
    CELL_B,
    CellWorld,
    evidence_events,
    runner_for,
    session_approval,
    session_environment,
)
from test_tier3_lifecycle import env as env  # noqa: PLC0414 - re-export pytest fixture
from test_tier3_lifecycle import obj, rt, supervisor_pod

JOB_ID = "1" * 32
SOURCE, SINK = "a" * 32, "b" * 32
WRITER = "Measured_Cloud_Tasks_STAGED_HASH__Writer."
COMMITTER = "Measured_Cloud_Tasks_STAGED_HASH__Committer."
STAGED_IDS = [
    WRITER + "stagedBytes",
    WRITER + "stagedReplayBudgetMillis",
    WRITER + "stagedTasks",
    WRITER + "inFlightTasks",
    COMMITTER + "pendingCommittables",
    COMMITTER + "tasksDeduplicated",
    "numRecordsIn",
    "numRecordsInPerSecond",
    "numRecordsSend",
    "backPressuredTimeMsPerSecond",
    "busyTimeMsPerSecond",
    # Present on the vertex but not selected.
    "numBytesIn",
    WRITER + "numRecordsSendErrors",
    "Source__Measured_input.numRecordsOut",
]
K11 = {**CELL_B, "id": "example-wiring-k11"}


class FlinkRest:
    """Canned Flink REST answers for the paths the observer reads."""

    def __init__(self, metric_ids=STAGED_IDS):
        self.calls, self.paths = [], []
        self.faults = []
        self.state = "RUNNING"
        self.timestamps = {"CREATED": 900, "RUNNING": 1000, "RESTARTING": 0}
        self.now = 1_800_000_000_000
        self.history = []
        self.taskmanagers = ["tm-1"]
        self.metric_ids = list(metric_ids)
        # Listings answered in order before the vertex reports metric_ids: a
        # real one answers nothing, then task names, then its operators'.
        self.listings = []
        self.vertices = [
            {"id": SOURCE, "name": "Source: Measured input", "parallelism": 1},
            {
                "id": SINK,
                "name": "Measured Cloud Tasks STAGED_HASH: Writer -> "
                "Measured Cloud Tasks STAGED_HASH: Committer",
                "parallelism": 4,
            },
        ]

    def checkpoint(self, checkpoint_id, status="COMPLETED"):
        return {
            "id": checkpoint_id,
            "status": status,
            "is_savepoint": False,
            "trigger_timestamp": checkpoint_id * 1000,
            "latest_ack_timestamp": checkpoint_id * 1000 + 40,
            "end_to_end_duration": 40,
            "checkpointed_size": 512,
            "state_size": 4096,
            "external_path": "gs://bucket/chk-" + str(checkpoint_id),
        }

    def request(self, method, path, body=None, **kwargs):
        assert method == "GET" and body is None and kwargs.get("limit")
        _, _, sub = path.partition("/proxy")
        self.paths.append(path)
        route, _, query = sub.partition("?")
        self.calls.append(route)
        for needle, error in list(self.faults):
            if needle in sub:
                self.faults.remove((needle, error))
                raise error
        query = urllib.parse.parse_qs(query)
        jobs = f"/jobs/{JOB_ID}"
        if route == jobs:
            return {
                "jid": JOB_ID,
                "state": self.state,
                "timestamps": dict(self.timestamps),
                "now": self.now,
                "vertices": copy.deepcopy(self.vertices),
            }
        if route == jobs + "/checkpoints":
            history = [self.checkpoint(i, s) for i, s in self.history]
            completed = [c for c in history if c["status"] == "COMPLETED"]
            return {
                "counts": {
                    "in_progress": len(history) - len(completed),
                    "completed": len(completed),
                    "failed": 0,
                    "restored": 0,
                },
                "latest": {
                    "completed": completed[-1] if completed else None,
                    "restored": None,
                    "savepoint": None,
                    "failed": None,
                },
                "history": history,
            }
        if route.startswith(jobs + "/checkpoints/details/"):
            checkpoint_id = int(route.rsplit("/", 1)[-1])
            task = {
                "checkpointed_size": 256,
                "state_size": 2048,
                "end_to_end_duration": 30,
                "latest_ack_timestamp": checkpoint_id * 1000 + 30,
                "num_acknowledged_subtasks": 4,
                "num_subtasks": 4,
                "summary": {"large": "ignored"},
            }
            return {
                **self.checkpoint(checkpoint_id),
                "tasks": {SOURCE: dict(task), SINK: dict(task), "c" * 32: dict(task)},
            }
        if route.startswith(jobs + "/vertices/") and route.endswith(
            "/subtasks/metrics"
        ):
            if "get" not in query:
                ids = self.listings.pop(0) if self.listings else self.metric_ids
                return [{"id": i} for i in ids]
            return [
                {"id": i, "min": 1.0, "max": 2.0, "sum": 3.0}
                for i in query["get"][0].split(",")
            ]
        if route == jobs + "/exceptions":
            assert query == {"maxExceptions": ["5"]}
            return {
                "rootException": "stack text",
                "exceptionHistory": {
                    "entries": [
                        {
                            "timestamp": 5000,
                            "exceptionName": "java.lang.RuntimeException",
                            "taskName": "Measured Cloud Tasks STAGED_HASH: Writer",
                            "stacktrace": "java.lang.RuntimeException: boom\n\tat ...",
                        }
                    ],
                    "truncated": False,
                },
            }
        if route == "/taskmanagers":
            return {
                "taskmanagers": [{"id": i, "slotsNumber": 4} for i in self.taskmanagers]
            }
        if route.startswith("/taskmanagers/") and route.endswith("/metrics"):
            return [{"id": i, "value": "1"} for i in query["get"][0].split(",")]
        raise AssertionError(sub)


class FakeKube:
    def __init__(self, rest):
        self.rest = rest
        self.pods = {}
        self.deletes = []
        self.delete_fault = None
        self.before_delete = None

    def path(self, kind, namespace, name):
        return f"/{namespace}/{kind}/{name}"

    def request(self, method, path, body=None, **kwargs):
        return self.rest.request(method, path, body, **kwargs)

    def get(self, kind, namespace, name):
        assert kind == "Pod" and namespace == rt.CLOUDTASKS
        return copy.deepcopy(self.pods.get(name))

    def delete(self, pod, force=False):
        if self.before_delete:
            self.before_delete()
        self.deletes.append((pod["metadata"]["uid"], force))
        if self.delete_fault:
            mode, error = self.delete_fault
            self.delete_fault = None
            if mode == "gone":
                self.pods.pop(pod["metadata"]["name"])
            elif mode == "replaced":
                # The name survives on a Pod the API server created afterwards.
                replacement = copy.deepcopy(self.pods[pod["metadata"]["name"]])
                replacement["metadata"]["uid"] = "replacement-uid"
                self.pods[pod["metadata"]["name"]] = replacement
            raise error
        return self.pods.pop(pod["metadata"]["name"], None) is not None


class FakeEnv:
    def __init__(self, kube, now=1000.0):
        self.kube, self.now = kube, now
        self.events = []
        self.evidence_failed = False
        self.fail_evidence = False

    def clock(self):
        return self.now

    def emit(self, event, payload):
        if self.fail_evidence:
            self.evidence_failed = True
            return
        self.events.append((event, copy.deepcopy(payload)))

    def of(self, event):
        return [payload for name, payload in self.events if name == event]


class FakeSession:
    """The seams CellSession exposes to its poll hook, with the real rest()."""

    rest = CellSession.rest

    def __init__(self, cell, rest=None):
        self.flink = rest or FlinkRest()
        self.env = FakeEnv(FakeKube(self.flink))
        self.meter = rt.Meter()
        self.job_id = JOB_ID
        self.cell = cell
        self.last_items = []
        self.last_service = {"metadata": {"name": cell["id"] + "-rest"}}
        self.last_queue = {
            "name": "q",
            "state": "PAUSED",
            "stats": {"tasksCount": "0", "executedLastMinuteCount": "0"},
        }
        self.pods = [pod(cell, "jobmanager"), pod(cell, "taskmanager")]
        for value in self.pods:
            self.env.kube.pods[value["metadata"]["name"]] = value

    def poll(self, observer, at=None):
        if at is not None:
            self.env.now = at
        observer.poll(self, self.cell, {}, copy.deepcopy(self.pods))
        return self.env.of("observation")[-1] if self.env.of("observation") else None


def pod(cell, component):
    value = obj("Pod", f"{cell['id']}-{component}", rt.CLOUDTASKS, cell["id"] + "-uid")
    value["metadata"]["labels"] = {"app": cell["id"], "component": component}
    value["spec"] = {
        "nodeName": "node-1",
        "containers": [
            {
                "name": "flink-main-container",
                "resources": {
                    "requests": {"cpu": "2", "memory": "8Gi"},
                    "limits": {"cpu": "2", "memory": "8Gi"},
                },
            }
        ],
    }
    value["status"] = {
        "phase": "Running",
        "containerStatuses": [{"restartCount": 1}, {"restartCount": 2}],
        "conditions": [{"type": "Ready", "status": "True"}],
    }
    return value


# --- discovery -------------------------------------------------------------------


def test_discovery_selects_sanitised_ids_and_flags_missing_staged_gauges():
    session, observer = FakeSession(CELL_B), CellObserver()
    drive(session, observer)
    [discovered] = session.env.of("cell-metrics-discovered")
    assert discovered["source"] == SOURCE and discovered["sink"] == SINK
    assert discovered["metrics"] == sorted(
        [
            WRITER + "stagedBytes",
            WRITER + "stagedReplayBudgetMillis",
            WRITER + "stagedTasks",
            WRITER + "inFlightTasks",
            COMMITTER + "pendingCommittables",
            COMMITTER + "tasksDeduplicated",
            "numRecordsIn",
            "numRecordsInPerSecond",
            "numRecordsSend",
            "backPressuredTimeMsPerSecond",
            "busyTimeMsPerSecond",
        ]
    )
    assert session.env.of("metrics-unavailable") == []
    # One listing per warm-up poll, and a values read on each of them because
    # this vertex answered its whole set on the first.
    assert len(listings(session.flink)) == len(WARMUP_POLLS)
    assert len(values(session.flink)) == len(WARMUP_POLLS)
    # Discovery is recorded once per cell, however long the cell runs.
    session.poll(observer, WARM_END + 15)
    assert len(session.env.of("cell-metrics-discovered")) == 1

    lacking = FakeSession(
        CELL_B, FlinkRest([i for i in STAGED_IDS if not i.endswith("stagedBytes")])
    )
    drive(lacking, CellObserver())
    assert lacking.env.of("metrics-unavailable") == [
        {"cell": CELL_B["id"], "missing": ["stagedBytes"]}
    ]
    unnamed = FakeSession(CELL_A, FlinkRest(["numRecordsIn", "busyTimeMsPerSecond"]))
    drive(unnamed, CellObserver())
    assert unnamed.env.of("metrics-unavailable") == []
    assert unnamed.env.of("cell-metrics-discovered")[0]["metrics"] == [
        "busyTimeMsPerSecond",
        "numRecordsIn",
    ]


def listings(rest):
    """The sink reads that ask what exists, not the ones that ask for values."""
    return [p for p in rest.paths if p.endswith(f"/vertices/{SINK}/subtasks/metrics")]


def values(rest):
    """The sink reads that ask for values. A negative index into ``paths``
    lands on the source read instead, and shifts again when the sink defers."""
    return [p for p in rest.paths if f"/vertices/{SINK}/subtasks/metrics?" in p]


def selected(ids):
    """``ids`` in the order the observer asks for them."""
    return sorted(
        i for i in ids if observe.metric_name(i) in observe.SELECTED_SINK_METRICS
    )


WARM_END = 1000.0 + CELL_B["warmup_seconds"]
# Warm-up divided by the supervisor's poll interval, plus the poll that ends it.
WARMUP_POLLS = [1000.0 + 15.0 * i for i in range(CELL_B["warmup_seconds"] // 15 + 1)]


def drive(session, observer, first=0, until=WARM_END):
    """Poll a cell from ``WARMUP_POLLS[first]`` to ``until``, inclusive."""
    for at in WARMUP_POLLS[first:]:
        if at > until:
            break
        observation = session.poll(observer, at)
    return observation


def test_sink_metric_ids_are_the_union_of_every_listing_taken_in_warmup():
    # A vertex answers nothing while its tasks come up, then its task-level
    # names, then its operators' -- and only the union holds all of them.
    session, observer = FakeSession(CELL_B), CellObserver()
    task_only = ["numRecordsIn", "busyTimeMsPerSecond"]
    gauge = WRITER + "stagedBytes"
    session.flink.listings = [[], task_only, [gauge]]
    first = session.poll(observer, WARMUP_POLLS[0])
    # Nothing selected yet, so the poll asks the vertex for no value at all.
    assert first["sink"] == {"unavailable": "no metric ids selected"}
    # Only the sink waits; the source ids are a constant, not a discovery.
    assert [m["id"] for m in first["source"]] == list(observe.SOURCE_METRICS)
    second = session.poll(observer, WARMUP_POLLS[1])
    # The union so far is sampled immediately; it does not wait for warm-up.
    assert [m["id"] for m in second["sink"]] == task_only[::-1]
    for at in WARMUP_POLLS[2:-1]:
        session.poll(observer, at)
    assert session.env.of("cell-metrics-discovered") == []
    observation = session.poll(observer, WARM_END)
    [discovered] = session.env.of("cell-metrics-discovered")
    assert discovered["source"] == SOURCE and discovered["sink"] == SINK
    # The listings run out, so the last ones add every remaining selected id
    # and the union is exactly what the vertex ever offered that is selected.
    assert discovered["metrics"] == selected(STAGED_IDS)
    assert set(task_only + [gauge]) <= set(discovered["metrics"])
    assert discovered["reads"] == len(WARMUP_POLLS) == len(listings(session.flink))
    assert [m["id"] for m in observation["sink"]] == discovered["metrics"]
    assert session.env.of("metrics-unavailable") == []
    # Settled ids freeze, so the window's samples ask one unchanging set.
    session.poll(observer, WARM_END + 15)
    assert len(listings(session.flink)) == len(WARMUP_POLLS)
    assert len(session.env.of("cell-metrics-discovered")) == 1


def test_a_listing_carrying_only_task_names_does_not_settle_the_staged_gauges():
    # The defect this guards: settling on the first non-empty listing would
    # freeze the task names and lose the gauges the staged arm exists to read.
    session, observer = FakeSession(CELL_B), CellObserver()
    session.flink.listings = [["numRecordsIn"]] * (len(WARMUP_POLLS) - 1)
    drive(session, observer, until=WARMUP_POLLS[-2])
    assert session.env.of("cell-metrics-discovered") == []
    session.poll(observer, WARM_END)
    [discovered] = session.env.of("cell-metrics-discovered")
    present = {observe.metric_name(i) for i in discovered["metrics"]}
    assert set(observe.STAGED_REQUIRED) <= present
    assert session.env.of("metrics-unavailable") == []


def test_sink_metric_ids_settle_empty_when_warmup_ends_without_them():
    session, observer = FakeSession(CELL_B), CellObserver()
    # A vertex that answers ids none of which are selected is not a cell that
    # is still coming up; the retry is spent and the cell settles with nothing.
    session.flink.metric_ids = ["numBytesIn", "Source__Measured_input.numRecordsOut"]
    drive(session, observer, until=WARMUP_POLLS[-2])
    assert session.env.of("cell-metrics-discovered") == []
    observation = session.poll(observer, WARM_END)
    [discovered] = session.env.of("cell-metrics-discovered")
    assert discovered["metrics"] == [] and discovered["reads"] == len(WARMUP_POLLS)
    assert observation["sink"] == {"unavailable": "no metric ids selected"}
    assert session.env.of("metrics-unavailable") == [
        {"cell": CELL_B["id"], "missing": ["stagedBytes", "stagedReplayBudgetMillis"]}
    ]
    # Settled is settled: the cell reports the absence once and stops asking.
    session.poll(observer, WARM_END + 15)
    assert len(listings(session.flink)) == len(WARMUP_POLLS)
    assert len(session.env.of("cell-metrics-discovered")) == 1


def test_a_failed_listing_read_is_not_a_read_and_does_not_hold_up_settling():
    session, observer = FakeSession(CELL_B), CellObserver()
    # Faults fire once, in request order, and the listing is the poll's first
    # read of this vertex; the read count below is what pins it to the listing.
    session.flink.faults = [
        (f"/vertices/{SINK}/subtasks/metrics", TransportError("reset"))
    ]
    observation = session.poll(observer, WARMUP_POLLS[0])
    assert session.env.of("cell-metrics-discovered") == []
    assert observation["sink"] == {"unavailable": "no metric ids selected"}
    observation = drive(session, observer, first=1)
    [discovered] = session.env.of("cell-metrics-discovered")
    assert discovered["reads"] == len(WARMUP_POLLS) - 1
    assert isinstance(observation["sink"], list)


def test_a_cell_that_ends_inside_warmup_still_records_the_ids_it_had():
    session, observer = FakeSession(CELL_B), CellObserver()
    session.poll(observer, WARMUP_POLLS[0])
    assert session.env.of("cell-metrics-discovered") == []
    observer.after_cell(session, CELL_B, "completed")
    [discovered] = session.env.of("cell-metrics-discovered")
    assert discovered["reads"] == 1 and discovered["metrics"]
    # A cell that never became reachable resolved nothing and records nothing.
    unseen, fresh = FakeSession(CELL_A), CellObserver()
    fresh.after_cell(unseen, CELL_A, "completed")
    assert unseen.env.of("cell-metrics-discovered") == []


def test_a_cell_whose_job_never_answers_resolves_nothing_and_records_nothing():
    # Polled to the end of warm-up and beyond, but the job read fails every
    # time, so no vertex is ever resolved. A receipt naming none would say
    # the cell discovered something; the honest record is no receipt at all.
    session, observer = FakeSession(CELL_B), CellObserver()
    for at in [*WARMUP_POLLS, WARM_END + 15]:
        session.flink.faults = [(f"/jobs/{JOB_ID}", rt.ApiError(503, "GET", "job"))]
        observation = session.poll(observer, at)
        assert observation["sink"] == {"unavailable": "job graph not yet discovered"}
    observer.after_cell(session, CELL_B, "completed")
    assert session.env.of("cell-metrics-discovered") == []
    assert session.env.of("metrics-unavailable") == []


@pytest.mark.parametrize(
    "vertices",
    [
        [{"id": SOURCE, "name": "Source: x", "parallelism": 1}],
        [
            {"id": SOURCE, "name": "Source: x", "parallelism": 4},
            {"id": SINK, "name": "Sink", "parallelism": 4},
        ],
        [
            {"id": SOURCE, "name": "Source: x", "parallelism": 1},
            {"id": SINK, "name": "Sink", "parallelism": 4},
            {"id": "c" * 32, "name": "Map", "parallelism": 4},
        ],
    ],
)
def test_unexpected_job_graph_fails_the_cell(vertices):
    session = FakeSession(CELL_B)
    session.flink.vertices = vertices
    with pytest.raises(rt.Failure, match="Unexpected job graph"):
        session.poll(CellObserver())
    assert session.env.of("observation") == []


# --- one observation per poll ------------------------------------------------------


def test_each_poll_emits_exactly_one_observation_with_the_jobmanager_offset():
    session = FakeSession(CELL_B)
    observer = CellObserver()
    for at in (1000.0, 1015.0, 1030.0):
        session.flink.now += 15_000
        observation = session.poll(observer, at)
        assert observation["at"] == at and observation["cell"] == CELL_B["id"]
        assert observation["job"] == {
            "state": "RUNNING",
            "timestamps": session.flink.timestamps,
            "now": session.flink.now,
        }
        assert observation["jm_offset_seconds"] == session.flink.now / 1000 - at
        assert "vertices" not in observation["job"]
    assert len(session.env.of("observation")) == 3
    assert observation["restarted"] is False and "exceptions" not in observation
    assert observation["sink"] == [
        {"id": i, "min": 1.0, "max": 2.0, "sum": 3.0} for i in selected(STAGED_IDS)
    ]
    assert [m["id"] for m in observation["source"]] == list(observe.SOURCE_METRICS)
    assert "agg=min,max,sum" in values(session.flink)[-1]
    assert observation["pods"][1] == {
        "name": CELL_B["id"] + "-taskmanager",
        "namespace": rt.CLOUDTASKS,
        "uid": CELL_B["id"] + "-taskmanager-uid",
        "phase": "Running",
        "restarts": 3,
        "node": "node-1",
        "deletion_timestamp": None,
        "resources": {
            "flink-main-container": {
                "requests": {"cpu": "2", "memory": "8Gi"},
                "limits": {"cpu": "2", "memory": "8Gi"},
            }
        },
        "disruption": None,
    }
    session.pods[1]["status"]["conditions"].append(
        {
            "type": "DisruptionTarget",
            "status": "True",
            "reason": "PreemptionByScheduler",
        }
    )
    session.pods[1]["metadata"]["deletionTimestamp"] = "2026-09-15T00:01:00Z"
    observation = session.poll(observer, 1045.0)
    assert observation["pods"][1]["disruption"]["reason"] == "PreemptionByScheduler"
    assert observation["pods"][1]["deletion_timestamp"] == "2026-09-15T00:01:00Z"


def test_no_observation_before_the_job_runs():
    session = FakeSession(CELL_B)
    session.last_service = None
    assert session.poll(CellObserver()) is None
    assert session.env.events == [] and session.flink.calls == []
    assert session.meter.snapshot()["read_ops"] == 0


# --- checkpoints ---------------------------------------------------------------------


def test_checkpoint_details_are_bounded_per_poll_and_skipped_ids_are_recorded():
    session = FakeSession(CELL_B)
    observer = CellObserver(read_details_per_poll=2)
    session.flink.history = [(i, "COMPLETED") for i in range(1, 6)] + [
        (6, "IN_PROGRESS")
    ]
    observation = session.poll(observer)
    assert observation["checkpoints"]["counts"] == {
        "in_progress": 1,
        "completed": 5,
        "failed": 0,
        "restored": 0,
    }
    assert observation["checkpoints"]["completed"] == {
        "id": 5,
        "trigger_timestamp": 5000,
        "latest_ack_timestamp": 5040,
        "end_to_end_duration": 40,
        "checkpointed_size": 512,
        "state_size": 4096,
    }
    assert observation["checkpoints"]["restored"] == {}
    assert observation["checkpoints"]["history"] == [1, 2, 3, 4, 5, 6]
    assert [d["id"] for d in observation["details"]] == [4, 5]
    assert observation["details_skipped"] == [1, 2, 3]
    assert observation["details"][1] == {
        "id": 5,
        "status": "COMPLETED",
        "trigger_timestamp": 5000,
        "latest_ack_timestamp": 5040,
        "end_to_end_duration": 40,
        "checkpointed_size": 512,
        "state_size": 4096,
        "tasks": {
            vertex: {
                "checkpointed_size": 256,
                "state_size": 2048,
                "end_to_end_duration": 30,
                "latest_ack_timestamp": 5030,
                "num_acknowledged_subtasks": 4,
            }
            for vertex in (SOURCE, SINK)
        },
    }
    details = [c for c in session.flink.calls if "/checkpoints/details/" in c]
    assert len(details) == 2
    # The in-progress checkpoint completes and one more arrives.
    session.flink.history[-1] = (6, "COMPLETED")
    session.flink.history.append((7, "COMPLETED"))
    observation = session.poll(observer, 1015.0)
    assert [d["id"] for d in observation["details"]] == [6, 7]
    assert observation["details_skipped"] == []
    observation = session.poll(observer, 1030.0)
    assert observation["details"] == [] and observation["details_skipped"] == []
    details = [c for c in session.flink.calls if "/checkpoints/details/" in c]
    assert len(details) == 4


# --- TaskManagers, restarts, queue, metering --------------------------------------------


def test_taskmanager_heap_and_gc_are_captured_through_the_root_path():
    session = FakeSession(CELL_B)
    session.flink.taskmanagers = ["tm-1", "tm-2"]
    observation = session.poll(CellObserver())
    assert [tm["id"] for tm in observation["taskmanagers"]] == ["tm-1", "tm-2"]
    assert [m["id"] for m in observation["taskmanagers"][0]["metrics"]] == [
        "Status.JVM.Memory.Heap.Used",
        "Status.JVM.Memory.Heap.Max",
        "Status.JVM.GarbageCollector.All.Time",
        "Status.JVM.GarbageCollector.All.Count",
        "Status.JVM.CPU.Load",
    ]
    service = f"/{rt.CLOUDTASKS}/Service/{CELL_B['id']}-rest:8081/proxy"
    assert service + "/taskmanagers" in session.flink.paths
    assert not any("/jobs/" in p and "/taskmanagers" in p for p in session.flink.paths)


def test_restarting_change_triggers_an_exceptions_read_without_stack_text():
    session = FakeSession(CELL_B)
    observer = CellObserver()
    assert session.poll(observer)["restarted"] is False
    session.flink.timestamps["RESTARTING"] = 5000
    observation = session.poll(observer, 1015.0)
    assert observation["restarted"] is True
    assert observation["exceptions"] == [
        {
            "timestamp": 5000,
            "exceptionName": "java.lang.RuntimeException",
            "taskName": "Measured Cloud Tasks STAGED_HASH: Writer",
        }
    ]
    assert session.flink.calls.count(f"/jobs/{JOB_ID}/exceptions") == 1
    observation = session.poll(observer, 1030.0)
    assert observation["restarted"] is False and "exceptions" not in observation
    assert session.flink.calls.count(f"/jobs/{JOB_ID}/exceptions") == 1


def test_queue_sample_comes_from_the_session_without_a_new_read():
    session = FakeSession(CELL_B)
    assert not hasattr(session, "queues") and not hasattr(session.env, "queues")
    observation = session.poll(CellObserver())
    assert observation["queue"] == {
        "state": "PAUSED",
        "stats": {"tasksCount": "0", "executedLastMinuteCount": "0"},
    }
    session.last_queue = None
    observation = session.poll(CellObserver(), 1015.0)
    assert observation["queue"] == {"unavailable": "queue readback failed this poll"}


def test_every_rest_read_is_metered_including_taskmanagers():
    session = FakeSession(CELL_B)
    session.flink.history = [(1, "COMPLETED")]
    session.flink.taskmanagers = ["tm-1", "tm-2"]
    session.poll(CellObserver())
    calls = session.flink.calls
    assert "/taskmanagers" in calls and "/taskmanagers/tm-2/metrics" in calls
    # job, listing, checkpoints, one detail, sink, source, TM list, two TMs.
    assert len(calls) == 9
    assert session.meter.snapshot() == {"read_ops": 9, "admin_write_ops": 0}
    session.flink.timestamps["RESTARTING"] = 1
    session.poll(CellObserver(), 1015.0)
    assert session.meter.snapshot()["read_ops"] == len(session.flink.calls) == 19


# --- fault tolerance ---------------------------------------------------------------------


def test_one_failed_read_marks_its_sample_unavailable_and_the_poll_still_emits():
    session = FakeSession(CELL_B)
    observer = CellObserver()
    session.flink.faults = [
        ("/taskmanagers", rt.ApiError(503, "GET", "taskmanagers")),
        (f"/vertices/{SINK}/subtasks/metrics?get", TransportError("reset")),
    ]
    observation = session.poll(observer)
    assert observation["taskmanagers"] == {"unavailable": "GET taskmanagers: HTTP 503"}
    assert observation["sink"] == {"unavailable": "reset"}
    assert isinstance(observation["source"], list)
    assert len(session.env.of("observation")) == 1
    # A failed job read defers discovery and the offset; the next poll recovers.
    fresh, observer = FakeSession(CELL_B), CellObserver()
    fresh.flink.faults = [(f"/jobs/{JOB_ID}", rt.ApiError(503, "GET", "job"))]
    observation = fresh.poll(observer)
    assert observation["job"] == {"unavailable": "GET job: HTTP 503"}
    assert observation["jm_offset_seconds"] is None
    assert observation["restarted"] is False
    assert observation["sink"] == {"unavailable": "job graph not yet discovered"}
    assert fresh.env.of("cell-metrics-discovered") == []
    observation = fresh.poll(observer, 1015.0)
    assert observation["job"]["state"] == "RUNNING"
    assert isinstance(observation["sink"], list)
    assert len(fresh.env.of("cell-metrics-discovered")) == 0
    observation = drive(fresh, observer, first=2)
    assert len(fresh.env.of("cell-metrics-discovered")) == 1
    # A per-TM read failure is scoped to that TaskManager.
    session.flink.taskmanagers = ["tm-1", "tm-2"]
    session.flink.faults = [("/taskmanagers/tm-1/", rt.ApiError(500, "GET", "tm"))]
    observation = session.poll(observer, 1030.0)
    assert observation["taskmanagers"][0]["metrics"] == {
        "unavailable": "GET tm: HTTP 500"
    }
    assert isinstance(observation["taskmanagers"][1]["metrics"], list)


def test_ownership_failures_from_the_session_propagate():
    session = FakeSession(CELL_B)

    def unverified(_service, _path, limit=rt.MIB):
        raise rt.Failure("REST service has no verified workload owner")

    session.rest = unverified
    with pytest.raises(rt.Failure, match="verified workload owner"):
        session.poll(CellObserver())
    assert session.env.of("observation") == []


# --- K11 interrupt control -----------------------------------------------------------------


def test_k11_persists_the_intent_before_one_forced_taskmanager_delete():
    session = FakeSession(K11)
    observer = CellObserver()
    kube = session.env.kube
    kube.before_delete = lambda: (
        # The intent is durable before the API call leaves.
        session.env.of("interrupt-intent") or pytest.fail("delete before intent")
    )
    start = 1000.0
    due = start + K11["warmup_seconds"] + observe.INTERRUPT_DELAY_SECONDS
    session.poll(observer, start)
    session.poll(observer, due - 1)
    assert kube.deletes == [] and session.env.of("interrupt-intent") == []
    session.poll(observer, due)
    tm_uid = K11["id"] + "-taskmanager-uid"
    assert kube.deletes == [(tm_uid, True)]
    [intent] = session.env.of("interrupt-intent")
    assert intent == {
        "cell": K11["id"],
        "pod": {"name": K11["id"] + "-taskmanager", "uid": tm_uid},
        "at": due,
        "cell_start": start,
    }
    [issued] = session.env.of("interrupt-issued")
    assert issued == {
        "cell": K11["id"],
        "pod_uid": tm_uid,
        "pod": K11["id"] + "-taskmanager",
        "at": due,
        "deleted": True,
        "response": "ok",
    }
    names = [name for name, _ in session.env.events]
    assert names.index("observation", 2) < names.index("interrupt-intent")
    assert names.index("interrupt-intent") < names.index("interrupt-issued")
    # Exactly once: the replacement TaskManager is left alone.
    session.pods[1] = pod(K11, "taskmanager")
    session.pods[1]["metadata"]["uid"] = "replacement-uid"
    session.poll(observer, due + 15)
    session.poll(observer, due + 30)
    assert kube.deletes == [(tm_uid, True)]
    assert len(session.env.of("interrupt-intent")) == 1
    # Ordinary cells never issue the control.
    plain = FakeSession(CELL_B)
    plain.poll(observer, start)
    plain.poll(observer, due + 300)
    assert plain.env.kube.deletes == [] and plain.env.of("interrupt-intent") == []


def test_k11_kind_field_also_names_the_control():
    session = FakeSession({**CELL_B, "kind": "interrupt-control"})
    observer = CellObserver()
    session.poll(observer, 1000.0)
    session.poll(observer, 1000.0 + CELL_B["warmup_seconds"] + 60)
    assert len(session.env.kube.deletes) == 1


@pytest.mark.parametrize(
    "error", [TransportError("lost"), rt.ApiError(503, "DELETE", "pod")]
)
def test_k11_lost_delete_response_is_accepted_only_when_the_pod_is_gone(error):
    due = 1000.0 + K11["warmup_seconds"] + 60
    gone = FakeSession(K11)
    gone.env.kube.delete_fault = ("gone", error)
    observer = CellObserver()
    gone.poll(observer, 1000.0)
    gone.poll(observer, due)
    [issued] = gone.env.of("interrupt-issued")
    assert issued["deleted"] is True and issued["response"] == "lost"
    assert len(gone.env.kube.deletes) == 1

    remains, observer = FakeSession(K11), CellObserver()
    remains.env.kube.delete_fault = ("remains", error)
    remains.poll(observer, 1000.0)
    with pytest.raises(rt.Failure, match="lost without a verified outcome"):
        remains.poll(observer, due)
    assert len(remains.env.of("interrupt-intent")) == 1
    assert remains.env.of("interrupt-issued") == []
    # The intent is durable; a retry would be a second, unplanned interrupt.
    remains.poll(observer, due + 15)
    assert len(remains.env.kube.deletes) == 1


def test_k11_does_not_delete_when_the_intent_did_not_persist():
    session = FakeSession(K11)
    observer = CellObserver()
    session.poll(observer, 1000.0)
    session.env.fail_evidence = True
    with pytest.raises(rt.Failure, match="not persisted"):
        session.poll(observer, 1000.0 + K11["warmup_seconds"] + 60)
    assert session.env.kube.deletes == []


def test_k11_waits_for_a_live_taskmanager():
    session = FakeSession(K11)
    observer = CellObserver()
    due = 1000.0 + K11["warmup_seconds"] + 60
    session.poll(observer, 1000.0)
    session.pods[1]["metadata"]["deletionTimestamp"] = "2026-09-15T00:00:00Z"
    session.poll(observer, due)
    assert session.env.kube.deletes == [] and session.env.of("interrupt-intent") == []
    del session.pods[1]["metadata"]["deletionTimestamp"]
    session.poll(observer, due + 15)
    assert len(session.env.kube.deletes) == 1


# --- inside a real session -------------------------------------------------------------------


def test_observer_runs_inside_a_real_session_and_the_session_succeeds(env, monkeypatch):
    manifests = session_approval(env)
    world = CellWorld(env, manifests, monkeypatch, polls_to_finish=3)
    flink = FlinkRest()
    flink.history = [(1, "COMPLETED"), (2, "COMPLETED"), (3, "COMPLETED")]
    world_request = world.request

    def request(method, path, body=None, **kwargs):
        if "/proxy/" in path:
            return flink.request(method, path, body, **kwargs)
        return world_request(method, path, body, **kwargs)

    monkeypatch.setattr(env[0], "request", request)
    environment = session_environment(env)
    supervisor = rt.Supervisor(environment, cells=manifests, hooks=CellObserver())
    job = env[0].put(obj("Job", "supervisor", rt.SYSTEM))
    environment.remember("supervisor", job)
    pod_ = supervisor_pod(env, job)
    runner = runner_for(env, environment.queues.actor())
    runner.cleanup.scale_operator(1)
    runner.cleanup.quota(rt.CLOUDTASKS, "session")
    runner.env.ledger.admit(runner.env.approval.cell_ids)
    ct.admit_queue(runner.env)
    runner.env.records.operations("runner", runner.env.queues.meter.snapshot())
    runner.env.records.set_phase(rt.Phase.READY)
    runner.env.records.set_phase(rt.Phase.RUNNING)

    supervisor.supervise(pod_["metadata"]["uid"])
    control = supervisor.env.refresh()
    assert control.phase == rt.Phase.CLEANED and control.success
    assert world.deployments_seen == [CELL_A["id"], CELL_B["id"]]
    events = evidence_events(env)
    assert events.count("cell-metrics-discovered") == 2
    assert events.count("observation") >= 4  # at least two polls per cell
    observations = [
        d["payload"]
        for (b, n), (d, _g) in env[1].data.items()
        if b == rt.EVIDENCE
        and n.startswith("runs/test-1310/supervisor/")
        and d["event"] == "observation"
    ]
    assert {o["cell"] for o in observations} == {CELL_A["id"], CELL_B["id"]}
    first = min(observations, key=lambda o: o["at"])
    assert first["queue"]["state"] == "PAUSED"
    assert first["job"]["state"] == "RUNNING" and first["jm_offset_seconds"]
    assert first["details_skipped"] == [1] and [d["id"] for d in first["details"]] == [
        2,
        3,
    ]
    assert {p["name"] for p in first["pods"]} >= {
        CELL_A["id"] + "-jobmanager",
        CELL_A["id"] + "-taskmanager",
    }
    assert first["taskmanagers"][0]["id"] == "tm-1"
    # Flink reads and queue reads share the supervisor's meter.
    assert supervisor.env.queues.meter.snapshot()["read_ops"] > len(flink.calls)
    assert "interrupt-intent" not in events


def test_the_observation_carries_the_queue_sample_of_its_own_poll():
    """The session re-reads the queue before the hook runs, not after it."""
    session = FakeSession(CELL_B)
    session.last_queue = {"state": "PAUSED", "stats": {"executedLastMinuteCount": "0"}}
    observer = CellObserver()
    first = session.poll(observer)
    assert first["queue"]["stats"]["executedLastMinuteCount"] == "0"
    session.last_queue = {"state": "PAUSED", "stats": {"executedLastMinuteCount": "7"}}
    second = session.poll(observer, 1015.0)
    assert second["queue"]["stats"]["executedLastMinuteCount"] == "7"


def test_a_replacement_pod_under_the_same_name_is_not_the_deleted_one():
    """The delete carries a UID precondition, so a new UID proves it landed."""
    due = 1000.0 + K11["warmup_seconds"] + 60
    session, observer = FakeSession(K11), CellObserver()
    session.env.kube.delete_fault = ("replaced", rt.ApiError(503, "DELETE", "pod"))
    session.poll(observer, 1000.0)
    session.poll(observer, due)
    [issued] = session.env.of("interrupt-issued")
    assert issued["response"] == "lost" and issued["deleted"] is True
    assert len(session.env.kube.deletes) == 1
    # The Pod that now carries the name is a different object.
    live = session.env.kube.get("Pod", rt.CLOUDTASKS, K11["id"] + "-taskmanager")
    assert live["metadata"]["uid"] != issued["pod_uid"]
