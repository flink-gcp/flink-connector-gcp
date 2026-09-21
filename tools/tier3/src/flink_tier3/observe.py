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
"""Per-poll Flink REST, queue and Pod observations of a Cloud Tasks cell."""

from __future__ import annotations

from .common import ApiError, Failure, TransportError
from .policy import CLOUDTASKS, MIB
from .supervisor import SessionHooks

# The connector's own gauges and counters (CloudTasksMetricNames.java). The
# sink vertex chains the writer and committer operators, and Flink's query
# service exposes an operator metric as "<sanitised operator name>.<name>",
# so the name is the suffix after the last dot of a discovered metric id.
CONNECTOR_METRICS = frozenset(
    {
        "inFlightTasks",
        "parkedTasks",
        "tasksDeduplicated",
        "recordsSkipped",
        "stagedTasks",
        "stagedBytes",
        "oldestStagedTaskAgeMillis",
        "stagedReplayBudgetMillis",
        "currentCommitOldestTaskAgeMillis",
        "currentCommitReplayBudgetMillis",
        "expiredEnvelopesFailed",
        "expiredEnvelopesAssumedCommitted",
        "expiredEnvelopesDropped",
        "expiredEnvelopeCreatesAuthorized",
    }
)
# Task-level names on the sink vertex, identical on both supported Flink lines.
SINK_TASK_METRICS = frozenset(
    {
        "pendingCommittables",
        "numRecordsIn",
        "numRecordsInPerSecond",
        "numRecordsSend",
        "backPressuredTimeMsPerSecond",
        "busyTimeMsPerSecond",
    }
)
SELECTED_SINK_METRICS = CONNECTOR_METRICS | SINK_TASK_METRICS
SOURCE_METRICS = (
    "numRecordsOut",
    "numRecordsOutPerSecond",
    "backPressuredTimeMsPerSecond",
)
JVM_METRICS = (
    "Status.JVM.Memory.Heap.Used",
    "Status.JVM.Memory.Heap.Max",
    "Status.JVM.GarbageCollector.All.Time",
    "Status.JVM.GarbageCollector.All.Count",
    "Status.JVM.CPU.Load",
)
# A staged arm without these two gauges cannot answer the staging questions.
STAGED_REQUIRED = ("stagedBytes", "stagedReplayBudgetMillis")
AGGREGATES = "min,max,sum"
COMPLETED_FIELDS = (
    "id",
    "trigger_timestamp",
    "latest_ack_timestamp",
    "end_to_end_duration",
    "checkpointed_size",
    "state_size",
)
RESTORED_FIELDS = ("id", "restore_timestamp", "external_path")
DETAIL_FIELDS = ("id", "status", *COMPLETED_FIELDS[1:])
TASK_FIELDS = (
    "checkpointed_size",
    "state_size",
    "end_to_end_duration",
    "latest_ack_timestamp",
    "num_acknowledged_subtasks",
)
EXCEPTION_FIELDS = ("timestamp", "exceptionName", "taskName")
JOB_FIELDS = ("state", "timestamps", "now")
# The K11 interrupt control: one forced TaskManager deletion, one minute into
# the observation window. Approval cells carry no kind yet, so the ID suffix
# names the control until they do.
INTERRUPT_KIND = "interrupt-control"
INTERRUPT_SUFFIX = "-k11"
INTERRUPT_DELAY_SECONDS = 60


def rest_root(session, service, path, limit=MIB):
    """Read one Flink REST path outside ``/jobs`` through the cell's Service."""
    session.meter.tick("read_ops")
    return session.env.kube.request(
        "GET",
        session.env.kube.path(
            "Service", CLOUDTASKS, service["metadata"]["name"] + ":8081"
        )
        + "/proxy"
        + path,
        limit=limit,
    )


def unavailable(sample):
    return isinstance(sample, dict) and "unavailable" in sample


def subset(value, fields):
    value = value or {}
    return {k: value[k] for k in fields if k in value}


def metric_name(metric_id):
    return metric_id.rsplit(".", 1)[-1]


def job_vertices(job):
    """The one source and the one sink of a measurement job's graph."""
    vertices = job.get("vertices") or []
    sources = [
        v
        for v in vertices
        if v.get("parallelism") == 1 and str(v.get("name", "")).startswith("Source:")
    ]
    sinks = [v for v in vertices if v not in sources]
    if len(sources) != 1 or len(sinks) != 1:
        raise Failure("Unexpected job graph")
    return {"source": sources[0]["id"], "sink": sinks[0]["id"]}


def is_interrupt_control(cell):
    return cell.get("kind") == INTERRUPT_KIND or cell["id"].endswith(INTERRUPT_SUFFIX)


class CellObserver(SessionHooks):
    """Emit one ``observation`` per poll and run the K11 interrupt control.

    Every read goes through the session meter. A single failed read marks its
    sample ``unavailable`` and the poll still emits; ownership failures raised
    by the session propagate unchanged.
    """

    def __init__(self, read_details_per_poll=2):
        self.read_details_per_poll = read_details_per_poll
        self.cells = {}

    def state(self, cell):
        return self.cells.setdefault(
            cell["id"],
            {
                "start": None,
                "source": None,
                "sink": None,
                "sink_seen": set(),
                "sink_reads": 0,
                "sink_settled": False,
                "seen": set(),
                "restarting": 0,
                "interrupted": False,
            },
        )

    @staticmethod
    def sample(read):
        try:
            return read()
        except (ApiError, TransportError) as error:
            return {"unavailable": str(error)}

    def poll(self, session, cell, app, pods):
        service = session.last_service
        if service is None:
            # The job is not RUNNING yet; nothing is reachable over REST.
            return
        state = self.state(cell)
        at = session.env.clock()
        if state["start"] is None:
            state["start"] = at
        job = self.sample(lambda: session.rest(service, ""))
        if not state["sink_settled"] and not unavailable(job):
            self.discover(session, service, cell, state, job, at)
        observation = {"at": at, "cell": cell["id"], "job_id": session.job_id}
        if unavailable(job):
            observation.update(job=job, jm_offset_seconds=None, restarted=False)
        else:
            observation["job"] = subset(job, JOB_FIELDS)
            now = job.get("now")
            observation["jm_offset_seconds"] = (
                now / 1000 - at if isinstance(now, int | float) else None
            )
            restarting = (job.get("timestamps") or {}).get("RESTARTING") or 0
            observation["restarted"] = restarting != state["restarting"]
            state["restarting"] = restarting
        if observation["restarted"]:
            observation["exceptions"] = self.exceptions(session, service)
        observation.update(self.checkpoints(session, service, state))
        if state["sink"] is None:
            pending = {"unavailable": "job graph not yet discovered"}
            observation.update(sink=pending, source=pending)
        else:
            observation["sink"] = self.metrics(
                session, service, state["sink"], sorted(state["sink_seen"])
            )
            observation["source"] = self.metrics(
                session, service, state["source"], SOURCE_METRICS
            )
        observation["taskmanagers"] = self.taskmanagers(session, service)
        queue = session.last_queue
        observation["queue"] = (
            {"state": queue.get("state"), "stats": queue.get("stats")}
            if queue
            else {"unavailable": "queue readback failed this poll"}
        )
        observation["pods"] = [self.pod(pod) for pod in pods]
        session.env.emit("observation", observation)
        if (
            is_interrupt_control(cell)
            and not state["interrupted"]
            and at >= state["start"] + cell["warmup_seconds"] + INTERRUPT_DELAY_SECONDS
        ):
            self.interrupt(session, cell, state, pods, at)

    def discover(self, session, service, cell, state, job, at):
        """Resolve the sink vertex once, then its metric ids over warm-up.

        The ids arrive in pieces: a task registers its metrics when it deploys
        and the sink's operators theirs when they open, so one listing can hold
        the task names, none of the connector gauges, or nothing at all. Every
        listing adds to what a poll samples, and the end of warm-up settles it.
        The set only grows, so no poll loses an id an earlier one had.
        """
        if state["sink"] is None:
            state.update(**job_vertices(job))
        listing = self.sample(
            lambda: session.rest(service, f"/vertices/{state['sink']}/subtasks/metrics")
        )
        if not unavailable(listing):
            state["sink_reads"] += 1
            state["sink_seen"].update(
                m["id"]
                for m in listing
                if isinstance(m, dict)
                and isinstance(m.get("id"), str)
                and metric_name(m["id"]) in SELECTED_SINK_METRICS
            )
        if at >= state["start"] + cell["warmup_seconds"]:
            self.settle(session, cell, state)

    def after_cell(self, session, cell, outcome):
        """A cell that resolved its graph records what it had; one that
        never reached its job resolved nothing."""
        state = self.cells.get(cell["id"])
        if state and state["sink"] is not None and not state["sink_settled"]:
            self.settle(session, cell, state)

    def settle(self, session, cell, state):
        """Freeze the ids the cell samples and record what it resolved."""
        ids = sorted(state["sink_seen"])
        session.env.emit(
            "cell-metrics-discovered",
            {
                "cell": cell["id"],
                "job_id": session.job_id,
                "source": state["source"],
                "sink": state["sink"],
                "metrics": ids,
                "reads": state["sink_reads"],
            },
        )
        if cell.get("arm", "").startswith("STAGED_"):
            present = {metric_name(i) for i in ids}
            missing = [name for name in STAGED_REQUIRED if name not in present]
            if missing:
                session.env.emit(
                    "metrics-unavailable", {"cell": cell["id"], "missing": missing}
                )
        # Last for the order of events, not for a retry: ``emit`` never
        # raises, and the supervisor fails the run on the next poll.
        state["sink_settled"] = True

    def checkpoints(self, session, service, state):
        summary = self.sample(lambda: session.rest(service, "/checkpoints"))
        if unavailable(summary):
            return {"checkpoints": summary, "details": [], "details_skipped": []}
        latest = summary.get("latest") or {}
        history = summary.get("history") or []
        completed = {
            entry["id"]
            for entry in history
            if entry.get("status") == "COMPLETED" and isinstance(entry.get("id"), int)
        }
        latest_id = (latest.get("completed") or {}).get("id")
        if isinstance(latest_id, int):
            completed.add(latest_id)
        new = sorted(completed - state["seen"])
        state["seen"].update(new)
        # The newest completions describe the current state; older ones that
        # arrived in the same poll are recorded as skipped, not queued.
        chosen = new[len(new) - self.read_details_per_poll :] if new else []
        skipped = new[: len(new) - len(chosen)]
        return {
            "checkpoints": {
                "counts": summary.get("counts", {}),
                "completed": subset(latest.get("completed"), COMPLETED_FIELDS),
                "restored": subset(latest.get("restored"), RESTORED_FIELDS),
                "history": [entry.get("id") for entry in history],
            },
            "details": [self.detail(session, service, state, i) for i in chosen],
            "details_skipped": skipped,
        }

    def detail(self, session, service, state, checkpoint_id):
        detail = self.sample(
            lambda: session.rest(service, f"/checkpoints/details/{checkpoint_id}")
        )
        if unavailable(detail):
            return {"id": checkpoint_id, **detail}
        tasks = detail.get("tasks") or {}
        return {
            **subset(detail, DETAIL_FIELDS),
            "tasks": {
                vertex: subset(tasks[vertex], TASK_FIELDS)
                for vertex in (state["source"], state["sink"])
                if vertex in tasks
            },
        }

    def metrics(self, session, service, vertex, ids):
        if not ids:
            return {"unavailable": "no metric ids selected"}
        query = f"?get={','.join(ids)}&agg={AGGREGATES}"
        return self.sample(
            lambda: session.rest(service, f"/vertices/{vertex}/subtasks/metrics{query}")
        )

    def taskmanagers(self, session, service):
        listing = self.sample(lambda: rest_root(session, service, "/taskmanagers"))
        if unavailable(listing):
            return listing
        query = "?get=" + ",".join(JVM_METRICS)
        result = []
        for manager in listing.get("taskmanagers") or []:
            path = f"/taskmanagers/{manager.get('id')}/metrics{query}"
            result.append(
                {
                    "id": manager.get("id"),
                    "metrics": self.sample(
                        lambda path=path: rest_root(session, service, path)
                    ),
                }
            )
        return result

    def exceptions(self, session, service):
        result = self.sample(
            lambda: session.rest(service, "/exceptions?maxExceptions=5")
        )
        if unavailable(result):
            return result
        entries = (result.get("exceptionHistory") or {}).get("entries") or []
        return [subset(entry, EXCEPTION_FIELDS) for entry in entries]

    @staticmethod
    def pod(pod):
        meta, spec = pod["metadata"], pod.get("spec", {})
        status = pod.get("status", {})
        disruption = next(
            (
                c
                for c in status.get("conditions", [])
                if c.get("type") == "DisruptionTarget"
            ),
            None,
        )
        return {
            "name": meta["name"],
            "namespace": meta.get("namespace"),
            "uid": meta["uid"],
            "phase": status.get("phase"),
            "restarts": sum(
                c.get("restartCount", 0) for c in status.get("containerStatuses", [])
            ),
            "node": spec.get("nodeName"),
            "deletion_timestamp": meta.get("deletionTimestamp"),
            "resources": {
                c.get("name"): c.get("resources", {})
                for c in spec.get("containers", [])
            },
            "disruption": disruption,
        }

    @staticmethod
    def taskmanager(cell, pods):
        for pod in pods:
            labels = pod["metadata"].get("labels", {})
            if (
                labels.get("component") == "taskmanager"
                and labels.get("app") == cell["id"]
                and not pod["metadata"].get("deletionTimestamp")
            ):
                return pod
        return None

    def interrupt(self, session, cell, state, pods, at):
        pod = self.taskmanager(cell, pods)
        if pod is None:
            # No live TaskManager this poll; the control waits for the next one.
            return
        env = session.env
        meta = pod["metadata"]
        env.emit(
            "interrupt-intent",
            {
                "cell": cell["id"],
                "pod": {"name": meta["name"], "uid": meta["uid"]},
                "at": at,
                "cell_start": state["start"],
            },
        )
        if env.evidence_failed:
            raise Failure("Interrupt intent was not persisted; no TaskManager deleted")
        # Once the intent is durable the control counts as issued, whatever the
        # delete response says: a repeat would be a second, unplanned interrupt.
        state["interrupted"] = True
        response = "ok"
        try:
            deleted = env.kube.delete(pod, force=True)
        except (ApiError, TransportError) as error:
            current = env.kube.get("Pod", meta["namespace"], meta["name"])
            if (
                current
                and current["metadata"]["uid"] == meta["uid"]
                and not current["metadata"].get("deletionTimestamp")
            ):
                raise Failure(
                    "TaskManager delete response lost without a verified outcome"
                ) from error
            deleted, response = True, "lost"
        env.emit(
            "interrupt-issued",
            {
                "cell": cell["id"],
                "pod_uid": meta["uid"],
                "pod": meta["name"],
                "at": env.clock(),
                "deleted": deleted,
                "response": response,
            },
        )
