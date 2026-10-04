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
"""Sample what the deployed BigQuery sink does, through the job's REST service."""

from ..metrics import JVM_METRICS, metric_name, sample, subtask_metrics, unavailable

# Flink's documented TaskManager memory metrics. Heap alone is the wrong
# instrument for this sink: the Storage Write path appends through native
# buffers, so what the process holds is not what the heap shows.
MEMORY_METRICS = (
    *JVM_METRICS,
    "Status.JVM.Memory.NonHeap.Used",
    "Status.JVM.Memory.NonHeap.Max",
    "Status.JVM.Memory.Direct.MemoryUsed",
    "Status.JVM.Memory.Direct.TotalCapacity",
    "Status.JVM.Memory.Mapped.MemoryUsed",
    "Status.Flink.Memory.Managed.Used",
    "Status.Flink.Memory.Managed.Total",
)
# Task-level network metrics. Nothing in this repository collected these, so
# a run could report throughput without saying whether the network was the
# thing limiting it.
NETWORK_METRICS = (
    "inPoolUsage",
    "outPoolUsage",
    "numBytesInPerSecond",
    "numBytesOutPerSecond",
)
# What the sink vertex reports about its own progress and stalls.
TASK_METRICS = (
    "numRecordsIn",
    "numRecordsInPerSecond",
    "numRecordsSend",
    "backPressuredTimeMsPerSecond",
    "busyTimeMsPerSecond",
    "idleTimeMsPerSecond",
)
# The connector's own gauges and counters (BigQueryMetricNames.java). These
# are the active-writer observation: how many destinations are open, how many
# appends are in flight, and how often a destination was evicted and re-made.
# The FILE_LOADS writer reports staged files instead of appends.
CONNECTOR_METRICS = (
    "openDestinations",
    "inFlightAppends",
    "inFlightBatches",
    "appendRetries",
    "destinationActivations",
    "activeCommitDestinations",
    "currentCommitDurationMillis",
    "recordsSkipped",
    "filesStaged",
    "pendingFiles",
    "capacityEvictions",
    "idleEvictions",
)
# What the FILE_LOADS committer reports from its own vertex: the load jobs it
# submitted and how long its commits queue and take.
COMMITTER_METRICS = (
    "loadJobsSubmitted",
    "queuedCommitDestinations",
    "activeCommitDestinations",
    "currentCommitDurationMillis",
    "lastCommitDurationMillis",
)
# Each subtask's share of the latest completed checkpoint, from Flink's
# checkpoint statistics (`SubtaskCheckpointStatistics`). A writer finalizes its
# staged files before it forwards the barrier, which the connector has no
# metric for (ADR-0146) and Flink times in neither the sync nor the async part:
# it shows in the writer's end-to-end duration and the committer's start delay.
CHECKPOINT_FIELDS = (
    "index",
    "status",
    "end_to_end_duration",
    "start_delay",
    "checkpoint",
    "alignment",
)
# The sink's metrics, partitioned into the families the run has to observe.
# One declaration serves both readers: discovery keeps an id when it belongs
# to some family, and the verdict asks which family a returned id belongs to.
SINK_FAMILIES = {
    "task": frozenset(TASK_METRICS),
    "network": frozenset(NETWORK_METRICS),
    "connector": frozenset(CONNECTOR_METRICS),
}
SINK_METRICS = frozenset(name for names in SINK_FAMILIES.values() for name in names)
SOURCE_METRICS = ("numRecordsOut", "numRecordsOutPerSecond", *NETWORK_METRICS)


def _discover(listing, wanted, previous):
    """Metric ids of `wanted` names in one listing, after those already found."""
    ids = list(previous)
    for item in listing:
        if (
            isinstance(item, dict)
            and isinstance(item.get("id"), str)
            and metric_name(item["id"]) in wanted
            and item["id"] not in ids
        ):
            ids.append(item["id"])
    return ids


def _sinks(listed, source, mode):
    """The writer vertex, and FILE_LOADS's separate committer vertex.

    The Storage Write sinks are one vertex, the EO committer chained to its
    writer. FILE_LOADS routes every committable to one committer subtask, so
    its committer is a vertex of its own, named `<sink>: Committer`.
    """
    sinks = [v for v in listed if v["id"] != source]
    if mode != "FILE_LOADS":
        return (sinks[0]["id"], None) if len(sinks) == 1 else None
    committers = [
        v
        for v in sinks
        if str(v.get("name", "")).endswith(": Committer")
        and ": Writer" not in str(v.get("name", ""))
    ]
    writers = [v for v in sinks if ": Writer" in str(v.get("name", ""))]
    if len(sinks) != 2 or len(committers) != 1 or len(writers) != 1:
        return None
    return writers[0]["id"], committers[0]["id"]


def vertices(rest, known=None, mode=None):
    """The source and sink vertex ids, and the sink's discovered metric ids.

    A connector metric is reported as ``<sanitised operator>.<name>``, so the
    id cannot be predicted; it is discovered and then asked for by id. A task
    registers its own metrics when it deploys and the sink's operators theirs
    when they open, so one listing can hold the task names and none of the
    connector gauges: ids are unioned across calls rather than frozen on the
    first answer, which would silently drop the active-writer observation for
    the rest of the run. A FILE_LOADS run also names its committer vertex and
    that vertex's metric ids.
    """
    plan = sample(lambda: rest.job(""))
    if unavailable(plan):
        return known if known and not unavailable(known) else plan
    listed = [v for v in (plan.get("vertices") or []) if v.get("id")]
    source = next(
        (v["id"] for v in listed if str(v.get("name", "")).startswith("Source")), None
    )
    found = _sinks(listed, source, mode) if source is not None else None
    if found is None:
        shape = "a writer and a committer" if mode == "FILE_LOADS" else "one sink"
        return {"unavailable": f"expected one source and {shape}, found {len(listed)}"}
    sink, committer = found
    listing = sample(lambda: rest.job(f"/vertices/{sink}/subtasks/metrics"))
    previous = known if known and not unavailable(known) else {}
    if unavailable(listing):
        return known if previous.get("sink_metrics") else listing
    state = {
        "source": source,
        "sink": sink,
        "sink_metrics": _discover(
            listing, SINK_METRICS, previous.get("sink_metrics", [])
        ),
    }
    if committer is not None:
        # The committer's ids are discovered the same way; until they are,
        # its reading is the named absence `metrics` returns for no ids.
        ids = previous.get("committer_metrics", [])
        listing = sample(lambda: rest.job(f"/vertices/{committer}/subtasks/metrics"))
        state["committer"] = committer
        state["committer_metrics"] = (
            ids
            if unavailable(listing)
            else _discover(listing, frozenset(COMMITTER_METRICS), ids)
        )
    return state


def taskmanagers(rest):
    listing = sample(lambda: rest.root("/taskmanagers"))
    if unavailable(listing):
        return listing
    query = "?get=" + ",".join(MEMORY_METRICS)
    result = []
    for manager in listing.get("taskmanagers") or []:
        path = f"/taskmanagers/{manager.get('id')}/metrics{query}"
        result.append(
            {
                "id": manager.get("id"),
                "metrics": sample(lambda path=path: rest.root(path)),
            }
        )
    return result


def checkpoint_durations(rest, state):
    """Each sink subtask's share of the latest completed checkpoint."""
    summary = sample(lambda: rest.job("/checkpoints"))
    if unavailable(summary):
        return summary
    latest = (summary.get("latest") or {}).get("completed") or {}
    checkpoint = latest.get("id")
    if type(checkpoint) is not int:
        return {"unavailable": "no completed checkpoint"}
    result = {"id": checkpoint, "vertices": {}}
    for role in ("sink", "committer"):
        detail = sample(
            lambda vertex=state[role]: rest.job(
                f"/checkpoints/details/{checkpoint}/subtasks/{vertex}"
            )
        )
        result["vertices"][role] = (
            detail
            if unavailable(detail)
            else [
                {field: item[field] for field in CHECKPOINT_FIELDS if field in item}
                for item in detail.get("subtasks") or []
                if isinstance(item, dict)
            ]
        )
    return result


def observation(rest, state):
    """One poll's measurements, with every absent reading named rather than dropped.

    A metric the job does not expose comes back as ``unavailable`` with its
    cause, because a missing sample and a zero reading mean opposite things
    when the question is whether the sink was the limit.
    """
    if unavailable(state):
        return {"vertices": state}
    reading = {
        "sink": subtask_metrics(rest, state["sink"], state["sink_metrics"]),
        "source": subtask_metrics(rest, state["source"], list(SOURCE_METRICS)),
        "taskmanagers": taskmanagers(rest),
    }
    if "committer" in state:
        reading["committer"] = subtask_metrics(
            rest, state["committer"], state["committer_metrics"]
        )
        reading["checkpoints"] = checkpoint_durations(rest, state)
    return reading
