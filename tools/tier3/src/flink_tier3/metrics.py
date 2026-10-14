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
"""Flink metric primitives and the observation coverage built from them, shared
by the scenarios that sample them."""

import json

from .common import ApiError, ReadCeilingExceeded, TransportError
from .policy import MIB

AGGREGATES = "min,max,sum"
# The evidence event carrying each poll's checkpoint statistics of the job:
# the supervisor writes it, and an offline analyzer reads it.
CHECKPOINTS_EVENT = "checkpoints"
# The query service reports an operator metric as "<sanitised operator>.<name>",
# so the name is the suffix after the last dot of a discovered metric id.
JVM_METRICS = (
    "Status.JVM.Memory.Heap.Used",
    "Status.JVM.Memory.Heap.Max",
    "Status.JVM.GarbageCollector.All.Time",
    "Status.JVM.GarbageCollector.All.Count",
    "Status.JVM.CPU.Load",
)


# Flink's documented TaskManager memory metrics. Heap alone is the wrong
# instrument for a connector whose client buffers natively, as the BigQuery
# Storage Write path appends through native buffers, so what the process holds
# is not what the heap shows.
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
# Task-level network metrics, so a run can say whether the network was the
# thing limiting its throughput.
NETWORK_METRICS = (
    "inPoolUsage",
    "outPoolUsage",
    "numBytesInPerSecond",
    "numBytesOutPerSecond",
)
# What a sink vertex reports about its own progress and stalls.
TASK_METRICS = (
    "numRecordsIn",
    "numRecordsInPerSecond",
    "numRecordsSend",
    "backPressuredTimeMsPerSecond",
    "busyTimeMsPerSecond",
    "idleTimeMsPerSecond",
)
# A source vertex's output, beside its network metrics.
SOURCE_METRICS = ("numRecordsOut", "numRecordsOutPerSecond", *NETWORK_METRICS)
# Each subtask's share of a completed checkpoint, from Flink's checkpoint
# statistics (`SubtaskCheckpointStatistics`).
CHECKPOINT_FIELDS = (
    "index",
    "status",
    "end_to_end_duration",
    "start_delay",
    "checkpoint",
    "alignment",
)


def metric_name(metric_id):
    return metric_id.rsplit(".", 1)[-1]


def discover(listing, wanted, previous):
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


def returned(reading, key, wanted, name=metric_name):
    """Which of the wanted metric names one endpoint's answer actually carried.

    Credit the response rather than the request. Discovery lists a metric id
    once and never drops it, so asking for a gauge whose operator no longer
    publishes it still succeeds, with that gauge simply absent from the
    answer — and a family counted from the listing would be reported as
    observed when only its name was ever obtained.

    A vertex metric's id carries its operator name and is stripped back to the
    metric; a TaskManager's is the documented name itself, so it is compared
    whole.
    """
    value = reading.get(key)
    if not isinstance(value, list):
        return frozenset()
    return frozenset(
        name(item["id"])
        for item in value
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    ) & frozenset(wanted)


def sample(read):
    """Read one metric endpoint, naming an absent reading rather than raising.

    An answer too large to read, or one that is not UTF-8 JSON a parser can
    hold, is as absent as one the service refused: none of them is a reading,
    and none says anything about the run.
    """
    try:
        return read()
    except (
        ApiError,
        TransportError,
        ReadCeilingExceeded,
        json.JSONDecodeError,
        UnicodeDecodeError,
        RecursionError,
    ) as error:
        return {"unavailable": str(error)}


def subtask_metrics(rest, vertex, ids):
    """The named metrics of one vertex, aggregated over its subtasks."""
    if not ids:
        return {"unavailable": "no metric ids selected"}
    query = f"?get={','.join(ids)}&agg={AGGREGATES}"
    return sample(lambda: rest.job(f"/vertices/{vertex}/subtasks/metrics{query}"))


def memory_read(reading):
    """Whether some TaskManager's answer carried one of its memory metrics."""
    managers = reading.get("taskmanagers")
    return isinstance(managers, list) and any(
        returned(manager, "metrics", MEMORY_METRICS, name=str)
        for manager in managers
        if isinstance(manager, dict)
    )


def checkpoint_read(reading, roles):
    """Whether every named vertex returned a subtask's checkpoint durations."""
    checkpoints = reading.get("checkpoints")
    vertices = checkpoints.get("vertices") if isinstance(checkpoints, dict) else None
    if not isinstance(vertices, dict):
        return False
    for role in roles:
        subtasks = vertices.get(role)
        if not isinstance(subtasks, list) or not any(
            isinstance(item, dict)
            and isinstance(item.get("checkpoint"), dict)
            and type(item.get("end_to_end_duration")) is int
            and all(
                type(item["checkpoint"].get(key)) is int for key in ("sync", "async")
            )
            for item in subtasks
        ):
            return False
    return True


def taskmanagers(rest):
    """Every TaskManager's memory metrics, each reading or its named absence."""
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


def checkpoint_statistics(rest, roles):
    """Each named vertex's subtasks' shares of the latest completed checkpoint.

    `roles` maps the name a reading files a vertex under to its id.
    """
    summary = sample(lambda: rest.job("/checkpoints"))
    if unavailable(summary):
        return summary
    latest = (summary.get("latest") or {}).get("completed") or {}
    checkpoint = latest.get("id")
    if type(checkpoint) is not int:
        return {"unavailable": "no completed checkpoint"}
    result = {"id": checkpoint, "vertices": {}}
    for role, vertex in roles.items():
        detail = sample(
            lambda vertex=vertex: rest.job(
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


def fold_coverage(previous, window, observed):
    """Fold one sample into its window's coverage, keeping what it ever read.

    Coverage is monotonic within a window and never crosses one. A sample with
    no window counts nowhere.
    """
    coverage = {
        name: dict(entry) for name, entry in previous.items() if isinstance(entry, dict)
    }
    if window is None:
        return coverage
    entry = coverage.setdefault(window, {"attempts": 0, "observed": []})
    entry["attempts"] += 1
    entry["observed"] = sorted(set(entry["observed"]) | set(observed))
    return coverage


def coverage_reasons(coverage, windows, families):
    """Why recorded coverage falls short of every family in every window.

    The record is untyped JSON: a window that took no sample observed nothing,
    whatever it claims, and `True` is not a sample count.
    """
    coverage = coverage if isinstance(coverage, dict) else {}
    reasons = []
    for name in windows:
        entry = coverage.get(name)
        entry = entry if isinstance(entry, dict) else {}
        attempts = entry.get("attempts")
        observed = entry.get("observed")
        observed = observed if isinstance(observed, list) else []
        if type(attempts) is not int or attempts < 1:
            reasons.append("unsampled-" + name)
            observed = []
        reasons.extend(
            f"unobserved-{family}-in-{name}"
            for family in families
            if family not in observed
        )
    return reasons


def unavailable(value):
    return isinstance(value, dict) and "unavailable" in value


def subset(value, fields):
    value = value or {}
    return {field: value[field] for field in fields if field in value}


class FlinkRest:
    """Read the running job's REST API through its own Service, by proxy.

    The namespace comes from the approval, so the same endpoints can be read
    from every recovery exercise.
    """

    def __init__(self, env, service, job_id):
        self.env, self.service, self.job_id = env, service, job_id

    def _read(self, suffix, limit):
        return self.env.kube.request(
            "GET",
            self.env.kube.path(
                "Service",
                self.env.approval.application_namespace,
                self.service["metadata"]["name"] + ":8081",
            )
            + "/proxy"
            + suffix,
            limit=limit,
        )

    def job(self, path, limit=MIB):
        return self._read(f"/jobs/{self.job_id}{path}", limit)

    def root(self, path, limit=MIB):
        return self._read(path, limit)
