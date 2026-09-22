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

from .metrics import AGGREGATES, JVM_METRICS, metric_name, sample, unavailable
from .policy import MIB

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
CONNECTOR_METRICS = (
    "openDestinations",
    "inFlightAppends",
    "inFlightBatches",
    "appendRetries",
    "destinationActivations",
    "activeCommitDestinations",
    "currentCommitDurationMillis",
    "recordsSkipped",
)
SINK_METRICS = frozenset((*NETWORK_METRICS, *TASK_METRICS, *CONNECTOR_METRICS))
SOURCE_METRICS = ("numRecordsOut", "numRecordsOutPerSecond", *NETWORK_METRICS)


class FlinkRest:
    """Read the running job's REST API through its own Service, by proxy.

    The Cloud Tasks observer has equivalents bound to that scenario's fixed
    namespace and to its session's meter; this one takes the namespace from
    the approval so the same endpoints can be read from a recovery exercise.
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


def vertices(rest, known=None):
    """The source and sink vertex ids, and the sink's discovered metric ids.

    A connector metric is reported as ``<sanitised operator>.<name>``, so the
    id cannot be predicted; it is discovered and then asked for by id. A task
    registers its own metrics when it deploys and the sink's operators theirs
    when they open, so one listing can hold the task names and none of the
    connector gauges: ids are unioned across calls rather than frozen on the
    first answer, which would silently drop the active-writer observation for
    the rest of the run.
    """
    plan = sample(lambda: rest.job(""))
    if unavailable(plan):
        return known if known and not unavailable(known) else plan
    listed = [v for v in (plan.get("vertices") or []) if v.get("id")]
    source = next(
        (v["id"] for v in listed if str(v.get("name", "")).startswith("Source")), None
    )
    sinks = [v["id"] for v in listed if v["id"] != source]
    if source is None or len(sinks) != 1:
        return {"unavailable": f"expected one source and one sink, found {len(listed)}"}
    sink = sinks[0]
    listing = sample(lambda: rest.job(f"/vertices/{sink}/subtasks/metrics"))
    previous = known.get("sink_metrics", []) if known and not unavailable(known) else []
    if unavailable(listing):
        return known if previous else listing
    ids = list(previous)
    for item in listing:
        if (
            isinstance(item, dict)
            and isinstance(item.get("id"), str)
            and metric_name(item["id"]) in SINK_METRICS
            and item["id"] not in ids
        ):
            ids.append(item["id"])
    return {"source": source, "sink": sink, "sink_metrics": ids}


def metrics(rest, vertex, ids):
    if not ids:
        return {"unavailable": "no metric ids selected"}
    query = f"?get={','.join(ids)}&agg={AGGREGATES}"
    return sample(lambda: rest.job(f"/vertices/{vertex}/subtasks/metrics{query}"))


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


def observation(rest, state):
    """One poll's measurements, with every absent reading named rather than dropped.

    A metric the job does not expose comes back as ``unavailable`` with its
    cause, because a missing sample and a zero reading mean opposite things
    when the question is whether the sink was the limit.
    """
    if unavailable(state):
        return {"vertices": state}
    return {
        "sink": metrics(rest, state["sink"], state["sink_metrics"]),
        "source": metrics(rest, state["source"], list(SOURCE_METRICS)),
        "taskmanagers": taskmanagers(rest),
    }
