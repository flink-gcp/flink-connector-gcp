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
"""Sample what the deployed Bigtable components do, through the job's REST service.

The families are the ones the BigQuery and Cloud Tasks trials read, for each
of the three components: its vertex's task, network and connector metrics and
its subtasks' shares of the latest completed checkpoint, and TaskManager
memory, which the components share and so describes the job.
"""

from ..metrics import (
    NETWORK_METRICS,
    SOURCE_METRICS,
    TASK_METRICS,
    checkpoint_statistics,
    named_vertices,
    subtask_metrics,
    taskmanagers,
    unavailable,
)

# The vertex each role is, by the start of the name Flink chains it under
# (kubernetes/apps/bigtable README).
VERTICES = {
    "input": "Source: Bigtable input",
    "eo": "Bigtable EXACTLY_ONCE",
    "alo": "Bigtable AT_LEAST_ONCE",
    "relay": "Source: Bigtable change stream",
}
COMPONENTS = ("eo", "alo", "relay")
# What the batching writer of the AT_LEAST_ONCE sink and the relay registers
# (BigtableWriterMetrics).
_WRITER = (
    "inFlightEntries",
    "inFlightBytes",
    "parkedEntries",
    "activeClients",
    "capacityEvictions",
    "idleEvictions",
    "recordsSkipped",
)
# Each component vertex's own metrics (BigtableMetricNames): the staged writer
# and its chained committer; the batching writer; and the change-stream reader
# chained to the relay's writer. The source's enumerator registers on the
# JobManager, which no vertex reading reaches.
CONNECTOR_METRICS = {
    "eo": (
        "stagedEntries",
        "stagedBytes",
        "recordsSkipped",
        "requestsDeduplicated",
        "requestsCompleted",
        "requestsFailed",
    ),
    "alo": _WRITER,
    "relay": (
        *_WRITER,
        "changeStreamMutationsRead",
        "changeStreamHeartbeatsRead",
        "changeStreamReadsStarted",
        "activeChangeStreamReads",
        "queuedChangeStreamPartitions",
        "queuedChangeStreamPartitionLagMillis",
        "missedHeartbeatIntervals",
        "changeStreamCloseStreamsRead",
        "changeStreamUserMutationsRead",
        "changeStreamGarbageCollectionMutationsRead",
        "changeStreamMutationEntriesFiltered",
        "changeStreamRecordsSkippedWithoutChange",
        "partitionLowWatermarkMillis",
    ),
}
# Each component's metrics, partitioned into the families the run has to observe.
FAMILIES = {
    component: {
        "task": frozenset(TASK_METRICS),
        "network": frozenset(NETWORK_METRICS),
        "connector": frozenset(CONNECTOR_METRICS[component]),
    }
    for component in COMPONENTS
}


def vertices(rest, known=None):
    """Each role's vertex id, and each component's discovered metric ids.

    The job is four vertices, two of them sources, so each is found by its
    name rather than by being the one source.
    """
    return named_vertices(
        rest, known, VERTICES, FAMILIES, "the input and three components"
    )


def observation(rest, state):
    """One poll's measurements, with every absent reading named rather than dropped."""
    if unavailable(state):
        return {"vertices": state}
    ids = state["vertices"]
    reading = {
        component: subtask_metrics(rest, ids[component], state["metrics"][component])
        for component in COMPONENTS
    }
    reading["source"] = subtask_metrics(rest, ids["input"], list(SOURCE_METRICS))
    reading["taskmanagers"] = taskmanagers(rest)
    reading["checkpoints"] = checkpoint_statistics(
        rest, {component: ids[component] for component in COMPONENTS}
    )
    return reading
