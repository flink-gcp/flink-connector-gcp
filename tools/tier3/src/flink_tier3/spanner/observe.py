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
"""Sample what the deployed Spanner components do, through the job's REST service.

The families are the Bigtable trial's, for each of the three components: its
vertex's task, network and connector metrics and its subtasks' shares of the
latest completed checkpoint, and TaskManager memory, which the components
share and so describes the job. The Change Streams source's output watermark
is among its connector metrics, and its coordinator's partition count is read
from the JobManager, for the report only.
"""

import math

from ..metrics import (
    NETWORK_METRICS,
    SOURCE_METRICS,
    TASK_METRICS,
    checkpoint_statistics,
    discover,
    metric_name,
    named_vertices,
    sample,
    subtask_metrics,
    taskmanagers,
    unavailable,
)

# The vertex each role is, by the start of the name Flink chains it under
# (kubernetes/apps/spanner README).
VERTICES = {
    "input": "Source: Spanner input",
    "sink": "Spanner input: Writer",
    "change_stream": "Source: Spanner change stream",
    "batch": "Source: Spanner seed",
}
COMPONENTS = ("sink", "change_stream", "batch")
# Flink's watermark gauge, which every operator of a chain registers; the
# one the change-stream source's operator registers is the source's.
WATERMARK = "currentOutputWatermark"
# Each component vertex's own metrics (SpannerMetricNames): the sink writer's,
# twice over for the relay and the batch copy, which write through a writer of
# their own chained behind their source; the Change Streams readers'; and the
# batch readers'. The sources' enumerators register on the JobManager, which no
# vertex reading reaches.
_WRITER = (
    "bufferedMutations",
    "bufferedCells",
    "bufferedBytes",
    "recordsSkipped",
    "mutationsRetried",
    "batchesSent",
)
CONNECTOR_METRICS = {
    "sink": _WRITER,
    "change_stream": (
        *_WRITER,
        "changeStreamQueriesStarted",
        "activeChangeStreamQueries",
        "queuedChangeStreamPartitions",
        "queuedChangeStreamPartitionLagMillis",
        "missedHeartbeatIntervals",
        "lastChangeStreamRecordWaitMillis",
        "longestChangeStreamRecordWaitMillis",
        "changeStreamRecordsFilteredByTable",
        "changeStreamRecordsSkippedWithoutChange",
        "changeStreamColumnOccurrencesFiltered",
        WATERMARK,
    ),
    "batch": (*_WRITER, "rowsRead", "partitionsReread"),
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
# The Change Streams coordinator's ledger of partitions
# (SpannerChangeStreamSplitEnumerator), read from the JobManager.
COORDINATOR_METRICS = frozenset(("changeStreamPartitionsDiscovered",))


def vertices(rest, known=None):
    """Each role's vertex id, and each component's discovered metric ids.

    The job is four vertices, three of them sources, so each is found by its
    name rather than by being the one source.
    """
    return named_vertices(
        rest, known, VERTICES, FAMILIES, "the input and three components"
    )


def coordinator(rest, vertex):
    """The change-stream vertex's coordinator metrics, or their named absence.

    Flink serves an operator coordinator's metrics as the vertex's JobManager
    operator metrics, under ids that carry its scope, so the ids are listed
    and then asked for, on every reading: they are reported, never judged.
    """
    path = f"/vertices/{vertex}/jm-operator-metrics"
    listing = sample(lambda: rest.job(path))
    if unavailable(listing):
        return listing
    ids = discover(
        listing if isinstance(listing, list) else [], COORDINATOR_METRICS, []
    )
    if not ids:
        return {"unavailable": "no coordinator metric listed"}
    return sample(lambda: rest.job(f"{path}?get={','.join(ids)}"))


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
    reading["coordinator"] = coordinator(rest, ids["change_stream"])
    reading["taskmanagers"] = taskmanagers(rest)
    reading["checkpoints"] = checkpoint_statistics(
        rest, {component: ids[component] for component in COMPONENTS}
    )
    return reading


def _figures(value, name):
    if not isinstance(value, list):
        return []
    return [
        item
        for item in value
        if isinstance(item, dict)
        and isinstance(item.get("id"), str)
        and metric_name(item["id"]) == name
    ]


def watermark(reading):
    """The change-stream source's output watermark, its subtasks' least, or None.

    Every operator of the vertex's chain registers the gauge; the source's is
    the one whose operator name begins with `Source`.
    """
    figures = [
        item.get("min")
        for item in _figures(reading.get("change_stream"), WATERMARK)
        if item["id"].startswith("Source")
    ]
    if len(figures) != 1:
        return None
    return _number(figures[0])


def partitions_reread(reading):
    """The batch readers' rereads, summed over their subtasks, or None."""
    figures = [
        item.get("sum") for item in _figures(reading.get("batch"), "partitionsReread")
    ]
    return _number(figures[0]) if len(figures) == 1 else None


def partitions_discovered(reading):
    """The coordinator's discovered Change Streams partitions, or None."""
    figures = [
        item.get("value")
        for item in _figures(
            reading.get("coordinator"), "changeStreamPartitionsDiscovered"
        )
    ]
    return _number(figures[0]) if len(figures) == 1 else None


def _number(value):
    """A REST figure, which Flink sends as a number or a decimal string, or None."""
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return None
    if type(value) not in (int, float) or math.isnan(value):
        return None
    return value
