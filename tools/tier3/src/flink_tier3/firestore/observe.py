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
"""Sample what the deployed Firestore components do, through the job's REST service.

The families are the Spanner trial's, for each of the four components: its
vertex's task, network and connector metrics and its subtasks' shares of the
latest completed checkpoint, and TaskManager memory, which the components
share and so describes the job.
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
# (kubernetes/apps/firestore README).
VERTICES = {
    "input": "Source: Firestore input",
    "native_sink": "Firestore native input: Writer",
    "datastore_sink": "Firestore datastore input: Writer",
    "native_batch": "Source: Firestore native seed",
    "datastore_batch": "Source: Firestore datastore seed",
}
COMPONENTS = ("native_sink", "datastore_sink", "native_batch", "datastore_batch")
# Each mode's writer metrics (FirestoreMetricNames, DatastoreMetricNames),
# which the input sink and the batch copy chained behind the source both
# register, and each mode's reader metrics. The sources' enumerators register
# on the JobManager, which no vertex reading reaches.
_NATIVE_WRITER = (
    "inFlightWrites",
    "inFlightBytes",
    "parkedWrites",
    "bufferedWrites",
    "recordsSkipped",
    "writesRetried",
    "bulkWritersReplaced",
    "writesConfirmedAlone",
    "idsRedrawn",
)
_DATASTORE_WRITER = (
    "bufferedMutations",
    "bufferedBytes",
    "recordsSkipped",
    "mutationsRetried",
    "batchesSent",
    "mutationsConfirmedAlone",
    "throttledMillis",
)
CONNECTOR_METRICS = {
    "native_sink": _NATIVE_WRITER,
    "datastore_sink": _DATASTORE_WRITER,
    "native_batch": (*_NATIVE_WRITER, "documentsRead"),
    "datastore_batch": (*_DATASTORE_WRITER, "entitiesRead"),
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

    The job is five vertices, three of them sources, so each is found by its
    name rather than by being the one source.
    """
    return named_vertices(
        rest, known, VERTICES, FAMILIES, "the input and four components"
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
