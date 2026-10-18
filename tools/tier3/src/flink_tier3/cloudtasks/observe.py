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
"""Sample what the deployed staged Cloud Tasks sink does, through the job's REST service.

The families are the ones the BigQuery trials read: the sink's task, network
and connector metrics, TaskManager memory and each sink subtask's share of the
latest completed checkpoint.
"""

from ..metrics import (
    NETWORK_METRICS,
    SOURCE_METRICS,
    TASK_METRICS,
    checkpoint_statistics,
    job_vertices,
    subtask_metrics,
    taskmanagers,
    unavailable,
)

# The staged sink's own metrics (CloudTasksMetricNames.java): what the writer
# holds until its checkpoint, and what the committer replayed, deduplicated and
# expired. The committer's two `currentCommit` gauges hold a value only while
# a commit runs, and a restore's commit runs once, before the task is running,
# so a sample catches it only by chance; their absence is not the family's.
CONNECTOR_METRICS = (
    "stagedTasks",
    "stagedBytes",
    "oldestStagedTaskAgeMillis",
    "stagedReplayBudgetMillis",
    "recordsSkipped",
    "tasksDeduplicated",
    "currentCommitOldestTaskAgeMillis",
    "currentCommitReplayBudgetMillis",
    "expiredEnvelopesFailed",
    "expiredEnvelopesAssumedCommitted",
    "expiredEnvelopesDropped",
    "expiredEnvelopeCreatesAuthorized",
)
# The sink's metrics, partitioned into the families the run has to observe.
SINK_FAMILIES = {
    "task": frozenset(TASK_METRICS),
    "network": frozenset(NETWORK_METRICS),
    "connector": frozenset(CONNECTOR_METRICS),
}
SINK_METRICS = frozenset(name for names in SINK_FAMILIES.values() for name in names)


def vertices(rest, known=None):
    """The source and sink vertex ids, and the sink's discovered metric ids.

    The job is two vertices: the source chained to the identity operator, and
    the sink, whose committer is chained to its writer, so the committer's
    metrics are the sink vertex's.
    """
    return job_vertices(rest, known, SINK_METRICS)


def observation(rest, state):
    """One poll's measurements, with every absent reading named rather than dropped."""
    if unavailable(state):
        return {"vertices": state}
    return {
        "sink": subtask_metrics(rest, state["sink"], state["sink_metrics"]),
        "source": subtask_metrics(rest, state["source"], list(SOURCE_METRICS)),
        "taskmanagers": taskmanagers(rest),
        "checkpoints": checkpoint_statistics(rest, {"sink": state["sink"]}),
    }
