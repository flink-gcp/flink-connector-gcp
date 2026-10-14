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
    discover,
    sample,
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
    the sink, whose committer is chained to its writer. A connector metric's id
    carries its operator name, so it is discovered and unioned across calls,
    as the BigQuery reader does: a listing taken before the committer opens
    holds none of its metrics.
    """
    plan = sample(lambda: rest.job(""))
    if unavailable(plan):
        return known if known and not unavailable(known) else plan
    listed = [v for v in (plan.get("vertices") or []) if v.get("id")]
    sources = [v for v in listed if str(v.get("name", "")).startswith("Source")]
    sinks = [v for v in listed if v not in sources]
    if len(sources) != 1 or len(sinks) != 1:
        return {"unavailable": f"expected one source and one sink, found {len(listed)}"}
    sink = sinks[0]["id"]
    listing = sample(lambda: rest.job(f"/vertices/{sink}/subtasks/metrics"))
    previous = known if known and not unavailable(known) else {}
    if unavailable(listing):
        return known if previous.get("sink_metrics") else listing
    return {
        "source": sources[0]["id"],
        "sink": sink,
        "sink_metrics": discover(
            listing, SINK_METRICS, previous.get("sink_metrics", [])
        ),
    }


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
