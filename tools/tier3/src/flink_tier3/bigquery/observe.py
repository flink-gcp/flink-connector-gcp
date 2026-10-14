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
# The sink's metrics, partitioned into the families the run has to observe.
# One declaration serves both readers: discovery keeps an id when it belongs
# to some family, and the verdict asks which family a returned id belongs to.
SINK_FAMILIES = {
    "task": frozenset(TASK_METRICS),
    "network": frozenset(NETWORK_METRICS),
    "connector": frozenset(CONNECTOR_METRICS),
}
SINK_METRICS = frozenset(name for names in SINK_FAMILIES.values() for name in names)


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
        "sink_metrics": discover(
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
            else discover(listing, frozenset(COMMITTER_METRICS), ids)
        )
    return state


def checkpoint_durations(rest, state):
    """Each sink subtask's share of the latest completed checkpoint.

    A writer finalizes its staged files before it forwards the barrier, which
    the connector has no metric for (ADR-0146) and Flink times in neither the
    sync nor the async part: it shows in the writer's end-to-end duration and
    the committer's start delay.
    """
    return checkpoint_statistics(
        rest, {role: state[role] for role in ("sink", "committer")}
    )


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
