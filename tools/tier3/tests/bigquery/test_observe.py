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
"""What the sink measurement reads, and what it says when a reading is absent."""

import json
from pathlib import Path

import pytest
from flink_tier3.bigquery.exercise import BigQueryExercise
from flink_tier3.bigquery.observe import (
    COMMITTER_METRICS,
    CONNECTOR_METRICS,
    MEMORY_METRICS,
    NETWORK_METRICS,
    observation,
    vertices,
)
from flink_tier3.bigquery.oracle import Trial
from flink_tier3.common import ApiError, Failure, ReadCeilingExceeded, TransportError
from flink_tier3.exercise import RecoveryExercise
from flink_tier3.metrics import FlinkRest, sample, unavailable
from flink_tier3.policy import BIGQUERY, BIGQUERY_OBSERVATIONS

JOB = "a" * 32
SERVICE = {"metadata": {"name": "run-rest"}}


class Cluster:
    """Answers the endpoints the real Flink service answers, by path shape."""

    def __init__(self, **failures):
        self.paths, self.failures, self.listing = [], failures, None

    def path(self, kind, namespace, name):
        return f"/{namespace}/{kind}/{name}"

    def request(self, method, path, **_kwargs):
        self.paths.append(path)
        for fragment, status in self.failures.items():
            if fragment in path:
                raise ApiError(status, method, path)
        if "/proxy/jobs/" in path and "/" not in path.split("/proxy/jobs/")[1]:
            return {
                "vertices": [
                    {"id": "sink", "name": "Sink: bigquery"},
                    {"id": "source", "name": "Source: datagen"},
                ]
            }
        if path.endswith("/subtasks/metrics"):
            if self.listing is not None:
                return self.listing
            return [
                {"id": "Sink__Writer.openDestinations"},
                {"id": "busyTimeMsPerSecond"},
                {"id": "outPoolUsage"},
                {"id": "Sink__Writer.somethingElse"},
            ]
        if "/subtasks/metrics?" in path:
            return [{"id": "openDestinations", "sum": "3"}]
        if path.endswith("/taskmanagers"):
            return {"taskmanagers": [{"id": "tm-0"}, {"id": "tm-1"}]}
        return [{"id": "Status.JVM.Memory.Direct.MemoryUsed", "value": "8"}]


class Environment:
    def __init__(self, cluster):
        self.kube = cluster
        self.approval = type("A", (), {"application_namespace": BIGQUERY})()
        self.now = 0.0

    def clock(self):
        return self.now


def reader(**failures):
    cluster = Cluster(**failures)
    return cluster, FlinkRest(Environment(cluster), SERVICE, JOB)


def test_discovery_keeps_only_the_metrics_this_sink_is_read_for():
    cluster, rest = reader()
    state = vertices(rest)
    assert state["source"] == "source" and state["sink"] == "sink"
    # A connector metric's id carries its operator name, so it is matched on
    # the suffix; an id this scenario does not read is left behind.
    assert state["sink_metrics"] == [
        "Sink__Writer.openDestinations",
        "busyTimeMsPerSecond",
        "outPoolUsage",
    ]
    assert any(path.endswith(f"/proxy/jobs/{JOB}") for path in cluster.paths)


def test_the_measurement_reads_the_sink_the_source_and_every_taskmanager():
    cluster, rest = reader()
    result = observation(rest, vertices(rest))
    assert not unavailable(result["sink"])
    assert not unavailable(result["source"])
    assert [manager["id"] for manager in result["taskmanagers"]] == ["tm-0", "tm-1"]
    asked = "".join(cluster.paths)
    for metric in (
        "Status.JVM.Memory.Direct.MemoryUsed",
        "Status.Flink.Memory.Managed.Used",
    ):
        assert metric in asked
    for metric in NETWORK_METRICS:
        assert metric in asked


@pytest.mark.parametrize("fragment", ["/taskmanagers", "/subtasks/metrics?"])
def test_an_absent_reading_is_named_rather_than_dropped(fragment):
    """A missing sample and a zero reading mean opposite things here."""
    _cluster, rest = reader(**{fragment: 503})
    result = observation(rest, vertices(rest))
    named = [value for value in result.values() if unavailable(value)]
    assert named and all("503" in value["unavailable"] for value in named)


def test_discovery_that_cannot_read_the_plan_reports_itself():
    _cluster, rest = reader(**{f"/proxy/jobs/{JOB}": 503})
    result = observation(rest, vertices(rest))
    assert unavailable(result["vertices"])


def test_the_source_is_identified_by_name_not_by_position():
    """The plan is conventionally topological; conventions are not contracts."""
    cluster, rest = reader()
    state = vertices(rest)
    assert state["source"] == "source" and state["sink"] == "sink"
    cluster.request = lambda *a, **k: {"vertices": [{"id": "only", "name": "Sink: x"}]}
    assert "expected one source" in vertices(rest)["unavailable"]


def test_the_connector_gauges_named_here_are_the_writer_observation():
    """These are what answers 'how many writers were active', per the issue."""
    for metric in ("openDestinations", "inFlightAppends", "destinationActivations"):
        assert metric in CONNECTOR_METRICS
    # Heap alone is the wrong instrument for a sink that appends off-heap.
    assert "Status.JVM.Memory.Direct.MemoryUsed" in MEMORY_METRICS
    assert "Status.JVM.Memory.Heap.Used" in MEMORY_METRICS


class Exercising:
    """The two fields `BigQueryExercise` keeps, without the rest of a run."""

    def __init__(self, cluster):
        self.env = Environment(cluster)
        self.env.emit = lambda event, payload: self.emitted.append((event, payload))
        self.emitted, self.rest, self.vertices = [], None, None
        self.measured_at, self.stage, self.coverage = None, "baseline", {}
        self.trial = Trial("observe-1312", "EO", 10, 20)

    timing = BIGQUERY_OBSERVATIONS
    attach_rest = BigQueryExercise.attach_rest
    measure = BigQueryExercise.measure
    track_rest = RecoveryExercise.track_rest
    measurement_due = RecoveryExercise.measurement_due

    def later(self):
        """Past the sampling interval, where the next measurement is due."""
        self.env.now += BIGQUERY_OBSERVATIONS["measure_seconds"]


def test_discovery_unions_across_polls_rather_than_freezing():
    """A task registers its metrics at deploy, the sink's operators at open.

    Freezing the first listing would drop the connector gauges — the whole
    active-writer observation — for the rest of the job's life.
    """
    cluster = Cluster()
    cluster.listing = [{"id": "busyTimeMsPerSecond"}]
    exercise = Exercising(cluster)
    exercise.attach_rest(SERVICE, JOB)
    exercise.measure()
    assert exercise.vertices["sink_metrics"] == ["busyTimeMsPerSecond"]
    cluster.listing = [{"id": "Sink__Writer.openDestinations"}]
    exercise.later()
    exercise.measure()
    assert exercise.vertices["sink_metrics"] == [
        "busyTimeMsPerSecond",
        "Sink__Writer.openDestinations",
    ]


def test_a_recovered_job_is_read_as_itself():
    """The upgrade gives the job a new id; the old one answers for nothing."""
    cluster = Cluster()
    exercise = Exercising(cluster)
    exercise.attach_rest(SERVICE, JOB)
    exercise.measure()
    exercise.attach_rest(SERVICE, "b" * 32)
    exercise.later()
    exercise.measure()
    assert exercise.rest.job_id == "b" * 32
    assert any(path.endswith("/proxy/jobs/" + "b" * 32) for path in cluster.paths)


def test_a_discovery_that_failed_is_tried_again():
    """The job was not ready yet; a cached failure would measure nothing after."""
    cluster = Cluster(**{f"/proxy/jobs/{JOB}": 503})
    exercise = Exercising(cluster)
    exercise.attach_rest(SERVICE, JOB)
    exercise.measure()
    assert unavailable(exercise.vertices)
    cluster.failures.clear()
    exercise.later()
    exercise.measure()
    assert not unavailable(exercise.vertices)
    assert exercise.emitted[-1][1]["sink"]


def test_sampling_is_bounded_by_its_own_interval_not_by_the_poll():
    """Six reads at the transport's timeout would otherwise eat a window."""
    cluster = Cluster()
    exercise = Exercising(cluster)
    exercise.attach_rest(SERVICE, JOB)
    exercise.measure()
    taken = len(cluster.paths)
    for _ in range(3):
        exercise.measure()
    assert len(cluster.paths) == taken
    assert len(exercise.emitted) == 1
    exercise.later()
    exercise.measure()
    assert len(exercise.emitted) == 2


def test_a_job_between_states_is_not_measured_against_its_last_service():
    """The loop resolves no service until the job is RUNNING or FINISHED."""
    cluster = Cluster()
    exercise = Exercising(cluster)
    exercise.attach_rest(SERVICE, JOB)
    exercise.measure()
    assert exercise.emitted
    exercise.attach_rest(None, "")
    exercise.later()
    exercise.measure()
    # One measurement, from before the job left its state.
    assert len(exercise.emitted) == 1
    assert exercise.rest is None


@pytest.mark.parametrize(
    "error",
    [
        ApiError(503, "GET", "/jobs"),
        TransportError("GET Kubernetes request failed: ReadTimeoutError"),
        ReadCeilingExceeded("Response exceeds its read ceiling"),
        json.JSONDecodeError("Expecting value", "<html>", 0),
    ],
)
def test_a_reading_that_is_not_one_is_named_unavailable(error):
    def read():
        raise error

    assert unavailable(sample(read))


def test_any_other_failure_still_stops_the_caller():
    def read():
        raise Failure("Recovery exercise deadline expired: after")

    with pytest.raises(Failure, match="deadline expired"):
        sample(read)


class FileLoadsCluster(Cluster):
    """The measured FILE_LOADS plan: its committer is a vertex of its own.

    Names as Flink 2.2.1 builds the application's job graph: the writer is
    chained to the checkpoint stamper, and the committer is fed through a
    global exchange.
    """

    def __init__(self, **failures):
        super().__init__(**failures)
        self.plan = [
            {"id": "committer", "name": "BigQuery FILE_LOADS: Committer"},
            {
                "id": "sink",
                "name": "BigQuery FILE_LOADS: Writer -> "
                "BigQuery FILE_LOADS: Stamp checkpoint ids",
            },
            {
                "id": "source",
                "name": "Source: BigQuery input -> BigQuery input identity",
            },
        ]

    def request(self, method, path, **kwargs):
        for fragment, status in self.failures.items():
            if fragment in path:
                self.paths.append(path)
                raise ApiError(status, method, path)
        if "/proxy/jobs/" in path and "/" not in path.split("/proxy/jobs/")[1]:
            self.paths.append(path)
            return {"vertices": self.plan}
        if path.endswith("/vertices/committer/subtasks/metrics"):
            self.paths.append(path)
            return [
                {"id": "BigQuery_FILE_LOADS__Committer.loadJobsSubmitted"},
                {"id": "BigQuery_FILE_LOADS__Committer.lastCommitDurationMillis"},
                {"id": "busyTimeMsPerSecond"},
            ]
        if path.endswith("/checkpoints"):
            self.paths.append(path)
            return {"latest": {"completed": {"id": 7}}}
        if "/checkpoints/details/7/subtasks/" in path:
            self.paths.append(path)
            return {
                "subtasks": [
                    {
                        "index": 0,
                        "status": "completed",
                        "checkpoint": {"sync": 3, "async": 40},
                        "alignment": {"duration": 1},
                        "start_delay": 2,
                        "end_to_end_duration": 50,
                        "state_size": 99,
                    }
                ]
            }
        return super().request(method, path, **kwargs)


def file_loads_reader(**failures):
    cluster = FileLoadsCluster(**failures)
    return cluster, FlinkRest(Environment(cluster), SERVICE, JOB)


def test_a_file_loads_plan_names_its_writer_and_its_committer():
    """One non-source vertex is the Storage Write shape; FILE_LOADS has two."""
    _cluster, rest = file_loads_reader()
    state = vertices(rest, mode="FILE_LOADS")
    assert (state["source"], state["sink"], state["committer"]) == (
        "source",
        "sink",
        "committer",
    )
    assert state["committer_metrics"] == [
        "BigQuery_FILE_LOADS__Committer.loadJobsSubmitted",
        "BigQuery_FILE_LOADS__Committer.lastCommitDurationMillis",
    ]
    # The same plan is not a Storage Write sink, whose committer is chained.
    assert "one sink" in vertices(rest, mode="EO")["unavailable"]


@pytest.mark.parametrize(
    "plan",
    [
        # The Storage Write shape: no committer vertex.
        [{"id": "sink", "name": "BigQuery FILE_LOADS: Writer"}],
        # A chained committer is not a vertex of its own.
        [{"id": "sink", "name": "X: Writer -> X: Committer"}, {"id": "c", "name": "Y"}],
        # Two committers cannot both be the one that commits.
        [
            {"id": "sink", "name": "X: Writer"},
            {"id": "c1", "name": "X: Committer"},
            {"id": "c2", "name": "Y: Committer"},
        ],
    ],
)
def test_a_file_loads_plan_of_another_shape_is_named_unavailable(plan):
    cluster, rest = file_loads_reader()
    cluster.plan = [*plan, {"id": "source", "name": "Source: input"}]
    assert (
        "a writer and a committer" in vertices(rest, mode="FILE_LOADS")["unavailable"]
    )


def test_a_file_loads_reading_samples_the_committer_and_checkpoint_durations():
    cluster, rest = file_loads_reader()
    reading = observation(rest, vertices(rest, mode="FILE_LOADS"))
    assert not unavailable(reading["committer"])
    assert reading["checkpoints"]["id"] == 7
    for role in ("sink", "committer"):
        (subtask,) = reading["checkpoints"]["vertices"][role]
        # Durations and where the time went; sizes are not this measurement.
        assert subtask["checkpoint"] == {"sync": 3, "async": 40}
        assert subtask["start_delay"] == 2 and "state_size" not in subtask
    asked = "".join(cluster.paths)
    assert "/checkpoints/details/7/subtasks/sink" in asked
    assert "/checkpoints/details/7/subtasks/committer" in asked
    assert "/vertices/committer/subtasks/metrics?get=" in asked


def test_an_absent_checkpoint_is_named_rather_than_read_as_zero():
    _cluster, rest = file_loads_reader(**{"/checkpoints": 503})
    reading = observation(rest, vertices(rest, mode="FILE_LOADS"))
    assert unavailable(reading["checkpoints"])
    cluster, rest = file_loads_reader()
    original = cluster.request

    def no_completed_checkpoint(method, path, **kwargs):
        if path.endswith("/checkpoints"):
            return {"latest": {}}
        return original(method, path, **kwargs)

    cluster.request = no_completed_checkpoint
    assert (
        "no completed"
        in observation(rest, vertices(rest, mode="FILE_LOADS"))["checkpoints"][
            "unavailable"
        ]
    )


def test_a_storage_write_reading_has_no_committer_or_checkpoint_entry():
    _cluster, rest = reader()
    reading = observation(rest, vertices(rest, mode="EO"))
    assert "committer" not in reading and "checkpoints" not in reading


def test_the_committer_metrics_are_the_ones_the_connector_registers():
    """FileLoadsCommitterMetrics; the writer's FILE_LOADS gauges are connector."""
    assert set(COMMITTER_METRICS) == {
        "loadJobsSubmitted",
        "queuedCommitDestinations",
        "activeCommitDestinations",
        "currentCommitDurationMillis",
        "lastCommitDurationMillis",
    }
    for metric in ("filesStaged", "pendingFiles", "capacityEvictions", "idleEvictions"):
        assert metric in CONNECTOR_METRICS


def test_committer_ids_are_unioned_and_survive_an_unreadable_listing():
    """One 503 must not drop the finalization observation for the run."""
    cluster, rest = file_loads_reader()
    first = vertices(rest, mode="FILE_LOADS")
    cluster.failures["/vertices/committer/subtasks/metrics"] = 503
    kept = vertices(rest, first, mode="FILE_LOADS")
    assert kept["committer_metrics"] == first["committer_metrics"]
    del cluster.failures["/vertices/committer/subtasks/metrics"]
    request = cluster.request

    def later(method, path, **kwargs):
        if path.endswith("/vertices/committer/subtasks/metrics"):
            return [{"id": "BigQuery_FILE_LOADS__Committer.queuedCommitDestinations"}]
        return request(method, path, **kwargs)

    cluster.request = later
    assert vertices(rest, kept, mode="FILE_LOADS")["committer_metrics"] == [
        *first["committer_metrics"],
        "BigQuery_FILE_LOADS__Committer.queuedCommitDestinations",
    ]


def test_the_file_loads_metric_names_are_the_ones_the_connector_registers():
    """A renamed metric would read as never published, not as a failure."""
    names = (
        Path(__file__).resolve().parents[4]
        / "flink-connector-gcp-bigquery/src/main/java/io/github/flink/gcp"
        / "connector/bigquery/BigQueryMetricNames.java"
    ).read_text()
    for metric in (
        *COMMITTER_METRICS,
        "filesStaged",
        "pendingFiles",
        "capacityEvictions",
        "idleEvictions",
    ):
        assert f'= "{metric}";' in names, metric
