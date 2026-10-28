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
"""Vertex discovery by name, the families and watermark a reading covers, and the verdict."""

import pytest
from flink_tier3.common import USABLE
from flink_tier3.metrics import metric_name
from flink_tier3.spanner.observe import (
    CONNECTOR_METRICS,
    FAMILIES,
    observation,
    vertices,
)
from flink_tier3.spanner.verdict import (
    NO_WATERMARK,
    OBSERVED,
    SAMPLED_STAGES,
    summarize,
    verdict,
)

# The chained vertex names of the application's README.
NAMES = {
    "in": "Source: Spanner input -> Spanner input identity",
    "sink": "Spanner input: Writer",
    "cs": "Source: Spanner change stream -> Spanner relay delivery"
    " -> Spanner relay: Writer",
    "batch": "Source: Spanner seed -> Spanner seed pace -> Spanner seed delivery"
    " -> Spanner batch copy: Writer",
}
# Each component's operators, as the query service prefixes their metric ids,
# the one registering the connector's metrics first.
OPERATORS = {
    "sink": ["Spanner_input__Writer"],
    "cs": ["Source__Spanner_change_stream", "Spanner_relay__Writer"],
    "batch": ["Source__Spanner_seed", "Spanner_batch_copy__Writer"],
}


class Rest:
    """The job's REST answers: every vertex lists and returns every metric asked for.

    An operator's metrics read `watermarks[operator]`, or 0, and none of them
    is returned where that is None.
    """

    def __init__(self, watermark=0):
        self.watermarks = {"Source__Spanner_change_stream": watermark}

    def job(self, path):
        if path == "":
            return {
                "vertices": [
                    {"id": vertex, "name": name} for vertex, name in NAMES.items()
                ]
            }
        if path == "/checkpoints":
            return {"latest": {"completed": {"id": 3}}}
        if path.startswith("/checkpoints/details/"):
            return {
                "subtasks": [
                    {
                        "index": 0,
                        "end_to_end_duration": 5,
                        "checkpoint": {"sync": 1, "async": 2},
                    }
                ]
            }
        if "/jm-operator-metrics" in path:
            if "?get=" in path:
                return [{"id": path.split("?get=")[1], "value": "12"}]
            return [{"id": "coordinator.enumerator.changeStreamPartitionsDiscovered"}]
        vertex = path.split("/")[2]
        if "?get=" in path:
            values = {
                metric: self.watermarks.get(metric.split(".")[0], 0)
                for metric in path.split("?get=")[1].split("&")[0].split(",")
            }
            return [
                {"id": metric, "min": value, "sum": 0}
                for metric, value in values.items()
                if value is not None
            ]
        component = {"cs": "change_stream"}.get(vertex, vertex)
        names = [*CONNECTOR_METRICS.get(component, ()), "numRecordsIn", "inPoolUsage"]
        first, *others = OPERATORS.get(vertex, ["x"])
        # Every operator of the chain registers a watermark.
        return [{"id": f"{first}.{name}"} for name in names] + [
            {"id": f"{operator}.currentOutputWatermark"} for operator in others
        ]

    def root(self, path):
        if path == "/taskmanagers":
            return {"taskmanagers": [{"id": "tm"}]}
        return [{"id": "Status.JVM.Memory.Heap.Used", "value": "1"}]


def record(coverage, judged=True):
    return {
        "stage": "complete",
        "outcomes": {
            "upgrade": {},
            "failover": {},
            "oracles": {
                "sink": {"verdict": "pass"},
                "change_stream": {"verdict": "pass", "judged": judged},
                "batch": {"verdict": "pass"},
            },
        },
        "coverage": coverage,
    }


def windows(watermarks):
    """Coverage of one reading per window, each with the source's watermark given."""
    coverage = {}
    state = vertices(Rest())
    for window, value in zip(SAMPLED_STAGES, watermarks, strict=True):
        reading = observation(Rest(value), state)
        coverage = summarize(coverage, window, {**reading, "window": window})
    return coverage


def test_the_four_vertices_are_found_by_name_and_every_family_is_read():
    state = vertices(Rest())
    assert state["vertices"] == {
        "input": "in",
        "sink": "sink",
        "change_stream": "cs",
        "batch": "batch",
    }
    coverage = windows([1, 2, 3])
    assert all(
        sorted(coverage[w]["observed"]) == sorted(OBSERVED) for w in SAMPLED_STAGES
    )
    assert coverage["finishing"]["maxima"] == {
        "watermark": 3,
        "partitions_reread": 0,
        "partitions_discovered": 12,
    }
    assert verdict(record(coverage)) == {"verdict": USABLE, "reasons": []}
    # A report that records no judgment is not one.
    assert verdict(record(coverage, judged=None))["reasons"] == [
        "change_stream-oracle-unjudged"
    ]


@pytest.mark.parametrize(
    ("watermarks", "reason"),
    [
        # Each recovery must be followed by a greater watermark.
        ([1, 2, 2], "change_stream-oracle-not-passed"),
        ([1, 1, 2], "change_stream-oracle-not-passed"),
        # Flink's none is not a watermark.
        ([NO_WATERMARK, 2, 3], "change_stream-oracle-not-passed"),
    ],
)
def test_the_watermark_must_advance_after_each_recovery(watermarks, reason):
    assert verdict(record(windows(watermarks)))["reasons"] == [reason]


def test_an_unread_watermark_leaves_the_change_stream_unjudged():
    """Only the source's own gauge is its watermark; an absence never passes."""
    coverage = windows([1, 2, 3])
    del coverage["upgraded"]["maxima"]["watermark"]
    assert verdict(record(coverage))["reasons"] == ["change_stream-oracle-unjudged"]
    rest = Rest()
    rest.watermarks = {
        "Source__Spanner_change_stream": None,
        "Spanner_relay__Writer": 5,
    }
    reading = observation(rest, vertices(rest))
    assert "watermark" not in summarize({}, "baseline", reading)["baseline"]["maxima"]


def test_a_component_family_never_read_leaves_the_run_unusable():
    """A family is judged by component, so another's metrics do not stand in."""

    class Partial(Rest):
        def job(self, path):
            answer = super().job(path)
            if path.startswith("/vertices/batch/subtasks/metrics?get="):
                connector = FAMILIES["batch"]["connector"]
                return [m for m in answer if metric_name(m["id"]) not in connector]
            return answer

    coverage = {}
    state = vertices(Rest())
    for window, value in zip(SAMPLED_STAGES, [1, 2, 3], strict=True):
        reading = observation(Partial(value), state)
        coverage = summarize(coverage, window, {**reading, "window": window})
    assert all("batch-connector" not in coverage[w]["observed"] for w in SAMPLED_STAGES)
    result = verdict(record(coverage))
    assert result["verdict"] != USABLE
    assert result["reasons"] == [
        f"unobserved-batch-connector-in-{window}" for window in SAMPLED_STAGES
    ]
