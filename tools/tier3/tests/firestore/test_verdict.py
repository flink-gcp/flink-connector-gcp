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
"""Vertex discovery by name, the families a reading covers, and the verdict."""

from flink_tier3.common import USABLE
from flink_tier3.firestore.observe import (
    COMPONENTS,
    CONNECTOR_METRICS,
    observation,
    vertices,
)
from flink_tier3.firestore.verdict import OBSERVED, SAMPLED_STAGES, summarize, verdict

# The chained vertex names of the application's README.
NAMES = {
    "in": "Source: Firestore input -> Firestore input identity",
    "ns": "Firestore native input: Writer",
    "ds": "Firestore datastore input: Writer",
    "nb": "Source: Firestore native seed -> Firestore native seed pace"
    " -> Firestore native seed delivery -> Firestore native batch copy: Writer",
    "db": "Source: Firestore datastore seed -> Firestore datastore seed pace"
    " -> Firestore datastore seed delivery -> Firestore datastore batch copy: Writer",
}
ROLES = dict(zip(("ns", "ds", "nb", "db"), COMPONENTS, strict=True))


class Rest:
    """The job's REST answers: every vertex lists and returns every metric asked for."""

    def __init__(self, silent=()):
        self.silent = set(silent)

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
        vertex = path.split("/")[2]
        if "?get=" in path:
            return [
                {"id": metric, "min": 0, "sum": 0}
                for metric in path.split("?get=")[1].split("&")[0].split(",")
                if metric.split(".")[-1] not in self.silent
            ]
        names = [
            *CONNECTOR_METRICS.get(ROLES.get(vertex), ()),
            "numRecordsIn",
            "inPoolUsage",
        ]
        return [{"id": f"Operator.{name}"} for name in names]

    def root(self, path):
        if path == "/taskmanagers":
            return {"taskmanagers": [{"id": "tm"}]}
        return [{"id": "Status.JVM.Memory.Heap.Used", "value": "1"}]


def windows(rest):
    coverage = {}
    state = vertices(rest)
    for window in SAMPLED_STAGES:
        coverage = summarize(
            coverage, window, {**observation(rest, state), "window": window}
        )
    return coverage


def record(coverage, failing=()):
    return {
        "stage": "complete",
        "outcomes": {
            "upgrade": {},
            "failover": {},
            "oracles": {
                component: {"verdict": "fail" if component in failing else "pass"}
                for component in COMPONENTS
            },
        },
        "coverage": coverage,
    }


def test_the_five_vertices_are_found_by_name_and_every_family_is_read():
    state = vertices(Rest())
    assert state["vertices"] == {
        "input": "in",
        "native_sink": "ns",
        "datastore_sink": "ds",
        "native_batch": "nb",
        "datastore_batch": "db",
    }
    coverage = windows(Rest())
    assert all(
        sorted(coverage[w]["observed"]) == sorted(OBSERVED) for w in SAMPLED_STAGES
    )
    assert verdict(record(coverage)) == {"verdict": USABLE, "reasons": []}


def test_each_databases_oracles_are_judged_on_their_own():
    result = verdict(record(windows(Rest()), failing=("datastore_sink",)))
    assert result["reasons"] == ["datastore_sink-oracle-not-passed"]


def test_a_component_family_never_read_leaves_the_run_unusable():
    """A family is judged by component, so another mode's metrics do not stand in."""
    # The Datastore-mode reader's and writer's names, which include every one
    # the Datastore-mode sink registers.
    silent = CONNECTOR_METRICS["datastore_batch"]
    result = verdict(record(windows(Rest(silent=silent))))
    assert result["verdict"] != USABLE
    assert result["reasons"] == [
        f"unobserved-{component}-connector-in-{window}"
        for window in SAMPLED_STAGES
        for component in ("datastore_sink", "datastore_batch")
    ]
