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

from flink_tier3.bigtable.observe import CONNECTOR_METRICS, observation, vertices
from flink_tier3.bigtable.verdict import OBSERVED, SAMPLED_STAGES, summarize, verdict
from flink_tier3.common import USABLE

# The chained vertex names of the application's README.
NAMES = {
    "in": "Source: Bigtable input -> Bigtable input identity",
    "eo": "Bigtable EXACTLY_ONCE: Writer -> Bigtable EXACTLY_ONCE: Committer",
    "alo": "Bigtable AT_LEAST_ONCE: Writer",
    "relay": "Source: Bigtable change stream -> Bigtable relay delivery -> Bigtable relay: Writer",
}


class Rest:
    """The job's REST answers: every vertex lists and returns every metric asked for."""

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
                {"id": metric}
                for metric in path.split("?get=")[1].split("&")[0].split(",")
            ]
        operator = {"eo": "Writer", "alo": "Writer", "relay": "Writer"}.get(vertex, "x")
        names = [*CONNECTOR_METRICS.get(vertex, ()), "numRecordsIn", "inPoolUsage"]
        return [{"id": f"{operator}.{name}"} for name in names]

    def root(self, path):
        if path == "/taskmanagers":
            return {"taskmanagers": [{"id": "tm"}]}
        return [{"id": "Status.JVM.Memory.Heap.Used", "value": "1"}]


def test_the_four_vertices_are_found_by_name_and_every_family_is_read():
    state = vertices(Rest())
    assert state["vertices"] == {
        "input": "in",
        "eo": "eo",
        "alo": "alo",
        "relay": "relay",
    }
    reading = observation(Rest(), state)
    coverage = {}
    for window in SAMPLED_STAGES:
        coverage = summarize(coverage, window, {**reading, "window": window})
    assert all(
        sorted(coverage[w]["observed"]) == sorted(OBSERVED) for w in SAMPLED_STAGES
    )
    record = {
        "stage": "complete",
        "outcomes": {
            "upgrade": {},
            "failover": {},
            "oracles": {c: {"verdict": "pass"} for c in ("eo", "alo")}
            | {"relay": {"verdict": "pass", "judged": True}},
        },
        "coverage": coverage,
    }
    assert verdict(record) == {"verdict": USABLE, "reasons": []}
    # A relay report that records no judgment is not one.
    del record["outcomes"]["oracles"]["relay"]["judged"]
    assert verdict(record)["reasons"] == ["relay-oracle-unjudged"]


def test_a_job_of_another_shape_is_not_measured():
    class Other(Rest):
        def job(self, path):
            if path == "":
                return {"vertices": [{"id": "a", "name": "Source: Bigtable input"}]}
            return super().job(path)

    assert "unavailable" in vertices(Other())
