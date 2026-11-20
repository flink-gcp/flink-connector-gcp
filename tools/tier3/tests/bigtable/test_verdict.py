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

import json

import pytest
from flink_tier3 import runs
from flink_tier3.bigtable import analyze, plan
from flink_tier3.bigtable.observe import CONNECTOR_METRICS, observation, vertices
from flink_tier3.bigtable.verdict import (
    OBSERVED,
    SAMPLED_STAGES,
    sequence_verdict,
    summarize,
    verdict,
)
from flink_tier3.common import INCONCLUSIVE, USABLE

from ..test_analyze import write_run

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


def test_a_sequence_run_is_assessed_by_its_oracles_without_samples(tmp_path):
    """A fault-sequence run (ADR-0204) samples nothing and lists its faults."""
    record = {
        "stage": "complete",
        "verdict": "usable",
        "reasons": [],
        "outcomes": {
            "oracles": {
                "eo": {"verdict": "pass"},
                "alo": {"verdict": "pass"},
                "relay": {"verdict": "pass", "judged": True},
            },
            "faults": [
                {"kind": "upgrade", "at": "2026-11-20T02:00:00Z"},
                {"kind": "jobmanager", "at": "2026-11-20T02:02:00Z"},
            ],
        },
    }
    run_dir = write_run(
        tmp_path,
        run_id="bt-s1",
        scenario="bigtable-recovery",
        events=[
            {
                "event": "recovery-complete",
                "at": "2026-11-20T02:30:00Z",
                "payload": record,
            }
        ],
    )
    approval = json.loads((run_dir / "approval.json").read_text())
    approval["bigtable_trial"] = plan.trial("sequence", "2026-11-20T01:50:00Z")
    (run_dir / "approval.json").write_text(json.dumps(approval))
    (assessed,) = analyze.assess_runs([runs.Run(run_dir)])
    assert assessed["verdict"] == USABLE
    assert assessed["problems"] == []
    assert assessed["faults"] == record["outcomes"]["faults"]
    # The report lists the faults rather than claiming windows it never had.
    [row] = [
        line for line in analyze.render([assessed]).splitlines() if "bt-s1" in line
    ]
    assert "| upgrade, jobmanager |" in row


PASSED = {
    "eo": {"verdict": "pass"},
    "alo": {"verdict": "pass"},
    "relay": {"verdict": "pass", "judged": True},
}


@pytest.mark.parametrize(
    ("stage", "component", "report", "reason"),
    [
        ("tables", None, None, "recovery-incomplete"),
        ("complete", "eo", {"verdict": "fail"}, "eo-oracle-not-passed"),
        ("complete", "alo", {"verdict": "fail"}, "alo-oracle-not-passed"),
        (
            "complete",
            "relay",
            {"verdict": "fail", "judged": True},
            "relay-oracle-not-passed",
        ),
        (
            "complete",
            "relay",
            {"verdict": "pass", "judged": False},
            "relay-oracle-unjudged",
        ),
    ],
)
def test_a_sequence_run_is_usable_only_complete_with_every_oracle_passed(
    stage, component, report, reason
):
    """Lost or duplicated data in any one table, or a relay not judged, withholds `usable`."""
    oracles = {**PASSED, **({component: report} if component else {})}
    decided = sequence_verdict({"stage": stage, "outcomes": {"oracles": oracles}})
    assert decided == {"verdict": INCONCLUSIVE, "reasons": [reason]}
    passed = sequence_verdict({"stage": "complete", "outcomes": {"oracles": PASSED}})
    assert passed == {"verdict": USABLE, "reasons": []}
