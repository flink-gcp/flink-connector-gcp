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

import json

from flink_tier3 import runs
from flink_tier3.common import USABLE
from flink_tier3.firestore import analyze, plan
from flink_tier3.firestore.observe import (
    COMPONENTS,
    CONNECTOR_METRICS,
    observation,
    vertices,
)
from flink_tier3.firestore.verdict import (
    OBSERVED,
    SAMPLED_STAGES,
    sequence_verdict,
    summarize,
    verdict,
)

from ..test_analyze import write_run

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


def test_the_report_has_a_column_for_them():
    item = {
        "run_id": "fs-a1",
        "verdict": "usable",
        "stored": "usable",
        "samples": 21,
        "oracles": None,
        "unplanned": ["Unplanned job restart"],
        "reasons": [],
        "problems": [],
        "recoveries": [],
    }
    row = next(line for line in analyze.render([item]).splitlines() if "fs-a1" in line)
    assert "| Unplanned job restart | - | - |" in row


def test_an_exported_run_reports_the_causes_its_events_name(tmp_path):
    """Read from the events, which a run stopped mid-wait still exported."""

    def waited(cause):
        return {
            "event": "unplanned-recovery",
            "at": "2026-11-11T01:42:00Z",
            "payload": {"stage": "finishing", "cause": cause},
        }

    run_dir = write_run(
        tmp_path,
        run_id="fs-a1",
        scenario="firestore-recovery",
        events=[
            waited("Unplanned workload interruption"),
            waited(None),
            waited(["not", "text"]),
            waited("Unplanned workload interruption"),
        ],
    )
    # The receipt keeps the record as the finishing stage last wrote it.
    result = json.loads((run_dir / "result.json").read_text())
    result["recovery"] = {"stage": "finishing", "coverage": {}, "outcomes": {}}
    (run_dir / "result.json").write_text(json.dumps(result))
    approval = json.loads((run_dir / "approval.json").read_text())
    approval["firestore_trial"] = plan.trial("jm-replacement")
    (run_dir / "approval.json").write_text(json.dumps(approval))
    (assessed,) = analyze.assess_runs([runs.Run(run_dir)])
    assert assessed["unplanned"] == ["Unplanned workload interruption"]
    assert "| Unplanned workload interruption |" in analyze.render([assessed])


def test_a_completed_record_names_its_causes_without_their_events(tmp_path):
    """An export that lost the event files still has the record's causes."""
    run_dir = write_run(
        tmp_path,
        run_id="fs-a2",
        scenario="firestore-recovery",
        events=[
            {
                "event": "recovery-complete",
                "at": "2026-11-11T02:00:00Z",
                "payload": {
                    "stage": "complete",
                    "coverage": {},
                    "outcomes": {"unplanned": ["Unplanned job restart", None]},
                },
            }
        ],
    )
    approval = json.loads((run_dir / "approval.json").read_text())
    approval["firestore_trial"] = plan.trial("jm-replacement")
    (run_dir / "approval.json").write_text(json.dumps(approval))
    (assessed,) = analyze.assess_runs([runs.Run(run_dir)])
    assert assessed["unplanned"] == ["Unplanned job restart"]


def test_a_sequence_run_is_assessed_by_its_oracles_without_samples(tmp_path):
    """A fault-sequence run (ADR-0204) samples nothing and lists its faults."""
    passed = {"verdict": "pass"}
    record = {
        "stage": "complete",
        "verdict": "usable",
        "reasons": [],
        "outcomes": {
            "oracles": {component: passed for component in COMPONENTS},
            "faults": [{"kind": "upgrade", "at": "2026-11-12T02:00:00Z"}],
        },
    }
    run_dir = write_run(
        tmp_path,
        run_id="fs-s1",
        scenario="firestore-recovery",
        events=[
            {
                "event": "recovery-complete",
                "at": "2026-11-12T02:20:00Z",
                "payload": record,
            }
        ],
    )
    approval = json.loads((run_dir / "approval.json").read_text())
    approval["firestore_trial"] = plan.trial("sequence")
    (run_dir / "approval.json").write_text(json.dumps(approval))
    (assessed,) = analyze.assess_runs([runs.Run(run_dir)])
    assert assessed["verdict"] == USABLE
    assert assessed["problems"] == []
    assert assessed["faults"] == record["outcomes"]["faults"]
    failing = {**passed, "verdict": "fail"}
    assert sequence_verdict(
        {
            **record,
            "outcomes": {
                "oracles": {**record["outcomes"]["oracles"], "native_sink": failing}
            },
        }
    )["reasons"] == ["native_sink-oracle-not-passed"]


def test_a_sequence_run_stopped_before_its_verdict_is_not_usable():
    """Four passing reports in a record that never completed decide nothing."""
    passed = {"verdict": "pass"}
    record = {
        "stage": "documents",
        "outcomes": {"oracles": {component: passed for component in COMPONENTS}},
    }
    assert sequence_verdict(record)["reasons"] == ["recovery-incomplete"]
