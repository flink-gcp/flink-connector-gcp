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
"""Synthetic report validation and evidence-export boundaries."""

import json
import os
import time

import pytest
from conftest import load_script

SOURCE = "flink-connector-gcp-fake/src/test/java/p/ExampleITCase.java"
GATE = "FAKE_IT_PROJECT"


@pytest.fixture()
def reports(tmp_path):
    tool = load_script("e2e-reports.py")
    tool.prepare(tmp_path, GATE, [SOURCE])
    path, _ = tool.report_path(tmp_path, SOURCE)
    path.parent.mkdir(parents=True, exist_ok=True)
    return tool, tmp_path, path


def report(
    *, tests=1, failures=0, errors=0, skipped=0, body=None, name="p.ExampleITCase"
):
    if body is None:
        body = '<testcase classname="p.ExampleITCase" name="works"/>'
    return f'<testsuite name="{name}" tests="{tests}" failures="{failures}" errors="{errors}" skipped="{skipped}">{body}</testsuite>'


def result(reports, xml):
    tool, root, path = reports
    path.write_text(xml)
    status = tool.collect(root, GATE, [SOURCE])
    evidence = json.loads(
        (root / f"target/e2e/evidence/{GATE}/results.json").read_text()
    )
    return status, evidence[0]


def test_valid_report_passes(reports):
    status, evidence = result(reports, report())
    assert status == 0
    assert evidence["tests"] == 1
    assert evidence["status"] == "PASSED"


@pytest.mark.parametrize(
    "xml",
    [
        report()[:-12],
        report(tests=2),
        report(tests=0, body=""),
        report(name="p.DifferentITCase"),
        report().replace(
            'classname="p.ExampleITCase"', 'classname="p.DifferentITCase"'
        ),
        report().replace('tests="1"', 'tests="-1"'),
        report().replace('errors="0"', ""),
        report(failures=1),
        report(body='<testcase classname="p.ExampleITCase" name=""/>'),
        report(
            body='<testcase classname="p.ExampleITCase" name="works"><flakyFailure/></testcase>'
        ),
    ],
)
def test_invalid_report_cannot_pass(reports, xml):
    status, evidence = result(reports, xml)
    assert status == 1
    assert evidence["status"] == "INVALID"


@pytest.mark.parametrize(
    ("tag", "counts"),
    [
        ("failure", {"failures": 1}),
        ("error", {"errors": 1}),
        ("skipped", {"skipped": 1}),
    ],
)
def test_failed_and_skipped_cases_cannot_pass(reports, tag, counts):
    status, evidence = result(
        reports,
        report(
            body=f'<testcase classname="p.ExampleITCase" name="works"><{tag}/></testcase>',
            **counts,
        ),
    )
    assert status == 1
    assert evidence["status"] == "FAILED"


def test_missing_report_is_distinct_from_a_test_failure(reports):
    tool, root, _ = reports
    assert tool.collect(root, GATE, [SOURCE]) == 1
    assert (
        '"NOT_RUN"' in (root / f"target/e2e/evidence/{GATE}/results.json").read_text()
    )


def test_prepare_removes_only_selected_old_reports(reports):
    tool, root, path = reports
    path.write_text(report())
    other = path.with_name("TEST-p.Unrelated.xml")
    other.write_text("unrelated")
    tool.prepare(root, GATE, [SOURCE])
    assert not path.exists()
    assert other.read_text() == "unrelated"
    assert tool.collect(root, GATE, [SOURCE]) == 1


def test_stale_report_is_rejected_even_if_copied_back(reports):
    tool, root, path = reports
    path.write_text(report())
    os.utime(path, ns=(1, 1))
    assert tool.collect(root, GATE, [SOURCE]) == 1
    assert (
        "predates this run"
        in (root / f"target/e2e/evidence/{GATE}/results.json").read_text()
    )


def test_changed_inventory_cannot_use_previous_results(reports):
    tool, root, path = reports
    path.write_text(report())
    with pytest.raises(ValueError, match="inventory differs"):
        tool.collect(root, GATE, [SOURCE.replace("Example", "Other")])


def test_export_excludes_properties_output_and_parameter_values(reports):
    secret = "PRIVATE_SENTINEL"
    xml = report(
        failures=1,
        body=f"""<properties><property name="token" value="{secret}"/></properties>
<testcase classname="p.ExampleITCase" name="works[1, {secret}]">
<failure message="{secret}">{secret}
  at p.ExampleITCase.works(ExampleITCase.java:42)
</failure><system-out>{secret}</system-out></testcase>
<system-err>{secret}</system-err>""",
    )
    status, evidence = result(reports, xml)
    assert status == 1
    assert evidence["cases"][0]["method"] == "works"
    assert evidence["cases"][0]["frames"] == [
        "p.ExampleITCase.works(ExampleITCase.java:42)"
    ]
    _, root, _ = reports
    for path in (root / "target/e2e/evidence").rglob("*"):
        assert path.is_dir() or secret not in path.read_text()


def test_empty_inventory_is_rejected(tmp_path):
    tool = load_script("e2e-reports.py")
    with pytest.raises(ValueError, match="empty"):
        tool.prepare(tmp_path, GATE, [])


def test_freshness_uses_the_filesystem_clock(tmp_path, monkeypatch):
    tool = load_script("e2e-reports.py")
    precise_now = time.time_ns()
    monkeypatch.setattr(time, "time_ns", lambda: precise_now + 10_000_000_000)
    tool.prepare(tmp_path, GATE, [SOURCE])
    path, _ = tool.report_path(tmp_path, SOURCE)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report())
    assert tool.collect(tmp_path, GATE, [SOURCE]) == 0


def test_class_setup_failure_retains_its_outcome_and_frames(reports):
    # Shape measured with Surefire 3.2.5 and JUnit 5.14.4: @BeforeAll throws.
    status, evidence = result(
        reports,
        report(
            errors=1,
            body="""<testcase name="" classname="p.ExampleITCase" time="0.017">
<error message="setup-failed" type="java.lang.IllegalStateException"><![CDATA[
java.lang.IllegalStateException: setup-failed
    at p.ExampleITCase.setup(ExampleITCase.java:5)
]]></error></testcase>""",
        ),
    )
    assert status == 1
    assert evidence["status"] == "FAILED"
    assert evidence["cases"] == [
        {
            "method": "class_lifecycle",
            "status": "ERROR",
            "frames": ["p.ExampleITCase.setup(ExampleITCase.java:5)"],
        }
    ]


def test_application_report_is_read_from_its_own_module(tmp_path):
    tool = load_script("e2e-reports.py")
    source = "kubernetes/apps/fake/src/test/java/p/ExampleITCase.java"
    path, fqcn = tool.report_path(tmp_path, source)
    assert path == (
        tmp_path
        / "kubernetes/apps/fake/target/surefire-reports/TEST-p.ExampleITCase.xml"
    )
    assert fqcn == "p.ExampleITCase"
    tool.prepare(tmp_path, GATE, [source])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report())
    assert tool.collect(tmp_path, GATE, [source]) == 0
    evidence = json.loads(
        (tmp_path / f"target/e2e/evidence/{GATE}/results.json").read_text()
    )
    assert evidence[0]["module"] == "kubernetes/apps/fake"


@pytest.mark.parametrize(
    "source",
    [
        "kubernetes/apps/src/test/java/p/ExampleITCase.java",
        "kubernetes/apps/fake/nested/src/test/java/p/ExampleITCase.java",
    ],
)
def test_application_source_outside_the_layout_is_refused(tmp_path, source):
    tool = load_script("e2e-reports.py")
    with pytest.raises(ValueError, match="invalid gated source path"):
        tool.report_path(tmp_path, source)


@pytest.mark.parametrize("name", ["results.json", "summary.md"])
def test_prepare_drops_a_previous_combined_result(reports, name):
    tool, root, _ = reports
    combined = root / "target/e2e/evidence" / name
    combined.write_text("an earlier run's combined result")
    tool.prepare(root, GATE, [SOURCE])
    assert not combined.exists()


@pytest.mark.parametrize("gate", ["", "../X", "lower_case", "A/B"])
def test_a_gate_must_be_an_environment_variable_name(tmp_path, gate):
    tool = load_script("e2e-reports.py")
    with pytest.raises(ValueError, match="invalid gate name"):
        tool.prepare(tmp_path, gate, [SOURCE])


OTHER = "flink-connector-gcp-other/src/test/java/p/OtherITCase.java"


@pytest.fixture()
def gates(tmp_path):
    """Two gates, each prepared apart, as a matrix job per gate leaves them."""
    tool = load_script("e2e-reports.py")
    for gate, source in (("ONE", SOURCE), ("TWO", OTHER)):
        tool.prepare(tmp_path, gate, [source])
        path, fqcn = tool.report_path(tmp_path, source)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            report(
                name=fqcn,
                body=f'<testcase classname="{fqcn}" name="works"/>',
            )
        )
        assert tool.collect(tmp_path, gate, [source]) == 0
    return tool, tmp_path


def combined(root):
    return json.loads((root / "target/e2e/evidence/results.json").read_text())


INVENTORY = {"ONE": [SOURCE], "TWO": [OTHER]}


def test_aggregate_accepts_every_gate_passing(gates):
    tool, root = gates
    assert tool.aggregate(root, INVENTORY) == 0
    assert [row["status"] for row in combined(root)] == ["PASSED", "PASSED"]


def test_aggregate_fails_for_a_gate_without_evidence(gates):
    tool, root = gates
    for path in (root / "target/e2e/evidence/TWO").iterdir():
        path.unlink()
    assert tool.aggregate(root, INVENTORY) == 1
    assert combined(root)[1]["status"] == "NOT_RUN"
    assert "TWO: no evidence" in (root / "target/e2e/evidence/summary.md").read_text()


def test_one_gates_evidence_cannot_stand_in_for_another(gates, capsys):
    tool, root = gates
    (root / "target/e2e/evidence/TWO/results.json").write_text(
        (root / "target/e2e/evidence/ONE/results.json").read_text()
    )
    assert tool.aggregate(root, INVENTORY) == 1
    assert combined(root)[1]["status"] == "NOT_RUN"
    assert "TWO: evidence for p.ExampleITCase, which is not one of its classes" in (
        capsys.readouterr().err
    )


def test_a_class_reported_twice_cannot_pass(gates, capsys):
    tool, root = gates
    path = root / "target/e2e/evidence/TWO/results.json"
    rows = json.loads(path.read_text())
    path.write_text(json.dumps(rows * 2))
    assert tool.aggregate(root, INVENTORY) == 1
    assert "TWO: p.OtherITCase is reported twice" in capsys.readouterr().err


def test_aggregate_fails_for_a_gate_whose_class_failed(gates, capsys):
    tool, root = gates
    path = root / "target/e2e/evidence/TWO/results.json"
    rows = json.loads(path.read_text())
    rows[0].update(status="FAILED", problems=["failures=1"])
    path.write_text(json.dumps(rows))
    assert tool.aggregate(root, INVENTORY) == 1
    assert "p.OtherITCase: FAILED failures=1" in capsys.readouterr().err
    assert combined(root)[1]["status"] == "FAILED"


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("{", "unreadable evidence"),
        ("null", "malformed evidence"),
        ("{}", "malformed evidence"),
        ('[{"class": "p.X"}]', "malformed evidence"),
        (
            json.dumps(
                [
                    {
                        "module": "flink-connector-gcp-other",
                        "class": "p.OtherITCase",
                        "status": "PASSED",
                    }
                ]
            ),
            "malformed evidence",
        ),
    ],
)
def test_aggregate_fails_for_malformed_evidence(gates, capsys, content, message):
    tool, root = gates
    (root / "target/e2e/evidence/TWO/results.json").write_text(content)
    assert tool.aggregate(root, INVENTORY) == 1
    assert f"TWO: {message}" in capsys.readouterr().err
    assert combined(root)[1]["status"] == "NOT_RUN"


def test_aggregate_fails_for_evidence_it_cannot_read(gates, capsys):
    tool, root = gates
    path = root / "target/e2e/evidence/TWO/results.json"
    path.unlink()
    path.mkdir()
    assert tool.aggregate(root, INVENTORY) == 1
    assert "TWO: unreadable evidence" in capsys.readouterr().err


def test_the_inventory_groups_sources_by_gate():
    tool = load_script("e2e-reports.py")
    assert (
        tool.read_inventory(
            [
                f"ONE\tflink-connector-gcp-fake\t{SOURCE}",
                f"TWO\tflink-connector-gcp-other\t{OTHER}",
            ]
        )
        == INVENTORY
    )


@pytest.mark.parametrize(
    "lines",
    [
        [],
        [SOURCE],
        [f"ONE\t{SOURCE}"],
        [f"ONE\tflink-connector-gcp-other\t{SOURCE}"],
        [
            f"ONE\tflink-connector-gcp-fake\t{SOURCE}",
            f"TWO\tflink-connector-gcp-fake\t{SOURCE}",
        ],
        [f"one\tflink-connector-gcp-fake\t{SOURCE}"],
    ],
)
def test_a_malformed_inventory_is_refused(lines):
    tool = load_script("e2e-reports.py")
    with pytest.raises(ValueError):
        tool.read_inventory(lines)
