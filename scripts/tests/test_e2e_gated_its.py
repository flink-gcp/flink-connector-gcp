# Copyright 2026 laughingman7743
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
"""Tests for scripts/e2e-gated-its.sh.

Everything here is about a wrong green. The script's three E2E modes exist so
that a run which skipped the gated ITCases cannot report success; --check-tags
(issue #245) exists so that a gated class cannot lose the @Tag("gated") that
keeps it out of ordinary builds — a class carrying the environment gate alone
runs, and bills, in any shell where the variable happens to be set.

The tree is synthetic: a real-tree assertion would make every Java test source
an input to lint.yaml, whose paths filter has to list every input to a lint.
What that costs is coverage of the real annotations' formatting, which is
bought back by `just check-gated-tags` running over the real tree in ci.yaml.
The one exception is the root pom at the end, which the same filter already
lists for test_ci_maven_args.py's sake.
"""

import os
import subprocess
import xml.etree.ElementTree as ET

import pytest
from conftest import SCRIPTS

SCRIPT = SCRIPTS / "e2e-gated-its.sh"
POM = SCRIPTS.parent / "pom.xml"
POM_NS = {"m": "http://maven.apache.org/POM/4.0.0"}

# The gates the E2E workflow sets, and the variables --require-env demands.
E2E_GATES = ("BQ_IT_PROJECT", "PUBSUB_IT_PROJECT", "BIGTABLE_IT_PROJECT")
REQUIRED = (
    "BQ_IT_PROJECT",
    "BQ_IT_DATASET",
    "BQ_IT_GCS_BUCKET",
    "PUBSUB_IT_PROJECT",
    "BIGTABLE_IT_PROJECT",
)

REPORT = '<?xml version="1.0"?>\n<testsuite name="{fqcn}" tests="{tests}" skipped="{skipped}">\n'


@pytest.fixture()
def tree(tmp_path):
    """A synthetic module tree the script is run against, plus a runner."""

    def add(
        name, *, module="flink-connector-gcp-fake", gate=None, tag=False, javadoc=False
    ):
        source = tmp_path / module / "src" / "test" / "java" / "p" / f"{name}.java"
        source.parent.mkdir(parents=True, exist_ok=True)
        body = ["package p;", ""]
        if javadoc:
            # How the base classes and RealBigQuery discuss the annotation:
            # named in prose, with no argument list. Must not read as a gate.
            body += [
                "/** The {@code @EnabledIfEnvironmentVariable} gate lives on subclasses. */"
            ]
        if tag:
            body += ['@Tag("gated")']
        if gate:
            body += [f'@EnabledIfEnvironmentVariable(named = "{gate}", matches = ".+")']
        body += [f"class {name} {{}}", ""]
        source.write_text("\n".join(body))
        return source

    def report(
        name, *, module="flink-connector-gcp-fake", tests=1, skipped=0, truncated=False
    ):
        path = tmp_path / module / "target" / "surefire-reports" / f"TEST-p.{name}.xml"
        path.parent.mkdir(parents=True, exist_ok=True)
        text = REPORT.format(fqcn=f"p.{name}", tests=tests, skipped=skipped)
        path.write_text('<?xml version="1.0"?>\n<testsuite' if truncated else text)
        return path

    def run(*args, env=None):
        return subprocess.run(
            [str(SCRIPT), *args],
            cwd=tmp_path,
            env={"PATH": os.environ["PATH"], **(env or {})},
            capture_output=True,
            text=True,
            check=False,
        )

    run.add = add
    run.report = report
    return run


def full_suite(tree, tag=True):
    """One gated class per E2E gate — what the discovery modes need to pass."""
    return [
        tree.add(f"{gate.split('_')[0].title()}ITCase", gate=gate, tag=tag)
        for gate in E2E_GATES
    ]


# --- --check-tags: the pairing (issue #245) ---


def test_paired_markers_pass(tree):
    full_suite(tree)
    result = tree("--check-tags")
    assert result.returncode == 0, result.stderr
    assert "3" in result.stdout


def test_environment_gate_without_tag_fails(tree):
    full_suite(tree)
    tree.add("UntaggedITCase", gate="BQ_IT_PROJECT")
    result = tree("--check-tags")
    assert result.returncode == 1
    assert "UntaggedITCase.java" in result.stderr
    # The message has to say what to do, since no skill documents this one.
    assert '@Tag("gated")' in result.stderr


def test_tag_without_environment_gate_fails(tree):
    full_suite(tree)
    tree.add("StrayTagITCase", tag=True)
    result = tree("--check-tags")
    assert result.returncode == 1
    assert "StrayTagITCase.java" in result.stderr


def test_every_offender_is_named(tree):
    """Not just the first: a sweep that fixes one at a time is the failure."""
    full_suite(tree)
    tree.add("FirstITCase", gate="BQ_IT_PROJECT")
    tree.add(
        "SecondITCase", module="flink-connector-gcp-other", gate="PUBSUB_IT_PROJECT"
    )
    tree.add("ThirdITCase", tag=True)
    result = tree("--check-tags")
    assert result.returncode == 1
    for name in ("FirstITCase", "SecondITCase", "ThirdITCase"):
        assert f"{name}.java" in result.stderr


def test_javadoc_mention_is_not_a_gate(tree):
    """The abstract base classes discuss the annotation; that is not a gate."""
    full_suite(tree)
    tree.add("AbstractRealGcpITCase", javadoc=True)
    assert tree("--check-tags").returncode == 0


def test_a_gate_outside_the_e2e_variables_still_needs_the_tag(tree):
    """BQ_IT_SCHEMA_EVOLUTION: outside the suite, ~2 h against the service."""
    full_suite(tree)
    tree.add("ProbeITCase", gate="BQ_IT_SCHEMA_EVOLUTION")
    result = tree("--check-tags")
    assert result.returncode == 1
    assert "ProbeITCase.java" in result.stderr

    tree.add("ProbeITCase", gate="BQ_IT_SCHEMA_EVOLUTION", tag=True)
    assert tree("--check-tags").returncode == 0


def test_no_gated_class_at_all_is_fatal(tree):
    """A grep that has stopped matching must not read as "nothing to fix"."""
    tree.add("PlainITCase")
    result = tree("--check-tags")
    assert result.returncode == 1
    # Asserted on the message, not just the status: without the guard the empty
    # match falls through the loop below and still exits 1, naming no file.
    assert "vacuously" in result.stderr


# --- the E2E modes, which now share one discovery function ---


def test_default_mode_prints_the_class_list(tree):
    full_suite(tree)
    tree.add("PlainITCase")
    result = tree()
    assert result.returncode == 0, result.stderr
    assert sorted(result.stdout.strip().split(",")) == [
        "BigtableITCase",
        "BqITCase",
        "PubsubITCase",
    ]


def test_default_mode_is_fatal_when_a_gate_matches_nothing(tree):
    tree.add("BqITCase", gate="BQ_IT_PROJECT", tag=True)
    result = tree()
    assert result.returncode == 1
    assert "PUBSUB_IT_PROJECT" in result.stderr


def test_require_env_accepts_a_complete_environment(tree):
    full_suite(tree)
    assert tree("--require-env", env=dict.fromkeys(REQUIRED, "x")).returncode == 0


@pytest.mark.parametrize("missing", REQUIRED)
def test_require_env_names_the_missing_variable(tree, missing):
    full_suite(tree)
    env = dict.fromkeys(REQUIRED, "x")
    del env[missing]
    result = tree("--require-env", env=env)
    assert result.returncode == 1
    assert missing in result.stderr


def test_assert_ran_accepts_reports_that_show_tests_running(tree):
    for source in full_suite(tree):
        tree.report(source.stem)
    result = tree("--assert-ran")
    assert result.returncode == 0, result.stderr


def test_assert_ran_rejects_a_missing_report(tree):
    sources = full_suite(tree)
    for source in sources[1:]:
        tree.report(source.stem)
    result = tree("--assert-ran")
    assert result.returncode == 1
    assert sources[0].stem in result.stderr


@pytest.mark.parametrize(
    ("tests", "skipped"),
    [
        (0, 0),  # nothing ran
        (1, 1),  # the environment gate skipped it: lost credentials
    ],
)
def test_assert_ran_rejects_a_report_that_did_not_run(tree, tests, skipped):
    for source in full_suite(tree):
        tree.report(source.stem, tests=tests, skipped=skipped)
    assert tree("--assert-ran").returncode == 1


def test_assert_ran_rejects_a_truncated_report(tree):
    for source in full_suite(tree):
        tree.report(source.stem, truncated=True)
    result = tree("--assert-ran")
    assert result.returncode == 1
    assert "truncated" in result.stderr


def test_unknown_mode_is_a_usage_error(tree):
    full_suite(tree)
    result = tree("--nonsense")
    assert result.returncode == 2
    assert "--check-tags" in result.stderr


# --- the switch the tag only means something through ---


def test_the_build_excludes_the_gated_tag_by_default():
    """Deleting this wiring is silent, and everything above would stay green.

    --check-tags cannot see it: both markers would still be on every class
    while the tag stopped excluding anything, so the suites would quietly run
    again in any shell holding the variables. Read from the real pom, which
    lint.yaml's paths already list.
    """
    root = ET.parse(POM).getroot()
    properties = root.find("m:properties", POM_NS)
    values = {
        el.tag.split("}")[-1]: (el.text or "").strip()
        for el in properties  # type: ignore[union-attr]
    }
    assert values["test.excluded.groups"] == "gated"

    surefire = [
        plugin
        for plugin in root.findall("m:build/m:plugins/m:plugin", POM_NS)
        if plugin.findtext("m:artifactId", "", POM_NS) == "maven-surefire-plugin"
    ]
    assert len(surefire) == 1
    # Plugin level, not execution level: Maven merges this into *every*
    # execution, so -Dtest= cannot pull a gated class through default-test.
    excluded = surefire[0].findtext("m:configuration/m:excludedGroups", "", POM_NS)
    assert excluded.strip() == "${test.excluded.groups}"
