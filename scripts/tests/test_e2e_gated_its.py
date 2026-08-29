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
"""Tests for scripts/e2e-gated-its.sh.

Everything here is about a wrong green. The script's three E2E modes exist so
that a run which skipped the gated ITCases cannot report success; --check-tags
(issue #245) exists so that a gated class cannot lose the @Tag("gated") that
keeps it out of ordinary builds — a class carrying the environment gate alone
runs, and bills, in any shell where the variable happens to be set.

The tree is synthetic: a real-tree assertion would make every Java test source
an input to lint.yaml, whose paths filter has to list every input to a lint.
What that costs is coverage of the real annotations' formatting, which is
bought back by `just check-gated-tags` running over the real tree in verify.yaml.
The one exception is the root pom at the end, which the same filter already
lists for test_ci_maven_args.py's sake.
"""

import os
import subprocess
import sys
import xml.etree.ElementTree as ET

import pytest
from conftest import SCRIPTS

SCRIPT = SCRIPTS / "e2e-gated-its.sh"
POM = SCRIPTS.parent / "pom.xml"
POM_NS = {"m": "http://maven.apache.org/POM/4.0.0"}

# The gates the E2E workflow sets, and the variables --require-env demands.
E2E_GATES = (
    "BQ_IT_PROJECT",
    "PUBSUB_IT_PROJECT",
    "BIGTABLE_IT_PROJECT",
    "SPANNER_IT_PROJECT",
    "CLOUDTASKS_IT_PROJECT",
)
REQUIRED = (
    "BQ_IT_PROJECT",
    "BQ_IT_DATASET",
    "BQ_IT_GCS_BUCKET",
    "PUBSUB_IT_PROJECT",
    "BIGTABLE_IT_PROJECT",
    "SPANNER_IT_PROJECT",
    "CLOUDTASKS_IT_PROJECT",
)

REPORT = '<?xml version="1.0"?>\n<testsuite name="{fqcn}" tests="{tests}" skipped="{skipped}">\n'


@pytest.fixture()
def tree(tmp_path):
    """A synthetic module tree the script is run against, plus a runner."""

    def weekly(opt_in=True):
        """The lane that runs @Tag("slow"), or a copy of it that forgot to."""
        path = tmp_path / ".github" / "workflows" / "weekly.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        flag = " -Dtest.excluded.groups=gated" if opt_in else ""
        path.write_text(f"jobs:\n  compile_and_test:\n    run: just verify{flag}\n")
        return path

    def add(
        name,
        *,
        module="flink-connector-gcp-fake",
        gate=None,
        tag=False,
        slow=False,
        javadoc=False,
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
        if slow:
            body += ['@Tag("slow")']
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
        inherited = {
            name: os.environ[name]
            for name in ("UV_RUN_RECURSION_DEPTH", "VIRTUAL_ENV")
            if name in os.environ
        }
        return subprocess.run(
            [str(SCRIPT), *args],
            cwd=tmp_path,
            env={"PATH": os.environ["PATH"], **inherited, **(env or {})},
            capture_output=True,
            text=True,
            check=False,
        )

    run.add = add
    run.report = report
    run.weekly = weekly
    return run


def full_suite(tree, tag=True):
    """One gated class per E2E gate — what the discovery modes need to pass."""
    return [
        tree.add(f"{gate.split('_')[0].title()}ITCase", gate=gate, tag=tag)
        for gate in E2E_GATES
    ]


# --- --check-tags: the pairing (issue #245) ---


def test_paired_markers_pass(tree):
    sources = full_suite(tree)
    result = tree("--check-tags")
    assert result.returncode == 0, result.stderr
    assert str(len(sources)) in result.stdout


def test_direct_run_ignores_an_ambient_parser_and_restores_the_lock(tree, tmp_path):
    sources = full_suite(tree)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    python = bin_dir / "python3"
    python.write_text(
        "#!/bin/sh\n"
        '[ "$1" = -c ] && exit 0\n'
        'echo "ambient python was used" >&2\n'
        "exit 97\n"
    )
    python.chmod(0o755)
    mise = bin_dir / "mise"
    mise.write_text(
        "#!/bin/sh\n"
        'while [ "$#" -gt 0 ] && [ "$1" != python ]; do shift; done\n'
        '[ "$1" = python ] || exit 99\n'
        "shift\n"
        'exec "$REAL_PYTHON" "$@"\n'
    )
    mise.chmod(0o755)

    result = tree(
        "--check-tags",
        env={
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "REAL_PYTHON": sys.executable,
            "UV_RUN_RECURSION_DEPTH": "1",
            "VIRTUAL_ENV": "/ambient",
        },
    )

    assert result.returncode == 0, result.stderr
    assert str(len(sources)) in result.stdout

    tree.add("UntaggedITCase", gate="BQ_IT_PROJECT")
    result = tree(
        "--check-tags",
        env={
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "REAL_PYTHON": sys.executable,
            "UV_RUN_RECURSION_DEPTH": "1",
            "VIRTUAL_ENV": "/ambient",
        },
    )
    assert result.returncode == 1
    assert "UntaggedITCase.java" in result.stderr


def test_uv_run_uses_its_parser_without_mise(tree, tmp_path):
    sources = full_suite(tree)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    python = bin_dir / "python3"
    python.write_text('#!/bin/sh\nexec "$REAL_PYTHON" "$@"\n')
    python.chmod(0o755)

    result = tree(
        "--check-tags",
        env={
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "REAL_PYTHON": sys.executable,
            "UV_RUN_RECURSION_DEPTH": "1",
            "VIRTUAL_ENV": str(SCRIPTS.parent / ".venv"),
        },
    )

    assert result.returncode == 0, result.stderr
    assert str(len(sources)) in result.stdout


def test_comments_inside_annotation_arguments_do_not_hide_the_markers(tree):
    sources = full_suite(tree)
    source = sources[0]
    text = source.read_text()
    text = text.replace('@Tag("gated")', '@Tag(/* category */ "gated")')
    text = text.replace(
        'named = "BQ_IT_PROJECT"',
        'named = /* environment */ "BQ_IT_PROJECT"',
    )
    source.write_text(text)

    result = tree("--check-tags")

    assert result.returncode == 0, result.stderr


def test_slow_tag_with_its_lane_passes(tree):
    full_suite(tree)
    tree.weekly()
    tree.add("SlowITCase", slow=True)
    result = tree("--check-tags")
    assert result.returncode == 0, result.stderr
    assert "slow classes the weekly lane runs: 1" in result.stdout


def test_slow_tag_without_its_lane_fails(tree):
    """The failure this whole tag needs a check for.

    Nothing else notices: the pom excludes "slow" everywhere, so dropping the
    weekly opt-in leaves the classes running in no lane at all, with every
    workflow still green.
    """
    full_suite(tree)
    tree.weekly(opt_in=False)
    tree.add("SlowITCase", slow=True)
    result = tree("--check-tags")
    assert result.returncode == 1
    assert "no longer passes -Dtest.excluded.groups=gated" in result.stderr
    assert "SlowITCase" in result.stderr


def test_the_lane_without_any_slow_tag_fails(tree):
    """The mirror: a flag that selects nothing would pass vacuously forever."""
    full_suite(tree)
    tree.weekly()
    result = tree("--check-tags")
    assert result.returncode == 1
    assert "no test class carries" in result.stderr


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
    """Discovery that finds no gated class must not read as "nothing to fix"."""
    tree.add("PlainITCase")
    result = tree("--check-tags")
    assert result.returncode == 1
    # Asserted on the message, not just the status: without the guard the empty
    # match falls through the loop below and still exits 1, naming no file.
    assert "vacuously" in result.stderr


def test_java_syntax_error_is_an_infrastructure_failure(tree):
    sources = full_suite(tree)
    sources[0].write_text("class Broken {")

    result = tree("--check-tags")

    assert result.returncode == 2
    assert "Java parser produced" in result.stderr


# --- the E2E modes, which now share one discovery function ---


def test_default_mode_prints_the_class_list(tree):
    sources = full_suite(tree)
    tree.add("PlainITCase")
    result = tree()
    assert result.returncode == 0, result.stderr
    # One name per gate and nothing else — PlainITCase carries no gate, so a
    # discovery that widened to every *ITCase would show up right here.
    assert sorted(result.stdout.strip().split(",")) == sorted(
        source.stem for source in sources
    )


def test_default_mode_is_fatal_when_a_gate_matches_nothing(tree):
    tree.add("BqITCase", gate="BQ_IT_PROJECT", tag=True)
    result = tree()
    assert result.returncode == 1
    assert "PUBSUB_IT_PROJECT" in result.stderr


def test_for_gate_prints_only_the_selected_classes(tree):
    sources = full_suite(tree)

    result = tree("--for-gate", "CLOUDTASKS_IT_PROJECT")

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == sources[-1].stem


def test_except_gate_omits_only_the_selected_classes(tree):
    sources = full_suite(tree)

    result = tree("--except-gate", "CLOUDTASKS_IT_PROJECT")

    assert result.returncode == 0, result.stderr
    assert sorted(result.stdout.strip().split(",")) == sorted(
        source.stem for source in sources[:-1]
    )


@pytest.mark.parametrize("mode", ["--for-gate", "--except-gate"])
def test_gate_selector_rejects_an_unknown_gate(tree, mode):
    full_suite(tree)

    result = tree(mode, "UNKNOWN_GATE")

    assert result.returncode == 2
    assert "usage:" in result.stderr


@pytest.mark.parametrize("mode", ["--for-gate", "--except-gate"])
def test_unknown_gate_is_diagnosed_before_parser_restoration(tree, mode):
    result = tree(mode, "BQ_IT_PROJEKT", env={"PATH": "/usr/bin:/bin"})

    assert result.returncode == 2
    assert "usage:" in result.stderr
    assert "parser is unavailable" not in result.stderr


def test_shell_gate_selector_mirrors_the_python_inventory(tree, check_gated_tags):
    full_suite(tree)
    assert E2E_GATES == check_gated_tags.E2E_GATES

    for gate in check_gated_tags.E2E_GATES:
        result = tree("--for-gate", gate)
        assert result.returncode == 0, result.stderr


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
    # Both tags, and "gated" first: weekly.yaml narrows this to exactly "gated"
    # to opt the "slow" classes back in, so the two are not interchangeable and
    # a reordering here would leave that flag meaning something else.
    excluded_tags = values["test.excluded.groups"].split(",")
    assert excluded_tags == ["gated", "slow"]

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
