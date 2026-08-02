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
"""Tests for scripts/ci-maven-args.py (issues #243, #253).

Two layers on purpose. The synthetic-repo tests pin the derivation rules —
classification, the two-phase closure, NOTICE detection — against a pom tree
built in tmp_path, so they cannot rot when this repository gains a module.
The real-repo CLI tests run the script exactly as ci.yaml does and assert
against the modules that exist today; adding a module is expected to touch
them, which is the point — they are what notices the wiring, not the rules.
"""

import json
import subprocess

import pytest
from conftest import SCRIPTS

MODULES = [
    "flink-connector-gcp-test-utils",
    "flink-connector-gcp-base",
    "flink-connector-gcp-bigquery",
    "flink-connector-gcp-pubsub",
    "flink-sql-connector-gcp-pubsub",
    "flink-connector-gcp-cloudtasks",
    "flink-connector-gcp-bigtable",
]

POM = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <artifactId>{artifact}</artifactId>
    {body}
</project>
"""


def write_pom(root, artifact, modules=(), deps=()):
    body = ""
    if modules:
        body += (
            "<modules>"
            + "".join(f"<module>{m}</module>" for m in modules)
            + "</modules>"
        )
    if deps:
        body += (
            "<dependencies>"
            + "".join(
                f"<dependency><groupId>io.github.flink-gcp</groupId>"
                f"<artifactId>{d}</artifactId></dependency>"
                for d in deps
            )
            + "</dependencies>"
        )
    directory = root if artifact == "root" else root / artifact
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "pom.xml").write_text(POM.format(artifact=artifact, body=body))


@pytest.fixture()
def fake_repo(tmp_path, ci_maven_args, monkeypatch):
    """A minimal reactor: base <- {a, b}, b <- shaded (with NOTICE.template)."""
    write_pom(tmp_path, "root", modules=["base", "a", "b", "shaded"])
    write_pom(tmp_path, "base")
    write_pom(tmp_path, "a", deps=["base"])
    write_pom(tmp_path, "b", deps=["base"])
    write_pom(tmp_path, "shaded", deps=["b"])
    (tmp_path / "shaded" / "NOTICE.template").write_text("")
    monkeypatch.setattr(ci_maven_args, "ROOT", tmp_path)
    return tmp_path


# --- classification rules, on the synthetic repo ---


def classify(mod, files):
    modules = mod.pom_modules()
    return mod.classify(files, modules)


@pytest.mark.parametrize(
    "path",
    [
        "README.md",
        "a/README.md",
        "a/deep/CLAUDE.md",
        "opentofu/main.tf",
        "tfaction-root.yaml",
        ".github/workflows/tofu-plan.yaml",
        ".github/workflows/tofu-apply.yaml",
        ".github/workflows/tofu-list.yaml",
        ".github/PULL_REQUEST_TEMPLATE.md",
        ".github/ISSUE_TEMPLATE/bug.md",
        ".github/CODEOWNERS",
        ".github/dependabot.yml",
    ],
)
def test_ignored_paths_select_nothing(fake_repo, ci_maven_args, path):
    ignored, selected, root_only, everything = classify(ci_maven_args, [path])
    assert ignored == [path]
    assert not selected and not root_only and not everything


@pytest.mark.parametrize(
    "path", [".github/workflows/ci.yaml", ".github/actions/setup/action.yml"]
)
def test_what_drives_ci_stays_unknown_territory(fake_repo, ci_maven_args, path):
    # The two directories under .github/ that decide what CI itself does are
    # the exception to the rule above: a change there is unknown territory.
    _, selected, root_only, everything = classify(ci_maven_args, [path])
    assert not selected and not root_only
    assert everything == [path]


def test_module_file_selects_its_module(fake_repo, ci_maven_args):
    _, selected, _, _ = classify(ci_maven_args, ["a/src/main/java/X.java"])
    assert selected == {"a"}


def test_module_readme_is_ignored_before_module_matching(fake_repo, ci_maven_args):
    ignored, selected, _, _ = classify(ci_maven_args, ["a/README.md"])
    assert ignored and not selected


@pytest.mark.parametrize(
    "path",
    [
        "docs/content/x.md",
        # Nothing Maven builds reads these, but the root module's rat run
        # scans them, so they buy `-pl .` rather than nothing (#253).
        "scripts/ci-maven-args.py",
        "scripts/tests/test_ci_gate.py",
        "scripts/option-docs.toml",
        "pyproject.toml",
        "uv.lock",
    ],
)
def test_root_only_paths_buy_the_rat_check(fake_repo, ci_maven_args, path):
    _, selected, root_only, everything = classify(ci_maven_args, [path])
    assert not selected and not everything
    assert root_only == [path]


@pytest.mark.parametrize(
    "path", ["scripts/licence-sources.toml", "scripts/check-notice.py"]
)
def test_the_notice_checkers_inputs_stay_full_reactor(fake_repo, ci_maven_args, path):
    # The check they feed is a step *inside* the build job, gated on
    # check_notice — which is false for `-pl .`, since no shaded module is
    # built. Routing these to the root-only class would skip the licence check
    # on exactly the change that edits the licence pins.
    _, selected, root_only, everything = classify(ci_maven_args, [path])
    assert not selected and not root_only
    assert everything == [path]


def test_root_only_and_unknown_paths_classify_apart(fake_repo, ci_maven_args):
    _, selected, root_only, everything = classify(
        ci_maven_args, ["docs/content/x.md", "justfile", "pom.xml"]
    )
    assert not selected
    assert root_only == ["docs/content/x.md"]
    # The justfile carries the Maven invocations themselves, so it is unknown
    # territory on purpose rather than a scripts-like sibling.
    assert sorted(everything) == ["justfile", "pom.xml"]


def test_bare_path_equal_to_module_name_is_not_the_module(fake_repo, ci_maven_args):
    # No trailing slash: a *file* named like a module directory is unknown
    # territory and must force the full reactor, not select the module.
    _, selected, _, everything = classify(ci_maven_args, ["a"])
    assert not selected
    assert everything == ["a"]


# --- the two-phase closure, on the synthetic repo ---


def close(mod, changed):
    modules = mod.pom_modules()
    return mod.close_over(set(changed), mod.module_dependencies(modules))


def test_dependents_join_transitively(fake_repo, ci_maven_args):
    # b changed: shaded consumes it; base rides along for reactor resolution.
    assert close(ci_maven_args, ["b"]) == {"base", "b", "shaded"}


def test_ride_along_dependencies_drag_no_dependents(fake_repo, ci_maven_args):
    # a changed: base rides along, but base's other dependents (b, shaded)
    # must NOT join — base did not change.
    assert close(ci_maven_args, ["a"]) == {"base", "a"}


def test_changed_dependency_fans_out_to_all_dependents(fake_repo, ci_maven_args):
    assert close(ci_maven_args, ["base"]) == {"base", "a", "b", "shaded"}


def test_a_stale_notice_input_stops_the_run(
    fake_repo, ci_maven_args, monkeypatch, capsys
):
    # The one allowlist here whose staleness is unsafe: a renamed entry stops
    # matching, its path rejoins the root-only class, and the licence check
    # quietly stops running on licence changes. So every run refuses to
    # proceed — asserted through main(), because the guard is only worth
    # anything if it is wired into it. (ROOT_ONLY_FILES needs no such guard:
    # a stale entry there over-builds, which announces itself in the clock.)
    # fake_repo has no scripts/ at all, which is the stale state.
    monkeypatch.setattr("sys.argv", ["ci-maven-args.py", "--files", "[]"])
    with pytest.raises(SystemExit) as error:
        ci_maven_args.main()
    assert error.value.code == 2
    assert "NOTICE_INPUTS names" in capsys.readouterr().err


def test_the_real_notice_inputs_all_exist(ci_maven_args):
    for path in ci_maven_args.NOTICE_INPUTS:
        assert (ci_maven_args.ROOT / path).is_file(), path


def test_unknown_flink_gcp_dependency_fails(fake_repo, ci_maven_args):
    write_pom(fake_repo, "a", deps=["no-such-module"])
    with pytest.raises(SystemExit) as e:
        ci_maven_args.module_dependencies(ci_maven_args.pom_modules())
    assert e.value.code == 1


# --- CLI against the real repository, exactly as ci.yaml runs it ---


def run_cli(*args):
    return subprocess.run(
        [str(SCRIPTS / "ci-maven-args.py"), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def outputs(result):
    assert result.returncode == 0, result.stderr
    return dict(line.split("=", 1) for line in result.stdout.splitlines())


def test_full_mode_builds_everything(ci_maven_args):
    out = outputs(run_cli("--full"))
    assert out == {"run_build": "true", "maven_args": "", "check_notice": "true"}


def test_one_connector_builds_its_slice(ci_maven_args):
    out = outputs(
        run_cli("--files", json.dumps(["flink-connector-gcp-bigtable/src/X.java"]))
    )
    assert out["maven_args"] == (
        "-pl .,flink-connector-gcp-test-utils,flink-connector-gcp-base,"
        "flink-connector-gcp-bigtable"
    )
    assert out["check_notice"] == "false"


def test_pubsub_pulls_the_sql_uber_jar_and_its_notice(ci_maven_args):
    out = outputs(
        run_cli("--files", json.dumps(["flink-connector-gcp-pubsub/pom.xml"]))
    )
    assert "flink-sql-connector-gcp-pubsub" in out["maven_args"]
    assert out["check_notice"] == "true"


def test_base_change_collapses_to_the_full_reactor(ci_maven_args):
    out = outputs(
        run_cli("--files", json.dumps(["flink-connector-gcp-base/src/X.java"]))
    )
    assert out["maven_args"] == ""
    assert out["check_notice"] == "true"


@pytest.mark.parametrize(
    "files",
    [
        ["docs/content/docs/x.md"],
        ["scripts/tests/test_ci_gate.py"],
        ["scripts/ci-maven-args.py", "pyproject.toml", "uv.lock"],
        # Mixed: docs and scripts are one class, not two that collide.
        ["docs/content/docs/x.md", "scripts/lint-something.sh"],
    ],
)
def test_root_only_changes_build_the_root_rat_check(ci_maven_args, files):
    out = outputs(run_cli("--files", json.dumps(files)))
    assert out == {"run_build": "true", "maven_args": "-pl .", "check_notice": "false"}


def test_a_root_only_path_does_not_swallow_a_module_selection(ci_maven_args):
    out = outputs(
        run_cli(
            "--files",
            json.dumps(["scripts/tests/test_ci_gate.py", "flink-connector-gcp-base/x"]),
        )
    )
    assert out["maven_args"] == ""  # base fans out to everything
    assert out["check_notice"] == "true"


def test_a_licence_pin_change_still_runs_the_notice_check(ci_maven_args):
    # The regression this class's boundary exists to prevent (#253): the NOTICE
    # check runs inside the build job behind check_notice, so a licence-pin
    # change must keep the reactor that makes it true.
    out = outputs(run_cli("--files", json.dumps(["scripts/licence-sources.toml"])))
    assert out == {"run_build": "true", "maven_args": "", "check_notice": "true"}


def test_ignored_only_skips_the_build(ci_maven_args):
    out = outputs(run_cli("--files", json.dumps(["opentofu/main.tf", "README.md"])))
    assert out == {"run_build": "false", "maven_args": "", "check_notice": "false"}


def test_real_module_list_matches_this_repository(ci_maven_args):
    assert ci_maven_args.pom_modules() == MODULES


def test_malformed_json_is_an_infrastructure_error():
    assert run_cli("--files", "[not json").returncode == 2


def test_modes_are_mutually_exclusive():
    assert run_cli("--files", "[]", "--full").returncode == 2
