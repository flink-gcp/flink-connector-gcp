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
"""Tests for scripts/ci-maven-args.py (issue #243).

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
    ],
)
def test_ignored_paths_select_nothing(fake_repo, ci_maven_args, path):
    ignored, selected, docs, everything = classify(ci_maven_args, [path])
    assert ignored == [path]
    assert not selected and not docs and not everything


def test_module_file_selects_its_module(fake_repo, ci_maven_args):
    _, selected, _, _ = classify(ci_maven_args, ["a/src/main/java/X.java"])
    assert selected == {"a"}


def test_module_readme_is_ignored_before_module_matching(fake_repo, ci_maven_args):
    ignored, selected, _, _ = classify(ci_maven_args, ["a/README.md"])
    assert ignored and not selected


def test_docs_and_unknown_paths_classify_apart(fake_repo, ci_maven_args):
    _, selected, docs, everything = classify(
        ci_maven_args, ["docs/content/x.md", "justfile", "pom.xml"]
    )
    assert not selected
    assert docs == ["docs/content/x.md"]
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


def test_docs_only_builds_the_root_rat_check(ci_maven_args):
    out = outputs(run_cli("--files", json.dumps(["docs/content/docs/x.md"])))
    assert out == {"run_build": "true", "maven_args": "-pl .", "check_notice": "false"}


def test_ignored_only_skips_the_build(ci_maven_args):
    out = outputs(run_cli("--files", json.dumps(["opentofu/main.tf", "README.md"])))
    assert out == {"run_build": "false", "maven_args": "", "check_notice": "false"}


def test_real_module_list_matches_this_repository(ci_maven_args):
    assert ci_maven_args.pom_modules() == MODULES


def test_malformed_json_is_an_infrastructure_error():
    assert run_cli("--files", "[not json").returncode == 2


def test_modes_are_mutually_exclusive():
    assert run_cli("--files", "[]", "--full").returncode == 2
