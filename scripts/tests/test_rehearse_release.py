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
"""Synthetic coverage for scripts/rehearse-release.py: the bundle inspection
directly, and the rehearsal in a scratch repository with a fake `just`."""

import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from conftest import SCRIPTS, load_script

GROUP = "io/github/flink-gcp"


@pytest.fixture(scope="session")
def rehearsal():
    return load_script("rehearse-release.py")


def published(artifact_id, version, *classifiers, signed=True):
    """The entries plugin 0.12.0 bundles for one artifact: files, checksums, signatures."""
    directory = f"{GROUP}/{artifact_id}/{version}"
    names = [f"{directory}/{artifact_id}-{version}.pom"]
    names += [f"{directory}/{artifact_id}-{version}{c}.jar" for c in classifiers]
    entries = []
    for name in names:
        entries.append((name, 1000))
        entries += [(name + s, 32) for s in (".md5", ".sha1", ".sha256", ".sha512")]
        if signed:
            entries.append((name + ".asc", 853))
    return entries


def bundle(version, signed=True):
    return published("flink-connector-gcp-parent", version, signed=signed) + published(
        "flink-connector-gcp-base", version, "", "-sources", "-javadoc", signed=signed
    )


def errors(rehearsal, version, entries, unsigned=False):
    return rehearsal.inspect(version, entries, unsigned).errors


class TestInspect:
    def test_a_well_formed_bundle_passes_and_counts_every_file(self, rehearsal):
        entries = bundle("1.3.0") + [(f"{GROUP}/", 0)]
        result = rehearsal.inspect("1.3.0", entries, unsigned=False)
        assert result.errors == []
        # Five published files, each with four checksums and a signature.
        assert result.files == 5 * 6
        assert result.bytes == 5 * (1000 + 4 * 32 + 853)

    @pytest.mark.parametrize(
        "name",
        [
            # What Maven 3.10.0 left in the v1.2.0 bundle (#1812), at both levels.
            f"{GROUP}/flink-connector-gcp-base/maven-metadata-local.xml",
            f"{GROUP}/flink-connector-gcp-base/1.3.0/_remote.repositories",
            f"{GROUP}/flink-connector-gcp-base/1.3.0/_remote.repositories.sha1",
            f"{GROUP}/flink-connector-gcp-base/maven-metadata-central-staging.xml.md5",
            f"{GROUP}/.locks/artifact.lock",
            f"{GROUP}/.locks/nested/artifact.lock",
        ],
    )
    def test_local_repository_files_are_rejected_once(self, rehearsal, name):
        # Signed, so the signature check runs, and an artifactId-level file would
        # also fail the directory check: neither reports it a second time.
        found = errors(rehearsal, "1.3.0", bundle("1.3.0") + [(name, 10)])
        assert found == [f"local-repository files that Central rejects: {name}"]

    def test_an_artifact_level_file_is_a_directory_without_a_pom(self, rehearsal):
        # Central's own message for #1812 named each module at its artifactId level.
        name = f"{GROUP}/flink-connector-gcp-base/notes.txt"
        found = errors(
            rehearsal, "1.3.0", bundle("1.3.0") + [(name, 10)], unsigned=True
        )
        assert (
            f"'{GROUP}/flink-connector-gcp-base' holds files but is not a "
            "<artifactId>/1.3.0 directory"
        ) in found

    def test_a_version_directory_without_its_pom_is_rejected(self, rehearsal):
        entries = [
            (n, s)
            for n, s in bundle("1.3.0")
            if not n.startswith(
                f"{GROUP}/flink-connector-gcp-base/1.3.0/flink-connector-gcp-base-1.3.0.pom"
            )
        ]
        found = errors(rehearsal, "1.3.0", entries)
        assert found == [
            f"'{GROUP}/flink-connector-gcp-base/1.3.0' has no flink-connector-gcp-base-1.3.0.pom"
        ]

    def test_a_pom_named_for_another_artifact_does_not_count(self, rehearsal):
        directory = f"{GROUP}/flink-connector-gcp-base/1.3.0"
        entries = [(f"{directory}/flink-connector-gcp-parent-1.3.0.pom", 10)]
        found = errors(rehearsal, "1.3.0", entries, unsigned=True)
        assert found == [f"'{directory}' has no flink-connector-gcp-base-1.3.0.pom"]

    def test_the_other_version_line_is_rejected(self, rehearsal):
        # The 2.x bundle checked as the 1.20 line, or the reverse.
        found = errors(rehearsal, "1.3.0-1.20", bundle("1.3.0"))
        assert len(found) == 2
        assert all("is not a <artifactId>/1.3.0-1.20 directory" in e for e in found)

    def test_a_shallow_path_is_not_a_maven_layout(self, rehearsal):
        found = errors(
            rehearsal, "1.3.0", bundle("1.3.0") + [("stray.jar", 1)], unsigned=True
        )
        assert "'/' is not a <group>/<artifactId>/<version> directory" in found

    def test_a_groupless_artifact_is_not_a_maven_layout(self, rehearsal):
        entries = [("base/1.3.0/base-1.3.0.pom", 1)]
        assert errors(rehearsal, "1.3.0", entries, unsigned=True) == [
            "'base/1.3.0' is not a <group>/<artifactId>/<version> directory"
        ]

    def test_a_one_segment_group_is_a_maven_layout(self, rehearsal):
        entries = [("group/base/1.3.0/base-1.3.0.pom", 1)]
        assert errors(rehearsal, "1.3.0", entries, unsigned=True) == []

    def test_an_empty_bundle_fails(self, rehearsal):
        assert errors(rehearsal, "1.3.0", [(f"{GROUP}/", 0)]) == [
            "the bundle holds no files"
        ]

    def test_unsigned_files_fail_unless_allowed(self, rehearsal):
        entries = bundle("1.3.0", signed=False)
        found = errors(rehearsal, "1.3.0", entries)
        assert len(found) == 1
        assert found[0].startswith("5 files without a .asc signature")
        assert errors(rehearsal, "1.3.0", entries, unsigned=True) == []

    def test_one_missing_signature_is_found(self, rehearsal):
        jar = f"{GROUP}/flink-connector-gcp-base/1.3.0/flink-connector-gcp-base-1.3.0-sources.jar"
        entries = [(n, s) for n, s in bundle("1.3.0") if n != jar + ".asc"]
        hint = "a rehearsal without the release key passes --unsigned"
        assert errors(rehearsal, "1.3.0", entries) == [
            f"1 files without a .asc signature, first {jar}; {hint}"
        ]


class TestReport:
    def test_totals_are_decimal_megabytes_of_uncompressed_bytes(self, rehearsal):
        first = rehearsal.Inspection("1.2.0", files=318, bytes=389_120_000)
        second = rehearsal.Inspection("1.2.0-1.20", files=318, bytes=389_030_000)
        table = rehearsal.report([first, second], unsigned=False)
        assert table.splitlines() == [
            "| Version line | Files | Size | Bytes |",
            "|---|---:|---:|---:|",
            "| `1.2.0` | 318 | 389.12 MB | 389,120,000 |",
            "| `1.2.0-1.20` | 318 | 389.03 MB | 389,030,000 |",
            "| Total | 636 | 778.15 MB | 778,150,000 |",
        ]

    def test_an_unsigned_rehearsal_says_its_counts_lack_signatures(self, rehearsal):
        table = rehearsal.report([rehearsal.Inspection("1.3.0")], unsigned=True)
        assert table.endswith("a release adds one `.asc` per signed file.\n")


# The fake `just stage-release <version> ...` records its arguments, cleans
# target/ as `clean deploy` does, re-versions both POMs as versions:set would,
# and writes a bundle for the version. The FAKE_* variables name a version to
# misbehave for: FAKE_LOCAL adds a local-repository file, FAKE_NO_BUNDLE writes
# none and cleans nothing (as a reactor without the root project), FAKE_FAIL exits 7, FAKE_LOCK leaves a git index lock behind, and
# FAKE_HANG waits for a signal, recording a SIGTERM before it exits.
# FAKE_UNSIGNED (any value) writes no signatures.
FAKE_JUST = """
import json, os, shutil, signal, sys, zipfile
with open(os.environ["CALLS"], "a") as f:
    f.write(json.dumps(sys.argv[1:]) + "\\n")
version = sys.argv[2]
if os.environ.get("FAKE_NO_BUNDLE") != version:
    shutil.rmtree("target", ignore_errors=True)
for pom in ("pom.xml", "module/pom.xml"):
    with open(pom, "w") as f:
        f.write(version)
if os.environ.get("FAKE_HANG") == version:
    def terminated(*_):
        open(os.environ["CALLS"] + ".terminated", "w").close()
        sys.exit(143)
    signal.signal(signal.SIGTERM, terminated)
    open(os.environ["CALLS"] + ".started", "w").close()
    signal.pause()
if os.environ.get("FAKE_LOCK") == version:
    open(".git/index.lock", "w").close()
if os.environ.get("FAKE_FAIL") == version:
    sys.exit(7)
if os.environ.get("FAKE_NO_BUNDLE") == version:
    sys.exit(0)
os.makedirs("target/central-publishing")
artifact = "io/github/flink-gcp/base"
directory = f"{artifact}/{version}"
with zipfile.ZipFile("target/central-publishing/central-bundle.zip", "w") as bundle:
    for name in (f"{directory}/base-{version}.pom", f"{directory}/base-{version}.jar"):
        bundle.writestr(name, "x")
        if not os.environ.get("FAKE_UNSIGNED"):
            bundle.writestr(name + ".asc", "x")
    if os.environ.get("FAKE_LOCAL") == version:
        bundle.writestr(f"{artifact}/maven-metadata-local.xml", "x")
"""

# Nothing from the developer's git context reaches the scratch repository.
ENV = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
ENV.pop("GITHUB_STEP_SUMMARY", None)


@pytest.fixture
def repo(tmp_path):
    root = tmp_path / "repo"
    (root / "scripts").mkdir(parents=True)
    (root / "module").mkdir()
    # The real LTS reader, so a change to its flag or its weekly.yaml parsing
    # fails here too.
    for name in ("rehearse-release.py", "check-flink-api-tiers.py"):
        shutil.copy2(SCRIPTS / name, root / "scripts")
    shutil.copytree(SCRIPTS / "lib", root / "scripts" / "lib")
    weekly = root / ".github" / "workflows" / "weekly.yaml"
    weekly.parent.mkdir(parents=True)
    weekly.write_text("env:\n  FLINK_LTS: '1.20.9'\n")
    for pom in ("pom.xml", "module/pom.xml"):
        (root / pom).write_text("1.3.0-SNAPSHOT")
    (root / "README").write_text("unrelated")
    git = ["git", "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false"]
    for command in (
        ["init", "-q"],
        ["add", "pom.xml", "module/pom.xml", "README"],
        ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "init"],
    ):
        subprocess.run(git + command, cwd=root, env=ENV, check=True)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    just = bin_dir / "just"
    just.write_text(f"#!{sys.executable}\n{FAKE_JUST}")
    just.chmod(0o755)
    return root, {
        **ENV,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "CALLS": str(tmp_path / "calls"),
        "TMPDIR": str(tmp_path),
    }


def command(*args):
    return [sys.executable, "scripts/rehearse-release.py", *args]


def rehearse(repo, *args, **env):
    root, base = repo
    return subprocess.run(
        command(*args),
        cwd=root,
        env={**base, **env},
        capture_output=True,
        text=True,
        check=False,
    )


def calls(repo):
    path = Path(repo[1]["CALLS"])
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


def poms(repo):
    return {(repo[0] / p).read_text() for p in ("pom.xml", "module/pom.xml")}


class TestRehearsal:
    def test_stages_both_lines_without_upload_and_restores_every_pom(
        self, repo, tmp_path
    ):
        summary = tmp_path / "summary.md"
        result = rehearse(
            repo, "1.3.0", "-Dmaven.repo.local=/m2", GITHUB_STEP_SUMMARY=str(summary)
        )
        assert result.returncode == 0, result.stderr
        assert calls(repo) == [
            ["stage-release", "1.3.0", "-Dmaven.repo.local=/m2", "-DskipUpload=true"],
            [
                "stage-release",
                "1.3.0-1.20",
                "-Dflink.version=1.20.9",
                "-Dmaven.repo.local=/m2",
                "-DskipUpload=true",
            ],
        ]
        # Both bundles survived the second build's clean.
        assert "| `1.3.0` | 4 |" in result.stdout
        assert "| `1.3.0-1.20` | 4 |" in result.stdout
        assert "Built without signatures" not in result.stdout
        assert poms(repo) == {"1.3.0-SNAPSHOT"}
        # A green run is the one whose size table matters.
        table = result.stdout[result.stdout.index("| Version line") :]
        assert summary.read_text() == "### Central bundle rehearsal\n\n" + table

    def test_no_forwarded_argument_can_turn_the_upload_back_on(self, repo):
        assert rehearse(repo, "1.3.0", "-DskipUpload=false").returncode == 0
        for call in calls(repo):
            # Maven keeps the last value a property is given.
            assert call[-1] == "-DskipUpload=true"

    def test_signatures_are_checked_unless_the_rehearsal_is_unsigned(self, repo):
        # Signing skipped by a forwarded argument is still checked, and fails.
        unsigned = rehearse(repo, "1.3.0", "-Dgpg.skip=true", FAKE_UNSIGNED="1")
        assert unsigned.returncode == 1
        assert "files without a .asc signature" in unsigned.stderr
        skipped = rehearse(repo, "1.3.0", "--unsigned", "-Dx=y", FAKE_UNSIGNED="1")
        assert skipped.returncode == 0, skipped.stderr
        assert "Built without signatures" in skipped.stdout
        for call in calls(repo)[-2:]:
            assert call[-3:] == ["-Dx=y", "-Dgpg.skip=true", "-DskipUpload=true"]
        # Only right after the version: anywhere else it is Maven's argument.
        later = rehearse(repo, "1.3.0", "-Dx=y", "--unsigned", FAKE_UNSIGNED="1")
        assert later.returncode == 1
        assert calls(repo)[-1][-2:] == ["--unsigned", "-DskipUpload=true"]

    @pytest.mark.parametrize("defective", ["1.3.0", "1.3.0-1.20"])
    def test_a_defect_in_either_bundle_fails_and_is_named_in_the_summary(
        self, repo, tmp_path, defective
    ):
        summary = tmp_path / "summary.md"
        result = rehearse(
            repo, "1.3.0", FAKE_LOCAL=defective, GITHUB_STEP_SUMMARY=str(summary)
        )
        assert result.returncode == 1
        assert "| Total | 9 |" in result.stdout
        error = (
            f"rehearse-release: {defective}: local-repository files that Central "
            "rejects: io/github/flink-gcp/base/maven-metadata-local.xml"
        )
        assert result.stderr.splitlines() == [error]
        assert summary.read_text().endswith(f"\n- {error}\n")

    def test_a_build_that_writes_no_bundle_fails_instead_of_copying_a_stale_one(
        self, repo
    ):
        assert rehearse(repo, "1.3.0").returncode == 0
        assert (repo[0] / "target/central-publishing/central-bundle.zip").exists()
        # The previous run left the 1.20 line's bundle, and the 2.x build
        # neither cleans it away nor writes its own.
        result = rehearse(repo, "1.3.0", FAKE_NO_BUNDLE="1.3.0")
        assert result.returncode == 1
        assert "the 1.3.0 build wrote no target/central-publishing" in result.stderr
        assert len(calls(repo)) == 3, "the 1.20 line must not be built"
        assert poms(repo) == {"1.3.0-SNAPSHOT"}

    def test_a_failed_build_returns_its_status_and_restores_the_poms(self, repo):
        result = rehearse(repo, "1.3.0", FAKE_FAIL="1.3.0-1.20")
        assert result.returncode == 7
        assert "just stage-release 1.3.0-1.20" in result.stderr
        assert poms(repo) == {"1.3.0-SNAPSHOT"}

    @pytest.mark.parametrize(
        "failing, status", [({"FAKE_FAIL": "1.3.0-1.20"}, 7), ({}, 1)]
    )
    def test_a_failed_restore_is_reported_and_fails_the_run(
        self, repo, failing, status
    ):
        # A failed build keeps its own status; a good one fails on the restore.
        result = rehearse(repo, "1.3.0", FAKE_LOCK="1.3.0-1.20", **failing)
        assert result.returncode == status
        assert "the POMs were NOT restored; run git checkout -- '*pom.xml'" in (
            result.stderr
        )
        assert "Traceback" not in result.stderr
        assert poms(repo) == {"1.3.0-1.20"}

    def test_a_closed_stderr_cannot_skip_the_restore(self, repo):
        root, env = repo
        with subprocess.Popen(
            command("1.3.0"),
            cwd=root,
            env={**env, "FAKE_FAIL": "1.3.0-1.20"},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        ) as process:
            # The failure message then has nowhere to go.
            process.stderr.close()
            assert process.wait(timeout=60) != 0
        assert poms(repo) == {"1.3.0-SNAPSHOT"}

    def test_sigterm_stops_the_build_before_the_poms_are_restored(self, repo):
        root, env = repo
        started = Path(env["CALLS"] + ".started")
        with subprocess.Popen(
            command("1.3.0"), cwd=root, env={**env, "FAKE_HANG": "1.3.0"}
        ) as process:
            deadline = time.monotonic() + 30
            while not started.exists():
                assert process.poll() is None, "the script exited before the build"
                assert time.monotonic() < deadline, "the fake build never started"
                time.sleep(0.05)
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=30) == 143
        # The build received the SIGTERM rather than being killed outright.
        assert Path(env["CALLS"] + ".terminated").exists()
        assert poms(repo) == {"1.3.0-SNAPSHOT"}

    def test_uncommitted_pom_edits_refuse_the_rehearsal(self, repo):
        (repo[0] / "module/pom.xml").write_text("edited")
        result = rehearse(repo, "1.3.0")
        assert result.returncode == 1
        assert "uncommitted POM edits" in result.stderr
        assert calls(repo) == []
        assert (repo[0] / "module/pom.xml").read_text() == "edited"

    def test_other_uncommitted_edits_do_not_refuse_it(self, repo):
        (repo[0] / "README").write_text("edited")
        assert rehearse(repo, "1.3.0").returncode == 0
        assert (repo[0] / "README").read_text() == "edited"

    @pytest.mark.parametrize("args", [[], ["--unsigned"], ["-Dx=y", "1.3.0"]])
    def test_usage(self, repo, args):
        result = rehearse(repo, *args)
        assert result.returncode == 2
        assert result.stderr.startswith("usage:")
        assert calls(repo) == []
