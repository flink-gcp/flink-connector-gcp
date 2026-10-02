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
"""The approved-checkout check every approval-bound service delivery shares."""

import subprocess

import pytest
from flink_tier3 import approval_bundle
from flink_tier3.common import Failure


@pytest.mark.parametrize(
    "change",
    [
        "none",
        "revision",
        "tracked",
        "untracked",
        "ignored",
        "unicode",
        "trial",
        "environment",
    ],
)
def test_approved_checkout_and_cue_inputs_are_required(tmp_path, monkeypatch, change):
    def git(*args):
        return subprocess.run(
            [
                "git",
                "-c",
                "commit.gpgsign=false",
                "-c",
                "core.hooksPath=/dev/null",
                *args,
            ],
            cwd=tmp_path,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init")
    kubernetes = tmp_path / "kubernetes"
    kubernetes.mkdir()
    (kubernetes / "common.cue").write_text("package test\n")
    git("add", "kubernetes")
    git(
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "-m",
        "Fixture",
    )
    revision = git("rev-parse", "HEAD")
    monkeypatch.setattr(approval_bundle.workflow, "ROOT", tmp_path)
    if change == "environment":
        monkeypatch.setenv("GIT_DIR", str(tmp_path / ".git"))
        monkeypatch.setenv("GIT_WORK_TREE", str(tmp_path))
        monkeypatch.setenv("GIT_INDEX_FILE", str(tmp_path / ".git/index"))
        monkeypatch.setattr(approval_bundle.workflow, "ROOT", tmp_path.parent)
    if change == "revision":
        revision = "0" * 40
    elif change == "tracked":
        (kubernetes / "common.cue").write_text("package changed\n")
    elif change in ("untracked", "ignored", "unicode"):
        (kubernetes / ("追加.cue" if change == "unicode" else "extra.cue")).write_text(
            "package test\n"
        )
        if change == "ignored":
            (tmp_path / ".git/info/exclude").write_text("extra.cue\n")
    elif change == "trial":
        # Dispatch reads a trial by name; one the approved commit lacks is not reviewed.
        trials = kubernetes / "lifecycle/pubsub-trials"
        trials.mkdir(parents=True)
        (trials / "local.toml").write_text("version = 3\n")
    if change == "none":
        approval_bundle.check_revision(revision)
    else:
        with pytest.raises(Failure):
            approval_bundle.check_revision(revision)


@pytest.mark.parametrize("failure", ["missing", "timeout", "exit"])
def test_git_failures_are_bounded_and_reported(monkeypatch, failure):
    def run(args, **kwargs):
        assert 0 < kwargs.get("timeout", float("inf")) <= 60
        if failure == "missing":
            raise FileNotFoundError("git is missing")
        if failure == "timeout":
            raise subprocess.TimeoutExpired(args, kwargs["timeout"])
        return subprocess.CompletedProcess(args, 1, "", "not a repository")

    monkeypatch.setattr(approval_bundle.subprocess, "run", run)
    with pytest.raises(Failure, match="Cannot verify the approved repository revision"):
        approval_bundle.check_revision("b" * 40)
