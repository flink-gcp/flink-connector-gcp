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
"""Evaluate the publication guards with synthetic GitHub event contexts."""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def workflow():
    return yaml.safe_load((ROOT / ".github/workflows/docs.yaml").read_text())


def expression(text, context):
    # These workflow expressions use only comparisons, boolean operators and
    # format. Missing contexts have GitHub's empty-string value in the fixtures.
    text = text.removeprefix("${{").removesuffix("}}").strip()
    text = re.sub(
        r"(?:github|needs)\.[a-zA-Z0-9_.]+", lambda m: repr(context.get(m[0], "")), text
    )
    text = text.replace("&&", " and ").replace("||", " or ")
    return eval(
        text,
        {"__builtins__": {}, "format": lambda template, *args: template.format(*args)},
    )


@pytest.mark.parametrize(
    "event,ref,conclusion,upstream_event,repo,publish,build,group",
    [
        (
            "push",
            "refs/heads/main",
            "",
            "",
            "1305440656",
            True,
            True,
            "docs-publication",
        ),
        (
            "workflow_dispatch",
            "refs/heads/main",
            "",
            "",
            "1305440656",
            True,
            True,
            "docs-publication",
        ),
        (
            "workflow_run",
            "refs/heads/main",
            "success",
            "push",
            "1305440656",
            True,
            True,
            "docs-publication",
        ),
        (
            "workflow_run",
            "refs/heads/main",
            "failure",
            "push",
            "1305440656",
            False,
            False,
            "docs-check-123",
        ),
        (
            "workflow_run",
            "refs/heads/main",
            "cancelled",
            "push",
            "1305440656",
            False,
            False,
            "docs-check-123",
        ),
        (
            "workflow_run",
            "refs/heads/main",
            "success",
            "workflow_dispatch",
            "1305440656",
            False,
            False,
            "docs-check-123",
        ),
        (
            "pull_request",
            "refs/pull/99/merge",
            "",
            "",
            "1305440656",
            False,
            True,
            "docs-check-123",
        ),
        (
            "push",
            "refs/heads/v1.20",
            "",
            "",
            "1305440656",
            False,
            True,
            "docs-check-123",
        ),
        (
            "workflow_dispatch",
            "refs/heads/topic",
            "",
            "",
            "1305440656",
            False,
            True,
            "docs-check-123",
        ),
        ("push", "refs/heads/main", "", "", "999", False, True, "docs-publication"),
    ],
)
def test_publication_and_skip_guards_agree(
    workflow, event, ref, conclusion, upstream_event, repo, publish, build, group
):
    context = {
        "github.event_name": event,
        "github.ref": ref,
        "github.repository_id": repo,
        "github.event.workflow_run.conclusion": conclusion,
        "github.event.workflow_run.event": upstream_event,
        "github.run_id": "123",
    }
    plan = workflow["jobs"]["plan"]
    assert bool(expression(plan["if"], context)) == build
    context["needs.plan.outputs.publish"] = (
        str(bool(expression(plan["outputs"]["publish"], context))).lower()
        if build
        else ""
    )
    assert bool(expression(workflow["jobs"]["deploy"]["if"], context)) == publish
    skipped = expression(
        workflow["jobs"]["docs_passed"]["steps"][-1]["env"]["SKIPPED_OK"], context
    )
    assert set(skipped.split()) == (
        set()
        if publish
        else {"deploy"}
        if build
        else {"plan", "build", "assemble", "deploy"}
    )
    assert expression(workflow["concurrency"]["group"], context) == group


def test_every_build_and_assembly_checks_out_the_frozen_controller(workflow):
    for name in ("build", "assemble"):
        checkout = workflow["jobs"][name]["steps"][0]
        assert checkout["with"]["ref"] == "${{ needs.plan.outputs.controller_sha }}"
    source = workflow["jobs"]["build"]["steps"][1]
    assert source["with"]["ref"] == "${{ matrix.sha }}"
    assert workflow["jobs"]["build"]["strategy"]["fail-fast"] is False


def test_release_completion_and_manual_recovery_are_wired(workflow):
    # YAML 1.1 resolves the unquoted `on` key as true.
    triggers = workflow[True]
    assert triggers["workflow_run"] == {
        "workflows": ["Release"],
        "types": ["completed"],
    }
    assert "workflow_dispatch" in triggers
    assert "scripts/docs-site.py" in triggers["push"]["paths"]
    assert "docs/**" in triggers["push"]["paths"]
