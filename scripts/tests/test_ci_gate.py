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
"""The truth table and wiring of scripts/ci-gate.py (issue #243).

ci.yaml's gate is the one check branch protection requires, and the same
script is the internal verdict of the children with a legitimately skippable
job, so every row matters: a wrong green is a pull request merging
unverified, and a wrong red is one blocked forever. The wiring tests pin the
one drift deriving the verdict from `needs` cannot see — a job that never
joined a `needs` list at all.
"""

import json
import re
import subprocess

import pytest
from conftest import SCRIPTS


def run_gate(needs, skipped_ok=None):
    env = {}
    if needs is not None:
        env["NEEDS"] = needs if isinstance(needs, str) else json.dumps(needs)
    if skipped_ok is not None:
        env["SKIPPED_OK"] = skipped_ok
    return subprocess.run(
        [str(SCRIPTS / "ci-gate.py")], env=env, capture_output=True, check=False
    )


def results(**jobs):
    return {job: {"result": result} for job, result in jobs.items()}


# --- the truth table ---


@pytest.mark.parametrize(
    ("needs", "skipped_ok", "expected"),
    [
        # The greens: everything succeeded, or a permitted job skipped.
        (results(build="success", lint="success"), None, 0),
        (results(build="skipped", checker="success"), "build", 0),
        (results(build="skipped", plan="skipped"), "build plan", 0),
        # A skip nothing permitted must not read as green — that is the
        # rewiring-mistake defense, and it is per job name.
        (results(build="skipped"), None, 1),
        (results(build="skipped"), "plan", 1),
        (results(build="success", plan="skipped"), "build", 1),
        # Failures and cancellations, permitted-to-skip or not.
        (results(build="failure"), None, 1),
        (results(build="cancelled"), None, 1),
        (results(build="failure"), "build", 1),
        # One bad job among good ones is enough.
        (results(a="success", b="failure", c="success"), None, 1),
    ],
)
def test_truth_table(needs, skipped_ok, expected):
    result = run_gate(needs, skipped_ok)
    assert result.returncode == expected, result.stderr


def test_the_failing_job_is_named():
    result = run_gate(results(lint="success", docs="failure"))
    assert result.returncode == 1
    assert b"docs: failure" in result.stderr
    assert b"lint" not in result.stderr


@pytest.mark.parametrize(
    "needs",
    [
        # A gate handed nothing has silently stopped vouching.
        None,
        "",
        "not json",
        "{}",
        "[]",
    ],
)
def test_an_unreadable_needs_fails_loudly(needs):
    assert run_gate(needs).returncode == 1


# --- the wiring the verdict cannot see ---
#
# The script judges whatever `needs` context it is handed, so a job that
# never joined the calling job's `needs` list is invisible to it — its
# failure would not redden the gate. These read the real workflows and hold
# each gate's `needs` to the full job list of its file; lint.yaml's push
# paths carry .github/workflows/** and scripts/**, and on pull requests the
# suite always runs, so they fire on every edit to either side.

WORKFLOWS = SCRIPTS.parent / ".github" / "workflows"

# workflow file -> its gate/verdict job. lint.yaml carries no verdict on
# purpose: nothing in it may skip, so any failure already reddens the child
# workflow, which ci.yaml's gate sees. docs.yaml grew one when it grew a
# `deploy` job that skips on everything but a push to main (#93).
GATES = {
    "ci.yaml": "ci_passed",
    "docs.yaml": "docs_passed",
    "verify.yaml": "verify_passed",
    "tofu-plan.yaml": "tofu_plan_passed",
}

# The children ci.yaml calls that carry no verdict, held against GATES by
# test_every_child_is_classified_as_gated_or_gateless below.
GATELESS = ["lint.yaml"]


def job_block(filename, gate):
    # Triggers are 2-space keys too, so only what follows `jobs:` counts.
    text = (WORKFLOWS / filename).read_text().split("\njobs:\n", 1)[1]
    jobs = re.findall(r"^  ([\w-]+):\n", text, re.MULTILINE)
    block = re.search(rf"^  {gate}:\n(?:^(?:    .*|)\n)+", text, re.MULTILINE)
    assert block, f"{filename} has no {gate} job"
    return set(jobs), block.group(0)


def jobs_and_needs(filename, gate):
    jobs, block = job_block(filename, gate)
    needs = re.search(r"needs:\s*\[([^\]]+)\]", block)
    assert needs, f"{filename}: {gate} has no needs list"
    return jobs, {job.strip() for job in needs.group(1).split(",")}


@pytest.mark.parametrize(("filename", "gate"), sorted(GATES.items()))
def test_every_job_is_enrolled_in_its_gates_needs(filename, gate):
    jobs, needs = jobs_and_needs(filename, gate)
    assert needs == jobs - {gate}


def test_every_child_the_orchestrator_calls_declares_workflow_call():
    text = (WORKFLOWS / "ci.yaml").read_text()
    children = re.findall(r"uses: \./\.github/workflows/([\w-]+\.yaml)", text)
    assert children, "ci.yaml calls no reusable workflows"
    for child in children:
        assert re.search(
            r"^  workflow_call:", (WORKFLOWS / child).read_text(), re.MULTILINE
        ), f"{child} is called by ci.yaml but declares no workflow_call trigger"


@pytest.mark.parametrize("filename", GATELESS)
def test_a_gateless_child_has_no_conditional_job(filename):
    # These carry no verdict job because nothing in them may skip — a skipped
    # job does not fail its workflow, so a job-level `if:` added here would be
    # invisible to ci.yaml's gate. Growing one means growing a verdict job
    # too, as verify.yaml, tofu-plan.yaml and docs.yaml have.
    text = (WORKFLOWS / filename).read_text().split("\njobs:\n", 1)[1]
    assert not re.search(r"^    if:", text, re.MULTILINE)


def test_every_child_is_classified_as_gated_or_gateless():
    # The two lists above are hand-written, and a child in neither is checked
    # by nothing: no `if:` audit and no enrollment audit, silently. An empty
    # GATELESS would also collect as a pass rather than a failure — pytest
    # reports an empty parametrize as `1 skipped`, exit 0 — so this is what
    # keeps the test above from quietly protecting nothing.
    text = (WORKFLOWS / "ci.yaml").read_text()
    children = set(re.findall(r"uses: \./\.github/workflows/([\w-]+\.yaml)", text))
    assert children <= set(GATES) | set(GATELESS)
    assert set(GATELESS), "the gateless inventory may not be emptied"


@pytest.mark.parametrize(("filename", "gate"), sorted(GATES.items()))
def test_a_verdict_reports_on_every_uncancelled_run(filename, gate):
    # `!cancelled()` is what makes a verdict report at all: without it GitHub
    # skips the verdict whenever any needed job skipped, and a skipped job
    # does not fail its workflow. docs.yaml is where this became load-bearing
    # rather than theoretical — its `deploy` skips on *every* pull request, so
    # losing the condition would stop the verdict vouching on all of them
    # while every check stayed green.
    _, block = job_block(filename, gate)
    assert re.search(r"^    if: \$\{\{ !cancelled\(\) \}\}$", block, re.MULTILINE), (
        f"{filename}: {gate} must carry `if: ${{{{ !cancelled() }}}}`"
    )


def test_no_skipped_ok_puts_the_empty_string_in_the_middle():
    # `A && '' || 'job'` reads as "job when A is false", and is not: `''` is
    # falsy, so `&&` yields it, `||` rejects it, and the expression returns
    # `'job'` on both branches — a verdict that permits every skip, including
    # the one it exists to catch. Shipped once, on the pull request that added
    # docs.yaml's verdict (#93), and invisible to actionlint. The working
    # shape puts the job name in the middle, as verify.yaml and tofu-plan.yaml
    # do.
    for workflow in sorted(WORKFLOWS.glob("*.yaml")):
        for line in workflow.read_text().splitlines():
            if "SKIPPED_OK:" in line:
                assert "&& '' ||" not in line, (
                    f"{workflow.name}: SKIPPED_OK yields the same value on "
                    f"both branches — {line.strip()}"
                )


def test_a_caller_grants_every_permission_its_children_ask_for():
    # A called workflow's nested jobs are validated against the caller's grant
    # *before* their `if:` is evaluated, so a permission declared in a child
    # and not in ci.yaml's calling job fails the whole run at validation —
    # every pull request, including ones that change nothing near it.
    # Measured: actionlint does not check this, and neither did anything else
    # here until docs.yaml's `deploy` needed `pages: write` (#93).
    text = (WORKFLOWS / "ci.yaml").read_text().split("\njobs:\n", 1)[1]
    calls = re.findall(
        r"^  [\w-]+:\n(?:^    .*\n)*?"
        r"^    uses: \./\.github/workflows/([\w-]+\.yaml)\n"
        r"^    permissions:\n((?:^      \w[\w-]*: \w+\n)+)",
        text,
        re.MULTILINE,
    )
    assert calls, "ci.yaml calls no child with a permissions block"
    for child, granted_block in calls:
        granted = dict(
            re.findall(r"^      ([\w-]+): (\w+)\n", granted_block, re.MULTILINE)
        )
        child_text = (WORKFLOWS / child).read_text().split("\njobs:\n", 1)[1]
        # Only inside a `permissions:` block, so that a `with:` input or an
        # `env` entry that happens to read `something: write` is not mistaken
        # for one.
        for asked_block in re.findall(
            r"^    permissions:\n((?:^      [\w-]+: \w+\n)+)", child_text, re.MULTILINE
        ):
            for name, level in re.findall(
                r"^      ([\w-]+): (write)\n", asked_block, re.MULTILINE
            ):
                assert granted.get(name) == level, (
                    f"{child} asks for `{name}: {level}` but ci.yaml's call "
                    f"grants `{name}: {granted.get(name, 'none')}`"
                )
