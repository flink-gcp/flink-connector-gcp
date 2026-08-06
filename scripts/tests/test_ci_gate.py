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

# workflow file -> its gate/verdict job. lint.yaml and docs.yaml carry no
# verdict on purpose: nothing in them may skip, so any failure already
# reddens the child workflow, which ci.yaml's gate sees.
GATES = {
    "ci.yaml": "ci_passed",
    "verify.yaml": "verify_passed",
    "tofu-plan.yaml": "tofu_plan_passed",
}


def jobs_and_needs(filename, gate):
    # Triggers are 2-space keys too, so only what follows `jobs:` counts.
    text = (WORKFLOWS / filename).read_text().split("\njobs:\n", 1)[1]
    jobs = re.findall(r"^  ([\w-]+):\n", text, re.MULTILINE)
    block = re.search(rf"^  {gate}:\n(?:^(?:    .*|)\n)+", text, re.MULTILINE)
    assert block, f"{filename} has no {gate} job"
    needs = re.search(r"needs:\s*\[([^\]]+)\]", block.group(0))
    assert needs, f"{filename}: {gate} has no needs list"
    return set(jobs), {job.strip() for job in needs.group(1).split(",")}


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


@pytest.mark.parametrize("filename", ["lint.yaml", "docs.yaml"])
def test_a_gateless_child_has_no_conditional_job(filename):
    # These two carry no verdict job because nothing in them may skip — a
    # skipped job does not fail its workflow, so a job-level `if:` added here
    # would be invisible to ci.yaml's gate. Growing one means growing a
    # verdict job too, as verify.yaml and tofu-plan.yaml have.
    text = (WORKFLOWS / filename).read_text().split("\njobs:\n", 1)[1]
    assert not re.search(r"^    if:", text, re.MULTILINE)
