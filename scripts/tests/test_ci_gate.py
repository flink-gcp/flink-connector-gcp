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
"""The full truth table of scripts/ci-gate.sh (issue #243).

The gate is the one check branch protection requires, and it vouches for the
unconditional checker jobs too, so every row matters: a wrong green here is a
pull request merging unverified, and a wrong red is one blocked forever.
"""

import subprocess

import pytest
from conftest import SCRIPTS

CHECKERS_GREEN = "api_tiers:success option_docs:success"


def run_gate(changes=None, build=None, run_build=None, checkers=CHECKERS_GREEN):
    env = {}
    if changes is not None:
        env["CHANGES_RESULT"] = changes
    if build is not None:
        env["BUILD_RESULT"] = build
    if run_build is not None:
        env["RUN_BUILD"] = run_build
    if checkers is not None:
        env["CHECKER_RESULTS"] = checkers
    return subprocess.run(
        [str(SCRIPTS / "ci-gate.sh")], env=env, capture_output=True, check=False
    )


@pytest.mark.parametrize(
    ("changes", "build", "run_build", "expected"),
    [
        # The two greens: the build ran and passed, or was skipped on purpose.
        ("success", "success", "true", 0),
        ("success", "skipped", "false", 0),
        # A skip the changes job did not ask for must not read as green.
        ("success", "skipped", "true", 1),
        ("success", "skipped", "", 1),
        # Build failures and cancellations.
        ("success", "failure", "true", 1),
        ("success", "cancelled", "true", 1),
        # Change detection itself failed: nothing downstream is trustworthy.
        ("failure", "skipped", "false", 1),
        ("cancelled", "skipped", "false", 1),
        ("skipped", "skipped", "false", 1),
    ],
)
def test_truth_table(changes, build, run_build, expected):
    result = run_gate(changes, build, run_build)
    assert result.returncode == expected, result.stderr


@pytest.mark.parametrize(
    "checkers",
    [
        # A failed checker, first or last in the list.
        "api_tiers:failure option_docs:success",
        "api_tiers:success option_docs:failure",
        # A skipped checker cannot happen through ci.yaml's wiring — the
        # checkers are unconditional — so it can only mean a rewiring mistake,
        # which must be a red gate rather than a silent pass.
        "api_tiers:skipped option_docs:success",
        # A pair that lost its result half.
        "api_tiers: option_docs:success",
    ],
)
def test_a_checker_not_succeeding_fails_the_gate(checkers):
    result = run_gate("success", "success", "true", checkers=checkers)
    assert result.returncode == 1, result.stderr


def test_the_failing_checker_is_named():
    result = run_gate(
        "success", "success", "true", checkers="api_tiers:success option_docs:failure"
    )
    assert result.returncode == 1
    assert b"option_docs: failure" in result.stderr


def test_missing_required_inputs_fail_loudly():
    assert run_gate(changes=None, build="success").returncode != 0
    assert run_gate(changes="success", build=None).returncode != 0
    # A gate invoked without CHECKER_RESULTS is a gate that silently stopped
    # vouching for the checkers.
    assert run_gate(changes="success", build="success", checkers=None).returncode != 0
