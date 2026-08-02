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

The gate is the check branch protection is to require, so every row matters:
a wrong green here is a pull request merging unverified, and a wrong red is
one blocked forever.
"""

import subprocess

import pytest
from conftest import SCRIPTS


def run_gate(changes=None, build=None, run_build=None):
    env = {}
    if changes is not None:
        env["CHANGES_RESULT"] = changes
    if build is not None:
        env["BUILD_RESULT"] = build
    if run_build is not None:
        env["RUN_BUILD"] = run_build
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


def test_missing_required_inputs_fail_loudly():
    assert run_gate(changes=None, build="success").returncode != 0
    assert run_gate(changes="success", build=None).returncode != 0
