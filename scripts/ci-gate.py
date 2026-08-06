#!/usr/bin/env python3
#
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
"""The gate/verdict step shared by every workflow with one (issue #243).

Branch protection requires exactly one context, ci.yaml's `CI passed`; that
gate `needs` the reusable workflows ci.yaml fans out to, and the children with
a legitimately skippable job (`verify`'s Maven build, `tofu-plan`'s plan) run
this same script as their own internal verdict, so a wrong skip reddens the
child and the child reddens the gate. A required check that never reports
blocks a pull request forever, which is why the gate reports on every
uncancelled run, turning an intended skip into an explicit green and
everything else into an explicit red.

The verdict is derived from the whole `needs` context rather than from any
per-job enumeration, so a job enrolls by joining the calling job's `needs`
list and nothing else — there is no second list to fall out of step with it.
The one drift derivation cannot see (a job never enrolled in `needs` at all)
is pinned by the wiring tests in scripts/tests/test_ci_gate.py.

Inputs are two environment variables, which is what makes the truth table
runnable by hand:

  NEEDS       ${{ toJSON(needs) }} — {"job": {"result": ...}, ...}
  SKIPPED_OK  space-separated job names whose `skipped` result is legitimate
              on THIS run; the calling workflow computes that where the
              knowledge lives (e.g. `run_build == 'false' && 'build' || ''`)

  NEEDS='{"build": {"result": "skipped"}}' SKIPPED_OK=build scripts/ci-gate.py

Exit 0: every needed job succeeded, or skipped with permission. Exit 1:
anything else — a failure, a cancellation, a skip the workflow's own wiring
should have made impossible, or a NEEDS this script cannot read, which would
otherwise be a gate that silently stopped vouching. Standard library only,
run by the runner image's python3 (nothing to install — the same precedent as
verify.yaml's changes job).
"""

import json
import os
import sys


def main() -> int:
    try:
        needs = json.loads(os.environ["NEEDS"])
    except KeyError:
        print("::error::NEEDS is required (pass toJSON(needs))", file=sys.stderr)
        return 1
    except json.JSONDecodeError as error:
        print(f"::error::NEEDS is not valid JSON: {error}", file=sys.stderr)
        return 1
    if not isinstance(needs, dict) or not needs:
        print(
            "::error::NEEDS names no jobs at all; a gate handed nothing has "
            "silently stopped vouching",
            file=sys.stderr,
        )
        return 1
    skipped_ok = os.environ.get("SKIPPED_OK", "").split()

    verdict = 0
    for job in sorted(needs):
        result = needs[job].get("result") if isinstance(needs[job], dict) else None
        if result == "success":
            print(f"{job} succeeded")
        elif result == "skipped" and job in skipped_ok:
            print(f"{job} was skipped, and this change asked it to be")
        elif result == "skipped":
            print(
                f"::error::{job} was skipped although its condition asked it "
                f"to run; that cannot happen through the workflow's own "
                f"wiring, so a rewiring mistake is loose",
                file=sys.stderr,
            )
            verdict = 1
        else:
            print(f"::error::{job}: {result}", file=sys.stderr)
            verdict = 1
    return verdict


if __name__ == "__main__":
    sys.exit(main())
