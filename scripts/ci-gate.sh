#!/usr/bin/env bash
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
#
# The "CI passed" gate (issue #243). ci.yaml's build job is skipped when a
# change cannot affect the Maven build, and a required check that never
# reports blocks a pull request forever — so this gate is the ONE job branch
# protection names: it reports on every run, turning an intended skip into an
# explicit green and everything else into an explicit red. The unconditional
# checker jobs are folded into the same verdict rather than each being a
# required context of its own, so what main requires is decided in ci.yaml
# (the gate's `needs` list and CHECKER_RESULTS) and a checker added or
# retired never needs a repository-settings edit — a settings-side list
# would also fall silently out of step with a renamed or deleted job, which
# is exactly the never-reports trap again.
#
# Inputs are four environment variables, which is what makes the truth table
# runnable by hand:
#
#   CHANGES_RESULT   needs.changes.result  (the module-selection job)
#   BUILD_RESULT     needs.build.result    (the Maven job)
#   RUN_BUILD        needs.changes.outputs.run_build ('true'/'false')
#   CHECKER_RESULTS  space-separated name:result pairs, one per unconditional
#                    checker job the gate vouches for
#
#   CHANGES_RESULT=success BUILD_RESULT=skipped RUN_BUILD=false \
#       CHECKER_RESULTS='api_tiers:success option_docs:success' scripts/ci-gate.sh
#
# Exit 0: the change's obligations are met — the build succeeded or change
# detection decided nothing needed building, and every checker succeeded.
# Exit 1: anything else, including "the build was skipped although change
# detection asked for it" and a skipped checker, neither of which can happen
# through ci.yaml's wiring — they exist so a future rewiring mistake fails
# loud instead of green. CHECKER_RESULTS is required for the same reason: a
# gate invoked without it is a gate that silently stopped vouching for the
# checkers.
set -euo pipefail

: "${CHANGES_RESULT:?CHANGES_RESULT is required (needs.changes.result)}"
: "${BUILD_RESULT:?BUILD_RESULT is required (needs.build.result)}"
: "${CHECKER_RESULTS:?CHECKER_RESULTS is required (name:result pairs)}"
RUN_BUILD="${RUN_BUILD:-}"

if [ "$CHANGES_RESULT" != 'success' ]; then
    echo "::error::change detection did not succeed ($CHANGES_RESULT), so nothing downstream of it can be trusted" >&2
    exit 1
fi

case "$BUILD_RESULT" in
    success)
        echo "build succeeded"
        ;;
    skipped)
        if [ "$RUN_BUILD" = 'false' ]; then
            echo "nothing in this change affects the Maven build"
        else
            echo "::error::the build was skipped although change detection asked for it" >&2
            exit 1
        fi
        ;;
    *)
        echo "::error::build: $BUILD_RESULT" >&2
        exit 1
        ;;
esac

# The checkers are unconditional, so anything but success — a failure, or a
# skip no wiring should be able to produce — is a red gate.
for pair in $CHECKER_RESULTS; do
    name="${pair%%:*}"
    result="${pair#*:}"
    if [ "$result" != 'success' ]; then
        echo "::error::${name}: ${result}" >&2
        exit 1
    fi
    echo "${name} succeeded"
done
