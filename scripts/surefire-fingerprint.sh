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
# Prints one "<report path><TAB><test count>" line per surefire report, sorted.
#
# Proves that swapping the Flink version on the classpath ran the *same* tests,
# not merely that whatever ran was green. A per-class fingerprint rather than a
# single total, so a class that stops running is visible instead of being masked
# by another that gained tests. flink-connector-parent runs both unit tests and
# integration tests through surefire, so both land here.
#
# The binary-compatibility job in .github/workflows/weekly.yaml uses it, but it
# is meant to be run by hand too — reproducing that job locally is the first
# thing to do when it goes red:
#
#     ./mvnw verify                                   # build against the floor
#     scripts/surefire-fingerprint.sh > /tmp/floor.txt
#     ./mvnw -Dflink.version=<newer> \
#         surefire:test@default-test surefire:test@integration-tests
#     scripts/surefire-fingerprint.sh | diff -u /tmp/floor.txt -
#
# The execution ids are load-bearing: a bare `surefire:test` ignores the
# execution-level includes/excludes and silently skips every ITCase.

set -euo pipefail

find . -path '*/target/surefire-reports/TEST-*.xml' -print |
    sort |
    while IFS= read -r report; do
        # The tests= attribute lives on the root <testsuite> element. `|| true`
        # so a report without it reaches the check below instead of dying here
        # with set -e and no explanation.
        count=$(grep -m1 -o 'tests="[0-9]*"' "$report" | tr -dc '0-9' || true)
        if [ -z "$count" ]; then
            # Deliberately fatal rather than defaulting to 0: a report truncated
            # by a crashed fork would produce 0 on both sides of the comparison
            # and so compare equal, turning the very failure this fingerprint
            # exists to catch into a pass.
            echo "$report has no tests= attribute; refusing to fingerprint a truncated report" >&2
            exit 1
        fi
        printf '%s\t%s\n' "${report#./}" "$count"
    done
