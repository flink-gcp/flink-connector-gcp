#!/usr/bin/env bash
#
# Prints one "<report path><TAB><test count>" line per surefire report, sorted.
#
# Used by the binary-compatibility job in weekly.yaml to prove that swapping the
# Flink version on the classpath ran the *same* tests, not merely that whatever
# ran was green. A per-class fingerprint rather than a single total, so a class
# that stops running is visible instead of being masked by another that gained
# tests. flink-connector-parent runs both unit tests and integration tests
# through surefire, so both land here.

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
