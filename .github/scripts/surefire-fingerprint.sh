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
        # The tests= attribute lives on the root <testsuite> element.
        count=$(grep -m1 -o 'tests="[0-9]*"' "$report" | tr -dc '0-9')
        printf '%s\t%s\n' "${report#./}" "${count:-0}"
    done
