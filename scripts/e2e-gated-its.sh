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
# The single source of truth for which ITCases are gated on real-GCP
# credentials: every test class annotated with
# @EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", ...) or
# @EnabledIfEnvironmentVariable(named = "PUBSUB_IT_PROJECT", ...) is one.
# Deriving the list from the gating annotation itself means a newly gated
# ITCase joins the E2E run automatically — and one added outside the modules
# the `e2e` recipe builds makes --assert-ran fail until the recipe learns to
# run it, which is the point: joining the E2E workflow is a decision, not an
# accident.
#
# Three modes, all called by the `e2e` recipe in the justfile (which the E2E
# workflow runs — see .github/workflows/e2e.yaml):
#
#   (default)      print the gated class names, comma-joined, for -Dtest=
#   --require-env  fail unless every variable the gates read is set.
#                  @EnabledIfEnvironmentVariable turns a missing variable into
#                  a silent skip; this turns it into an error before any build
#                  minutes are spent
#   --assert-ran   fail unless every gated class has a surefire report showing
#                  tests ran and none skipped — the after-the-fact proof that
#                  green meant "ran", not "skipped". Without it, a workflow
#                  that lost its credentials would go green, the worst failure
#                  mode for a job whose purpose is catching what the emulator
#                  cannot.

set -euo pipefail

# Fatal when a gate matches nothing: zero gated ITCases for a connector means
# the annotation moved or the tree layout changed, and every mode below would
# otherwise degenerate into a vacuous pass for that connector while the other
# one keeps the union non-empty.
sources=''
for gate in BQ_IT_PROJECT PUBSUB_IT_PROJECT; do
    matched=$(grep -rl --include='*.java' "named = \"$gate\"" ./*/src/test/java | sort) || {
        echo "::error::no test class is gated on $gate; the gating annotation moved or the tree layout changed" >&2
        exit 1
    }
    sources="$sources$matched"$'\n'
done
sources=$(printf '%s' "$sources" | sort -u)

case "${1:-}" in
    '')
        # Simple class names: -Dtest= matches on them, and every module keeps
        # its test classes uniquely named.
        while IFS= read -r src; do
            basename "$src" .java
        done <<< "$sources" | paste -sd, -
        ;;
    --require-env)
        # The union of what the gates read: BQ_IT_PROJECT/BQ_IT_DATASET gate
        # every BigQuery class, BQ_IT_GCS_BUCKET additionally gates the
        # FILE_LOADS ones, PUBSUB_IT_PROJECT gates the Pub/Sub suite.
        for var in BQ_IT_PROJECT BQ_IT_DATASET BQ_IT_GCS_BUCKET PUBSUB_IT_PROJECT; do
            if [ -z "${!var:-}" ]; then
                echo "::error::$var is not set, so the gated real-GCP ITCases would silently skip. Locally the variables come from the uncommitted .env at the repository root, which mise loads." >&2
                exit 1
            fi
        done
        ;;
    --assert-ran)
        failed=0
        while IFS= read -r src; do
            module=${src%%/src/test/java/*}
            fqcn=${src#*/src/test/java/}
            fqcn=${fqcn%.java}
            fqcn=${fqcn//\//.}
            report="$module/target/surefire-reports/TEST-$fqcn.xml"
            if [ ! -f "$report" ]; then
                echo "::error::$fqcn produced no surefire report ($report): the gated ITCase did not run" >&2
                failed=1
                continue
            fi
            # Both attributes live on the root <testsuite> element. Their
            # absence is fatal rather than defaulted, for the same reason as in
            # surefire-fingerprint.sh: a report truncated by a crashed fork
            # must not read as anything.
            tests=$(grep -m1 -o 'tests="[0-9]*"' "$report" | tr -dc '0-9' || true)
            skipped=$(grep -m1 -o 'skipped="[0-9]*"' "$report" | tr -dc '0-9' || true)
            if [ -z "$tests" ] || [ -z "$skipped" ]; then
                echo "::error::$report has no tests=/skipped= attributes; refusing to trust a truncated report" >&2
                failed=1
            elif [ "$tests" -eq 0 ] || [ "$skipped" -ne 0 ]; then
                echo "::error::$fqcn: tests=$tests skipped=$skipped — gated ITCases were skipped, not run (missing BQ_IT_* variables?)" >&2
                failed=1
            else
                echo "$fqcn: $tests tests ran"
            fi
        done <<< "$sources"
        exit "$failed"
        ;;
    *)
        echo "usage: $0 [--require-env | --assert-ran]" >&2
        exit 2
        ;;
esac
