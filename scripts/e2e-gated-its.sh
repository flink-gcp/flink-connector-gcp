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
# @EnabledIfEnvironmentVariable(named = "<gate>", ...) for one of the gates in
# the loop below is one.
# Deriving the list from the gating annotation itself means a newly gated
# ITCase joins the E2E run automatically — and one added outside the modules
# the `e2e` recipe builds makes --assert-ran fail until the recipe learns to
# run it, which is the point: joining the E2E workflow is a decision, not an
# accident.
#
# Six modes. The `e2e` recipe uses the four lifecycle modes below; default is
# useful for an undivided manual run, and the final mode is a per-pull-request
# check in ci.yaml via `just check-gated-tags`:
#
#   (default)      print the gated class names, comma-joined, for -Dtest=
#   --require-env  fail unless every variable the gates read is set.
#                  @EnabledIfEnvironmentVariable turns a missing variable into
#                  a silent skip; this turns it into an error before any build
#                  minutes are spent. Still as strict as it was before the tag
#                  below existed (issue #245 asked): the tag makes the suite
#                  opt-in per command, and `just e2e` *is* that opt-in, so a
#                  variable missing inside it is a broken run, not a choice
#   --assert-ran   fail unless every gated class has a surefire report showing
#                  tests ran and none skipped — the after-the-fact proof that
#                  green meant "ran", not "skipped". Without it, a workflow
#                  that lost its credentials would go green, the worst failure
#                  mode for a job whose purpose is catching what the emulator
#                  cannot.
#   --for-gate     print only the classes using the named gate. The App Engine
#                  lifecycle uses this to start its billed instance around the
#                  Cloud Tasks class and nothing else.
#   --except-gate  print every class except those using the named gate. The
#                  remaining suites run only after the App Engine fixture has
#                  returned to its stopped state.
#   --check-tags   fail unless the environment gate and @Tag("gated") sit
#                  together on every class carrying either (issue #245)
#
# The tag is what keeps these classes out of an ordinary build: surefire
# excludes it by default (test.excluded.groups in the root pom), because the
# environment gate alone is all-or-nothing for a *shell* — a `just verify` in
# one carrying BIGTABLE_IT_PROJECT creates two one-node Bigtable instances.
# Forgetting the tag therefore fails in the expensive direction, which is what
# --check-tags exists to catch.

set -euo pipefail

gates=(
    BQ_IT_PROJECT
    PUBSUB_IT_PROJECT
    BIGTABLE_IT_PROJECT
    SPANNER_IT_PROJECT
    CLOUDTASKS_IT_PROJECT
)

known_gate() {
    local candidate=$1 gate
    for gate in "${gates[@]}"; do
        [ "$candidate" != "$gate" ] || return 0
    done
    return 1
}

# The E2E suite: every test class gated on one of the variables that workflow
# sets. Fatal when a gate matches nothing, since zero gated ITCases for a
# connector means the annotation moved or the tree layout changed, and the
# modes below would otherwise degenerate into a vacuous pass for that connector
# while the others keep the union non-empty.
#
# A function rather than a top-level block: --check-tags is deliberately
# gate-agnostic (see there) and must not require this particular list to match.
gated_sources() {
    local only_gate=${1:-} except_gate=${2:-} sources='' gate matched
    for gate in "${gates[@]}"; do
        [ -z "$only_gate" ] || [ "$gate" = "$only_gate" ] || continue
        [ -z "$except_gate" ] || [ "$gate" != "$except_gate" ] || continue
        matched=$(grep -rl --include='*.java' "named = \"$gate\"" ./*/src/test/java | sort) || {
            echo "::error::no test class is gated on $gate; the gating annotation moved or the tree layout changed" >&2
            return 1
        }
        sources="$sources$matched"$'\n'
    done
    printf '%s' "$sources" | sort -u
}

print_classes() {
    local sources
    sources=$(gated_sources "${1:-}" "${2:-}")
    while IFS= read -r src; do
        basename "$src" .java
    done <<< "$sources" | paste -sd, -
}

case "${1:-}" in
    '')
        # Simple class names: -Dtest= matches on them, and every module keeps
        # its test classes uniquely named.
        print_classes
        ;;
    --require-env)
        # The union of what the gates read: BQ_IT_PROJECT/BQ_IT_DATASET gate
        # every BigQuery class, BQ_IT_GCS_BUCKET additionally gates the
        # FILE_LOADS ones, PUBSUB_IT_PROJECT gates the Pub/Sub suite,
        # BIGTABLE_IT_PROJECT the Bigtable one, SPANNER_IT_PROJECT the Spanner
        # one and CLOUDTASKS_IT_PROJECT the Cloud Tasks one. Neither Bigtable
        # nor Spanner needs a companion variable:
        # unlike the BigQuery dataset and the GCS bucket, nothing persistent is
        # provisioned for them — each suite creates and deletes an instance of
        # its own — so there is no resource name to pass in. Cloud Tasks reads
        # its service and version from OpenTofu, while the lifecycle wrapper
        # exports the observed instance id only after startup.
        [ "$#" -eq 1 ] || {
            echo "usage: $0 [--require-env | --assert-ran | --for-gate GATE | --except-gate GATE | --check-tags]" >&2
            exit 2
        }
        for var in BQ_IT_PROJECT BQ_IT_DATASET BQ_IT_GCS_BUCKET PUBSUB_IT_PROJECT BIGTABLE_IT_PROJECT SPANNER_IT_PROJECT CLOUDTASKS_IT_PROJECT; do
            if [ -z "${!var:-}" ]; then
                echo "::error::$var is not set, so the gated real-GCP ITCases would silently skip. Locally the variables come from the uncommitted .env at the repository root, which mise loads." >&2
                exit 1
            fi
        done
        ;;
    --for-gate)
        if [ "$#" -ne 2 ] || ! known_gate "${2:-}"; then
            echo "usage: $0 [--require-env | --assert-ran | --for-gate GATE | --except-gate GATE | --check-tags]" >&2
            exit 2
        fi
        print_classes "$2"
        ;;
    --except-gate)
        if [ "$#" -ne 2 ] || ! known_gate "${2:-}"; then
            echo "usage: $0 [--require-env | --assert-ran | --for-gate GATE | --except-gate GATE | --check-tags]" >&2
            exit 2
        fi
        print_classes '' "$2"
        ;;
    --assert-ran)
        [ "$#" -eq 1 ] || {
            echo "usage: $0 [--require-env | --assert-ran | --for-gate GATE | --except-gate GATE | --check-tags]" >&2
            exit 2
        }
        failed=0
        sources=$(gated_sources)
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
    --check-tags)
        [ "$#" -eq 1 ] || {
            echo "usage: $0 [--require-env | --assert-ran | --for-gate GATE | --except-gate GATE | --check-tags]" >&2
            exit 2
        }
        # Deliberately gate-agnostic — any variable, not just the three the E2E
        # workflow sets: BigQueryDefaultStreamSchemaEvolutionITCase gates on
        # BQ_IT_SCHEMA_EVOLUTION precisely to stay out of that suite, and at a
        # measured ~2 hours against the real service it is the class an
        # ordinary build can least afford to pick up.
        #
        # Matching on the annotation's own name and its first argument keeps
        # the javadoc that discusses it (RealBigQuery, the two Abstract*RealGcp
        # base classes) out of the result: those write {@code
        # @EnabledIfEnvironmentVariable} with no argument list. The leading @ is
        # deliberately not part of the pattern, so this and the gate-by-gate
        # discovery above agree on what an annotation looks like — a
        # fully-qualified one would otherwise be discovered and not checked,
        # which is the direction that costs money.
        failed=0
        env_gated=$(grep -rl --include='*.java' 'EnabledIfEnvironmentVariable(named = "' ./*/src/test/java | sort) || {
            echo "::error::no test class carries @EnabledIfEnvironmentVariable(named = \"…\"); the gating annotation moved or the tree layout changed, and this check would pass vacuously" >&2
            exit 1
        }
        while IFS= read -r src; do
            if ! grep -q '@Tag("gated")' "$src"; then
                echo "::error::$src is gated on an environment variable but carries no @Tag(\"gated\"), so any build in a shell where that variable is set runs it — for the real-GCP suites, at the cost of billed resources. Add @Tag(\"gated\") beside the gate, or remove the gate if the class is not part of a gated suite." >&2
                failed=1
            fi
        done <<< "$env_gated"
        # Kept separate rather than diffed against the list above: a class may
        # be tagged and not gated, which is the mirror failure and needs its
        # own message. This side has no javadoc exemption to make — the literal
        # is short enough to appear in prose — so a class whose *comment*
        # quotes @Tag("gated") reads as tagged: document the convention in a
        # AGENTS.md rather than in javadoc, as the base classes do.
        tagged=$(grep -rl --include='*.java' '@Tag("gated")' ./*/src/test/java | sort) || true
        if [ -n "$tagged" ]; then
            while IFS= read -r src; do
                if ! grep -q 'EnabledIfEnvironmentVariable(named = "' "$src"; then
                    echo "::error::$src carries @Tag(\"gated\") but no @EnabledIfEnvironmentVariable(named = \"…\"), so nothing runs it: ordinary builds exclude the tag and \`just e2e\` selects classes by the environment gate. Add the gate, or remove the tag." >&2
                    failed=1
                fi
            done <<< "$tagged"
        fi
        if [ "$failed" -eq 0 ]; then
            echo "gated classes carrying both markers: $(printf '%s\n' "$env_gated" | wc -l | tr -d ' ')"
        fi
        exit "$failed"
        ;;
    *)
        echo "usage: $0 [--require-env | --assert-ran | --for-gate GATE | --except-gate GATE | --check-tags]" >&2
        exit 2
        ;;
esac
