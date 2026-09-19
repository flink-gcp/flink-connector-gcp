#!/usr/bin/env bash
#
# Copyright 2026 The flink-gcp authors
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
# check-gated-tags.py's E2E_GATES is one.
# Deriving the list from the gating annotation itself means a newly gated
# ITCase joins the E2E run automatically — and one added outside the modules
# the `e2e` recipe builds makes --assert-ran fail until the recipe learns to
# run it, which is the point: joining the E2E workflow is a decision, not an
# accident.
#
# The `e2e` recipe uses --run to own execution and final reporting; default is
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
#   --run -- CMD   run the complete suite and aggregate failures.
#   --prepare-reports remove selected old reports and record this run's inventory.
#   --assert-ran   fail unless every gated class has a complete fresh XML report
#                  showing passing tests and none skipped — the after-the-fact proof that
#                  green meant "ran", not "skipped". Without it, a workflow
#                  that lost its credentials would go green, the worst failure
#                  mode for a job whose purpose is catching what the emulator
#                  cannot.
#   --for-gate     print only the classes using the named gate. The App Engine
#                  lifecycle uses this to start its billed instance around the
#                  Cloud Tasks class and nothing else.
#   --except-gate  print every class except those using the named gate, for
#                  callers selecting an explicit subset outside --run.
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

# Validate selector arguments before restoring the parser, so a typo reports
# the usage error even on a machine that has not installed mise yet. This list
# mirrors check-gated-tags.py's E2E_GATES, which remains the discovery owner.
known_gate() {
    case "$1" in
        BQ_IT_PROJECT|PUBSUB_IT_PROJECT|BIGTABLE_IT_PROJECT|SPANNER_IT_PROJECT|CLOUDTASKS_IT_PROJECT) return 0 ;;
        *) return 1 ;;
    esac
}

# The E2E suite: every test class gated on one of the variables that workflow
# sets. Fatal when a gate matches nothing, since zero gated ITCases for a
# connector means the annotation moved or the tree layout changed, and the
# modes below would otherwise degenerate into a vacuous pass for that connector
# while the others keep the union non-empty.
#
# A function rather than a top-level block: --check-tags is deliberately
# gate-agnostic (see there) and must not require every configured E2E gate to match.
gated_sources() {
    local only_gate=${1:-} except_gate=${2:-} arguments=(--root "$PWD")
    [ -z "$only_gate" ] || arguments+=(--for-gate "$only_gate")
    [ -z "$except_gate" ] || arguments+=(--except-gate "$except_gate")
    run_tag_checker "${arguments[@]}"
}

run_repository_python() {
    local python_script=$1
    shift
    local script_dir repository_root
    script_dir=$(cd "$(dirname "$0")" && pwd)
    repository_root=$(cd "$script_dir/.." && pwd)
    if [ "${UV_RUN_RECURSION_DEPTH:-0}" -gt 0 ] \
        && [ "${VIRTUAL_ENV:-}" = "$repository_root/.venv" ] \
        && command -v python3 >/dev/null 2>&1 \
        && python3 -c 'import tree_sitter, tree_sitter_java' >/dev/null 2>&1; then
        python3 "$script_dir/$python_script" "$@"
        return
    fi
    if ! command -v mise >/dev/null 2>&1; then
        echo "::error::the locked Tree-sitter parser is unavailable; run through 'just e2e' or 'just check-gated-tags', or install mise" >&2
        return 2
    fi
    if ! mise x -C "$repository_root" uv -- \
        uv run --project "$repository_root" --locked python \
        -c 'import tree_sitter, tree_sitter_java'; then
        echo "::error::could not restore the locked Tree-sitter parser" >&2
        return 2
    fi
    mise x -C "$repository_root" uv -- \
        uv run --project "$repository_root" --locked python \
        "$script_dir/$python_script" "$@"
}

run_tag_checker() {
    run_repository_python check-gated-tags.py "$@"
}

suite_child=''

run_child() {
    local status=0
    "$@" &
    suite_child=$!
    wait "$suite_child" || status=$?
    suite_child=''
    return "$status"
}

cancel_suite() {
    local status=$1
    trap - INT TERM
    if [ -n "$suite_child" ]; then
        kill -TERM "$suite_child" 2>/dev/null || true
        wait "$suite_child" || true
    fi
    exit "$status"
}

finish_suite() {
    local primary=$? reports=0
    trap - EXIT
    scripts/e2e-gated-its.sh --assert-ran || reports=$?
    if [ "$primary" -ne 0 ]; then
        exit "$primary"
    fi
    exit "$reports"
}

run_suite() {
    local outcome=0 gate module classes
    local maven=("$@")
    scripts/e2e-gated-its.sh --prepare-reports
    trap finish_suite EXIT
    trap 'cancel_suite 130' INT
    trap 'cancel_suite 143' TERM
    scripts/e2e-gated-its.sh --require-env
    run_child "${maven[@]}" -pl .,flink-connector-gcp-base,flink-connector-gcp-test-utils -DskipTests -Drat.skip=true install
    run_child "${maven[@]}" -pl flink-connector-gcp-bigquery,flink-connector-gcp-pubsub,flink-connector-gcp-cloudtasks,flink-connector-gcp-bigtable,flink-connector-gcp-spanner test-compile
    classes=$(scripts/e2e-gated-its.sh --for-gate CLOUDTASKS_IT_PROJECT)
    run_child scripts/appengine-e2e-fixture.sh run -- "${maven[@]}" -pl flink-connector-gcp-cloudtasks \
        surefire:test@integration-tests -Dtest.excluded.groups= -Dsurefire.rerunFailingTestsCount=0 "-Dtest=$classes" || outcome=1
    # A test failure can return the same status as a failed stop. Verify the idle state
    # independently before starting more billed fixtures.
    run_child scripts/appengine-e2e-fixture.sh stop
    for gate in BQ_IT_PROJECT PUBSUB_IT_PROJECT BIGTABLE_IT_PROJECT SPANNER_IT_PROJECT; do
        case "$gate" in
            BQ_IT_PROJECT) module=bigquery ;;
            PUBSUB_IT_PROJECT) module=pubsub ;;
            BIGTABLE_IT_PROJECT) module=bigtable ;;
            SPANNER_IT_PROJECT) module=spanner ;;
        esac
        classes=$(scripts/e2e-gated-its.sh --for-gate "$gate")
        run_child "${maven[@]}" -pl "flink-connector-gcp-$module" surefire:test@integration-tests \
            -Dtest.excluded.groups= -Dsurefire.rerunFailingTestsCount=0 "-Dtest=$classes" || outcome=1
    done
    return "$outcome"
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
            echo "usage: $0 [--run -- MAVEN [ARG ...] | --require-env | --prepare-reports | --assert-ran | --for-gate GATE | --except-gate GATE | --check-tags]" >&2
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
            echo "usage: $0 [--run -- MAVEN [ARG ...] | --require-env | --prepare-reports | --assert-ran | --for-gate GATE | --except-gate GATE | --check-tags]" >&2
            exit 2
        fi
        print_classes "$2"
        ;;
    --except-gate)
        if [ "$#" -ne 2 ] || ! known_gate "${2:-}"; then
            echo "usage: $0 [--run -- MAVEN [ARG ...] | --require-env | --prepare-reports | --assert-ran | --for-gate GATE | --except-gate GATE | --check-tags]" >&2
            exit 2
        fi
        print_classes '' "$2"
        ;;
    --run)
        if [ "$#" -lt 3 ] || [ "$2" != -- ]; then
            echo "usage: $0 --run -- MAVEN [ARG ...]" >&2
            exit 2
        fi
        shift 2
        run_suite "$@"
        ;;
    --prepare-reports|--assert-ran)
        [ "$#" -eq 1 ] || {
            echo "usage: $0 --prepare-reports | --assert-ran" >&2
            exit 2
        }
        sources=$(gated_sources)
        mode=assert
        [ "$1" != --prepare-reports ] || mode=prepare
        run_repository_python e2e-reports.py "$mode" --root "$PWD" <<< "$sources"
        ;;
    --check-tags)
        [ "$#" -eq 1 ] || {
            echo "usage: $0 [--run -- MAVEN [ARG ...] | --require-env | --prepare-reports | --assert-ran | --for-gate GATE | --except-gate GATE | --check-tags]" >&2
            exit 2
        }
        run_tag_checker --root "$PWD" --check
        exit
        ;;
    *)
        echo "usage: $0 [--run -- MAVEN [ARG ...] | --require-env | --prepare-reports | --assert-ran | --for-gate GATE | --except-gate GATE | --check-tags]" >&2
        exit 2
        ;;
esac
