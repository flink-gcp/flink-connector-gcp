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
# ITCase joins the E2E run automatically, and so does the module holding it,
# which each gate's run compiles from the same derivation. A new gate is the
# decision: in these scripts it joins E2E_GATES, known_gate below, and
# gate_variables if it reads a companion variable, which e2e.yaml then also
# sets; its job and project variable follow from its name. A gate whose
# suite needs a fixture around it, as Cloud Tasks does, also gets a branch in
# run_gate. The repository guide lists what else a gate needs outside them.
#
# The `e2e` recipe uses --run and the `e2e-gate` recipe --run-gate to own execution and
# final reporting; default is useful for an undivided manual run, and the final mode is a
# per-pull-request check in ci.yaml via `just check-gated-tags`:
#
#   (default)      print the gated class names, comma-joined, for -Dtest=
#                  (a Tier-3 application's classes also need its profile, so an
#                  undivided run over the default reactor reports them NOT_RUN)
#   --gates        print the E2E gates, one per line, after checking that each
#                  selects at least one class. e2e.yaml builds its job matrix
#                  from this, so a new gate needs no workflow edit to get a job.
#   --require-env  fail unless every variable the gates read is set.
#                  @EnabledIfEnvironmentVariable turns a missing variable into a
#                  silent skip; the run modes turn it into an error, for their
#                  own gates, before any build minutes are spent.
#   --run -- CMD   run every gate, one after another, and aggregate failures.
#   --run-gate GATE -- CMD
#                  run one gate's classes and assert that they ran. e2e.yaml
#                  runs one job per gate this way, in parallel.
#   --prepare-reports remove selected old reports and record every gate's
#                  inventory, as the run modes do for their own gates.
#   --assert-ran   fail unless every gated class has a complete fresh XML report
#                  showing passing tests and none skipped — the after-the-fact
#                  proof that green meant "ran", not "skipped" — and then
#                  aggregate, as --aggregate does. Without it, a workflow that
#                  lost its credentials would go green, the worst failure mode
#                  for a job whose purpose is catching what the emulator cannot.
#                  The run modes make the same assertion for their own gates.
#   --aggregate    fail unless every gate left evidence under
#                  target/e2e/evidence/<gate>/ and, together, that evidence
#                  shows every gated class passing. A gate whose run was
#                  cancelled, skipped or lost leaves none, and must not read as
#                  green; e2e.yaml's last job runs this over every job's evidence.
#                  It reads whatever evidence is present, so it cannot tell this
#                  run's from an earlier one's, nor see a gate that failed after
#                  its tests passed; e2e.yaml also requires each gate job's
#                  success, and --run returns each gate's own status.
#   --for-gate     print only the classes using the named gate, comma-joined,
#                  for a manual -Dtest= selection; the run modes select per
#                  module instead, as --modules-for-gate prints them.
#   --except-gate  print every class except those using the named gate, for
#                  callers selecting an explicit subset outside the run modes.
#   --modules-for-gate
#                  print one line per module holding the named gate's classes:
#                  its directory, then those classes comma-joined. The run modes
#                  select modules the same way, so a module is never handed
#                  another module's class names, and so a Tier-3 application
#                  under kubernetes/apps, outside the default reactor, runs
#                  behind its tier3-<name> profile.
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

usage() {
    echo "usage: $0 [--run -- MAVEN [ARG ...] | --run-gate GATE -- MAVEN [ARG ...] | --gates | --require-env | --prepare-reports | --assert-ran | --aggregate | --for-gate GATE | --except-gate GATE | --modules-for-gate GATE | --check-tags]" >&2
    exit 2
}

# Validate selector arguments before restoring the parser, so a typo reports
# the usage error even on a machine that has not installed mise yet. This list
# mirrors check-gated-tags.py's E2E_GATES, which remains the discovery owner.
known_gate() {
    case "$1" in
        BQ_IT_PROJECT|PUBSUB_IT_PROJECT|BIGTABLE_IT_PROJECT|SPANNER_IT_PROJECT|FIRESTORE_IT_PROJECT|CLOUDTASKS_IT_PROJECT) return 0 ;;
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
        # An explicit status: a bare `return` in a function called from a trap handler returns
        # the status from before the trap, which in finish_suite is the failure that ended the
        # run, whatever Python returned. Measured with bash 5.2; macOS's 3.2 returns the
        # function's own status, so only a Linux run of the tests catches the bare form.
        local status=0
        python3 "$script_dir/$python_script" "$@" || status=$?
        return "$status"
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

# The gates a run covers, and whether it also combines their evidence: `--run` does, while
# `--run-gate` leaves that to whoever collects every gate's evidence (e2e.yaml's last job).
suite_gates=()
suite_aggregates=false

finish_suite() {
    local primary=$? reports=0 lines selected
    trap - EXIT
    # Read again rather than reusing the run's inventory, so a tree that changed under the run
    # is not checked against the inventory it started from.
    if lines=$(inventory); then
        # Every step here is guarded: errexit would end the trap before the exit status below.
        if selected=$(select_gates "$lines" "${suite_gates[@]}"); then
            run_repository_python e2e-reports.py assert --root "$PWD" <<< "$selected" \
                || reports=1
        else
            reports=1
        fi
        if [ "$suite_aggregates" = true ]; then
            run_repository_python e2e-reports.py aggregate --root "$PWD" <<< "$lines" \
                || reports=1
        fi
    else
        reports=1
    fi
    if [ "$primary" -ne 0 ]; then
        exit "$primary"
    fi
    exit "$reports"
}

start_suite() {
    local selected
    [ -n "$suite_inventory" ] || suite_inventory=$(inventory)
    selected=$(select_gates "$suite_inventory" "${suite_gates[@]}")
    run_repository_python e2e-reports.py prepare --root "$PWD" <<< "$selected"
    trap finish_suite EXIT
    trap 'cancel_suite 130' INT
    trap 'cancel_suite 143' TERM
    require_env "${suite_gates[@]}"
}

# One `GATE<TAB>MODULE<TAB>SOURCE` line per gated class, read in a single pass over the test
# sources.
suite_inventory=''

inventory() {
    run_tag_checker --root "$PWD" --inventory
}

# The lines of an inventory that belong to the named gates.
select_gates() {
    local lines=$1
    shift
    awk -F '\t' -v gates=" $* " 'index(gates, " " $1 " ")' <<< "$lines"
}

# One line per module holding the gate's classes in the run's inventory: its directory, then
# those classes comma-joined, as --modules-for-gate prints them.
gate_modules() {
    awk -F '\t' -v gate="$1" '$1 == gate {
        class = $3
        sub(/^.*\//, "", class)
        sub(/\.java$/, "", class)
        if (!($2 in classes)) {
            modules[++count] = $2
            classes[$2] = class
        } else {
            classes[$2] = classes[$2] "," class
        }
    }
    END { for (i = 1; i <= count; i++) print modules[i], classes[modules[i]] }' <<< "$suite_inventory"
}

# Prepares or asserts every gate's reports, each against its own part of the inventory, and
# after an assertion aggregates them: the whole-suite checks, without running anything.
all_reports() {
    local mode=$1 lines status=0
    lines=$(inventory) || return
    run_repository_python e2e-reports.py "$mode" --root "$PWD" <<< "$lines" || status=1
    if [ "$mode" = assert ]; then
        run_repository_python e2e-reports.py aggregate --root "$PWD" <<< "$lines" || status=1
    fi
    return "$status"
}

# Reads every gate's evidence back against that gate's part of the inventory, so that a gate
# whose run is missing fails here rather than leaving its classes out of a green summary.
aggregate_reports() {
    local lines
    lines=$(inventory) || return
    run_repository_python e2e-reports.py aggregate --root "$PWD" <<< "$lines"
}

list_gates() {
    run_tag_checker --root "$PWD" --gates
}

# The variables each gate's classes read: BQ_IT_PROJECT/BQ_IT_DATASET gate every BigQuery
# class, and BQ_IT_GCS_BUCKET additionally gates the FILE_LOADS ones. Every other gate reads
# only itself. None of Bigtable, Spanner and Firestore needs a companion variable: unlike the
# BigQuery dataset and the GCS bucket, nothing persistent is provisioned for them — each suite
# creates and deletes an instance or a database of its own — so there is no resource name to
# pass in. Cloud Tasks reads its service and version from OpenTofu, while the lifecycle
# wrapper exports the observed instance id only after startup.
gate_variables() {
    case "$1" in
        BQ_IT_PROJECT) echo BQ_IT_PROJECT BQ_IT_DATASET BQ_IT_GCS_BUCKET ;;
        *) echo "$1" ;;
    esac
}

# @EnabledIfEnvironmentVariable turns a missing variable into a silent skip; this turns it into
# an error before any build minutes are spent.
require_env() {
    local gate var
    for gate in "$@"; do
        for var in $(gate_variables "$gate"); do
            if [ -z "${!var:-}" ]; then
                echo "::error::$var is not set, so the gated real-GCP ITCases would silently skip. Locally the variables come from the uncommitted .env at the repository root, which mise loads." >&2
                return 1
            fi
        done
    done
}

# Runs one module's gated classes. The command, and any options that must precede the module
# selection, come first: Maven itself, or Maven behind the App Engine fixture or a profile.
run_module_its() {
    local module=$1 classes=$2
    shift 2
    run_child "$@" -pl "$module" surefire:test@integration-tests \
        -Dtest.excluded.groups= -Dsurefire.rerunFailingTestsCount=0 "-Dtest=$classes" < /dev/null
}

# The connector modules resolve the base and test-utils modules from the local repository,
# since every later build is a -pl subset of the reactor.
install_shared_modules() {
    run_child "$@" -pl .,flink-connector-gcp-base,flink-connector-gcp-test-utils -DskipTests -Drat.skip=true install < /dev/null
}

# Runs one gate's classes, module by module, and fails if any of them fails. --run invokes it
# in a condition, where errexit does not apply, so every step states its own failure.
run_gate() {
    local gate=$1 outcome=0 modules module classes compile=''
    shift
    modules=$(gate_modules "$gate")
    if [ -z "$modules" ]; then
        echo "::error::no gated class selects $gate in this run's inventory" >&2
        return 1
    fi
    while read -r module classes; do
        case "$module" in
            kubernetes/apps/*) ;;
            *) compile=${compile:+$compile,}$module ;;
        esac
    done <<< "$modules"
    if [ "$gate" = CLOUDTASKS_IT_PROJECT ]; then
        # The App Engine fixture serves the Cloud Tasks connector alone, so the gate must list
        # exactly one module and it must be that one; anything else is refused rather than run
        # without the fixture or handed to that module.
        read -r module classes <<< "$modules"
        if [ "$modules" != "$module $classes" ] || [ "$module" != flink-connector-gcp-cloudtasks ]; then
            echo "::error::CLOUDTASKS_IT_PROJECT gates classes outside flink-connector-gcp-cloudtasks: $modules" >&2
            return 1
        fi
        run_child "$@" -pl "$compile" test-compile < /dev/null || return 1
        run_module_its "$module" "$classes" scripts/appengine-e2e-fixture.sh run -- "$@" \
            || outcome=1
        # A test failure can return the same status as a failed stop. Verify the idle state
        # independently, so a version left serving fails this gate rather than waiting for the
        # sweep. No other gate depends on it: each billed fixture has a lifecycle of its own.
        run_child scripts/appengine-e2e-fixture.sh stop || outcome=1
        return "$outcome"
    fi
    if [ -n "$compile" ]; then
        run_child "$@" -pl "$compile" test-compile < /dev/null || return 1
    fi
    while read -r module classes; do
        case "$module" in
            kubernetes/apps/*)
                # A Tier-3 application sits outside the default reactor, behind its
                # tier3-<name> profile, and resolves its connector from the local
                # repository. Install that connector from this tree first, so a stale
                # snapshot cannot stand in for it.
                run_child "$@" -pl "flink-connector-gcp-${module##*/}" \
                    -DskipTests -Drat.skip=true install < /dev/null \
                    && run_module_its "$module" "$classes" "$@" -P "tier3-${module##*/}" test-compile \
                    || outcome=1
                ;;
            *)
                run_module_its "$module" "$classes" "$@" || outcome=1
                ;;
        esac
    done <<< "$modules"
    return "$outcome"
}

# One gate end to end: its own inventory, variables, build and assertion. This is the unit
# e2e.yaml runs in a job per gate.
run_gate_suite() {
    local gate=$1
    shift
    suite_gates=("$gate")
    start_suite
    install_shared_modules "$@"
    run_gate "$gate" "$@"
}

# Every gate, one after another, then the same aggregation e2e.yaml's last job performs. Each
# gate runs even if an earlier one failed; a failed shared install stops the run.
run_suite() {
    local outcome=0 gate gates
    suite_inventory=$(inventory)
    gates=$(cut -f 1 <<< "$suite_inventory" | uniq)
    # shellcheck disable=SC2206 # one word per gate
    suite_gates=($gates)
    suite_aggregates=true
    start_suite
    install_shared_modules "$@"
    for gate in "${suite_gates[@]}"; do
        run_gate "$gate" "$@" || outcome=1
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
        # Still as strict as it was before the tag existed (issue #245 asked): the tag makes the
        # suite opt-in per command, and `just e2e` *is* that opt-in, so a variable missing inside
        # it is a broken run, not a choice.
        [ "$#" -eq 1 ] || usage
        gates=$(list_gates)
        # shellcheck disable=SC2086 # one word per gate
        require_env $gates
        ;;
    --gates)
        [ "$#" -eq 1 ] || usage
        list_gates
        ;;
    --for-gate)
        if [ "$#" -ne 2 ] || ! known_gate "${2:-}"; then
            usage
        fi
        print_classes "$2"
        ;;
    --modules-for-gate)
        if [ "$#" -ne 2 ] || ! known_gate "${2:-}"; then
            usage
        fi
        suite_inventory=$(inventory)
        gate_modules "$2"
        ;;
    --except-gate)
        if [ "$#" -ne 2 ] || ! known_gate "${2:-}"; then
            usage
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
    --run-gate)
        if [ "$#" -lt 4 ] || ! known_gate "$2" || [ "$3" != -- ]; then
            echo "usage: $0 --run-gate GATE -- MAVEN [ARG ...]" >&2
            exit 2
        fi
        gate=$2
        shift 3
        run_gate_suite "$gate" "$@"
        ;;
    --prepare-reports|--assert-ran)
        [ "$#" -eq 1 ] || {
            echo "usage: $0 --prepare-reports | --assert-ran" >&2
            exit 2
        }
        mode=assert
        [ "$1" != --prepare-reports ] || mode=prepare
        all_reports "$mode"
        ;;
    --aggregate)
        [ "$#" -eq 1 ] || usage
        aggregate_reports
        ;;
    --check-tags)
        [ "$#" -eq 1 ] || usage
        run_tag_checker --root "$PWD" --check
        exit
        ;;
    *)
        usage
        ;;
esac
