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
# Return every billed E2E fixture to its idle state (issue #246): delete stale
# Bigtable (#218) and Spanner (#224) instances, and stop the fixed App Engine
# version used by Cloud Tasks (#630).
#
# Bigtable and Spanner sweep at the start of each gated class, and the App
# Engine lifecycle wrapper stops its version after use. A killed runner,
# cancelled job or process crash can bypass those guards. This script repeats
# all three cleanups daily so their worst-case billed lifetime is one day.
#
# One script rather than one recipe line per resource type, and the reason is
# not tidiness: a just recipe stops at its first failing line, so one cleanup
# failure could silently skip the others — the guardrail failing quietly in the
# direction that costs money. Here each cleanup is attempted independently and
# the exit status is the worst of them.
#
# The prefix and the staleness threshold are READ FROM THE JAVA SOURCE rather
# than repeated here, in the e2e-gated-its.sh tradition: a second copy of
# "flink-it-" would go stale silently, and a sweep that matches nothing looks
# exactly like a sweep with nothing to do. Every grep is a hard error.
#
# An id that carries no parsable epoch is left alone, matching the Java: this
# deletes instances, and a name it cannot date is a name it does not
# understand.
#
# Exit codes: 0 every fixture is idle; 1 a cleanup failed; 2 infrastructure
# error (no project, source or constant not found, no gcloud).

set -euo pipefail

dry_run=false
case "${1:-}" in
    --dry-run) dry_run=true ;;
    "") ;;
    *)
        echo "usage: $0 [--dry-run]" >&2
        exit 2
        ;;
esac
# An argument this does not understand must not be dropped on the floor: the
# one a caller is most likely to reach for is a narrowing flag, and silently
# ignoring it would widen a delete rather than refuse it.
if [ "$#" -gt 1 ]; then
    echo "usage: $0 [--dry-run]" >&2
    exit 2
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v gcloud >/dev/null 2>&1; then
    echo "gcloud is not on PATH. CI installs it with setup-gcloud." >&2
    exit 2
fi

# Bigtable refuses to delete an instance while any table has Change Streams enabled. Every table
# in an eligible instance belongs to this test suite, so clearing the setting on all of them is
# both safe and necessary: a crashed Change Streams test is exactly the stale instance this sweep
# exists to reclaim.
prepare_bigtable_delete() {
    local project=$1 instance=$2 tables table_name table_id
    if ! tables="$(gcloud bigtable instances tables list \
        --instance="$instance" --project="$project" --format='value(name)')"; then
        echo "could not list Bigtable tables in ${instance}" >&2
        return 1
    fi
    while IFS= read -r table_name; do
        [ -n "$table_name" ] || continue
        table_id="${table_name##*/}"
        echo "disabling Change Streams on Bigtable table ${instance}/${table_id}"
        if ! gcloud bigtable instances tables update "$table_id" \
            --instance="$instance" --project="$project" \
            --clear-change-stream-retention-period --quiet >/dev/null; then
            echo "failed to disable Change Streams on ${instance}/${table_id}" >&2
            return 1
        fi
    done <<< "$tables"
}

# Sweeps one service. Both services are reached through `gcloud <group>
# instances list|delete`, and both suites name their instances the same way, so
# the group and the class that owns the naming are the only things that differ.
#
# Usage: sweep_service <gcloud group> <project env var> <module> <class name>
# Echoes progress; returns 0 clean, 1 a delete failed, 2 infrastructure error.
sweep_service() {
    local group=$1 project_var=$2 module=$3 class=$4
    local project=${!project_var:-}
    if [ -z "$project" ]; then
        echo "$project_var is not set; it names the project to sweep for $group." >&2
        return 2
    fi

    local source_file
    source_file="$(find "$root/$module/src/test" -name "$class.java" -print -quit 2>/dev/null || true)"
    if [ -z "$source_file" ]; then
        echo "Cannot find $class.java, which owns the instance prefix and the" \
            "staleness threshold this sweep must agree with. It moved or was" \
            "renamed; update this script." >&2
        return 2
    fi

    local prefix hours
    prefix="$(sed -n 's/.*INSTANCE_PREFIX = "\([^"]*\)".*/\1/p' "$source_file" | head -1)"
    hours="$(sed -n 's/.*STALE_AFTER = Duration\.ofHours(\([0-9][0-9]*\)).*/\1/p' \
        "$source_file" | head -1)"
    if [ -z "$prefix" ] || [ -z "$hours" ]; then
        echo "Could not read INSTANCE_PREFIX / STALE_AFTER from ${source_file#"$root"/}." \
            "They are this sweep's single source of truth, so a shape this script" \
            "cannot parse is a hard error rather than a sweep that matches nothing." >&2
        return 2
    fi

    local cutoff
    cutoff=$(( $(date +%s) - hours * 3600 ))
    echo "Sweeping ${project} for '${prefix}*' ${group} instances older than ${hours}h."

    # Listed into a variable rather than piped into the loop on purpose. A
    # failing process substitution does not trip `set -e` — the loop simply
    # reads nothing — so an unauthenticated or erroring gcloud would report
    # "0 stale instances swept" and exit 0, which is the one outcome a
    # guardrail must never fake.
    #
    # The failure is checked here rather than left to `set -e`, and that is not
    # belt and braces: this function is called from a `|| outcome=$?` compound,
    # which suppresses errexit for everything inside it. Relying on the
    # assignment's own exit status brought the faked "0 swept" straight back —
    # test_a_listing_that_fails_is_not_an_empty_sweep is what caught it.
    local instances
    if ! instances="$(gcloud "$group" instances list \
        --project="$project" --format='value(name)')"; then
        echo "Could not list ${group} instances in ${project}." >&2
        return 2
    fi

    local failed=0 swept=0 name id stamp
    while IFS= read -r name; do
        [ -n "$name" ] || continue
        # The two services do not print the same thing for `value(name)`, measured 2026-08-10:
        # bigtable gives the resource path `projects/P/instances/ID`, spanner gives a bare `ID`.
        # Stripping to the last path segment handles both, because a string with no slash comes
        # through a `##*/` unchanged — so this is one line rather than a per-service field.
        id="${name##*/}"
        case "$id" in
            "$prefix"*) ;;
            *) continue ;;
        esac
        # <prefix><epoch seconds>-<run id>; the run id is absent in no case
        # today, but the Java tolerates it and so does this.
        stamp="${id#"$prefix"}"
        stamp="${stamp%%-*}"
        case "$stamp" in
            "" | *[!0-9]*) continue ;;
        esac
        [ "$stamp" -lt "$cutoff" ] || continue
        swept=$(( swept + 1 ))
        if [ "$dry_run" = true ]; then
            echo "would delete ${group} ${id} (created $(( ( $(date +%s) - stamp ) / 3600 ))h ago)"
            continue
        fi
        if [ "$group" = bigtable ] && ! prepare_bigtable_delete "$project" "$id"; then
            failed=$(( failed + 1 ))
            continue
        fi
        # A concurrent sweeper (a manually dispatched E2E run) can win the race
        # and leave this one a NOT_FOUND, which reports as a failed delete and
        # turns the job red for something benign. Deliberately not
        # special-cased: telling that apart means matching gcloud's error text,
        # and a rare red job a human reads once beats a string match that
        # silently swallows a real permission failure.
        echo "deleting ${group} ${id}"
        if ! gcloud "$group" instances delete "$id" \
            --project="$project" --quiet; then
            echo "failed to delete ${group} ${id}" >&2
            failed=$(( failed + 1 ))
        fi
    done <<< "$instances"

    if [ "$failed" -gt 0 ]; then
        echo "${failed} of ${swept} stale ${group} instance(s) could not be deleted." >&2
        return 1
    fi
    echo "${swept} stale ${group} instance(s) swept."
    return 0
}

# Each service is attempted whatever the previous one did, and the worst status
# wins — an infrastructure error (2) over a failed delete (1) over clean.
status=0

sweep() {
    local outcome=0
    sweep_service "$@" || outcome=$?
    if [ "$outcome" -gt "$status" ]; then
        status=$outcome
    fi
}

sweep bigtable BIGTABLE_IT_PROJECT \
    flink-connector-gcp-bigtable AbstractBigtableRealGcpITCase
sweep spanner SPANNER_IT_PROJECT \
    flink-connector-gcp-spanner AbstractSpannerRealGcpITCase

# App Engine is a fixed persistent version rather than a per-run instance, so
# its safe steady state is STOPPED with zero instances. The same wrapper is
# used after OpenTofu apply and by the gated E2E orchestrator; keeping the state
# transition in one place prevents the three cleanup paths from disagreeing.
appengine_args=(stop)
if [ "$dry_run" = true ]; then
    appengine_args+=(--dry-run)
fi
appengine_outcome=0
"$root/scripts/appengine-e2e-fixture.sh" "${appengine_args[@]}" || appengine_outcome=$?
if [ "$appengine_outcome" -gt "$status" ]; then
    status=$appengine_outcome
fi

exit "$status"
