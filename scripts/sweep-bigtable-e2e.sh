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
# Delete Bigtable instances the E2E suite abandoned (issue #246).
#
# AbstractBigtableRealGcpITCase already sweeps at the start of a gated class,
# but the only thing that schedules a gated class is the weekly E2E workflow —
# so a run whose teardown never executed (killed runner, cancelled job, a JVM
# crash after @BeforeAll) leaves a one-node instance standing until the next
# Saturday. At $0.65 a node-hour that is about $109 per leak, and two classes
# run per execution. This script is the same sweep on a daily schedule, which
# turns a 7-day worst case into a 1-day one.
#
# The prefix and the staleness threshold are READ FROM THE JAVA SOURCE rather
# than repeated here, in the e2e-gated-its.sh tradition: a second copy of
# "flink-it-" would go stale silently, and a sweep that matches nothing looks
# exactly like a sweep with nothing to do. Both greps are hard errors.
#
# An id that carries no parsable epoch is left alone, matching the Java: this
# deletes instances, and a name it cannot date is a name it does not
# understand.
#
# Exit codes: 0 nothing stale, or everything stale deleted; 1 a delete failed;
# 2 infrastructure error (no project, source or constant not found, no gcloud).

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

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -z "${BIGTABLE_IT_PROJECT:-}" ]; then
    echo "BIGTABLE_IT_PROJECT is not set; it names the project to sweep." >&2
    exit 2
fi

if ! command -v gcloud >/dev/null 2>&1; then
    echo "gcloud is not on PATH. CI installs it with setup-gcloud." >&2
    exit 2
fi

source_file="$(find "$root/flink-connector-gcp-bigtable/src/test" \
    -name AbstractBigtableRealGcpITCase.java -print -quit 2>/dev/null || true)"
if [ -z "$source_file" ]; then
    echo "Cannot find AbstractBigtableRealGcpITCase.java, which owns the" \
        "instance prefix and the staleness threshold this sweep must agree" \
        "with. It moved or was renamed; update this script." >&2
    exit 2
fi

prefix="$(sed -n 's/.*INSTANCE_PREFIX = "\([^"]*\)".*/\1/p' "$source_file" | head -1)"
hours="$(sed -n 's/.*STALE_AFTER = Duration\.ofHours(\([0-9][0-9]*\)).*/\1/p' \
    "$source_file" | head -1)"
if [ -z "$prefix" ] || [ -z "$hours" ]; then
    echo "Could not read INSTANCE_PREFIX / STALE_AFTER from ${source_file#"$root"/}." \
        "They are this sweep's single source of truth, so a shape this script" \
        "cannot parse is a hard error rather than a sweep that matches nothing." >&2
    exit 2
fi

cutoff=$(( $(date +%s) - hours * 3600 ))
echo "Sweeping ${BIGTABLE_IT_PROJECT} for '${prefix}*' instances older than ${hours}h."

# Listed into a variable rather than piped into the loop on purpose. A failing
# process substitution does not trip `set -e` — the loop simply reads nothing —
# so an unauthenticated or erroring gcloud would report "0 stale instances
# swept" and exit 0, which is the one outcome a guardrail must never fake. An
# assignment's exit status *is* the command substitution's, so this fails.
instances="$(gcloud bigtable instances list \
    --project="$BIGTABLE_IT_PROJECT" --format='value(name)')"

failed=0
swept=0
while IFS= read -r name; do
    [ -n "$name" ] || continue
    id="${name##*/}"
    case "$id" in
        "$prefix"*) ;;
        *) continue ;;
    esac
    # <prefix><epoch seconds>-<run id>; the run id is absent in no case today,
    # but the Java tolerates it and so does this.
    stamp="${id#"$prefix"}"
    stamp="${stamp%%-*}"
    case "$stamp" in
        "" | *[!0-9]*) continue ;;
    esac
    [ "$stamp" -lt "$cutoff" ] || continue
    swept=$(( swept + 1 ))
    if [ "$dry_run" = true ]; then
        echo "would delete ${id} (created $(( ( $(date +%s) - stamp ) / 3600 ))h ago)"
        continue
    fi
    echo "deleting ${id}"
    if ! gcloud bigtable instances delete "$id" \
        --project="$BIGTABLE_IT_PROJECT" --quiet; then
        echo "failed to delete ${id}" >&2
        failed=$(( failed + 1 ))
    fi
done <<< "$instances"

if [ "$failed" -gt 0 ]; then
    echo "${failed} of ${swept} stale instance(s) could not be deleted." >&2
    exit 1
fi
echo "${swept} stale instance(s) swept."
