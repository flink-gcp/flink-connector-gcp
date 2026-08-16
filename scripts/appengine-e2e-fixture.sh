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
# Start or stop the fixed App Engine Standard version used by the Cloud Tasks
# gated acceptance suite. OpenTofu owns the version's configuration but not its
# serving status: a stopped manually scaled version has zero running instances,
# which is the repository's non-E2E steady state.
#
# Exit codes: 0 reached the requested state; 1 an operation failed or timed
# out; 2 local configuration, source parsing, or observation failed.

set -euo pipefail

usage() {
    echo "usage: $0 start | stop [--dry-run] | run -- command [args...]" >&2
}

command_name=${1:-}
dry_run=false
run_command=()
case "$command_name" in
    start)
        [ "$#" -eq 1 ] || {
            usage
            exit 2
        }
        ;;
    stop)
        case "${2:-}" in
            --dry-run) dry_run=true ;;
            "") ;;
            *)
                usage
                exit 2
                ;;
        esac
        [ "$#" -le 2 ] || {
            usage
            exit 2
        }
        ;;
    run)
        [ "${2:-}" = "--" ] && [ "$#" -ge 3 ] || {
            usage
            exit 2
        }
        shift 2
        run_command=("$@")
        ;;
    *)
        usage
        exit 2
        ;;
esac

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture="$root/opentofu/flink-gcp/appengine-e2e.tf"
if [ ! -f "$fixture" ]; then
    echo "Cannot find ${fixture#"$root"/}; the App Engine fixture moved." >&2
    exit 2
fi

read_fixture_local() {
    local name=$1
    sed -n "s/^[[:space:]]*${name}[[:space:]]*=[[:space:]]*\"\([^\"]*\)\".*/\1/p" \
        "$fixture" | head -1
}

service="$(read_fixture_local cloudtasks_appengine_e2e_service)"
version="$(read_fixture_local cloudtasks_appengine_e2e_version)"
if [ -z "$service" ] || [ -z "$version" ]; then
    echo "Could not read the App Engine service/version from ${fixture#"$root"/}." \
        "They are the lifecycle wrapper's single source of truth." >&2
    exit 2
fi

project=${CLOUDTASKS_IT_PROJECT:-}
if [ -z "$project" ]; then
    echo "CLOUDTASKS_IT_PROJECT is not set; it names the App Engine fixture project." >&2
    exit 2
fi

poll_attempts=${APPENGINE_E2E_POLL_ATTEMPTS:-60}
poll_seconds=${APPENGINE_E2E_POLL_SECONDS:-5}
case "$poll_attempts" in
    "" | *[!0-9]* | 0)
        echo "APPENGINE_E2E_POLL_ATTEMPTS must be a positive integer." >&2
        exit 2
        ;;
esac
case "$poll_seconds" in
    "" | *[!0-9]*)
        echo "APPENGINE_E2E_POLL_SECONDS must be a non-negative integer." >&2
        exit 2
        ;;
esac

if ! command -v gcloud >/dev/null 2>&1; then
    echo "gcloud is not on PATH. CI installs it with setup-gcloud." >&2
    exit 2
fi

status=""
instances=""
instance_count=0
instance_id=""

observe() {
    if ! status="$(gcloud app versions describe "$version" \
        --service="$service" --project="$project" \
        --format='value(servingStatus)')"; then
        echo "Could not describe App Engine version ${service}/${version} in ${project}." >&2
        return 2
    fi
    if ! instances="$(gcloud app instances list \
        --service="$service" --version="$version" --project="$project" \
        --format='value(id)')"; then
        echo "Could not list App Engine instances for ${service}/${version} in ${project}." >&2
        return 2
    fi

    instance_count=0
    instance_id=""
    local observed
    while IFS= read -r observed; do
        [ -n "$observed" ] || continue
        instance_count=$(( instance_count + 1 ))
        instance_id=$observed
    done <<< "$instances"
}

wait_for() {
    local desired_status=$1 desired_instances=$2 attempt
    for (( attempt = 1; attempt <= poll_attempts; attempt++ )); do
        observe || return $?
        if [ "$status" = "$desired_status" ] && \
            [ "$instance_count" -eq "$desired_instances" ]; then
            return 0
        fi
        if [ "$attempt" -lt "$poll_attempts" ]; then
            sleep "$poll_seconds"
        fi
    done
    echo "Timed out waiting for ${service}/${version} to reach ${desired_status} with" \
        "${desired_instances} instance(s); observed ${status:-<empty>} with" \
        "${instance_count}." >&2
    return 1
}

start_fixture() {
    echo "Starting App Engine fixture ${project}/${service}/${version}." >&2
    if ! gcloud app versions start "$version" \
        --service="$service" --project="$project" --quiet >/dev/null; then
        echo "Failed to start App Engine version ${service}/${version}." >&2
        return 1
    fi
    wait_for SERVING 1
}

stop_fixture() {
    observe || return $?
    if [ "$status" = STOPPED ] && [ "$instance_count" -eq 0 ]; then
        echo "App Engine fixture ${project}/${service}/${version} is stopped with zero instances."
        return 0
    fi
    if [ "$dry_run" = true ]; then
        echo "would stop App Engine fixture ${project}/${service}/${version}" \
            "(${status:-<empty>}, ${instance_count} instance(s))"
        return 0
    fi
    echo "Stopping App Engine fixture ${project}/${service}/${version}."
    if ! gcloud app versions stop "$version" \
        --service="$service" --project="$project" --quiet >/dev/null; then
        echo "Failed to stop App Engine version ${service}/${version}." >&2
        return 1
    fi
    wait_for STOPPED 0 || return $?
    echo "App Engine fixture ${project}/${service}/${version} is stopped with zero instances."
}

child_pid=''

finish_run() {
    local primary_status=$? cleanup_status=0
    trap - EXIT INT TERM
    set +e
    stop_fixture
    cleanup_status=$?
    set -e
    if [ "$cleanup_status" -ne 0 ] && [ "$primary_status" -ne 0 ]; then
        echo "App Engine cleanup also failed with exit code ${cleanup_status}." >&2
    fi
    if [ "$primary_status" -ne 0 ]; then
        exit "$primary_status"
    fi
    exit "$cleanup_status"
}

terminate_child() {
    local signal_status=$1
    if [ -n "$child_pid" ] && kill -0 "$child_pid" 2>/dev/null; then
        # A non-interactive shell starts asynchronous children with SIGINT
        # ignored. SIGTERM therefore provides the reliable termination path
        # for both wrapper signals; the wrapper still preserves 130 versus 143.
        kill -TERM "$child_pid" 2>/dev/null || true
        wait "$child_pid" 2>/dev/null || true
        child_pid=''
    fi
    exit "$signal_status"
}

case "$command_name" in
    start)
        start_fixture
        # Machine-readable stdout: the orchestrator in #632 passes this exact
        # instance id to the gated test. Progress stays on stderr.
        printf '%s\n' "$instance_id"
        ;;
    stop)
        stop_fixture
        ;;
    run)
        trap finish_run EXIT
        trap 'terminate_child 130' INT
        trap 'terminate_child 143' TERM

        start_fixture
        export CLOUDTASKS_IT_APPENGINE_SERVICE=$service
        export CLOUDTASKS_IT_APPENGINE_VERSION=$version
        export CLOUDTASKS_IT_APPENGINE_INSTANCE=$instance_id

        "${run_command[@]}" &
        child_pid=$!
        set +e
        wait "$child_pid"
        command_status=$?
        set -e
        child_pid=''
        exit "$command_status"
        ;;
esac
