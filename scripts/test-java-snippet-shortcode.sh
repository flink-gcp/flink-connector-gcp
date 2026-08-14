#!/usr/bin/env bash
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

set -euo pipefail

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fixture_root="${repository_root}/docs/tests/fixtures/java-snippet"
temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/java-snippet-shortcode.XXXXXX")
trap 'rm -rf -- "${temporary_root}"' EXIT
exercised_cases=()

run_hugo() {
    local case_name=$1
    local destination="${temporary_root}/${case_name}"

    exercised_cases+=("${case_name}")
    hugo \
        --source "${fixture_root}" \
        --contentDir "cases/${case_name}" \
        --destination "${destination}" \
        --cacheDir "${temporary_root}/cache" \
        --noBuildLock \
        --panicOnWarning
}

assert_case_exercised() {
    local case_name=$1
    local exercised_case

    for exercised_case in "${exercised_cases[@]}"; do
        if [[ "${exercised_case}" == "${case_name}" ]]; then
            return
        fi
    done

    printf '%s\n' "Fixture case ${case_name} is not exercised by this runner" >&2
    exit 1
}

fail_with_log() {
    local message=$1
    local log_file=$2

    printf '%s\n' "${message}" >&2
    printf '%s\n' "Hugo output:" >&2
    sed 's/^/  /' "${log_file}" >&2
    exit 1
}

require_fragment() {
    local log_file=$1
    local fragment=$2

    if ! grep -Fq -- "${fragment}" "${log_file}"; then
        fail_with_log "Expected Hugo output to contain: ${fragment}" "${log_file}"
    fi
}

expect_failure() {
    local case_name=$1
    shift
    local log_file="${temporary_root}/${case_name}.log"

    if run_hugo "${case_name}" >"${log_file}" 2>&1; then
        fail_with_log "Expected fixture ${case_name} to fail" "${log_file}"
    fi
    if ! grep -Eq "cases/${case_name}/fixture\\.md:[0-9]+:[0-9]+" "${log_file}"; then
        fail_with_log "Expected fixture ${case_name} to report its page position" "${log_file}"
    fi
    for fragment in "$@"; do
        require_fragment "${log_file}" "${fragment}"
    done
}

valid_log="${temporary_root}/valid.log"
if ! run_hugo valid >"${valid_log}" 2>&1; then
    fail_with_log "Expected fixture valid to render" "${valid_log}"
fi

valid_html="${temporary_root}/valid/fixture/index.html"
require_fragment "${valid_html}" "renderedRegionSentinel"
for excluded in \
    wrapperBeforeSentinel \
    wrapperAfterSentinel \
    SupportTypeSentinel \
    'tag::valid' \
    'end::valid'; do
    if grep -Fq -- "${excluded}" "${valid_html}"; then
        fail_with_log "Expected rendered HTML to exclude: ${excluded}" "${valid_html}"
    fi
done

expect_failure missing-file-argument \
    "requires non-empty named file and tag arguments"
expect_failure missing-tag-argument \
    "requires non-empty named file and tag arguments"
expect_failure missing-source-file \
    'file "Missing.java"' \
    'tag "missing-source"' \
    "was not found"
expect_failure missing-start-marker \
    'file "MarkerFixtures.java"' \
    'start marker for tag "missing-start"' \
    "found 0"
expect_failure duplicate-start-marker \
    'file "MarkerFixtures.java"' \
    'start marker for tag "duplicate-start"' \
    "found 2"
expect_failure missing-end-marker \
    'file "MarkerFixtures.java"' \
    'end marker for tag "missing-end"' \
    "found 0"
expect_failure duplicate-end-marker \
    'file "MarkerFixtures.java"' \
    'end marker for tag "duplicate-end"' \
    "found 2"
expect_failure reversed-region \
    'file "MarkerFixtures.java"' \
    'end marker before the start marker for tag "reversed"'
expect_failure empty-region \
    'file "MarkerFixtures.java"' \
    'empty region for tag "empty"'
expect_failure trailing-start-text \
    'file "MarkerFixtures.java"' \
    'start marker for tag "trailing-start"' \
    "found 0"
expect_failure trailing-end-text \
    'file "MarkerFixtures.java"' \
    'end marker for tag "trailing-end"' \
    "found 0"

for case_directory in "${fixture_root}"/cases/*; do
    if [[ -d "${case_directory}" ]]; then
        assert_case_exercised "${case_directory##*/}"
    fi
done

printf '%s\n' "java-snippet shortcode fixtures passed"
