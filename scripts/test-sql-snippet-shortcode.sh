#!/usr/bin/env bash
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

set -euo pipefail

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fixture_root="${repository_root}/docs/tests/fixtures/sql-snippet"
temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/sql-snippet-shortcode.XXXXXX")
trap 'rm -rf -- "${temporary_root}"' EXIT
exercised_cases=()

run_hugo() {
    local case_name=$1
    local config=${2:-hugo.toml}
    local destination="${temporary_root}/${case_name}"

    exercised_cases+=("${case_name}")
    hugo \
        --source "${fixture_root}" \
        --config "${config}" \
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
    local source_file=$1
    local fragment=$2

    if ! grep -Fq -- "${fragment}" "${source_file}"; then
        fail_with_log "Expected output to contain: ${fragment}" "${source_file}"
    fi
}

expect_failure_with_config() {
    local case_name=$1
    local config=$2
    shift 2
    local log_file="${temporary_root}/${case_name}.log"

    if run_hugo "${case_name}" "${config}" >"${log_file}" 2>&1; then
        fail_with_log "Expected fixture ${case_name} to fail" "${log_file}"
    fi
    if ! grep -Eq "cases/${case_name}/.*\\.md:[0-9]+:[0-9]+" "${log_file}"; then
        fail_with_log "Expected fixture ${case_name} to report its page position" "${log_file}"
    fi
    for fragment in "$@"; do
        require_fragment "${log_file}" "${fragment}"
    done
}

expect_failure() {
    local case_name=$1
    shift

    expect_failure_with_config "${case_name}" hugo.toml "$@"
}

valid_log="${temporary_root}/valid.log"
if ! run_hugo valid >"${valid_log}" 2>&1; then
    fail_with_log "Expected fixture valid to render" "${valid_log}"
fi

valid_html="${temporary_root}/valid/fixture/index.html"
valid_text="${temporary_root}/valid.txt"
expected_text="${temporary_root}/valid.expected"
require_fragment "${valid_html}" "rendered_sql_sentinel"
require_fragment "${valid_html}" 'data-sql-snippet-file="Fixture.sql"'
require_fragment "${valid_html}" 'data-sql-snippet-tag="valid"'
sed -n '/<code class="language-sql"/,/<\/code>/p' "${valid_html}" \
    | sed -E 's/<[^>]+>//g' \
    >"${valid_text}"
printf '%s\n' \
    "CREATE TABLE rendered_sql_sentinel (" \
    "  id BIGINT" \
    ");" \
    "" \
    "SELECT id" \
    "FROM rendered_sql_sentinel;" \
    >"${expected_text}"
if ! diff -u "${expected_text}" "${valid_text}" >"${temporary_root}/valid.diff"; then
    fail_with_log "Rendered SQL did not match its exact tagged region" \
        "${temporary_root}/valid.diff"
fi
for excluded in \
    'Copyright 2026' \
    'tag::valid' \
    'end::valid'; do
    if grep -Fq -- "${excluded}" "${valid_html}"; then
        fail_with_log "Expected rendered HTML to exclude: ${excluded}" "${valid_html}"
    fi
done

literal_log="${temporary_root}/literal-sql-fence.log"
if ! run_hugo literal-sql-fence >"${literal_log}" 2>&1; then
    fail_with_log "Expected an SQL fence shown inside an outer Markdown fence to render" \
        "${literal_log}"
fi
require_fragment "${temporary_root}/literal-sql-fence/fixture/index.html" "literal_example"

passthrough_log="${temporary_root}/passthrough-options.log"
if ! run_hugo passthrough-options >"${passthrough_log}" 2>&1; then
    fail_with_log "Expected an SQL fence outside the protected paths to preserve theme options" \
        "${passthrough_log}"
fi
passthrough_html="${temporary_root}/passthrough-options/reference/index.html"
require_fragment "${passthrough_html}" 'class="book-codeblock-filename"'
require_fragment "${passthrough_html}" 'setup.sql'
require_fragment "${passthrough_html}" 'href="https://example.test/setup.sql"'
require_fragment "${passthrough_html}" '<table class="lntable">'
require_fragment "${passthrough_html}" 'generic.txt'
require_fragment "${passthrough_html}" 'href="https://example.test/generic.txt"'

commented_log="${temporary_root}/commented-shortcode.log"
if ! run_hugo commented-shortcode >"${commented_log}" 2>&1; then
    fail_with_log "Expected a commented SQL shortcode to render no snippet" "${commented_log}"
fi
commented_html="${temporary_root}/commented-shortcode/fixture/index.html"
for excluded in \
    'data-sql-snippet-file' \
    'rendered_sql_sentinel'; do
    if grep -Fq -- "${excluded}" "${commented_html}"; then
        fail_with_log "Expected commented shortcode output to exclude: ${excluded}" \
            "${commented_html}"
    fi
done

unclosed_commented_log="${temporary_root}/unclosed-commented-shortcode.log"
if ! run_hugo unclosed-commented-shortcode >"${unclosed_commented_log}" 2>&1; then
    fail_with_log "Expected an unclosed commented SQL shortcode to render no snippet" \
        "${unclosed_commented_log}"
fi
unclosed_commented_html="${temporary_root}/unclosed-commented-shortcode/fixture/index.html"
for excluded in \
    'data-sql-snippet-file' \
    'rendered_sql_sentinel'; do
    if grep -Fq -- "${excluded}" "${unclosed_commented_html}"; then
        fail_with_log "Expected unclosed commented shortcode output to exclude: ${excluded}" \
            "${unclosed_commented_html}"
    fi
done

expect_failure missing-source-file \
    'sql-snippet file "Missing.sql"' \
    'tag "missing-source"' \
    'was not found'
expect_failure missing-start-marker \
    'sql-snippet file "Fixture.sql"' \
    'start marker for tag "missing-start"' \
    'found 0'
expect_failure duplicate-end-marker \
    'sql-snippet file "Fixture.sql"' \
    'end marker for tag "duplicate-end"' \
    'found 2'
expect_failure raw-sql-fence \
    'SQL fences in /fixture must use sql-snippet' \
    'source-backed region'
expect_failure raw-sql-fence-descendant \
    'SQL fences in /fixture/child must use sql-snippet' \
    'source-backed region'
expect_failure_with_config missing-boundary-config \
    'hugo.toml,hugo-empty-source-backed-paths.toml' \
    'sql-snippet page /fixture must be inside Site.Params.SourceBackedSqlPaths'
expect_failure_with_config missing-boundary-hook \
    'hugo.toml,hugo-empty-source-backed-paths.toml' \
    'Site.Params.SourceBackedSqlPaths must name the paths protected from raw SQL fences'

for case_directory in "${fixture_root}"/cases/*; do
    if [[ -d "${case_directory}" ]]; then
        assert_case_exercised "${case_directory##*/}"
    fi
done

printf '%s\n' "sql-snippet shortcode fixtures passed"
