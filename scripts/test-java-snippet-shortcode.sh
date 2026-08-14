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

render_plain_text() {
    local html_file=$1
    local text_file=$2

    sed -n '/<code class="language-java"/,/<\/code>/p' "${html_file}" \
        | sed -E 's/<[^>]+>//g' \
        >"${text_file}"
}

require_exact_text() {
    local text_file=$1
    local label=$2
    shift 2
    local expected_file="${temporary_root}/${label}.expected"
    local diff_file="${temporary_root}/${label}.diff"

    printf '%s\n' "$@" >"${expected_file}"
    if ! diff -u "${expected_file}" "${text_file}" >"${diff_file}"; then
        fail_with_log "Rendered text for ${label} did not match its expected block" "${diff_file}"
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
valid_text="${temporary_root}/valid-first.txt"
require_fragment "${valid_html}" "renderedRegionSentinel"
render_plain_text "${valid_html}" "${valid_text}"
require_exact_text "${valid_text}" valid-first \
    "" \
    "int renderedRegionSentinel =" \
    "        firstValue" \
    "                + secondValue;" \
    "" \
    "render(renderedRegionSentinel);" \
    ""
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

valid_second_html="${temporary_root}/valid/second/index.html"
valid_second_text="${temporary_root}/valid-second.txt"
require_fragment "${valid_second_html}" "secondPageSentinel"
render_plain_text "${valid_second_html}" "${valid_second_text}"
require_exact_text "${valid_second_text}" valid-second \
    "" \
    "String secondPageSentinel =" \
    "        firstPart" \
    "                + secondPart;" \
    "" \
    "render(secondPageSentinel);" \
    ""
for excluded in \
    wrapperBeforeSentinel \
    wrapperAfterSentinel \
    SupportTypeSentinel \
    'tag::valid-second' \
    'end::valid-second'; do
    if grep -Fq -- "${excluded}" "${valid_second_html}"; then
        fail_with_log "Expected rendered HTML to exclude: ${excluded}" "${valid_second_html}"
    fi
done

valid_mixed_html="${temporary_root}/valid/mixed/index.html"
valid_mixed_text="${temporary_root}/valid-mixed.txt"
require_fragment "${valid_mixed_html}" "mixedWhitespacePrefixSentinel"
render_plain_text "${valid_mixed_html}" "${valid_mixed_text}"
require_exact_text "${valid_mixed_text}" valid-mixed \
    $'\t        String mixedWhitespacePrefixSentinel =' \
    "        firstPart" \
    $'\t                + secondPart;'
for excluded in \
    wrapperBeforeSentinel \
    wrapperAfterSentinel \
    SupportTypeSentinel \
    'tag::mixed-whitespace-prefixes' \
    'end::mixed-whitespace-prefixes'; do
    if grep -Fq -- "${excluded}" "${valid_mixed_html}"; then
        fail_with_log "Expected rendered HTML to exclude: ${excluded}" "${valid_mixed_html}"
    fi
done

blank_fixture_root="${temporary_root}/blank-whitespace-fixture"
mkdir -p \
    "${blank_fixture_root}/assets/java-snippets" \
    "${blank_fixture_root}/content" \
    "${blank_fixture_root}/layouts/_default"
cp "${fixture_root}/layouts/_default/single.html" \
    "${blank_fixture_root}/layouts/_default/single.html"
printf '%s\n' \
    'title = "blank whitespace fixture"' \
    'disableKinds = ["404", "home", "RSS", "robotsTXT", "section", "sitemap", "taxonomy", "term"]' \
    '' \
    '[module]' \
    '  [[module.mounts]]' \
    '    source = "assets"' \
    '    target = "assets"' \
    '  [[module.mounts]]' \
    '    source = "layouts"' \
    '    target = "layouts"' \
    '  [[module.mounts]]' \
    "    source = \"${repository_root}/docs/layouts/_shortcodes\"" \
    '    target = "layouts/_shortcodes"' \
    >"${blank_fixture_root}/hugo.toml"
printf '%s\n' \
    '---' \
    'title: Blank whitespace' \
    '---' \
    '{{< java-snippet file="BlankWhitespace.java" tag="blank-whitespace" >}}' \
    >"${blank_fixture_root}/content/fixture.md"
printf '%s\n' \
    '// tag::blank-whitespace[]' \
    '    first();' \
    '        ' \
    '        second();' \
    '// end::blank-whitespace[]' \
    >"${blank_fixture_root}/assets/java-snippets/BlankWhitespace.java"
blank_log="${temporary_root}/blank-whitespace.log"
if ! hugo \
    --source "${blank_fixture_root}" \
    --destination "${blank_fixture_root}/public" \
    --cacheDir "${temporary_root}/cache" \
    --noBuildLock \
    --panicOnWarning \
    >"${blank_log}" 2>&1; then
    fail_with_log "Expected blank-whitespace fixture to render" "${blank_log}"
fi
blank_html="${blank_fixture_root}/public/fixture/index.html"
blank_text="${temporary_root}/blank-whitespace.txt"
render_plain_text "${blank_html}" "${blank_text}"
require_exact_text "${blank_text}" blank-whitespace \
    "first();" \
    "        " \
    "    second();"

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
