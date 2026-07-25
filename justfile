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
# The commands this repository is worked with, each under one name. CI calls the
# same recipes (.github/workflows/), so what runs here is what runs there.
#
# Tools come from mise.toml. In a shell without mise activated, run
# `mise x -- just <recipe>`; recipes that need a mise-managed tool other than
# java and maven reach it through `mise x <tool> -- …` themselves, so they
# behave the same either way.
#
# Naming the tool is required, not style. Bare `mise x -- cmd` activates *every*
# tool in mise.toml and installs whatever is missing — in CI that means the lint
# job downloading a JDK, Maven and Hugo it has no use for, silently undoing the
# `install_args` that is supposed to limit it. `mise x shellcheck -- …` installs
# shellcheck and nothing else (measured, not assumed).
#
# Nothing here loads .env: mise.toml already does, with redact = true for the
# BQ_IT_* credentials, and a second loader would create a path where that
# redaction does not apply.
#
# The comment line directly above a recipe is what `just --list` prints, so it
# is kept to one line and the reasoning goes in the block above it.
#
# No top-level variable is assigned from a shell command: just evaluates those
# on every invocation, whichever recipe was asked for. A default parameter value
# runs only when its own recipe does, which is why the ceiling in
# check-flink-release is one.

set shell := ["bash", "-euo", "pipefail", "-c"]

mvn := "./mvnw -ntp"

# List the recipes.
default:
    @just --list

# Runs spotless/checkstyle (validate), unit tests, integration tests, packaging
# and the apache-rat license-header check. Extra arguments go to Maven, which is
# how the weekly version matrix selects a Flink version — passing none means the
# version pinned in pom.xml.
#
# Full build: format and license checks, unit tests, integration tests.
verify *args:
    {{ mvn }} {{ args }} verify

# The same against one specific Flink version, e.g. `just verify-flink 2.3.0`.
verify-flink version *extra:
    @just verify -Dflink.version={{ version }} {{ extra }}

# One module, e.g. `just verify-module flink-connector-gcp-bigquery`.
verify-module module:
    {{ mvn }} -pl {{ module }} verify

# Apply the formatter — CI fails on unformatted code, so run before committing.
format:
    {{ mvn }} spotless:apply

# Does a jar built against the Flink version pinned in pom.xml still run on
# <ceiling> without being recompiled? This is the measurement behind the claim
# that one artifact covers the whole supported range, and reproducing it is the
# first thing to do when the weekly binary_compat job goes red.
#
# The second run invokes surefire's goals directly rather than a lifecycle
# phase, so nothing is recompiled and the only thing that changes is the Flink
# on the classpath — that is what makes this a binary-compatibility measurement
# rather than a second build.
#
# The rest of the shape is load-bearing too. The fingerprint has to be taken
# between the two runs, because the second overwrites the surefire reports. And
# the execution ids are required: the includes/excludes that separate unit tests
# from integration tests live in execution-level configuration in
# flink-connector-parent, so a bare `surefire:test` would ignore them and
# silently skip every ITCase while still reporting green.
#
# Check that a floor-built jar still runs on Flink <ceiling>, uncompiled.
binary-compat ceiling:
    @echo '==> Build against the floor (the Flink version pinned in pom.xml)'
    {{ mvn }} verify
    @echo '==> Record which tests the floor build ran'
    @mkdir -p target
    scripts/surefire-fingerprint.sh > target/floor-tests.txt
    @echo '==> Re-run the floor-built classes on Flink {{ ceiling }}'
    {{ mvn }} -Dflink.version={{ ceiling }} surefire:test@default-test surefire:test@integration-tests
    @echo '==> Assert the same tests ran'
    scripts/surefire-fingerprint.sh > target/ceiling-tests.txt
    diff -u target/floor-tests.txt target/ceiling-tests.txt || { echo "::error::A different set of tests ran on Flink {{ ceiling }} than on the floor, so this run proves nothing about binary compatibility."; exit 1; }
    @echo "Same tests ran on both: $(wc -l < target/floor-tests.txt | tr -d ' ') classes."

# The ceiling defaults to the one the weekly workflow verifies, so the version
# still has exactly one home.
#
# Has Flink released a minor newer than the supported ceiling?
check-flink-release ceiling=`grep -m1 "FLINK_CEILING:" .github/workflows/weekly.yaml | cut -d"'" -f2`:
    scripts/check-flink-release.sh {{ ceiling }}

# Spotless and checkstyle cover the Java sources inside `just verify`; this is
# everything else.
#
# Lint the shell scripts and the justfile.
lint:
    mise x shellcheck -- shellcheck --version
    mise x shellcheck -- shellcheck scripts/*.sh
    just --fmt --check --unstable

# --panicOnWarning turns deprecations, unresolved relrefs and missing shortcodes
# into build failures.
#
# Build the documentation site, as the docs workflow does.
docs:
    mise x hugo-extended go -- hugo --gc --minify --source docs --panicOnWarning

# Preview the documentation site at http://localhost:1313.
docs-serve:
    mise x hugo-extended go -- hugo serve --source docs

# Regenerate the syntax-highlighting palettes (verbatim generator output).
docs-chroma:
    cd docs && mise x hugo-extended -- hugo gen chromastyles --style=github > assets/_chroma-light.scss
    cd docs && mise x hugo-extended -- hugo gen chromastyles --style=github-dark > assets/_chroma-dark.scss

# Pin every GitHub Actions reference to a commit SHA.
pin-actions:
    mise x pinact -- pinact run
