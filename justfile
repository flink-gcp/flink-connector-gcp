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
# Those two forms differ on purpose. Bare `mise x -- cmd` activates *every* tool
# in mise.toml and installs whatever is missing, which is what the entrypoint
# wants — `just verify` needs java and maven on PATH and names neither — and is
# wrong inside a recipe, where it silently undoes the `install_args` limiting a
# CI job: the lint job first ran that way and downloaded a JDK, Maven, Hugo, Go
# and a second copy of just before reaching `shellcheck --version`.
# `mise x shellcheck -- …` installs shellcheck and nothing else (measured
# against an empty MISE_DATA_DIR, not assumed).
#
# Nothing here loads .env: mise.toml already does, with redact = true for the
# BQ_IT_* credentials, and a second loader would create a path where that
# redaction does not apply.
#
# `just --list` prints the *last* line of the comment block above a recipe, so
# that line is a one-line description and the reasoning goes above it. Get this
# wrong and the listing reads as a column of sentence fragments.
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

# `just help` would otherwise be read as a recipe name and fail with "justfile
# does not contain recipe `help`", which is a poor answer to someone asking what
# there is to run. `-h` and `--help` are just's own CLI help — a hundred lines of
# options, not this project's commands — so they are not the same thing.
alias help := default

# Runs spotless/checkstyle (validate), unit tests, integration tests, packaging
# and the apache-rat license-header check. Extra arguments go to Maven, which is
# how the weekly version matrix selects a Flink version — passing none means the
# version pinned in pom.xml.
#
# Full build: format and license checks, unit tests, integration tests.
verify *args:
    {{ mvn }} {{ args }} verify

# The same against one specific Flink version, e.g. `just verify-flink 2.3.0`.
# A 1.x version also selects the flink1 compat source root (see the
# flink.compat property in pom.xml) — forgetting that flag by hand is exactly
# the mistake this recipe exists to absorb.
verify-flink version *extra:
    @just verify -Dflink.version={{ version }} {{ if version =~ '^1\.' { '-Dflink.compat=flink1' } else { '' } }} {{ extra }}

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
# The install step exists because the rerun, being goal-only, cannot resolve
# inter-module dependencies from the reactor — the same mechanism the licence
# goal's phase rule guards against (see CLAUDE.md under Build). The SQL
# uber-jar bundles the Pub/Sub connector (#138, the repository's first
# inter-module dependency), so without the install the rerun dies resolving it
# (#181) — and excluding that module instead would drop the most
# binary-compat-shaped case there is: the jar shaded on the floor, tested on
# the ceiling. The test-utils module installs for the same reason: every
# module's tests depend on it (#27) — as does the base module, which every
# connector compiles against (#61). The root pom installs with them because
# the installed poms name it as parent. Cost, when run by hand:
# io.github.flink-gcp SNAPSHOTs land in ~/.m2 — the primed-local-repo hazard —
# `rm -rf ~/.m2/repository/io/github/flink-gcp` undoes it.
#
# Check that a floor-built jar still runs on Flink <ceiling>, uncompiled.
binary-compat ceiling:
    @echo '==> Build against the floor (the Flink version pinned in pom.xml)'
    {{ mvn }} verify
    @echo '==> Install what the goal-only rerun cannot resolve from the reactor'
    {{ mvn }} -pl .,flink-connector-gcp-base,flink-connector-gcp-pubsub,flink-connector-gcp-test-utils -DskipTests install
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
# still has exactly one home. That default is long enough that `just --list`
# wraps this entry onto two lines — a [doc] attribute does not help, the
# signature itself is what wraps — which is accepted in exchange for the
# listing showing where the version comes from.
#
# Has Flink released a minor newer than the supported ceiling?
check-flink-release ceiling=`grep -m1 "FLINK_CEILING:" .github/workflows/weekly.yaml | cut -d"'" -f2`:
    scripts/check-flink-release.sh {{ ceiling }}

# The ITCases gated on BQ_IT_* variables, which `just verify` silently skips:
# what they check is exactly what the emulator cannot (see the testing section
# of the BigQuery connector documentation). The E2E workflow
# (.github/workflows/e2e.yaml) runs this same recipe weekly via WIF; locally the
# variables come from the uncommitted .env, which mise loads — so a worktree
# cannot run this until #156 settles how .env reaches one.
#
# The shape is scripts/e2e-gated-its.sh three times around two Maven calls. The
# pre-flight makes a missing variable an error before any build minutes are
# spent, and the assertion afterwards proves the gated classes ran — without
# it, @EnabledIfEnvironmentVariable turns lost credentials into a green run.
#
# The execution id on the second Maven call is load-bearing, same as in
# binary-compat: -Dtest overrides includes on *every* surefire execution, so
# without @integration-tests the default-test execution would run the same
# classes a second time.
#
# Make the main checkout's .env reachable from this worktree (issue #156;
# the guard logic lives in the script, where shellcheck reads it).
worktree-env:
    scripts/worktree-env.sh

# Run the real-GCP gated ITCases and assert they actually ran.
#
# The install step mirrors binary-compat's: the two -pl builds below are
# reactor subsets, so the test-utils module the gated tests depend on (#27)
# and the base module the connectors compile against (#61) must come from
# ~/.m2, not the reactor. Same hand-run cost too — the
# io.github.flink-gcp SNAPSHOTs it leaves behind are removed with
# `rm -rf ~/.m2/repository/io/github/flink-gcp`.
#
# rat is skipped in that install because building `.` runs apache-rat over the
# whole working tree, and in the E2E workflow the tree contains the WIF
# credentials file google-github-actions/auth writes into the workspace root
# (gha-creds-*.json, no licence header — measured 2026-08-01: the first
# dispatch after #27 added this step failed exactly there, on a tree ci.yaml
# had just passed). The header check is not lost: ci.yaml's verify runs rat on
# every pull request, and this install only primes ~/.m2.
e2e:
    scripts/e2e-gated-its.sh --require-env
    {{ mvn }} -pl .,flink-connector-gcp-base,flink-connector-gcp-test-utils -DskipTests -Drat.skip=true install
    {{ mvn }} -pl flink-connector-gcp-bigquery,flink-connector-gcp-pubsub test-compile
    {{ mvn }} -pl flink-connector-gcp-bigquery,flink-connector-gcp-pubsub surefire:test@integration-tests -Dtest="$(scripts/e2e-gated-its.sh)"
    scripts/e2e-gated-its.sh --assert-ran

# Spotless and checkstyle cover the Java sources inside `just verify`; this is
# everything else.
#
# `just --fmt --check` is deliberately not here. It is an unstable feature, and
# just's compatibility guarantee covers stable ones only — so a newer just could
# reformat and fail a pull request that changed nothing, which is exactly the
# failure mode that makes shellcheck worth pinning. Since `just` is installed
# unpinned (see ci.yaml), depending on it would reintroduce the problem the pin
# exists to solve, to check the formatting of a single file.
#
# actionlint shells out to shellcheck for inline `run:` blocks, and picks it off
# PATH by default. That default is wrong here: the GitHub runner image ships its
# own shellcheck (0.9.0 on ubuntu-24.04) and would quietly be preferred to the
# version mise.toml pins, so the workflows would be linted by a shellcheck this
# repository did not choose. `mise which` names the pinned one outright. Passing
# an empty string to -shellcheck disables the integration entirely, so a typo
# here silently stops checking `run:` blocks rather than failing.
#
# ruff is pinned exactly, for the same reason as shellcheck and actionlint: a
# linter that gains a rule fails a pull request that changed nothing. `check` and
# `format --check` are separate goals in ruff, and running only the first would
# leave formatting unchecked.
#
# tofu contributes only `fmt -check` here. `tofu validate` needs an init that
# downloads the google provider — too slow for a lint meant to report in
# seconds — and every pull request that touches opentofu/ gets a full plan
# from the tofu-plan workflow, which subsumes validate on exactly the changes
# that need it. Locally, `just tofu validate` runs it directly.
#
# Lint the shell and Python scripts, the workflows, the rendered markdown, and
# the OpenTofu formatting. markdownlint's file set and rule deviations live in
# .markdownlint-cli2.jsonc (discovered from the working directory).
lint:
    mise x shellcheck -- shellcheck --version
    mise x shellcheck -- shellcheck scripts/*.sh
    mise x ruff -- ruff --version
    mise x ruff -- ruff check scripts/
    mise x ruff -- ruff format --check scripts/
    mise x actionlint -- actionlint -shellcheck "$(mise which shellcheck)"
    mise x npm:markdownlint-cli2 -- markdownlint-cli2
    mise x opentofu -- tofu fmt -check -recursive opentofu/

# The GCP resources behind the real-GCP integration tests (service accounts,
# WIF, buckets, the dataset) live in opentofu/flink-gcp, planned and applied
# by CI (tofu-plan.yaml / tofu-apply.yaml). This is the local escape hatch —
# state inspection, validate, and the bootstrap documented in
# opentofu/README.md. Credentials come from GOOGLE_APPLICATION_CREDENTIALS,
# which .env sets — the google provider does not read CLOUDSDK_CONFIG (only
# the gcloud CLI does; see opentofu/README.md).
#
# Run OpenTofu in the flink-gcp root module, e.g. `just tofu plan`.
tofu *args:
    mise x opentofu -- tofu -chdir=opentofu/flink-gcp {{ args }}

# Regenerates the resolved-licence report first, because the check is only as
# current as that file — a stale one would report a bundle that no longer exists.
# Reusable as-is by the other flink-sql-connector-gcp-* modules to come.
#
# A lifecycle phase with `-am`, not a bare `license:add-third-party` goal. The
# goal form was tried and fails in CI: a goal invocation does not build reactor
# siblings, so the module cannot resolve the connector it bundles — `-am` does not
# change that — and it only appears to work where an earlier `install` left the
# artifact in the local repository. A phase builds the sibling, so this is
# self-contained. Any phase at or after `compile` would do — that is the property
# that matters, since it is what puts the sibling's `target/classes` in front of
# the reactor. generate-test-resources is where the licence goal is bound, beside
# the two maven-dependency-plugin executions that feed the module's own tests.
#
# Does the module's generated META-INF/NOTICE still match what it bundles?
check-notice module:
    {{ mvn }} -pl {{ module }} -am generate-test-resources
    scripts/check-notice.py {{ module }}

# Rewrites META-INF/NOTICE from the module's NOTICE.template and the resolved
# bundle, and re-materialises META-INF/licenses/ from the pinned sources in
# scripts/licence-sources.toml (fetching over HTTPS where the artifact's own jar
# ships no licence text). Run after a dependency change, review the diff, commit.
#
# Regenerate the module's META-INF/NOTICE and META-INF/licenses/.
update-notice module:
    {{ mvn }} -pl {{ module }} -am generate-test-resources
    scripts/check-notice.py --update {{ module }}

# Classifies every org.apache.flink type the main sources import by its
# class-level stability annotation, read from the -sources.jars (never class
# files: a class file's constant pool lists method-level annotations too, the
# bug that produced the wrong numbers on issue #103). @Internal, @Experimental
# and unannotated types must each have a reasoned allowlist entry in
# scripts/flink-api-tiers.toml; anything new — or stale — fails. Downloads the
# sources jars from Maven Central into target/flink-api-tiers/ on first run,
# which is why this is not part of `just lint` (that stays offline).
#
# Do the main sources depend only on allowlisted Flink API tiers?
check-flink-api-tiers:
    scripts/check-flink-api-tiers.py

# A goal on its own, and the one place in this repository where that is right.
# The licence-goal rule in CLAUDE.md — a goal-only invocation selects the reactor
# modules without building them, so a module cannot resolve its siblings — does
# not reach this goal: javadoc:aggregate *forks* the compile phase across the
# whole reactor, so flink-connector-gcp-base is built in the same session and
# resolves from it. Measured, not assumed: this succeeds from a clean target/
# with no io.github.flink-gcp artifact in ~/.m2 at all (#88). Prefixing a phase
# would only add a jar and a shade nothing here consumes.
#
# Output lands in docs/static/api/java, which Hugo copies into the site verbatim;
# the previous run is removed first so a deleted class does not linger.
#
# Generate the JavaDoc API reference into the documentation site.
docs-javadoc:
    rm -rf docs/static/api/java
    {{ mvn }} javadoc:aggregate

# --panicOnWarning turns deprecations, unresolved relrefs and missing shortcodes
# into build failures.
#
# The API reference is a separate recipe (`just docs-javadoc`) so that iterating
# on prose stays a seconds-long build rather than a Maven one.
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
