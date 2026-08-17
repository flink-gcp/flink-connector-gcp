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
# that line is a one-line description, and the reasoning goes above it behind a
# `#` separator. Get this wrong and the listing reads as a column of sentence
# fragments.
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

# Runs the exact Java-header check, then spotless/checkstyle (validate), unit tests,
# integration tests, packaging and apache-rat. Extra arguments go to Maven, which is
# how the weekly version matrix selects a Flink version — passing none means the
# version pinned in pom.xml.
#
# Full build: format and license checks, unit tests, integration tests.
verify *args:
    @just check-license-headers
    {{ mvn }} {{ args }} verify

# RAT identifies the approved licence family from one distinctive line. This stricter
# check holds every Java source to a complete copyright-bearing or ASF header.
check-license-headers:
    python3 scripts/check-license-headers.py

# A 1.x version also selects the flink1 compat source root (see the
# flink.compat property in pom.xml) — forgetting that flag by hand is exactly
# the mistake this recipe exists to absorb.
#
# The full build against one Flink version, e.g. `just verify-flink 2.3.0`.
verify-flink version *extra:
    @just verify -Dflink.version={{ version }} {{ if version =~ '^1\.' { '-Dflink.compat=flink1' } else { '' } }} {{ extra }}

# The opt-in module is outside the ordinary reactor so its dependency on every connector does not
# widen connector-only builds. `-am` is still load-bearing here: the snippets must compile against
# the working tree, never io.github.flink-gcp SNAPSHOTs left in ~/.m2.
#
# Check the ordinary GitHub-rendered copies of compiled module README examples.
check-readme-examples:
    python3 scripts/check-readme-examples.py

# Check and compile the source-backed Java examples rendered in documentation, module READMEs and
# Javadoc.
check-doc-snippets *args:
    just check-readme-examples
    python3 scripts/check-javadoc-examples.py
    {{ mvn }} -Pdocs-snippets -pl flink-connector-gcp-docs-validation -am {{ args }} test-compile

# The fixture site mounts this repository's shortcode and supplies synthetic pages and Java
# sources, so changes to the parser are measured without depending on the live documentation.
#
# Test the java-snippet shortcode's rendering and validation branches.
test-java-snippet-shortcode:
    mise x hugo-extended -- scripts/test-java-snippet-shortcode.sh

# -am is load-bearing: without it the io.github.flink-gcp siblings resolve from
# ~/.m2 rather than from the reactor, so the recipe reports on whichever jar
# happens to be installed there instead of on the working tree. That fails both
# ways — a stale jar fails a tree that is green (a test-utils jar predating #323
# gave `NoClassDefFoundError: LogCapture$Event` at test discovery, met on #324),
# and a newer one passes a reactor change that is broken. CI sees neither,
# because it builds the reactor. The cost is running the upstream modules' own
# tests too: seconds, and the honest scope anyway, since a module's build depends
# on them. `binary-compat` and `e2e` install their siblings instead, for a reason
# that does not apply here — those run goal-only or repeated -pl invocations that
# no single reactor can span.
#
# One module and what it is built on, e.g. `just verify-module flink-connector-gcp-bigquery`.
verify-module module:
    {{ mvn }} -pl {{ module }} -am verify

# CI's module-selection decision (issue #243): verify.yaml's changes job calls this
# with --diff HEAD^1 (the pull_request checkout is the base-into-head merge
# commit, so that diff is the pull request's net change) or --full (push and
# workflow_dispatch build the whole reactor). The mapping is derived from the
# poms, never configured — the script's docstring is the specification.
# positional-arguments because `{{ args }}` interpolation re-splits words and
# strips quotes — --files's quoted JSON argument does not survive it — while
# "$@" hands the arguments through verbatim.
#
# Which Maven modules does a change build? e.g. `just ci-maven-args --diff origin/main`.
[positional-arguments]
ci-maven-args *args:
    scripts/ci-maven-args.py "$@"

# pytest over scripts/ (the CI deriver and the CI gate today), through the
# root uv project (pyproject.toml): uv is pinned in mise.toml like the
# linters, pytest is pinned in uv.lock, and --locked makes a drifted lockfile
# fail instead of silently re-resolving. The scripts stay standard-library
# executables bar check-skill-frontmatter.py, which declares PyYAML in PEP 723
# metadata of its own and runs through `uv run --no-project`; because the tests
# load every script by file path, that import has to resolve here too, which is
# why pyyaml is in the dev group and not in the project's dependencies.
#
# Run the scripts/tests suite with pytest.
test-scripts:
    mise x uv -- uv run --locked pytest

# The profile is explicit because its internal module is outside the ordinary reactor, but its
# Java snippet sources still follow the same Spotless contract as every connector source.
# Apply the formatter — CI fails on unformatted code, so run before committing.
format:
    {{ mvn }} -Pdocs-snippets spotless:apply

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
# goal's phase rule guards against (see AGENTS.md under Build). Each SQL
# uber-jar bundles its connector (#138, the repository's first inter-module
# dependency; #290 added the BigQuery pair), so without the install the rerun
# dies resolving it (#181) — and excluding those modules instead would drop the
# most binary-compat-shaped case there is: the jar shaded on the floor, tested
# on the ceiling. The test-utils module installs for the same reason: every
# module's tests depend on it (#27) — as does the base module, which every
# connector compiles against (#61). The root pom installs with them because
# the installed poms name it as parent. Cost, when run by hand:
# io.github.flink-gcp SNAPSHOTs land in ~/.m2 — the primed-local-repo hazard —
# `rm -rf ~/.m2/repository/io/github/flink-gcp` undoes it.
#
# Which modules those are is derived from the poms rather than named here, and
# that is the fix for issue #932: the list used to be written out, the rule it
# copied (ADR-0053 — everything some other module depends on) stayed correct, and
# the copy went stale the moment a fifth uber-jar landed, so the weekly
# binary_compat job died resolving flink-connector-gcp-spanner. The assignment is
# load-bearing too: `-pl "$(…)"` inline discards the script's exit status even
# under `set -euo pipefail`, and Maven reads the empty `-pl` it is then handed as
# the whole reactor (measured: 13 of 13 modules) — so a broken script would
# install more than asked and leave this recipe green, hiding itself. Assigning
# first fails the line instead.
#
# Check that a floor-built jar still runs on Flink <ceiling>, uncompiled.
binary-compat ceiling:
    @echo '==> Build against the floor (the Flink version pinned in pom.xml)'
    {{ mvn }} verify
    @echo '==> Install what the goal-only rerun cannot resolve from the reactor'
    modules="$(scripts/ci-maven-args.py --install-modules)" && {{ mvn }} -pl "$modules" -DskipTests install
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

# The guard logic lives in scripts/worktree-env.sh, where shellcheck reads it
# (issue #156).
#
# Make the main checkout's .env reachable from this worktree.
worktree-env:
    scripts/worktree-env.sh

# The ITCases gated on the BQ_IT_*, PUBSUB_IT_PROJECT, BIGTABLE_IT_PROJECT,
# SPANNER_IT_PROJECT and CLOUDTASKS_IT_PROJECT
# variables: what they check is exactly what the emulators cannot (see the
# testing sections of the connector documentation). The E2E workflow
# (.github/workflows/e2e.yaml) runs this same recipe weekly via WIF; locally
# the variables come from the uncommitted .env, which mise loads — and which a
# fresh worktree does not have until `just worktree-env` links it (issue #156).
#
# The shape is scripts/e2e-gated-its.sh around four Maven calls. The Cloud
# Tasks App Engine class runs first inside appengine-e2e-fixture.sh's exact
# start/test/stop lifecycle; the other connectors start only after that script
# has verified STOPPED with zero instances.
# The pre-flight makes a missing variable an error before any build minutes are
# spent, and the assertion afterwards proves the gated classes ran — without
# it, @EnabledIfEnvironmentVariable turns lost credentials into a green run.
#
# The execution id on the surefire:test call is load-bearing, same as in
# binary-compat: -Dtest overrides includes on *every* surefire execution, so
# without @integration-tests the default-test execution would run the same
# classes a second time.
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
# dispatch after #27 added this step failed exactly there, on a tree the Maven
# workflow had just passed). The header check is not lost: verify.yaml runs rat
# on every pull request, and this install only primes ~/.m2.
#
# This costs real money beyond runner minutes, which the BigQuery and Pub/Sub
# halves do not: the Cloud Tasks suite briefly starts its persistent B1 App
# Engine version, while Bigtable and Spanner create one ephemeral instance per
# gated class. A killed run is bounded by the daily sweep rather than left
# standing indefinitely; normal Cloud Tasks completion verifies zero running
# App Engine instances before either of the longer suites begins.
#
# -Dtest.excluded.groups= is that opt-in (issue #245): the gated classes carry
# @Tag("gated"), which the root pom excludes from every surefire execution, so
# this recipe — and nothing else — brings them back. Clearing it is what makes
# the cost above a property of the command rather than of the shell's
# environment, which is where it used to live.
#
# Run the real-GCP gated ITCases and assert they actually ran.
e2e:
    scripts/e2e-gated-its.sh --require-env
    {{ mvn }} -pl .,flink-connector-gcp-base,flink-connector-gcp-test-utils -DskipTests -Drat.skip=true install
    {{ mvn }} -pl flink-connector-gcp-bigquery,flink-connector-gcp-pubsub,flink-connector-gcp-cloudtasks,flink-connector-gcp-bigtable,flink-connector-gcp-spanner test-compile
    scripts/appengine-e2e-fixture.sh run -- {{ mvn }} -pl flink-connector-gcp-cloudtasks surefire:test@integration-tests -Dtest.excluded.groups= -Dtest="$(scripts/e2e-gated-its.sh --for-gate CLOUDTASKS_IT_PROJECT)"
    {{ mvn }} -pl flink-connector-gcp-bigquery,flink-connector-gcp-pubsub,flink-connector-gcp-bigtable,flink-connector-gcp-spanner surefire:test@integration-tests -Dtest.excluded.groups= -Dtest="$(scripts/e2e-gated-its.sh --except-gate CLOUDTASKS_IT_PROJECT)"
    scripts/e2e-gated-its.sh --assert-ran

# The two markers a gated real-GCP ITCase carries have to stay together: the
# @EnabledIfEnvironmentVariable the E2E suite is discovered by, and the
# @Tag("gated") that keeps the class out of every ordinary build. Forgetting
# the tag fails in the expensive direction — the suite runs during
# `just verify` in any shell holding the variable — so the pairing is checked
# rather than merely documented (issue #245). A verify.yaml job rather than part of
# `just lint`, whose paths filter would have had to grow to every Java test
# source; the check needs neither a JDK nor the network.
#
# Does every gated ITCase carry both the environment gate and @Tag("gated")?
check-gated-tags:
    scripts/e2e-gated-its.sh --check-tags

# Returns every billed E2E fixture to its idle state (issue #246): stale
# Bigtable and Spanner instances are deleted, and the fixed Cloud Tasks App
# Engine version is stopped. The Java instance prefixes and thresholds and the
# OpenTofu App Engine identifiers are read from their owning sources rather
# than repeated. Each cleanup is attempted independently so one failure cannot
# hide another. Needs BIGTABLE_IT_PROJECT, SPANNER_IT_PROJECT,
# CLOUDTASKS_IT_PROJECT and an authenticated gcloud; `--dry-run` only reports
# the changes it would make.
#
# Delete abandoned Bigtable and Spanner instances and stop the fixed App Engine
# fixture left serving by an interrupted E2E run.
sweep-e2e *args:
    scripts/sweep-e2e.sh {{ args }}

# Spotless and checkstyle cover the Java sources inside `just verify`; this is
# everything else.
#
# `just --fmt --check` is deliberately not here. It is an unstable feature, and
# just's compatibility guarantee covers stable ones only — so a newer just could
# reformat and fail a pull request that changed nothing, which is exactly the
# failure mode that makes shellcheck worth pinning. Since `just` is installed
# unpinned (see verify.yaml), depending on it would reintroduce the problem the pin
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
# markdownlint's file set and its rule deviations live in
# .markdownlint-cli2.jsonc, discovered from the working directory.
#
# Lint the shell and Python scripts, the workflows, the markdown and OpenTofu.
lint:
    mise x shellcheck -- shellcheck --version
    mise x shellcheck -- shellcheck scripts/*.sh
    mise x ruff -- ruff --version
    mise x ruff -- ruff check scripts/ opentofu/flink-gcp/appengine-e2e/main.py
    mise x ruff -- ruff format --check scripts/ opentofu/flink-gcp/appengine-e2e/main.py
    mise x actionlint -- actionlint -shellcheck "$(mise which shellcheck)"
    mise x npm:markdownlint-cli2 -- markdownlint-cli2
    mise x opentofu -- tofu fmt -check -recursive opentofu/
    mise x tflint -- tflint --chdir opentofu/flink-gcp --call-module-type=all

# Does every skill's frontmatter parse, so Claude Code can load it at all? A
# broken one is invisible: the file stays valid markdown, no build step reads
# it, and the skill's absence looks like Claude choosing not to use it. It
# happened to a *mandatory* review skill (ADR-0069).
#
# Through `uv run`, and --no-project so it uses the PEP 723 metadata in the
# script's own header rather than the repository's uv project: parsing YAML
# needs a parser the standard library does not have, and keeping that dependency
# inline leaves the script self-contained and installs one package rather than
# the test suite's five. That download is also why this is a verify.yaml job and
# not part of `just lint`, which stays offline — check-flink-api-tiers's rule,
# applied rather than excepted.
check-skill-frontmatter:
    mise x uv -- uv run --no-project scripts/check-skill-frontmatter.py

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
# Reusable as-is by every flink-sql-connector-gcp-* module; both take the module
# as their only argument.
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
# scripts/config/licence-sources.toml (fetching over HTTPS where the artifact's own jar
# ships no licence text). Run after a dependency change, review the diff, commit.
#
# Regenerate the module's META-INF/NOTICE and META-INF/licenses/.
update-notice module:
    {{ mvn }} -pl {{ module }} -am generate-test-resources
    scripts/check-notice.py --update {{ module }}

# Re-fetches every pinned licence-text source and requires the regenerated
# NOTICE/licences byte-identical to the checked-in ones — the offline
# check-notice never consults the recorded sources at all, so this is what
# notices an upstream text edit or a deleted tag (issue #343). Network by
# design, which is why it runs from weekly.yaml (the notice_sources job) and
# from verify.yaml only when the change touches a licence-source input, and is
# not part of `just lint`: that stays offline, the same rule that keeps
# check-flink-api-tiers out of it.
#
# Do the pinned licence sources still serve what is checked in?
check-notice-sources:
    scripts/check-notice-sources.sh

# Classifies every org.apache.flink type the main sources import by its
# class-level stability annotation, read from the -sources.jars (never class
# files: a class file's constant pool lists method-level annotations too, the
# bug that produced the wrong numbers on issue #103). @Internal, @Experimental
# and unannotated types must each have a reasoned allowlist entry in
# scripts/config/flink-api-tiers.toml; anything new — or stale — fails. Downloads the
# sources jars from Maven Central into target/flink-api-tiers/ on first run,
# which is why this is not part of `just lint` (that stays offline).
#
# Do the main sources depend only on allowlisted Flink API tiers?
check-flink-api-tiers:
    scripts/check-flink-api-tiers.py

# Holds docs/content/docs/reference/ to the options the connectors actually
# take, in both directions: every builder setter and every Table API
# ConfigOption must be named in a table whose first column is `Option`, and
# every option those tables name must exist. Mappings, the four exempt bulk
# overloads and the three FactoryUtil keys live in scripts/config/option-docs.toml.
#
# The pages are hand-written rather than generated (issue #89): their tables
# group knobs and carry defaults the sources do not hold, since an unset knob's
# default belongs to the client library. This is the drift protection that
# choice would otherwise have cost. Offline and stdlib-only, but a verify.yaml job
# rather than part of `just lint`, because its inputs include every Java main
# source, which lint.yaml's paths filter would have had to grow to cover.
#
# Is every connector option documented, and every documented option real?
check-option-docs:
    scripts/check-option-docs.py

# Holds the metrics tables on the DataStream pages to what the connectors
# actually register (issue #296), in both directions: every name in a module's
# *MetricNames inventory must appear in a table whose first column is `Metric`
# with the Type column matching the counter/gauge registration, and every name
# those tables carry must be registered, a subgroup template the module wires,
# or marked `(Flink standard)`. Mappings and the two base.metrics subgroup
# sources live in scripts/config/metric-docs.toml. Also holds the mechanical half of
# the #280 naming rule: no name registered here takes Flink's `num` prefix.
# Offline and stdlib-only, but a verify.yaml job rather than part of `just lint`,
# for check-option-docs's reason: its inputs include every Java main source,
# which lint.yaml's paths filter would have had to grow to cover.
#
# Is every connector metric documented, and every documented metric real?
check-metric-docs:
    scripts/check-metric-docs.py

# Fails on a Javadoc member reference written without a parameter list that gets
# something other than the member it names (issues #897, #930, #931): one
# shadowed by a field renders no anchor at all, one naming an overloaded method
# renders an anchor on whichever overload Javadoc picks, and one naming a field
# the reference does not document — private or package-private — renders no
# anchor either. All three were measured on the generated reference, because
# `just docs-javadoc` exits 0 on every one of them: failOnWarnings does not cover
# this shape, so nothing else in CI sees it. The failure carries the repair — a
# parameter list where there is a method to name, `{@code member}` where the
# sentence means the state — which is why there is no allowlist and so no
# curate-* skill. It judges only types this repository declares, and members
# those types declare themselves, so an inherited or vendor member is left alone,
# and a reference that already carries a parameter list is not matched against
# the declared signature. Offline and stdlib-only, but a verify.yaml job rather
# than part of `just lint`, for check-option-docs's reason: its inputs are every
# Java main source.
#
# Does every Javadoc member reference resolve to the member it names?
check-javadoc-links:
    scripts/check-javadoc-links.py

# A goal on its own, and the one place in this repository where that is right.
# The licence-goal rule in AGENTS.md — a goal-only invocation selects the reactor
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
