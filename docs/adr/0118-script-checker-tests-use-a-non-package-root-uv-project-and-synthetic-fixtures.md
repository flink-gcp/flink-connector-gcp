<!--
Copyright 2026 The flink-gcp authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ADR-0118: Script checker tests use a non-package root uv project and synthetic fixtures

- Status: Accepted
- Date: 2026-08-02; revised by [#1115] (2026-08-29); revised by [#1185] (2026-09-06)
- Issues: [#243], [#249], [#1115], [#1185]
- Modules: scripts, CI
- Current behavior: [`pyproject.toml`](../../pyproject.toml), [`just test-scripts`](../../justfile),
  [script test sources](../../scripts/tests)

## Context

[PR #247](https://github.com/flink-gcp/flink-connector-gcp/pull/247) introduced Python and shell tests for the CI module selector and aggregate gate.
Before that change, the Python checkers encoded non-trivial parsing rules but had no harness in which a deliberately malformed source tree could be exercised.

The code under test is a collection of repository executables, not an importable Python package.
The suite still needs a dependency lock, pytest configuration and room for test-only dependencies.
Putting pytest alone in mise through pipx would pin one executable but supply neither a project configuration home nor a transitive lock for the suite.

## Decision

The repository root holds the uv project for `scripts/tests`.
`pyproject.toml` sets `package = false` because the project never installs the scripts as a package, and `testpaths = ["scripts/tests"]` is the one repository-layout override pytest needs.
Scripts may remain directly executable and standard-library-only even when their tests have dependencies.

uv is pinned in `mise.toml`, while test dependency versions are resolved in the committed `uv.lock`.
`just test-scripts` runs `uv run --locked pytest`, so CI and local runs reject an unrecorded resolution change.
The machine-generated lockfile is explicitly excluded from Apache RAT because it cannot carry the repository's license header.

The root project owns test dependencies and runtime dependencies shared by multiple checker commands.
Shared checker dependencies run through `uv run --locked`, so their compatible range and resolved version have one owner.
A dependency needed by only one standalone script remains in that script's PEP 723 metadata and may run through `uv run --no-project`; the dev group repeats that dependency only when the pytest suite loads the script by file path.

Checker tests build synthetic trees under `tmp_path` and redirect the checker's root, configuration and other derived paths to those fixtures.
They do not assert the live repository tree because that would make every Java or documentation input an input to the lint workflow's paths filter and would let an unrelated change land a stale expectation while the suite never ran.
The real-repository CLI layer of `test_ci_maven_args.py` is the named exception, and the poms and NOTICE templates it reads are consequently listed in that workflow filter.

Workflow programs can also be tested directly: `test_release_workflow.py` reads the checked-in release workflow with PyYAML and exercises its shell commands against synthetic environment variables and fake executables.
The workflow is the program under test; checker inputs still use synthetic trees.
Changes to `.github/workflows/**` already trigger the lint workflow, so the suite reruns when this program changes.
PyYAML remains in the test dependency group because the production release helper uses only the standard library.

The primary negative direction is a checker finding less than it should.
Each parsing or policy rule therefore needs a fixture that fails when the rule is removed, and mutation probes verify that the fixture discriminates instead of merely executing the line.

## Evidence

[PR #247](https://github.com/flink-gcp/flink-connector-gcp/pull/247) chose uv over a mise `pipx:pytest` entry with the owner and moved the project from `scripts/` to the repository root after review found the stale path references.
The initial harness carried 35 tests for module classification, dependency closure and the CI gate.

[PR #252](https://github.com/flink-gcp/flink-connector-gcp/pull/252) added 134 cases for the three pre-existing checkers, bringing the suite at that time to 169 tests.
The fixtures constructed source jars, Maven-shaped repositories, TOML configuration and Markdown tables without network access or a live reactor.

That pull request's final record contains 16 rule-level mutants.
Two survived the first pass, and both exposed non-discriminating tests rather than equivalent mutants: an `Option` header control also failed the mutated `startswith` rule, and a commented setter sat outside the declaration regex's reachable indentation.
The controls were rewritten before the mutants were counted as killed.

[#1115] moved seven Java-aware checkers to one Tree-sitter-backed parser and added Tree-sitter and its Java grammar to the root project.
The committed lock gives those commands one parser version, while the project constraint excludes tree-sitter 0.26.0 because its `Point` getter reference-counting regression can crash a large compilation-unit traversal.
The upstream fix is present on the main branch but was not released when the constraint was added.

## Alternatives declined

- **Install pytest through mise/pipx only**: it would pin an executable but provide no dependency lock or configuration home for the growing suite.
- **Create a packaged Python project under `scripts/`**: the production scripts are executables loaded by path, so package metadata would describe a distribution the repository never builds.
- **Assert every checker against the live repository**: the lint workflow would need to enumerate every source and documentation input, and a missed path would make the test stale silently.
- **Treat line coverage as sufficient evidence**: a test can execute a rule while accepting both the original and mutated behavior, as the two surviving controls demonstrated.
- **Give each Java-aware checker separate PEP 723 metadata**: seven copies would let their compatible ranges and resolved parser versions drift even though they consume one shared parsing module.

## Consequences

New or changed checker behavior adds synthetic positive and negative fixtures under `scripts/tests`, with a mutation or equivalent control showing that the new assertion can fail for the intended defect.
No checker test reaches the network unless the test explicitly owns and controls that boundary.

The root uv project may grow test dependencies as the suite loads more scripts by path and shared runtime dependencies when multiple checker commands use the same library.
It remains non-packaged, and a dependency used by only one standalone script does not move into the project's runtime set.

[#243]: https://github.com/flink-gcp/flink-connector-gcp/issues/243
[#249]: https://github.com/flink-gcp/flink-connector-gcp/issues/249
[#1115]: https://github.com/flink-gcp/flink-connector-gcp/issues/1115
[#1185]: https://github.com/flink-gcp/flink-connector-gcp/issues/1185
