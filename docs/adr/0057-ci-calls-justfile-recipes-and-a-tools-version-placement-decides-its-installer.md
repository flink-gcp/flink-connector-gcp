<!--
Copyright 2026 laughingman7743

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

# ADR-0057: CI calls justfile recipes, and a tool's version placement decides its installer

- Status: Accepted
- Date: 2026-07-26 ([#111], with the shim incident on PR
  [#113](https://github.com/laughingman7743/flink-connector-gcp/pull/113)); gated-suite
  opt-in settled 2026-08-02 ([#245])
- Issues: [#111], [#245], [#132]
- Modules: all (build/CI)
- Current behavior: root `CLAUDE.md` § Build (the imperative rules), `justfile`

## Decision

**A multi-step sequence is named once, in the `justfile`, and CI calls that recipe**
([#111]). `binary_compat` is one step invoking `just binary-compat` rather than four `run:`
blocks, so the order the sequence depends on has a single definition and is rerunnable by
hand — reproducing a red weekly row locally is the first move when one goes red. The cost was
weighed and accepted: a failure names the `==>` phase inside the recipe rather than a step in
the GitHub UI. Around the rule:

- **CI helpers live in `scripts/` as files**, not inline in workflow `run:` blocks, so they
  can be run by hand. `tools/` is not the place: it holds build tool *configuration*
  (`tools/maven/checkstyle.xml`), following Flink's layout. Two consequences: `scripts/` is
  outside the `.github/**` rat exclude, so each file carries the plain Apache-2.0 header, and
  `just lint` shellchecks them — and also runs actionlint, which shellchecks inline `run:`
  blocks, so a script stays linted whether it lives in `scripts/` or in a `run:` block.
- **A `justfile` recipe is neither** — nothing lints inside one — so a recipe body holds
  commands, one command per line, and anything that grows into a script goes to `scripts/`.
  The boundary is shellcheck coverage: it reads `scripts/`, actionlint reads inline `run:`
  blocks, and nothing reads inside a recipe.
- A top-level justfile variable assigned from a shell command runs on **every** `just`
  invocation, whichever recipe was asked for; a default *parameter* value runs only when its
  own recipe does — which is why `check-flink-release`'s ceiling is a parameter default.
- The standing exception is the tfaction pair (ADR-0063): tfaction is itself the named,
  rerunnable sequence, and `just tofu <args>` is the local equivalent.

**Where a tool's version lives decides how CI installs it.** Pin in `mise.toml` and install
with `jdx/mise-action` + `install_args` when a version skew can fail a pull request that
changed nothing — shellcheck (0.9.0 on ubuntu-24.04, 0.11.0 on 26.04, so an `ubuntu-latest`
migration would fail an unrelated PR) and hugo/go (`docs.yaml` moved onto this shape in
[#111], retiring its `HUGO_VERSION`-plus-"keep in sync" duplication). `install_args` matters:
it names the subset of `mise.toml` the job needs. Otherwise install with
`taiki-e/install-action` and no version — that is `just`, whose 1.x compatibility guarantee
("there will never be a 2.0") means a newer release cannot break an unchanged justfile, so
`mise.toml` says `just = "1"` and CI says `tool: just`. **That guarantee covers stable
features only**, so nothing CI runs may depend on a `--unstable` one; `just --fmt --check` is
kept out of `just lint` for exactly this reason (an unstable feature under an unpinned `just`
could fail an unchanged pull request). Reach for an unstable feature and the tool needs a
pin, which means an inline version in every install-action step. Two rules protect the
placement scheme:

- **Inside a recipe, always name the tool: `mise x <tool> -- …`, never bare `mise x -- …`.**
  The bare form activates every tool in `mise.toml` and installs what is missing, silently
  undoing the `install_args` meant to limit a CI job. Caught in CI on PR
  [#113](https://github.com/laughingman7743/flink-connector-gcp/pull/113): `mise x --
  shellcheck` in the lint job pulled a JDK, Maven, Hugo, Go and a second copy of `just`,
  shadowing the one `install-action` had already put on `PATH`. The bare form stays right for
  the *entrypoint* (`mise x -- just <recipe>`), which does want everything.
- **`jdx/mise-action` beside `setup-java` needs `add_shims_to_path: false`.** `mise.toml`
  pins java and maven, and the action defaults **both** `add_shims_to_path` and `export_path`
  to `true`, so out of the box its shims and env paths land in front of the JDK the job just
  installed. That is a default to disarm, not a combination to forbid — the rule said "must
  not run together" until it was checked, extrapolated from the PR #113 incident, which was a
  *different* mechanism; the two have never actually run together in any workflow here. When
  a `setup-java` job does need a mise-pinned tool: set `add_shims_to_path: false`, scope
  `install_args` to that tool, and invoke it by explicit path — `just lint` hands actionlint
  `-shellcheck "$(mise which shellcheck)"`. **Better still, arrange not to need it**: a tool
  the Java build depends on should come from Maven where it can, because its version then
  lives beside the dependency it has to track and no workflow changes at all — protoc
  resolves as `com.google.protobuf:protoc:<version>:exe:<platform>`, which is why [#132]
  added code generation without touching CI. None of this changes why `just` comes from
  `taiki-e/install-action` in every workflow: one binary on `PATH` and no shims at all.

**Gated real-GCP suites are opt-in per command, and `just e2e` is the recipe that *is* the
opt-in** ([#245]). Each gated ITCase carries `@Tag("gated")`, which the root pom's
`test.excluded.groups` excludes from every surefire execution; `just e2e` is the only thing
that clears it, with `-Dtest.excluded.groups=`. The `@EnabledIfEnvironmentVariable` gate
alone had made the choice per *shell*, all-or-nothing: `just verify` runs the same
`integration-tests` execution, so a shell holding `BIGTABLE_IT_PROJECT` created two one-node
Bigtable instances on every full build. The tag makes the choice per command instead, for
`./mvnw verify` as much as for `just verify`. The environment annotation stays exactly where
it is (the E2E discovery greps it), so **the two markers must be kept together**, which
`just check-gated-tags` enforces in both directions — a gate with no tag runs a billed suite
during any `verify` in a holding shell, a tag with no gate runs nowhere at all, since
`just e2e` selects by the gate. The check is deliberately **gate-agnostic**, matching the
annotation rather than the three variables the E2E workflow sets, so
`BigQueryDefaultStreamSchemaEvolutionITCase` — outside that suite on purpose, ~2 h against
the real service — is covered too. Because the recipe is now the opt-in, its pre-flight makes
a missing variable an error, and a post-run assertion (`scripts/e2e-gated-its.sh`, which
derives the class list from the gating annotation) checks the gated classes actually
executed.

[#111]: https://github.com/laughingman7743/flink-connector-gcp/issues/111
[#132]: https://github.com/laughingman7743/flink-connector-gcp/issues/132
[#245]: https://github.com/laughingman7743/flink-connector-gcp/issues/245
