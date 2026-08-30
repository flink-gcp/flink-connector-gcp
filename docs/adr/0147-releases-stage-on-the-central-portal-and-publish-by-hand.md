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

# ADR-0147: Releases stage on the Central Portal and publish by hand

- Status: Accepted
- Date: 2026-08-30
- Issues: [#724], [#39], [#29]
- Modules: all (build and release engineering)
- Current behavior: `justfile` § `stage-release`, `.agents/references/repository-guide.md`
  § Version policy

## Context

The `io.github.flink-gcp` namespace is verified on the Central Portal ([#724], 2026-08-15) and
the first published version is `1.0.0` directly — early milestones were closed without artifacts
and, in the end, without tags. A version published to Maven Central cannot be deleted, so the
pipeline's design question is where the one-way step sits and who takes it.

The maintainer runs fully automatic tag-triggered publishing for PyPI (PyAthena: tag push →
build → publish → GitHub release, no manual gate) and wanted to know whether the same shape was
hard here. It is not — the mechanism is one trigger block — so the decision below is about the
irreversibility of a first Central release, not about feasibility.

## Decision

**A tag push stages the release; a person publishes it from the Portal UI.**
`.github/workflows/release.yaml` runs `just stage-release` once per version line on a `v*`
tag push, and its `workflow_dispatch` trigger is the validate-then-drop dry run.
The recipe re-versions the reactor with `versions:set` and runs
`deploy -Drelease -DskipTests -Djapicmp.skip=true`. `-Drelease` activates two profiles at once:
the connector parent's `release` (GPG signing at verify, the javadoc jar, the enforcer floor) and
this project's `central-release` (the sources jar the parent profile does not attach, a compiler
re-pin, and `central-publishing-maven-plugin` with `autoPublish=false` and
`waitUntil=validated`). The upload stops at *validated*; Publish — or a dry run's Drop — is a
deliberate click. Once the pipeline has survived a real release, the manual gate can be
retired — two changes together, `autoPublish=true` and the workflow's `draft: false`, per the
Consequences below.

**Each release stages two version lines** (decided with the maintainer 2026-08-30, resolving
the publishing half that ADR-0054 had left with [#39]'s suffix scheme): bare `X.Y.Z` is the
Flink 2.x line — ADR-0053's one artifact covering the supported 2.x range — and `X.Y.Z-1.20`
is the same 12 coordinates compiled for the 1.x LTS from the same tree, the `-1.20` suffix
form settled on [#39]. `just stage-release` adds `-Dflink.compat=flink1` itself for a `-1.20`
version, requires the matching `-Dflink.version=1.20.<patch>`, and refuses a mislabelled
pairing in either direction — a 2.x-built jar under a `-1.20` version would be discovered only
after an irreversible publish. The refusal is layered, because argument inspection loses to
`-D`'s `--define` long form and to `MAVEN_OPTS`/`.mvn/maven.config`:
`scripts/stage-release-guard.py` (tested under `scripts/tests/`) probes Maven's *effective*
`flink.version`/`flink.compat` with the deploy line's own fixed properties and fails before
`versions:set` touches the tree, and the `enforce-version-line-toolchain` enforcer rule in the
`central-release` profile re-checks the same invariant inside the release build against its
final effective model — the only place no probe/deploy divergence can exist.
`japicmp.referenceVersion` tracks the bare line only; the LTS
jar keeps ADR-0054's per-major stance, and no cross-major binary claim is added.

**The published set per line is 12 artifacts**: the parent POM, `flink-connector-gcp-base`, the five
connectors, and the five SQL uber-jars. `flink-connector-gcp-test-utils` stays reactor-only:
every dependent consumes it in `test` scope, which no consumer resolves, and publishing it would
freeze its `Fake*` doubles under the same compatibility discipline as the connector API for no
user benefit — adding it later is cheap, retracting it is impossible. The guard is a
`skipPublishing` declaration in the module's own POM, the same module-local shape as
docs-validation's deploy skip, rather than a root-level artifactId exclusion list that would
silently stop matching on a rename. The five SQL uber-jars carry no sources, so Central's
per-jar javadoc requirement is met by the sanctioned stub: one profile-managed jar execution
attaches an `index.html` pointing at the published API reference, active only where a module
provides `src/main/central-javadoc-stub`.

**The signing key is a dedicated project release key**, not a personal identity key: UID naming
the project, private key and passphrase held as repository secrets plus an offline backup, public
key on `keyserver.ubuntu.com`. The namespace maps to a Portal Organization so publish rights can
be granted to future members; a personal key would re-create the single-person coupling that
structure removed. Signing uses maven-gpg-plugin 3.x's Bouncy Castle signer reading
`MAVEN_GPG_KEY` / `MAVEN_GPG_PASSPHRASE`, so CI needs neither a gpg binary nor a keyring import —
the parent's pinned 1.4 predates all of that and is overridden.

**The release build carries three deliberate flags.** `versions:set` from the tag keeps the tree
at `<next>-SNAPSHOT` with no release commit (the tagged tree is what CI verified; only the
version differs). `-DskipTests` because the staged commit already passed every CI lane and the
staging build's job is packaging, signing and upload. `-Djapicmp.skip=true` unconditionally,
because the release whose version equals `japicmp.referenceVersion` resolves the reference from
the reactor itself and reads the old side's whole compile classpath as old API (ADR-0124);
every verify lane runs the real check.

## Declined alternatives

- **Tag → fully automatic publish (`autoPublish=true`).** Declined *for the first release only*,
  with the maintainer's explicit preference: Maven Central is new to this project, a published
  version is permanent, and the Portal's validated-deployment page is also the dry-run mechanism
  — the same workflow proves the pipeline by staging a bundle that is dropped instead of
  published. The Consequences below name the two-change path back to the PyPI shape.
- **Publishing `flink-connector-gcp-test-utils`.** Declined; reasons in the decision. Revisit if
  a user asks for the test doubles as a dependency.
- **A `maven-release-plugin` release-commit flow** (`release:prepare`/`release:perform`).
  Declined — not as a bad tool, but as a mismatch with this repository's invariants:
  `release:prepare` pushes two version-bump commits and the tag from the build, making the tag
  an *output* of CI where this repository treats it as an *input* cut by a person, and pushing
  to a protected `main` that otherwise only merges pull requests; its two-build
  prepare/perform model doubles the surface over one `versions:set` in an ephemeral checkout;
  and the connector parent's own release-plugin configuration targets the ASF/OSSRH flow
  (`-Psonatype-oss-release`), not the Central Portal.
- **Re-pinning nothing and inheriting the parent's release profile as-is.** Its
  `maven.compiler.source/target=11` override would ship bytecode no CI lane tested — the
  ordinary build targets 17 — and its maven-gpg-plugin 1.4 cannot read keys from the
  environment. Both are overridden in `central-release`.

## Consequences

- The dry run [#724] requires is the same workflow with the Portal's Drop button — with one
  honest limit: a dispatch stages **one line per run** (rehearsing both lines is two
  dispatches, and the deployment is named "dry run" on the Portal), and the draft-Release step
  only executes on a real tag, so its first real execution is the first release.
- The staged bundle's validation report (signing, checksums, POM completeness, sources/javadoc
  presence) is produced by the Portal on every upload, giving each release a pre-publish
  checklist for free.
- A future `autoPublish=true` flip removes the Portal gate but is **two** changes, not one:
  the workflow's `draft: true` must flip with it, or Central goes live while the GitHub
  Release stays invisible.
- A `vX.Y.Z` tag — the only shape the trigger admits, so a release is always both version
  lines — runs the staging recipe once per line and drafts the GitHub Release with generated
  notes and the ten SQL uber-jars (five per line) attached; a release is then two Portal
  deployments, and publishing them and the draft Release are the manual steps, taken
  together.
- **The two staging runs are not atomic, and recovery is manual.** A failure in the LTS half
  leaves the bare line VALIDATED on the Portal; a re-run stages it again, and the Portal keeps
  both — deployments are named with the run id so they are tellable apart, and the stale one
  is Dropped by hand. A draft Release that failed half-made is deleted before re-running
  (GitHub allows several drafts on one tag name, and a draft survives deleting the tag). A tag
  cut on the wrong commit is recoverable **only before** Publish: Drop the deployments, delete
  the draft, delete and re-push the tag — the workflow refuses a tag whose commit is not an
  ancestor of `main`, and a tag only triggers it at all if the tagged tree already contains
  `release.yaml`. After Publish, Central is immutable and the only move is the next patch
  version.

[#724]: https://github.com/flink-gcp/flink-connector-gcp/issues/724
[#39]: https://github.com/flink-gcp/flink-connector-gcp/issues/39
[#29]: https://github.com/flink-gcp/flink-connector-gcp/issues/29
