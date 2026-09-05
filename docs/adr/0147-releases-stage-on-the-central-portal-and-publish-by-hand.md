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

# ADR-0147: Releases publish after both Central Portal deployments validate

- Status: Accepted
- Date: 2026-08-30; revised by [#1185] (2026-09-06)
- Issues: [#724], [#39], [#29], [#1185]
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

**A tag push stages both version lines and publishes only after both validate.**
`.github/workflows/release.yaml` runs `just stage-release` once per version line on a `vX.Y.Z` tag push.
Its `workflow_dispatch` trigger rehearses the same pair from a bare `X.Y.Z` input and drops both deployments after validation.
The recipe re-versions the reactor with `versions:set` and runs
`deploy -Drelease -DskipTests -Djapicmp.skip=true`. `-Drelease` activates two profiles at once:
the connector parent's `release` (GPG signing at verify, the javadoc jar, the enforcer floor) and
this project's `central-release` (the sources jar the parent profile does not attach, a compiler
re-pin, and `central-publishing-maven-plugin` with `autoPublish=false` and
`waitUntil=validated`).
The upload stops at *validated*.
`scripts/central-portal.py` captures each upload ID before the plugin waits for validation, preserving it even if Maven subsequently fails.
After both staging commands and both artifact collections succeed, the helper checks both deployment states before sending either Publish request.
It waits for both to be PUBLISHED before the workflow creates a public GitHub Release with generated notes, ten SQL uber-jars and their ten signatures.
The pinned `softprops/action-gh-release` first creates an ordinary release as a draft, uploads its assets, then applies `draft: false` when finalizing it.

**The first release used manual Publish and Drop; [#1185] retires those clicks.**
Release run `33323702321` exercised both staging paths for `1.0.0` and `1.0.0-1.20`.
The maintainer found that the tagged commit already fixed what would ship, and no further review happened between VALIDATED and Publish.
The revision keeps the two independent deployments and `autoPublish=false`: the workflow, rather than either Maven build, owns the decision to publish.
It changes the GitHub Release to `draft: false` at the same time.

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
staging build's job is packaging, signing and upload. `-Djapicmp.skip=true` unconditionally:
every verify lane runs the real check, and for a release whose version equals
`japicmp.referenceVersion` — the 1.0.0 case — the skip is also load-bearing, because Maven
resolves the reference from the reactor itself and japicmp reads the old side's whole compile
classpath as old API (ADR-0124).

## Declined alternatives

- **Publishing each line with `autoPublish=true`.**
  The first decision deferred automatic publication until the first real release.
  The revision automates it through the publisher API instead: plugin auto-publication could publish the 2.x line before the 1.20 build or validation failed.
  Per-PR CI covers 2.x; the weekly LTS lane does not prove that every tagged commit has passed a 1.20 build.
- **Combining both version lines into one Portal bundle.**
  Considered for [#1185]; the maintainer chose to retain two deployments and require both validations before either publication.
  This keeps the existing staging builds and their module-local publication exclusions.
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

- A dispatch now stages **both lines per run** and names each deployment with its version, run ID, attempt and "dry run".
  Its final cleanup attempts to Drop every captured deployment in VALIDATED or FAILED state, including the first if the second staging run failed.
  A deployment still validating, a failed Drop, or runner termination can leave a deployment behind; its captured ID and the failure remain in the run log and summary.
  The event guard excludes both Publish and GitHub Release creation even when a dispatch targets a tag.
  Rehearsals exercise ID capture, status and Drop authentication; Publish first runs at a real release.
- The staged bundle's validation report (signing, checksums, POM completeness, sources/javadoc
  presence) is produced by the Portal on every upload, giving each release a pre-publish
  checklist for free.
- A failure in either build, validation, ID capture or artifact collection skips both publications and the GitHub Release.
  The upload plugin stays at `autoPublish=false`; changing that setting would bypass the shared validation gate.
- **The guarantee ends at the decision to publish.**
  The [Portal API] publishes one deployment per request; the two requests and GitHub Release creation are not a transaction.
  A network failure after the first request can leave one line PUBLISHED and the other VALIDATED.
  The helper stops on preflight or Publish errors and on invalid or permanently rejected status responses.
  During the publication wait, it retries read-only status polls after transport failures, HTTP 429 or HTTP 5xx; it uses a 30-second socket timeout and checks a 30-minute deadline between polling rounds.
  The run summary records state transitions; repeated unchanged polls remain in the log.
  It does not blindly retry a Publish whose response was lost.

### Recovery

Use `CENTRAL_TOKEN_USERNAME` and `CENTRAL_TOKEN_PASSWORD`, the same token pair as staging, for `just central-portal`.
Take deployment IDs from the run summary or the upload logs, and inspect them with `just central-portal status <2x-id> <1x-id>`.

Before publication starts, Drop leftover VALIDATED or FAILED deployments with `just central-portal drop <id> ...` before rerunning the workflow.
Drop attempts every supplied ID even if one deletion fails, tolerates an already absent ID, and refuses deployments in other states.
Leave a still-validating deployment until its state can be checked again.

After any Publish request, **do not rerun the entire workflow**: that would upload another pair under immutable release coordinates.
Resume with `just central-portal publish <2x-id> <1x-id>`.
It checks every ID before acting, publishes only VALIDATED deployments, and waits for already PUBLISHING deployments without issuing another Publish.
PUBLISHED deployments need no further publication.
If publication fails permanently, inspect the Portal error before choosing a recovery; the helper cannot roll back Central.

If only GitHub Release creation failed, confirm both deployments are PUBLISHED, download the ten published SQL jars and their `.asc` files from Maven Central, then use `gh release create --draft --verify-tag --generate-notes` with the original tag and those twenty files.
If the Release already exists with incomplete attachments, use `gh release upload` for the missing files.
If it remains a draft, confirm all twenty files are attached, then publish it with `gh release edit <tag> --draft=false`.
Do not restage Central to repair GitHub attachments.
A tag cut on the wrong commit remains correctable only before Publish; afterward, changed artifacts require a new version.

[#724]: https://github.com/flink-gcp/flink-connector-gcp/issues/724
[#39]: https://github.com/flink-gcp/flink-connector-gcp/issues/39
[#29]: https://github.com/flink-gcp/flink-connector-gcp/issues/29
[#1185]: https://github.com/flink-gcp/flink-connector-gcp/issues/1185
[Portal API]: https://central.sonatype.org/publish/publish-portal-api/
