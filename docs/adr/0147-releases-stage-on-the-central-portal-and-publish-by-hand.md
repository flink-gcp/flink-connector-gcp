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
- Date: 2026-08-30; revised by [#1185] (2026-09-06), [#1818] (2026-10-17), [#1971]
  (2026-10-31), [#1980] (2026-11-01) and [#2068] (2026-11-07)
- Issues: [#724], [#39], [#29], [#1185], [#1818], [#1971], [#1980], [#2068]
- Modules: all (build and release engineering)
- Current behavior: `justfile` § `stage-release` and § `rehearse-release`,
  `scripts/rehearse-release.py`, `scripts/central-portal.py`, `scripts/release-assets.py`,
  `.github/workflows/release.yaml`, `.agents/references/repository-guide.md`
  § Version policy, and the repository settings: the `release` environment and the
  `release tags` ruleset

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
Its `workflow_dispatch` trigger rehearses the same pair from a bare `X.Y.Z` input.
By default it builds and inspects both bundles without uploading them; `mode: validate` uploads both and drops them after validation.
The recipe re-versions the reactor with `versions:set` and runs
`deploy -Drelease -DskipTests`. `-Drelease` activates two profiles at once:
the connector parent's `release` (GPG signing at verify, the javadoc jar, the enforcer floor) and
this project's `central-release` (the sources jar the parent profile does not attach, a compiler
re-pin, and `central-publishing-maven-plugin` with `autoPublish=false` and
`waitUntil=validated`).
The upload stops at *validated*.
`scripts/central-portal.py` captures each upload ID before the plugin waits for validation, preserving it even if Maven subsequently fails.
After both staging commands and both artifact collections succeed, the helper checks both deployment states before sending either Publish request.
It waits for both to be PUBLISHED before the workflow creates a public GitHub Release with generated notes and every SQL uber-jar of both lines with its signature.

**Staging, publication and the GitHub Release are three jobs, and a failed run is finished by re-running its failed jobs ([#1980]).**
v1.3.0's two deployments stayed PUBLISHING for about 2.5 hours, where 1.1.0's had taken about 14 minutes.
The 30-minute publication wait then expired after Central had accepted both Publish requests, so the single job failed and skipped the GitHub Release and the docs; the maintainer finished both by hand ([#1968]).
Re-running that job would have staged and uploaded another pair, which is why the Recovery section forbade it.
The wait is now 3 hours, which covers that measurement, and the workflow is split so a re-run no longer stages:

- `stage` builds, uploads, validates and collects both lines, then hands the version and both deployment IDs on as job outputs.
  The step that hands them on runs only after every earlier step succeeded, and the jobs after `stage` start only on its success, so neither can start from a run that failed any of those gates.
- `publish-central` runs `just central-portal publish <2x-id> <1x-id>`: it sends Publish only for a VALIDATED deployment, waits on a PUBLISHING one and returns at once for PUBLISHED ones.
- `github-release` downloads both lines' SQL uber-jars and their `.asc` files from Maven Central with `scripts/release-assets.py`.
  It accepts a jar only when it matches its `.sha1` and gpg reports a valid signature by the release key's pinned fingerprint, fetched from `keyserver.ubuntu.com`; Central publishes no checksum for a `.asc`.
  It then creates the Release as a draft with `--verify-tag` when none exists, deletes attachments whose upload never finished, attaches the missing files, and publishes the draft only once its attachments are exactly those files with matching digests.

When `publish-central` or `github-release` fails, "Re-run failed jobs" runs it again with the job outputs of the original `stage` attempt, which GitHub keeps for the re-run (measured on a throwaway workflow, run 41286883270: the re-run job read the first attempt's output and the succeeded job was not re-run).
Each of those jobs is safe to repeat, so a publication that outlasts the wait, or a GitHub error during the Release, needs only the button.
A successful re-run completes the run with the `push` event, which is what `docs.yaml` publishes the docs on; each attempt's completion has fired its own `workflow_run` event in this repository's history.
"Re-run all jobs" after a successful `stage` would stage the version again, so `stage` first reads every earlier attempt's jobs and refuses before anything is signed if it succeeded in any of them.
A refused attempt has no outputs for the later jobs, so after that mistake the run is finished by hand.
After `stage` failed in every earlier attempt, a re-run stages afresh, as before.
The Release is built from Central's files on every attempt rather than from the jars `stage` built: the first attempt and a re-run then take the same path, no artifact has to cross jobs or attempts, and the attachments are checked to be what Maven Central publishes.
The jobs divide the credentials: `stage` holds the signing key and the Central token with read-only scopes, `publish-central` holds the Central token without a write scope, and `github-release` holds `contents: write` and no release secret.

**The first release used manual Publish and Drop; [#1185] retires those clicks.**
Release run `33323702321` exercised both staging paths for `1.0.0` and `1.0.0-1.20`.
The maintainer found that the tagged commit already fixed what would ship, and no further review happened between VALIDATED and Publish.
The revision keeps the two independent deployments and `autoPublish=false`: the workflow, rather than either Maven build, owns the decision to publish.
It changes the GitHub Release to `draft: false` at the same time.

**Each release stages two version lines** (decided with the maintainer 2026-08-30, resolving
the publishing half that ADR-0054 had left with [#39]'s suffix scheme): bare `X.Y.Z` is the
Flink 2.x line — ADR-0053's one artifact covering the supported 2.x range — and `X.Y.Z-1.20`
is the same coordinates compiled for the 1.x LTS from the same tree, the `-1.20` suffix
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
The LTS jar keeps ADR-0054's per-major stance, and no cross-major binary claim is added.

**The published set per line is the parent POM, `flink-connector-gcp-base`, and each connector
module with its SQL uber-jar**: 12 artifacts in 1.0.0 and 1.1.0, and 14 from 1.2.0, which adds
Firestore. `flink-connector-gcp-test-utils` stays reactor-only:
every dependent consumes it in `test` scope, which no consumer resolves, and publishing it would
freeze its `Fake*` doubles under the same compatibility discipline as the connector API for no
user benefit — adding it later is cheap, retracting it is impossible. The guard is a
`skipPublishing` declaration in the module's own POM, the same module-local shape as
docs-validation's deploy skip, rather than a root-level artifactId exclusion list that would
silently stop matching on a rename. The SQL uber-jars carry no sources, so Central's
per-jar javadoc requirement is met by the sanctioned stub: one profile-managed jar execution
attaches an `index.html` pointing at the published API reference, active only where a module
provides `src/main/central-javadoc-stub`.

**The signing key is a dedicated project release key**, not a personal identity key: UID naming
the project, private key and passphrase held as secrets of the `release` environment plus an
offline backup, public
key on `keyserver.ubuntu.com`. The namespace maps to a Portal Organization so publish rights can
be granted to future members; a personal key would re-create the single-person coupling that
structure removed. Signing uses maven-gpg-plugin 3.x's Bouncy Castle signer reading
`MAVEN_GPG_KEY` / `MAVEN_GPG_PASSPHRASE`, so CI needs neither a gpg binary nor a keyring import —
the parent's pinned 1.4 predates all of that and is overridden.

**The release build carries two deliberate flags.** `versions:set` from the tag keeps the tree
at `<next>-SNAPSHOT` with no release commit (the tagged tree is what CI verified; only the
version differs). `-DskipTests` because the staged commit already passed every CI lane and the
staging build's job is packaging, signing and upload.

**Every tag is preceded by a rehearsal that uploads nothing ([#1818]).**
The first `v1.2.0` tag run failed Central validation: Maven 3.10.0 wrote local-repository files into the plugin's staging directory, and plugin 0.11.0 bundled them ([#1812]).
The validate dry run would have caught it, but it had been skipped because a validated-then-dropped deployment may count toward the namespace's monthly Central usage.
Plugin 0.12.0's `skipUpload` writes the real bundle and stops before the upload, without reading Central credentials.
`just rehearse-release` (`scripts/rehearse-release.py`) stages both lines that way and fails on a directory without its POM, a local-repository file, the other line's version or an unsigned file.
It reports each line's files and uncompressed bytes, which is how the Usage Center counts them.
Those numbers inform a limit-raise request to Sonatype before the tag; they do not gate the run, because the limit is raised on request.
The rehearsal became the dispatch's default mode, so a dispatch costs no Central usage unless `mode: validate` is chosen.

**The release credentials are secrets of a ref-restricted environment ([#1971]).**
Through 1.2.0, the signing key, its passphrase and the Central token pair were repository secrets, which a workflow file on any branch of this repository can read.
The rules that a release is a tag on `main` and that a dispatch never publishes lived only in the workflow file at the triggering ref.
Anyone with write access could therefore push a `v*` tag on a branch commit whose `release.yaml` drops the ancestry check, or dispatch a modified workflow from a branch, and that run would receive the key and the token.
The `stage` job now names the environment `release`, and the four secrets exist only there.
The environment's deployment policy admits the `main` branch and `v*` tags, and GitHub, not the workflow file, enforces it: a job whose ref is neither fails before its first step, without the secrets.
A tag ruleset on `refs/tags/v*` restricts creation, update and deletion to the repository admin role, so a write-access account cannot create a `v*` tag that the policy would then admit.
The environment has no required reviewers.
Both settings live in the repository's settings rather than in source; flink-datastream-protobuf carries the same pair.
The environment guards against unmerged refs only.
What reaches `main` is governed by `main`'s branch protection, which requires CI but not an approving review, so a change merged to `main` runs with the secrets.

## Declined alternatives

- **Publishing each line with `autoPublish=true`.**
  The first decision deferred automatic publication until the first real release.
  The revision automates it through the publisher API instead: plugin auto-publication could publish the 2.x line before the 1.20 build or validation failed.
  Per-PR CI covers 2.x; the weekly LTS lane does not prove that every tagged commit has passed a 1.20 build.
- **Inspecting the bundle inside the tag run.**
  The plugin builds and uploads in one goal, so a tag run can inspect its bundle only after the upload; Central validation already covers that point.
  Splitting the goal to upload through the Portal API would replace the upload path for a check the pre-tag rehearsal already makes.
- **Failing the rehearsal when the total exceeds the monthly limit.**
  Considered for [#1818], together with publishing the two lines in different calendar months.
  Declined: Sonatype raises a namespace's limit on request, so the route for a large release is a request before the tag, not a different way of publishing.
- **Combining both version lines into one Portal bundle.**
  Considered for [#1185]; the maintainer chose to retain two deployments and require both validations before either publication.
  This keeps the existing staging builds and their module-local publication exclusions.
- **A `resume` dispatch with typed deployment IDs.**
  Considered for [#1980] and drafted first.
  Typed IDs are where a recovery goes wrong: a mistyped ID, or the pair of a run that failed before Publish, would be published irreversibly, and a check of the deployments' Portal names could not tell whether the run had passed its collection gate.
  The dispatch also needed `actions: write` to start the docs, and a `workflow_dispatch` runs the dispatched ref's copy of the workflow, so tags cut before a fix never see it.
  Re-running the failed job carries the IDs from the job that captured them, and needs no input.
- **Telling a published version from coordinates.**
  Considered for [#1980].
  repo1 shows a version only once it is PUBLISHED, and the Portal documents no lookup by coordinates or listing of deployments, so a run could not tell PUBLISHING from never uploaded and might upload a second bundle while the first is still publishing.
- **Building the Release from the jars `stage` built.**
  They would have to cross to another job as an artifact and survive into a re-run attempt, and the recovery path would then differ from the first attempt's.
- **Required reviewers on the `release` environment.**
  Considered for [#1971]; declined because an approval would restore the manual step that [#1185] removed, and the tag ruleset already limits who can start a release.
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

- A validate dispatch stages **both lines per run** and names each deployment with its version, run ID, attempt and "dry run".
  Its final cleanup attempts to Drop every captured deployment in VALIDATED or FAILED state, including the first if the second staging run failed.
  A deployment still validating, a failed Drop, or runner termination can leave a deployment behind; its captured ID and the failure remain in the run log and summary.
  The event guard excludes both Publish and GitHub Release creation even when a dispatch targets a tag.
  Validate rehearsals exercise ID capture, status and Drop authentication; Publish first runs at a real release.
  The default rehearsal exercises the signed build and the bundle, but neither the upload nor Central's validation; a defect only Central detects still surfaces at the tag, before Publish.
- A dispatch, in either mode, runs only from `main` or a `v*` tag, because the rehearsal also signs.
  A change to `release.yaml` itself is therefore first exercised after it merges.
  A local `just rehearse-release` checks the bundle that `scripts/rehearse-release.py` builds, not the workflow.
- A re-run uses the workflow and scripts of the tagged commit, so this recovery applies to tags cut after [#1980]; an older tag is recovered by hand.
- The staged bundle's validation report (signing, checksums, POM completeness, sources/javadoc
  presence) is produced by the Portal on every upload, giving each release a pre-publish
  checklist for free.
- A failure in either build, validation, ID capture or artifact collection skips both publications and the GitHub Release.
  The upload plugin stays at `autoPublish=false`; changing that setting would bypass the shared validation gate.
- **The guarantee ends at the decision to publish.**
  The [Portal API] publishes one deployment per request; the two requests and GitHub Release creation are not a transaction.
  A network failure after the first request can leave one line PUBLISHED and the other VALIDATED.
  The helper stops on preflight or Publish errors and on invalid or permanently rejected status responses.
  During the publication wait, it retries read-only status polls after transport failures, HTTP 429 or HTTP 5xx; it uses a 30-second socket timeout and checks a 3-hour deadline between polling rounds.
  The run summary records state transitions; repeated unchanged polls remain in the log.
  It does not blindly retry a Publish whose response was lost.

### Recovery

Use `CENTRAL_TOKEN_USERNAME` and `CENTRAL_TOKEN_PASSWORD`, the same token pair as staging, for `just central-portal`.
Take deployment IDs from the run summary or the upload logs, and inspect them with `just central-portal status <2x-id> <1x-id>`.

Before publication starts, Drop leftover VALIDATED or FAILED deployments with `just central-portal drop <id> ...` before rerunning the workflow.
Drop attempts every supplied ID even if one deletion fails, tolerates an already absent ID, and refuses deployments in other states.
Leave a still-validating deployment until its state can be checked again.

After a Publish request, the recovery is **"Re-run failed jobs"** on the failed run, never "Re-run all jobs", which would upload another pair under immutable release coordinates.
`stage` refuses that re-run before anything is signed, but the refused attempt leaves no outputs, so the run must then be finished by hand with the IDs from the summary of the attempt whose `stage` succeeded.
The re-run repeats `publish-central`, `github-release` or both with the original IDs, so a publication that outlasted the wait resumes from the wait, and a GitHub-side failure repeats only the Release.
GitHub offers a re-run for 30 days after the run.
If `github-release` stops at an attachment it cannot vouch for (a finished attachment outside the published set, or one whose digest differs from Maven Central's file), delete or replace that attachment by hand after comparing it with Central's file, then re-run the failed job again.
If publication fails permanently, inspect the Portal error before choosing a recovery; the helper cannot roll back Central.

Without the workflow, for example for a tag cut before [#1980], after a refused "Re-run all jobs", or for a failure no re-run fixes, the same steps run by hand.
`just central-portal publish <2x-id> <1x-id>` checks every ID before acting, publishes only VALIDATED deployments, and waits for already PUBLISHING deployments without issuing another Publish.
`scripts/release-assets.py download <version> <new-dir>` fetches and verifies the release's files (six uber-jars and six signatures per line from 1.2.0, five of each for 1.0.0 and 1.1.0), and `scripts/release-assets.py complete <version> <dir>` creates or completes the Release.
Run both with a checkout of the release tag as the working directory, whose `flink-sql-connector-gcp-*` modules fix the set; a tag cut before [#1980] has no copy of the script, so run `main`'s copy from that directory, for example from a `git worktree add <dir> v<version>`.
Trigger the docs with `gh workflow run docs.yaml --ref main`.
Do not restage Central to repair GitHub attachments.
A tag cut on the wrong commit remains correctable only before Publish; afterward, changed artifacts require a new version.

[#724]: https://github.com/flink-gcp/flink-connector-gcp/issues/724
[#39]: https://github.com/flink-gcp/flink-connector-gcp/issues/39
[#29]: https://github.com/flink-gcp/flink-connector-gcp/issues/29
[#1185]: https://github.com/flink-gcp/flink-connector-gcp/issues/1185
[#1812]: https://github.com/flink-gcp/flink-connector-gcp/issues/1812
[#1818]: https://github.com/flink-gcp/flink-connector-gcp/issues/1818
[#1971]: https://github.com/flink-gcp/flink-connector-gcp/issues/1971
[#1968]: https://github.com/flink-gcp/flink-connector-gcp/issues/1968
[#1980]: https://github.com/flink-gcp/flink-connector-gcp/issues/1980
[#2068]: https://github.com/flink-gcp/flink-connector-gcp/issues/2068
[Portal API]: https://central.sonatype.org/publish/publish-portal-api/
