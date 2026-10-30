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

# ADR-0191: libraries-bom updates are proposed weekly by Renovate

- Status: Accepted
- Date: 2026-10-31 (Renovate behavior read from its source on 2026-10-31)
- Issues: [#1836](https://github.com/flink-gcp/flink-connector-gcp/issues/1836), [#1555](https://github.com/flink-gcp/flink-connector-gcp/issues/1555), [#1817](https://github.com/flink-gcp/flink-connector-gcp/issues/1817)
- Modules: CI, build
- Current behavior: [Google Cloud library updates](../content/docs/development/contributing.md#google-cloud-library-updates); `.agents/references/repository-guide.md` § Version policy

## Context

Every Google Cloud client library version comes from `com.google.cloud:libraries-bom`, which shipped thirteen times between 2026-04-14 and 2026-10-27, three days to four weeks apart and about two weeks on average.
[#1555](https://github.com/flink-gcp/flink-connector-gcp/issues/1555) made adopting the newest BOM the first step of each minor release candidate, because Dependabot's monthly `maven` group lags the BOM: 1.1.0 shipped 26.87.0 while 26.89.0 was already on Maven Central.
That rule adopts a BOM once per release, by hand, weeks after it ships, and the release candidate then absorbs every client change at once.

A BOM pull request cannot pass as Dependabot opens it.
The property alone moves every shaded bundle, so `just check-notice` fails until each shaded module's `META-INF/NOTICE` and licence texts are regenerated.
Beyond that mechanical step, a bump can owe a relocation for a newly bundled package root, the org.json check the root POM prescribes, a move of `opentelemetry-bom.version` ([ADR-0186](0186-client-library-metrics-reach-flink-reporters-through-a-narrow-opentelemetry-api.md)), and a re-read of claims about what a moved client does or lacks.

The repository already runs self-hosted Renovate on the organization's App for the emulator images ([ADR-0151](0151-emulator-image-updates-are-proposed-monthly-by-renovate.md), [ADR-0121](0121-ci-push-back-runs-as-an-org-owned-github-app-with-per-use-downscoped-tokens.md)), so proposing the BOM needs no new service or credential.

## Decision

**Renovate proposes each new `libraries-bom` weekly, and `main` adopts it as it ships.**
A `libraries_bom` job in `renovate.yaml` runs every Monday at 04:43 UTC and on dispatch.
Its Maven manager is limited by package rules to `com.google.cloud:libraries-bom`, grouped onto one `renovate-bom/libraries-bom` branch whatever the update type, and opened as a draft whose body lists the follow-ups a reviewer owes.
Dependabot ignores the BOM for every update type, so the two never propose the same bump; it keeps every other Maven dependency.

**Each Renovate job selects its manager and branch prefix in its own global configuration.**
The emulator job sets `enabledManagers` to `custom.regex` and keeps the default `renovate/` prefix; the BOM job sets `maven` and `renovate-bom/`.
`renovate.json5` sets neither, because a repository value overrides the global one and would make both jobs run both managers.
The prefixes must not share a start: a run collects every branch whose name starts with its `branchPrefix`, and closes the pull request of each one it did not produce in that run and deletes the branch, unless a person has edited it (`pruneStaleBranches`, read in Renovate's `lib/workers/repository/finalize/prune.ts`).
A BOM run under `renovate/` would therefore close the open emulator pull request, and an emulator run would close the BOM one.

**The workflow regenerates the NOTICE files on the update branch, in two jobs.**
After the Renovate step, the BOM job looks the branch up; when it exists, a credential-free job checks out its head, runs `just update-notices`, and uploads the staged difference as a patch.
A second job on a fresh runner applies the patch to the same head, fails if it touches anything but a shaded module's `META-INF/NOTICE` or `META-INF/licenses/*` (listing both sides of a rename), and only then mints a contents-write App token, commits, and pushes with a lease on the head the build used.
The split keeps the token out of the environment where the branch's build code runs; the path check bounds what that build can get pushed.
The build job keeps no Maven cache, because a cache saved by a scheduled run lands in `main`'s scope, where the weekly and E2E jobs would restore whatever the branch's build wrote into `~/.m2`.
The regeneration runs on every BOM run while the branch exists, not only when Renovate changed it, because Renovate recreates the branch from `main` when it moves the version or resolves a conflict, which drops the earlier NOTICE commit.

**The NOTICE commit carries the identity Renovate commits as.**
Renovate decides that a person has edited its branch when any commit's author or committer email differs from its own git author, other than GitHub's `noreply@github.com`, which commits its platform commits; it then stops updating the branch.
On an App token it derives that author from the token's bot user, as `<id>+<slug>[bot]@users.noreply.github.com` (`lib/modules/platform/github/index.ts`); its emulator commit `f2a6d1913` carries `317469580+flink-gcp-bot[bot]@users.noreply.github.com`, and the push job computes the same address from the App slug.
A NOTICE commit under any other identity would freeze the branch at the first BOM it proposed.

**A minor release confirms the BOM rather than adopting it.**
The release candidate's first step becomes checking that `main` carries the newest BOM on Maven Central, dispatching the job if one is pending, and naming the adopted version in the release notes.
This refines [#1555](https://github.com/flink-gcp/flink-connector-gcp/issues/1555)'s rule and the procedure [#1817](https://github.com/flink-gcp/flink-connector-gcp/issues/1817) carries for 1.3.0.

## Alternatives declined

**Renovate's `postUpgradeTasks`.**
They would run the regeneration inside Renovate's own commit, but in Renovate's container, whose default image carries no JDK, Maven or `just` (checked in the `44` image on 2026-10-31).
They would also run the branch's build in the process that holds the write token.

**A `pull_request`-triggered follow-up job.**
The issue first proposed a job on the update pull request's own events, under ADR-0121's trust rules.
Self-hosted Renovate changes the branch only inside these scheduled or dispatched runs, so a job in the same run sees every Renovate push without adding a secret-bearing `pull_request` workflow.
A person who pushes to the branch regenerates the files with `just update-notices`, and the pull request's `check-notice` turns red if they do not.

**One job for both managers, with a Renovate `schedule` on the emulator group.**
A Renovate schedule would also block a dispatched run outside its window, losing the recovery property ADR-0151 keeps by leaving the cadence to the workflow.

**Disabling `pruneStaleBranches` instead of separating prefixes.**
It would keep each job from closing the other's pull request, but also leave an obsolete update pull request open when its update disappears, for example after a hand bump on `main`.

**A verified commit through the Git Data API.**
Neither `main` nor the update branch requires signed commits, so a plain `git push` needs no tree-building code.
The cost is that the NOTICE commit shows as unverified beside Renovate's verified one.

## Consequences

The BOM arrives on `main` within about a week of its publication, one client set at a time, instead of in a release candidate.
Each update pull request still needs a reviewer for everything except the NOTICE files, and its body lists that work.

The pull request's CI runs twice per Renovate push: once on Renovate's commit, where `check-notice` fails whenever the bundles moved, and again on the NOTICE commit.
While a branch is open, each weekly run spends one Maven reactor build regenerating it; the weekly `notice_sources` job, which runs the same build, spent 90, 127 and 83 seconds in that step on 2026-10-12, 10-18 and 10-25, without Docker, from a warm Maven cache; this job has none, so it also downloads the reactor's dependencies from Google's mirror.
If the branch moves or is deleted between the build and the push, the lease fails the push and the run; a plain push would have recreated a deleted branch.
The next run regenerates whatever branch exists then.
The regeneration fails closed: when one module cannot be regenerated, the run fails and pushes nothing, so the branch keeps whatever NOTICE files it last had, which may not match the BOM it now carries.
The first BOM this job will see shows the common case.
On 26.91.0 the Spanner bundle moves `bcprov-jdk18on` from 1.80 to 1.85, and the publisher's `LICENSE.md` at `r1rv85` adds an `MIT License` line and moves the copyright year, so the sha256 pin no longer matches (measured 2026-10-31 by running `just update-notices` on a local bump).
The pin cannot be moved ahead of the bump, because `main` still bundles 1.80; a reviewer updates it on the update branch, as the pull request body says.

Once a person pushes to the branch, Renovate stops updating it, so a newer BOM waits until that pull request is merged or closed.

The end-to-end path, from a dispatched run to a draft pull request carrying the regenerated files, can only run from `main`: Renovate reads its configuration from the default branch, and the jobs are guarded to `main`.
It is verified by the first dispatched runs after this decision merges, recorded on [#1836](https://github.com/flink-gcp/flink-connector-gcp/issues/1836): one opening the pull request, and, after the reviewer's pin commit, one pushing the regenerated files.
That first cycle cannot show Renovate continuing to update a branch that carries only its own and the NOTICE commit, because the reviewer's pin commit already marks the branch as edited.
That property rests on the identity match above until a later BOM arrives without a licence change.
