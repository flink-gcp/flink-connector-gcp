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

# ADR-0121: CI push-back runs as an org-owned GitHub App with per-use downscoped tokens

- Status: Accepted
- Date: 2026-08-16
- Issues: [#177]
- Modules: CI, opentofu
- Current behavior: [pinact workflow](../../.github/workflows/pinact.yaml)

## Context

Until now no workflow in this repository pushed a commit.
Every job authenticated with the run's own `GITHUB_TOKEN`, which is enough to plan, apply, comment and label, and CI's only write-back reached GCP over Workload Identity Federation.

Two properties of `GITHUB_TOKEN` keep a whole class of automation out of reach.
A push authenticated with it starts no new workflow run, so any commit a job makes lands behind checks that ran against the state before the fix.
It also cannot write under `.github/workflows/`, whatever permissions the job declares.

Three features wanted those pushes.
tfaction recovers a failed apply with a follow-up pull request whose fresh plan picks up the remainder, because the failed apply has bumped the state serial and left the reviewed plan stale; without a token that can create it, that pull request is written by hand ([ADR-0063](0063-persistent-gcp-infrastructure-is-one-tofu-root-module-applied-by-tfaction-over-wif.md) records the runbook).
tfaction's `test` action commits `tofu fmt` and `tflint --fix` results.
pinact pins action references to commit SHAs, editing precisely the workflow files `GITHUB_TOKEN` may not touch.

ADR-0063 deferred the App to the dedicated organization at go-public time, on the reasoning that an org-owned App is the right owner and that a personal one would have to be replaced.
The repository is now public under `flink-gcp`, so that condition is met and this record executes the deferral.

## Decision

**One App, owned by the `flink-gcp` organization and installed on this repository alone.**
`flink-gcp-bot` holds three repository permissions and no others: contents write, pull requests write, and workflows write.
Workflows write exists only for pinact; contents and pull requests cover the follow-up pull request, its comment and assignee, and the `test` action's fix commits.
Issues write is absent: the two tfaction features that would want it — drift detection's issue automation and the follow-up pull request's group labels — both stay off.

**A job mints its own token at the step that needs it, downscoped below that ceiling.**
`actions/create-github-app-token` takes `permission-contents` and `permission-pull-requests`, so the apply workflow's follow-up step holds no more permission than it uses, and its `post:` step revokes the token when the job ends.
The App's private key is therefore the credential that persists; the tokens derived from it live for one job and expire in an hour regardless.
pinact-action mints its own tokens the same way from `app_id` and `app_private_key`, downscoping the read it does for `pinact run` separately from the write it does for the push.

**Credentials live at the repository, not the organization**: variable `BOT_APP_ID`, secret `BOT_APP_PRIVATE_KEY`.
One repository is installed, so an org-level secret would widen the blast radius without shortening the wiring.

**A run without the credentials degrades to checking rather than fixing.**
pinact-action treats either credential supplied without the other as a hard error rather than a fallback, so `pinact.yaml` gates both inputs on both being present and otherwise runs under `GITHUB_TOKEN` with fixing disabled: an unpinned reference turns the check red naming the problem, and the contributor runs `just pin-actions`.
Gating on the credentials rather than on the head repository is what makes that cover all three cases it has to.
Fork pull requests are the obvious one.
Dependabot's are not — its branches live in this repository, so a head-repository guard reads them as trusted, while GitHub swaps in the Dependabot secret store, where these secrets do not exist.
The third is a half-finished credential rotation, which degrades to checking instead of reddening every pull request.
Losing the credentials therefore costs the automatic fix, not the check.
The `test` action, which has no such fallback, is skipped outright on forks; `just lint`'s `tofu fmt -check` continues to cover formatting there.

**`pinact.yaml` stays outside the required `CI passed` gate.**
Enrolment is opt-in under [ADR-0059](0059-ci-yaml-orchestrates-pull-request-ci-behind-one-required-check.md), and the reason not to take it is the one [ADR-0058](0058-verify-yaml-selects-what-a-pull-request-builds-instead-of-filtering-whether-it-runs.md) already settled: a required check that never reports blocks a pull request forever, and a paths-filtered workflow reports on nothing outside its paths.
Enrolling it would mean dropping the filter and running it on every pull request to buy a guarantee the credential-less fallback already gives.

**Drift detection remains off**, now by decision rather than for want of a token.
It wants three more workflows and apply-job changes; the infrastructure in `opentofu/flink-gcp` changes rarely enough that the detection interval it would buy is not worth that surface.

## Evidence

The App private key is the first long-lived credential this repository holds — `secrets.` appears nowhere else under `.github/` — so the containment was checked rather than assumed, and two of the answers are narrower than they first look.

The jobs that carry a token run on `pull_request`, never `pull_request_target`.
A fork therefore cannot reach the secret, which was the property being bought.
It does not follow that only maintainers can cause a secret-bearing run: anyone with read access can open a pull request whose head is an existing branch of this repository.
What they cannot do is influence what that run executes, and content control over a branch here is what the containment actually rests on.
It follows that the key is reachable by anyone who can push to this repository, by construction rather than by oversight — such a person can add a step to this workflow as easily as read the secret from one.
One file in the checkout is trusted input for the same reason: pinact reads `.pinact.y*ml` or `.github/pinact.y*ml` from the working directory, and a pull request adding one decides what the scan covers.
All four names are in the trigger's paths, so such a change at least runs the check rather than landing with no signal at all — but the control is review, not the filter.
An `aqua.yaml` at the repository root may take similar precedence over the action's own checksum-pinned configuration; that was reasoned from aqua's documented search order rather than measured, and it is not the reason to worry, because anyone who can add one can already add a step to this workflow.

No service account key exists and the App token cannot mint one, but the GCP side is not untouched.
The plan service account's WIF binding admits any workflow of this repository on any ref, so contents write is transitively project-wide read plus object write on the state bucket.
A write collaborator could already do that; what the App adds is a standing credential that grants it.
Narrowing that binding is a separate change against `opentofu/flink-gcp`.

pinact-action's behavior was read at the pinned commit rather than taken from its README.
`fix: "false"` routes to a validate-only path that raises "GitHub Actions aren't pinned" and never invokes git, which is what makes the credential-less fallback a clean red rather than a partial run.
`getToken` throws in both directions — an id without a key, and a key without an id — which is why both inputs are gated on both.
The fixing path is the quieter one: pinact exits zero in fix mode, so the run reports green with an error annotation and the pushed commit is the signal.

The checkout inputs are load-bearing and were wrong in the first draft.
`ref: ${{ github.head_ref }}` resolves a bare ref name against the base repository, where a fork's branch does not exist.
Most fork pull requests would have died on a git error instead of the promised red check, and one whose head branch shared a name with a branch here — `main`, for the ordinary fork-and-push flow — would have scanned this repository's copy and reported green without looking at the pull request at all.
Naming the head repository and the head commit is what makes the fork path examine the fork's content.

Version choice went against the reference configuration it was modeled on.
pinact-action v3.0.0 carries pinact v4.0.0, matching the `pinact = "4"` major that `just pin-actions` runs locally, while the v2.0.0 used elsewhere carries a 3.x.
Two pinact majors formatting the same file is a churn source with no upside, so the newer action was pinned.

The push path was exercised end to end on a throwaway pull request before this one was called ready: one `actions/checkout` reference downgraded to a bare tag, `flink-gcp-bot[bot]` pushed `chore(pinact): pin GitHub Actions` restoring the SHA, and **that push started a fresh CI run** while the run on the unpinned commit was cancelled — the property the whole App exists to buy.
Three things were measured there rather than assumed: the commit came back `verified: true`, so GitHub signs an installation-token commit even though the client sends no signature; the diff was exactly the one line pinact rewrote, so the `git diff`-derived file set does not sweep in neighbours; and the run that pushed reported green, so the commit is the only signal.
The read half is visible in the same feature's own run: `Creating GitHub App token: {"owner":"flink-gcp","permissions":{"contents":"read"}}`, then `Revoking GitHub App token`.

The follow-up pull request cannot be verified without a real failed apply, and manufacturing one would write to real state and need a hand-written recovery of its own.
The first genuine failure is the live test.
Its expected shape, from tfaction's source at the pinned version: a pull request on a `follow-up-<pr>-opentofu__flink-gcp-<timestamp>` branch, assigned to the original author, carrying a commit that touches `opentofu/flink-gcp/.tfaction/failed-prs`.
That path matters beyond bookkeeping — it sits under the root module, so `list-targets` selects the target on the follow-up pull request by construction, and the fresh plan appears without further wiring.
Whether it arrives as a draft depends on `draft_pr`, which the companion change sets.

## Alternatives declined

- **Keep `GITHUB_TOKEN` and add nothing**: it cannot write workflow files at all, and its pushes start no run, so a fix commit would leave the pull request showing checks from before the fix.
- **A personal access token**: it cannot produce verified commits, it binds automation to one person's account and leaves with them, and its scope is the account's repositories rather than one installation. The signing difference is not in the client — pinact-action sends no signature either way — but in GitHub, which signs a Git Data API commit made under an App installation token and does not sign one made under a token belonging to a user.
- **A personal GitHub App**: rejected in ADR-0063 for the reason that still holds — the organization now exists and owning the App there survives any change of maintainer.
- **Securefix Action**: it keeps the private key out of the workflow entirely by moving commit creation to a separate server repository. That is the stronger posture, and it costs a second repository plus its own App for a single-repository need. Revisit if more repositories join the organization.
- **A dispatch-triggered fresh-apply workflow** as an App-free route to the same recovery: built and withdrawn on [PR #176](https://github.com/flink-gcp/flink-connector-gcp/pull/176) in the App's favour, recorded in ADR-0063.
- **Organization-level credentials**: no second repository is installed, so this only widens what a leak reaches.

## Consequences

A pull request from this repository can now arrive with a commit its author did not write.
The push flow has to absorb that: pull before the next local commit, because a force-push from a worktree that never saw the App's commit discards it.

Adding a push-back feature is now a permission question first.
A feature that needs a permission outside the three granted requires widening the App, which is a decision to record here rather than a settings change to make quietly.

Losing the private key degrades pinact to checking without fixing, which is quiet by design, and disables the failed-apply recovery, which is quieter still because it only runs after an apply has already failed. Neither surfaces as a red check, so a rotation has to be verified rather than assumed.

[#177]: https://github.com/flink-gcp/flink-connector-gcp/issues/177
