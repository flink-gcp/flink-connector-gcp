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

# ADR-0063: Persistent GCP infrastructure is one tofu root module, applied by tfaction from the reviewed plan over WIF

- Status: Accepted
- Date: 2026-07-29 ([#5], landed by PR
  [#168](https://github.com/laughingman7743/flink-connector-gcp/pull/168)); the apply
  misconfiguration found and the recovery runbook recorded 2026-08-01 (PRs
  [#170](https://github.com/laughingman7743/flink-connector-gcp/pull/170) and
  [#176](https://github.com/laughingman7743/flink-connector-gcp/pull/176)); the GitHub App
  deferred to go-public ([#177])
- Issues: [#5], [#177]
- Modules: opentofu
- Current behavior: `opentofu/README.md` (bootstrap, service-agent one-offs, credentials)

## Decision

- **`opentofu/flink-gcp` is the single root module for the project's persistent GCP
  resources** ([#5]): enabled APIs, the state bucket, the WIF pool/provider, three service
  accounts and the shared IT bucket/dataset. Fine-grained test resources (tables, topics,
  subscriptions, queues) are created by the tests themselves and never belong here. A new
  connector's API and E2E grants are added in the PR that first needs them, not in advance.
- **CI is tfaction v2** (`tfaction-root.yaml` at the root): pull requests touching
  `opentofu/**` get a plan comment (`tofu-plan.yaml`), the merge applies that reviewed plan
  file from GitHub Artifacts and comments the result (`tofu-apply.yaml`); both resolve the
  changed root modules through the shared `tofu-list.yaml`. State locking is the GCS
  backend's native locking. These two workflows are the standing exception to the
  just-recipe rule (ADR-0057): tfaction is itself the named, rerunnable sequence, and
  `just tofu <args>` is the local equivalent.
- **Plain `GITHUB_TOKEN`, no GitHub App** — tfaction's push-back features (auto-fix commits,
  follow-up PRs) are unused. The App is deliberately deferred to the dedicated org at
  go-public time ([#177]; decided with the user on PR
  [#176](https://github.com/laughingman7743/flink-connector-gcp/pull/176), where a
  dispatch-triggered fresh-apply workflow was built as an alternative and withdrawn in the
  App's favour).
- **The apply workflow must set `TFACTION_IS_APPLY: "true"`** — tfaction's job_type is
  "terraform" for plan and apply alike, and setup falls back to `terraform_plan_config` (the
  read-only account) without it. That misconfiguration shipped with PR
  [#168](https://github.com/laughingman7743/flink-connector-gcp/pull/168) and hid behind
  no-change applies until the first real write (PR
  [#170](https://github.com/laughingman7743/flink-connector-gcp/pull/170)), whose 403s were
  then misdiagnosed twice:
  a missing service agent was blamed on evidence that never included the authenticated
  principal. **Read the auth step's log first.**
- **A failed apply is recovered by a follow-up pull request**, never by re-running the apply
  job: the failure bumps the state serial, making the saved plan stale, so a re-run can only
  fail again ("Saved plan is stale") — and tofu cancels unstarted operations on the first
  error, so assume nothing from the failed apply exists until measured (PR
  [#176](https://github.com/laughingman7743/flink-connector-gcp/pull/176)).
- **No service account keys, ever.** All CI credentials are short-lived WIF tokens; the
  provider condition pins the immutable repository/owner IDs, and per-account bindings
  restrict the apply account to `push` on `main` and the E2E account to
  `push`/`schedule`/`workflow_dispatch` on `main`. Plan runs read-only (`roles/viewer` +
  `roles/iam.securityReviewer`, plus state-bucket writes for the lock). Local runs
  authenticate via `GOOGLE_APPLICATION_CREDENTIALS` from the uncommitted `.env` — the google
  provider does not read `CLOUDSDK_CONFIG`; only the gcloud CLI does. The bootstrap that
  created the backend's own bucket is recorded in `opentofu/README.md`.
- One service-agent fact worth keeping: enabling an API does **not** create its service agent
  (they provision lazily; `gcloud beta services identity create` is the per-service one-off,
  in `opentofu/README.md`).
- **The tofu version is pinned twice on purpose**: `mise.toml` (what installs) and
  `versions.tf` `required_version` (what refuses to run on a skew) — a bump edits both.

[#5]: https://github.com/laughingman7743/flink-connector-gcp/issues/5
[#177]: https://github.com/laughingman7743/flink-connector-gcp/issues/177
