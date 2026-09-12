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

# ADR-0063: Persistent GCP infrastructure is one tofu root module, applied by tfaction from the reviewed plan over WIF

- Status: Accepted
- Date: 2026-07-29 ([#5], landed by PR
  [#168](https://github.com/flink-gcp/flink-connector-gcp/pull/168)); the apply
  misconfiguration found and the recovery runbook recorded 2026-08-01 (PRs
  [#170](https://github.com/flink-gcp/flink-connector-gcp/pull/170) and
  [#176](https://github.com/flink-gcp/flink-connector-gcp/pull/176)); the GitHub App
  deferred to go-public ([#177]) and adopted 2026-08-16 once the org existed ([#177],
  ADR-0121); the plan lookup repointed at `ci.yaml` 2026-08-09 ([#444]);
  extended for Tier-3 deployment infrastructure 2026-09-11 ([#38], [#1246])
- Issues: [#5], [#38], [#177], [#444], [#1246]
- Modules: opentofu, kubernetes
- Current behavior: `opentofu/README.md` (bootstrap, service-agent one-offs, credentials), `kubernetes/README.md` (manifest hierarchy, validation and rendering)

## Decision

- **`opentofu/flink-gcp` is the single root module for the project's persistent GCP
  resources** ([#5]): enabled APIs, the state bucket, the WIF pool/provider, the service
  accounts (the plan/apply/E2E CI identities, later joined by the deliberately unprivileged
  `e2e-no-pubsub` probe) and the shared IT bucket/dataset. Fine-grained test resources (tables,
  topics,
  subscriptions, queues) are created by the tests themselves and never belong here. A new
  connector's API and E2E grants are added in the PR that first needs them, not in advance.
- **CI is tfaction v2** (`tfaction-root.yaml` at the root): pull requests touching
  `opentofu/**` get a plan comment from `tofu-plan.yaml`, which since ADR-0059 runs as
  `ci.yaml`'s `tofu_plan` job; the merge applies that reviewed plan file from GitHub
  Artifacts and comments the result (`tofu-apply.yaml`); both resolve the
  changed root modules through the shared `tofu-list.yaml`. State locking is the GCS
  backend's native locking. These two workflows are the standing exception to the
  just-recipe rule (ADR-0057): tfaction is itself the named, rerunnable sequence.
  Direct local OpenTofu commands select their root module with `-chdir`;
  `opentofu/README.md` documents them.
- **Plan, apply, comments and labels run on plain `GITHUB_TOKEN`** and continue to. The App
  was deferred to the dedicated org at go-public time ([#177]; decided with the user on PR
  [#176](https://github.com/flink-gcp/flink-connector-gcp/pull/176), where a
  dispatch-triggered fresh-apply workflow was built as an alternative and withdrawn in the
  App's favour), and the org now exists, so push-back runs as `flink-gcp-bot`
  ([ADR-0121](0121-ci-push-back-runs-as-an-org-owned-github-app-with-per-use-downscoped-tokens.md)),
  which now covers the follow-up pull request after a failed apply and the `test` action's fix
  commits. Which features that turns on, and which stay off, is recorded there: drift detection is
  declined, and trivy inside the `test` action is off against a measurement rather than for want
  of a token.
- **The apply workflow must set `TFACTION_IS_APPLY: "true"`** — tfaction's job_type is
  "terraform" for plan and apply alike, and setup falls back to `terraform_plan_config` (the
  read-only account) without it. That misconfiguration shipped with PR
  [#168](https://github.com/flink-gcp/flink-connector-gcp/pull/168) and hid behind
  no-change applies until the first real write (PR
  [#170](https://github.com/flink-gcp/flink-connector-gcp/pull/170)), whose 403s were
  then misdiagnosed twice:
  a missing service agent was blamed on evidence that never included the authenticated
  principal. **Read the auth step's log first.**
- **A failed apply is recovered by a follow-up pull request**, never by re-running the apply
  job: the failure bumps the state serial, making the saved plan stale, so a re-run can only
  fail again ("Saved plan is stale") — and tofu cancels unstarted operations on the first
  error, so assume nothing from the failed apply exists until measured (PR
  [#176](https://github.com/flink-gcp/flink-connector-gcp/pull/176)).
- **`plan_workflow_name` names the workflow whose *run* owns the plan artifact, which is
  `ci.yaml`** ([#444]) — not the file the plan steps live in. tfaction passes the string
  as the `workflow_id` of `listWorkflowRuns`, filtered by the pull request's head branch and
  `per_page: 1`, requires that single newest run's head SHA to equal the pull request's, and
  downloads the artifact from *that run id*; a `workflow_call` child has no runs of its own,
  so its artifacts belong to the caller. Two consequences: `ci.yaml` must stay
  `pull_request`-only, since there is no event filter and another trigger could make the
  newest run one that never planned; and renaming `ci.yaml` breaks every apply until this
  setting changes with it. Moving the plan under the orchestrator (ADR-0059) left the setting
  naming `tofu-plan.yaml`, which still resolves to a workflow — one with runs, but none since
  it stopped being triggered directly, and so none on the branch the lookup filters by — so the
  apply failed with "No workflow run is found". It stayed invisible for three days because nothing
  touched `opentofu/` in between, and the first change that did merged reviewed, planned and
  **unapplied**: the `roles/bigquery.readSessionUser` binding of PR
  [#433](https://github.com/flink-gcp/flink-connector-gcp/pull/433) was in the code and
  not in the project. **Moving a workflow under an orchestrator can break a lookup that names
  it, and the break surfaces only on the next change of that kind.**
- **A recovery pull request must itself change a file under the root module.** tfaction's
  `list-targets` selects targets from the pull request's changed files alone — a change to
  `tfaction-root.yaml` or to the workflows selects nothing, and no label forces a target — so a
  fix to the CI wiring plans nothing, and the apply it was meant to unblock still applies
  nothing, unless the same pull request touches `opentofu/flink-gcp/**` ([#444]).
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

## Tier-3 deployment infrastructure (2026-09-11)

[Issue #38](https://github.com/flink-gcp/flink-connector-gcp/issues/38) extends the existing GCP root with a shared regional GKE Autopilot cluster, private network, node identity and Artifact Registry repository.
The user's Kubernetes direction for [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246) replaces that investigation's dedicated-VM proposal before any resources were applied.
Cloud Tasks keeps a separate runtime identity and temporary bucket, reached through one namespace/ServiceAccount binding rather than the node identity.
The GCP root retains ownership of persistent cloud resources; a subsequent Kubernetes root owns only the Operator's Helm release after the cluster exists.
That ordering permits CI to plan the Helm release against a real endpoint.

The settled [#38 constraints](https://github.com/flink-gcp/flink-connector-gcp/issues/38#issuecomment-5301543921) remain: cost first, Operator replicas zero in tracked state, webhook disabled, all other Kubernetes resources in CUE, and on-demand tests outside the release gate.
The benchmark records actual admitted Pod resources and scheduling interruptions.
The steady-state Spot policy and each paid workload require a preregistered run manifest.
Scale-down follows verified workload deletion because an inactive Operator does not stop managed jobs.

Private nodes fetch frozen images from the dedicated regional Artifact Registry repository over Private Google Access.
The IAM-authenticated DNS control-plane endpoint allows operator/CI access without a public control-plane IP, bastion or NAT.
Node telemetry/image access and connector data access use separate service accounts.
The temporary Cloud Tasks bucket has one-day object expiry and no soft-delete retention; evidence is exported before exact-prefix cleanup.

Autopilot introduces a standing management-fee exposure.
The [published rate](https://cloud.google.com/kubernetes-engine/pricing), checked on 2026-09-11, is $0.10 per cluster-hour with a $74.40 monthly credit per billing account.
The credit's availability is not verified, so zero idle cost is not an acceptance claim.
The foundation's merge therefore requires approval of that persistent exposure as well as its saved resource plan.
Cluster deletion protection makes decommissioning a deliberate reviewed action.
The Operator install, lifecycle smoke/recovery suite and Cloud Tasks measurements remain dependent work; this foundation closes neither issue.

### CUE manifest management

The persistent Kubernetes ownership in this section is superseded by [ADR-0165](0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md).
CUE now owns application deliveries; the original decision below is retained as history.

The shared manifest module lives in `kubernetes/`, with CUE 0.17.1 pinned in `mise.toml`.
Root and leaf directories use the same package so ancestor files partially define the same `delivery.resources` struct as each leaf.
The root supplies cluster identity, Kubernetes type constraints and common metadata; `runs/` adds the run ID, expiry, namespace and Flink job policy.
Bootstrap leaves are siblings of run leaves because permanent CRDs and namespaces must not inherit a run's deadline or workload namespace.
Sibling run leaves are separate instances, and each delivery corresponds to one YAML output file.
Concrete delivery directories are leaves; CI rejects nested `delivery.cue` markers while allowing shared definitions in intermediate directories.
`cli_tool.cue` defines a `cue cmd render` command that prints only delivery resources as a YAML document stream.
It sorts by the delivery's Kind priorities, then internal resource key, so ConfigMaps precede FlinkDeployments independently of their names.
The CLI does not write files or apply resources; callers redirect its output.
CI discovers `delivery.cue` files under `bootstrap/` and `runs/` and renders each directory separately without injecting tags or placeholder inputs.
Committed deliveries supply concrete values required by their resources; incomplete deliveries fail validation.
A multi-directory `cue cmd` invocation is not used: the two-delivery probe combined the same-named resources into one output rather than checking separate deliveries.

Generated packages live in `gen/flink/`; `pkg/flink.#Application` embeds the generated type and supplies overridable application defaults.
Run policy remains in the ancestor package, so changing a library default cannot relax the environment's constraints.
These ordinary module directories avoid the deprecated `cue.mod/{gen,pkg,usr}` dependency layout.

Native Kubernetes schemas come from the pinned `cue.dev/x/k8s.io` module.
Flink validation definitions are generated by `cue get crd` from the checksum-pinned Operator 1.15.0 chart.
Complete CRD objects are imported separately from the same archive to preserve Kubernetes extensions and subresources when rendered for application.
The distributed chart, rather than the source repository's release tag, is authoritative for these packages.
Generated CUE is tracked and checked by regeneration; exported manifests are temporary build output.
Static CI needs no GCP credentials and is part of the aggregate PR gate.
Its type checks do not replace server admission or the Operator webhook's disabled semantic checks.

CUE owns all four Flink CRDs from their first installation, the `tier3-system` and `tier3-smoke` namespaces, workload RBAC/ServiceAccounts, quotas and run objects.
The separate Helm root will skip CRDs and namespace creation and disable chart-managed job RBAC/ServiceAccounts.
Helm owns the Operator Deployment, configuration, ServiceAccount and Operator RBAC only.
The Operator initially watches `tier3-smoke`; its normal replica count stays zero and its webhook stays disabled.
CRD upgrades precede Helm upgrades, and routine run cleanup preserves the CRDs and namespaces.

Delivery follows separate stages: static CUE; GCP access and CUE bootstrap without Pods; the idle Helm release; image/lifecycle tooling; then a generic smoke run with separately approved limits and stop conditions.
The planned smoke runtime is Flink 2.2.1 on Java 17, selected through the chart's `v2_2` API enum.
An expiry annotation alone stops nothing: the lifecycle stage must implement independent expiry enforcement before accepting a paid run.
The [manifest README](../../kubernetes/README.md) records the current commands and what remains unapplied.

[#5]: https://github.com/flink-gcp/flink-connector-gcp/issues/5
[#38]: https://github.com/flink-gcp/flink-connector-gcp/issues/38
[#177]: https://github.com/flink-gcp/flink-connector-gcp/issues/177
[#444]: https://github.com/flink-gcp/flink-connector-gcp/issues/444
[#1246]: https://github.com/flink-gcp/flink-connector-gcp/issues/1246
