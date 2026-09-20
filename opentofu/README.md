# OpenTofu persistent layer

The GCP resources behind the real-GCP integration tests
([#5](https://github.com/flink-gcp/flink-connector-gcp/issues/5)): the
`flink-gcp` project's service accounts, Workload Identity Federation, buckets
and the BigQuery dataset, plus the on-demand Tier-3 rig's standing cluster.
*Persistent* means resources applied once and kept; the fine-grained resources (tables, topics, subscriptions,
queues) are created and deleted by the tests themselves.

## Layout

| Path | Contents |
|---|---|
| `flink-gcp/` | GCP root module — one GCP project and state |
| `tier3-bootstrap/` | CI-managed Kubernetes foundation and separate state; [runbook](tier3-bootstrap/README.md) |
| `tier3-operator/` | Idle Operator Helm release and separate state; [runbook](tier3-operator/README.md) |
| `flink-gcp/versions.tf` | tofu pin (mirrors `mise.toml`) and provider constraint |
| `flink-gcp/backend.tf` | GCS state backend (`flink-gcp-opentofu`, native locking) |
| `flink-gcp/main.tf` | Provider and the pinned GitHub identifiers |
| `flink-gcp/services.tf` | Enabled APIs |
| `flink-gcp/state-bucket.tf` | The state bucket itself |
| `flink-gcp/wif.tf` | WIF pool/provider and the repository-pinning condition |
| `flink-gcp/opentofu-sa.tf` | Plan (read-only) and apply service accounts |
| `flink-gcp/e2e-sa.tf` | The E2E test service account and its scoped grants |
| `flink-gcp/it-resources.tf` | Pre-existing bucket/dataset, adopted via import blocks |
| `flink-gcp/appengine-e2e.tf` | The stopped App Engine Standard fixture used by Cloud Tasks acceptance |
| `flink-gcp/tier3.tf` | Shared Autopilot cluster, private network, node identity and image repository |
| `flink-gcp/cloudtasks-benchmark.tf` | Workload identity and temporary-object bucket for separately approved Cloud Tasks benchmarks |
| `flink-gcp/cloudtasks-lifecycle.tf` | Queue control and benchmark state cleanup grants for the runner and supervisor |
| `flink-gcp/tier3-smoke.tf` | Generic smoke workload identity and one-day checkpoint/savepoint/HA bucket |
| `flink-gcp/tier3-bigquery.tf` | Isolated dataset, workload identity, state bucket and lifecycle grants for BigQuery trials |
| `flink-gcp/tier3-pubsub.tf` | Pub/Sub recovery workload identity, one-day state bucket and lifecycle state access |
| `flink-gcp/tier3-lifecycle.tf` | Lifecycle identities, immutable run evidence and mutable coordination records |
| `flink-gcp/pubsub-lifecycle.tf` | Project-wide Pub/Sub lifecycle control and an unbound resource consumer role |
| `flink-gcp/pubsub-e2e-iam.tf` | Service-agent and E2E-account IAM the Pub/Sub source real-GCP suite needs beyond `roles/pubsub.editor` |
| `flink-gcp/tfaction.yaml` | Marks the directory as a tfaction root module |
| `flink-gcp/.terraform.lock.hcl` | Committed provider release pin |
| `/tfaction-root.yaml` | Global tfaction configuration (repository root) |

CI: a pull request touching `opentofu/**` gets a plan comment from
`.github/workflows/tofu-plan.yaml`, which runs as a job of `ci.yaml` — so the
run that carries the plan file is `ci.yaml`'s; the merge to `main` applies that
reviewed plan file and comments the result (`tofu-apply.yaml`).
For direct local commands, see [Local use](#local-use). OpenTofu validation,
formatting and TFLint run for each selected root in the plan job.

The bootstrap root also has a tfaction marker: its runbook covers the initial manual
permission grants, PR plans and saved-plan apply after merge.
The [Operator root](tier3-operator/README.md) independently manages the idle Helm release through the same workflows.

## tfaction configuration decisions

[tfaction](https://suzuki-shunsuke.github.io/tfaction/) v2 drives the CI;
`/tfaction-root.yaml` holds the configuration. What is on, what is off, and
why:

| Setting | State | Why |
|---|---|---|
| `terraform_command: tofu` | on | This is an OpenTofu repository |
| Plan/apply as separate WIF service accounts | on | A pull request's plan job never holds write credentials |
| Plan file via GitHub Artifacts | on (built in) | The apply runs exactly the plan the PR reviewed; no extra storage |
| `plan_workflow_name` | `ci.yaml` | It names the workflow whose *run* owns the plan artifact, not the file the plan steps live in: the plan runs as a `workflow_call` child, whose artifacts belong to the caller's run. `ci.yaml` must therefore stay `pull_request`-only — the lookup takes the newest run on the head branch with no event filter ([#444](https://github.com/flink-gcp/flink-connector-gcp/issues/444)) |
| `dismiss_approval_before_plan` | on (default) | A re-plan dismisses stale approvals, so an approval always refers to the plan that will apply |
| `hide-comment` job in the plan workflow | on | Outdated plan comments are hidden; the visible comment is the one that would apply |
| GitHub App | on | The org-owned `flink-gcp-bot` ([#177](https://github.com/flink-gcp/flink-connector-gcp/issues/177); ADR-0121). Each step that pushes mints its own token from `BOT_APP_ID` / `BOT_APP_PRIVATE_KEY`, downscoped below the App's contents/pull-requests/workflows ceiling. Plan, apply, comments and labels stay on plain `GITHUB_TOKEN`, which suffices for them |
| `test` action (`fmt`, `validate`, check-providers, tflint) | on | Runs in the plan job, after init, under the App token — which is what makes it usable: a fix commit pushed with `GITHUB_TOKEN` would not retrigger CI, so the branch would sit behind checks that ran before the fix. A fixable finding is pushed and the step then fails the run; the push starts the next one. Two rounds when tflint and `fmt` both have work, because tflint throws before `fmt` runs. When the App token is unavailable, the plan job runs checking-only validate, fmt and TFLint commands instead; authentication and init must still succeed |
| `trivy` inside the `test` action | off | The original bucket-only scan reported five findings. A Trivy 0.74.0 scan on 2026-09-11 with the Tier-3 foundation reports eleven: CMEK on three buckets (LOW), access logging on three (MEDIUM), versioning on two temporary buckets (MEDIUM), two subnet flow-logging checks (LOW/MEDIUM), and master authorized networks on GKE (HIGH). Bucket and flow-log dispositions retain the existing cost policy: no extra key management, log storage or retained temporary-data versions. The GKE finding checks IP authorized networks, while this cluster disables IP endpoints and uses its IAM-authenticated DNS endpoint; adding an IP allowlist would not control that endpoint. tfaction fails on any finding, so the scan remains non-gating. These are configuration findings, not a runtime reachability measurement |
| `tflint` inside the `test` action | on | Clean against this configuration today, and `fix: true` lets it push the correction rather than only report it. It applies the bundled `terraform` ruleset to the selected root's `.tf` files; no plugins are configured. It also puts a PR-controlled plugin loader in a step holding a write token (ADR-0121 records why that is acceptable). Pinned in `mise.toml`, run in the plan job by tfaction or its checking-only fallback as a plain PATH command |
| `drift_detection` | off (default) | Declined 2026-08-16, no longer for want of a token: it wants three more workflows and apply-job changes, and this configuration changes rarely enough that the detection interval would not repay that surface |

## Security model

- **No service account keys, ever.** Every workflow credential is a
  short-lived token minted through WIF; local runs use your own ADC.
- The WIF provider condition pins the immutable repository and owner IDs, so
  a renamed or look-alike repository can never authenticate.
- Per-account bindings narrow further: the plan account is reachable from any
  event of this repository (and is read-only plus state-lock writes); the
  apply account only from a `push` to `main`; the E2E account from
  `push` / `schedule` / `workflow_dispatch` on `main`; the
  [image publisher](../kubernetes/images/README.md) only from `workflow_dispatch`
  on `main` through `tier3-images.yaml`, with Writer access to the Tier-3 GAR repository only.
  Fork pull requests are
  excluded outright — GitHub does not grant `id-token: write` to runs
  triggered from forks.

## Local use

Two variables in the repository-root `.env` (uncommitted, loaded by mise)
point everything at the dedicated gcloud configuration — two because they
have different readers. `CLOUDSDK_CONFIG` steers the `gcloud` CLI, but the
google provider's Go auth library does not read it (measured: with only
`CLOUDSDK_CONFIG` set, tofu silently fell back to the default
`~/.config/gcloud` ADC of a different account); the provider needs
`GOOGLE_APPLICATION_CREDENTIALS` naming the credentials file directly:

```text
CLOUDSDK_CONFIG=/Users/<you>/.config/flink-gcp
GOOGLE_APPLICATION_CREDENTIALS=/Users/<you>/.config/flink-gcp/application_default_credentials.json
```

From the repository root, select the GCP root module explicitly:

```sh
mise x opentofu -- tofu -chdir=opentofu/flink-gcp plan
mise x opentofu -- tofu -chdir=opentofu/flink-gcp validate
mise x opentofu -- tofu -chdir=opentofu/flink-gcp state list
```

The Cloud Tasks fixture is the exception whose runtime state is managed
outside OpenTofu. Its App Engine application is permanently located in
`us-central` (the App Engine name corresponding to Cloud Tasks `us-central1`),
and Google does not support deleting the application or changing that location.
The checked-in version uses one manually scaled instance only while acceptance
runs. `scripts/appengine-e2e-fixture.sh start` waits for `SERVING` with exactly
one instance and prints its id; `stop` waits for `STOPPED` with zero instances.
OpenTofu ignores only `manual_scaling.instances`, so those lifecycle changes do
not create drift while every other version setting remains managed. Both
commands require `CLOUDTASKS_IT_PROJECT` and authenticated `gcloud` access.

## Tier-3 Kubernetes environment

[Issue #38](https://github.com/flink-gcp/flink-connector-gcp/issues/38) owns the shared GKE Autopilot rig.
[Issue #1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246) supplies its first Cloud Tasks performance scenario.
[Issue #1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361) adds the [Pub/Sub recovery preparation](#pubsub-tier-3-preparation).
Routine E2E remains on MiniCluster; this rig is on demand and does not gate a release.

### Persistent foundation

The root creates the regional `flink-tier3` Autopilot cluster in `us-central1`, its private subnet, a dedicated node identity and the `flink-tier3` Artifact Registry Docker repository.
Nodes have private addresses, and operator access uses the IAM-authenticated DNS endpoint with IP endpoints disabled.
There is no Cloud NAT, bastion or IAP SSH firewall.
Mirror the pinned Operator and Flink images into `us-central1-docker.pkg.dev/flink-gcp/flink-tier3` before starting Pods; public registries are outside this network path.
The [image publication runbook](../kubernetes/images/README.md) describes the dedicated WIF publisher, digest-pinned mirrors, BuildKit build and seven-day deletion.
The node identity has telemetry permissions and read access to that repository.
It has no connector data permissions.
Autopilot's required managed Prometheus collection stays enabled, with automatic workload monitoring disabled.
Optional application scraping and its ingestion cost belong to each run's CUE definition and budget.
System and workload logging also remain enabled; a run must bound application log volume and include its ingestion cost.

The separate `cloudtasks-benchmark` runtime account has project-wide `roles/cloudtasks.editor` and object read/write/delete access only to `flink-gcp-cloudtasks-benchmark`.
The Kubernetes identity `tier3-cloudtasks/cloudtasks-benchmark` may impersonate it through Workload Identity Federation for GKE; the workload ServiceAccount must carry the corresponding GCP account annotation.
Queue operations remain project-wide, so the run supervisor must enforce exact queue names.
Neither the node account nor the runtime account receives IAM administration permissions.
The cluster foundation grants the apply identity `compute.networkAdmin`, `container.clusterAdmin`, `artifactregistry.admin` and permission to attach the dedicated node account.
The [Cloud Tasks lifecycle control](#cloud-tasks-lifecycle-control) extension also grants it project-wide custom-role administration.

A merge applies this foundation and creates a cluster; it therefore requires resource and cost approval before merge.
The [GKE price list](https://cloud.google.com/kubernetes-engine/pricing) checked on 2026-09-11 charges $0.10 per cluster-hour.
The $74.40 monthly free-tier credit is shared by a billing account, and its availability for this project has not been verified.
Without that credit, a 744-hour month costs $74.40 in cluster management fees even with no workload Pods.
Image/object storage, application Pods and other metered services are additional.
No claim of zero idle cost follows from this configuration.
Cluster deletion protection prevents accidental removal; deliberate decommissioning requires a reviewed change disabling it before deletion.

### Cloud Tasks lifecycle control

The [lifecycle grants](flink-gcp/cloudtasks-lifecycle.tf) explicitly select the existing runner and supervisor identities for the later Cloud Tasks benchmark admission path.
Adding a different identity to the shared lifecycle registry does not extend these grants.
Both receive custom roles with `cloudtasks.queues.get/pause/delete` and `cloudtasks.tasks.get/list`; only the runner also receives `cloudtasks.queues.create`.
These roles omit queue resume/update/purge/IAM changes and task creation/deletion/run/fullView.
The bindings are project-wide: IAM does not restrict them to the `ct1246-` prefix or an active run.
The [lifecycle runtime](../kubernetes/lifecycle/README.md#cloud-tasks-session) therefore binds its client to the one queue named after the approved run, creates it with its full configuration only after a read proves it absent, pauses it, reads back the paused state and zero dispatch counts before admitting a cell, re-reads it on every poll, and deletes it during cleanup.

Each identity also receives Object Viewer on `flink-gcp-cloudtasks-benchmark` and Object User conditional on object names under `runs/`.
Listing and reads cover the whole bucket; writes and deletes cover every run prefix, not just the current run.
The existing benchmark worker's editor grant is unchanged.
The apply identity receives project-wide `roles/iam.roleAdmin`, and custom-role creation depends on that binding.
This permits administration of project custom roles beyond these two names; it does not grant Role Admin to either lifecycle identity.

The GCP plan should add nine resources: two custom roles, two project bindings, four bucket bindings and the apply identity's Role Admin binding.
The Operator plan should update the idle Helm release in place, adding a Role/RoleBinding in `tier3-cloudtasks` while preserving zero replicas and all idle quotas.
Review the actual CI plans and rendered chart before merge.
The dependent admission path was implemented after that apply, idle verification and empty refreshed plans.
No queue, workload or paid measurement is admitted by these grants; execution still requires the session approval phrase, a published application digest and the separately approved cell list.

### BigQuery trial foundation

[Issue #1312](https://github.com/flink-gcp/flink-connector-gcp/issues/1312) uses the existing Flink 2.2.1 and Operator 1.15.0 baseline.
Its [GCP foundation](flink-gcp/tier3-bigquery.tf) creates the persistent `flink_gcp_tier3_bigquery` dataset and `flink-gcp-tier3-bigquery` state bucket in `us-central1`, plus the `tier3-bigquery` workload account.
The dataset is empty between trials and has a 24-hour default table expiration; the runner must still delete each trial's tables and verify their absence.
Dataset destruction is protected and does not delete contents automatically.
The state bucket uses Standard storage, uniform access, public-access prevention, no object versioning or soft delete, and a one-day lifecycle for `runs/` objects.
The Kubernetes identity `tier3-bigquery/bigquery` may impersonate the workload account.

Dataset-scoped custom roles separate the three actors:

| Actor | Dataset permissions |
| --- | --- |
| Workload | `bigquery.tables.get/updateData`: read table metadata and append to pre-created tables |
| Runner | `bigquery.datasets.get`, `bigquery.tables.create/get/getData/list/delete`: check the dataset, admit tables, validate and clean up |
| Supervisor | The runner's dataset permissions except `bigquery.tables.create` |

The workload receives no query-job, table-creation, table-deletion or IAM-administration grant.
The runtime must use `CREATE_NEVER` with an explicit location and pre-created matching schemas.
The dataset bindings cover every table in this dedicated dataset, not one run prefix.
The runner and supervisor also receive project-wide `bigquery.jobs.create/get/update` so either actor can inspect and cancel an outstanding validation query after the other fails.
These job permissions can inspect and cancel other principals' jobs in the project; they are not scoped to the dataset or an approved run.
The later runtime must record exact query IDs before submission, enforce the approved query and byte budget, and limit recovery to those IDs.
The existing project-wide Role Admin grant lets the apply identity manage these four custom roles.

All three actors can list and read the dedicated state bucket and use Object User under `runs/`.
That grant covers all runs in the bucket; run ownership must be checked by the lifecycle code before deletion.
The workload receives no access to the evidence bucket or environment lock.
The runtime must keep checkpoint, savepoint and HA paths under `runs/`.
A separately approved checkpoint write and restore with the pinned Flink GCS filesystem is a follow-up acceptance requirement; these static grants alone do not establish filesystem compatibility.

The GCP plan should add 19 resources without changing existing resources.
The [BigQuery namespace prerequisites](tier3-bootstrap/README.md#administrator-prerequisites-for-bigquery) precede the bootstrap plan, which imports six objects and adds five job/lifecycle objects.
The administrator extends the two existing ClusterRoles before that plan; after refresh their rules should already match, with only possible metadata reconciliation.
Review the actual plans before merge and verify successful applies, idle inventory and empty refreshed plans afterward.
The [foundation apply](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35482844068) completed with empty refreshed bootstrap and GCP plans; runner-impersonated reads verified the new namespace's zero quota and empty workload inventory.
That acceptance added BigQuery as the fourth namespace inspected by the helper and the third watched application namespace; Pub/Sub acceptance below extends the current scope.
Application publication, runtime admission and all paid trials require subsequent changes and separate execution approval.

### Pub/Sub Tier-3 preparation

[Issue #1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361) separates deployed Pub/Sub recovery from routine MiniCluster E2E.
The first preparation stage defines the `tier3-pubsub` GSA and `flink-gcp-tier3-pubsub` STANDARD bucket in `us-central1`.
The workload has `roles/storage.objectUser` over the entire bucket, including every run's state.
The planned checkpoint, savepoint and HA layout is beneath `runs/<run-id>/`; this path convention is not an IAM restriction.
Uniform bucket access and public access prevention are enabled; versioning and soft delete are disabled, and objects become deletion-eligible after one day.
Export final evidence through the existing Tier-3 evidence facility before temporary state expires.
An object lifecycle rule cannot stop a running job.

The GSA trusts only `tier3-pubsub/pubsub` through GKE Workload Identity.
The existing runner and supervisor receive bucket-wide object reads/listing plus `roles/storage.objectUser` restricted to the `runs/` object prefix for cleanup.
These permissions allow the later lifecycle implementation to inspect and remove state without borrowing the workload identity or receiving bucket administration.
Run ownership checks still belong to that implementation; a prefix grant does not identify which run the caller may clean.

This stage changes only the GCP root and defines eight resource instances: one GSA, one bucket, the workload bucket grant, the KSA impersonation grant, and two state-access grants for each lifecycle identity.
Review the PR's saved plan for exactly these additions and no unrelated changes before merging; merge triggers the ordinary CI apply.
After apply, inspect the bucket policy and IAM, confirm the apply result and an empty refreshed GCP plan, and retain that evidence in the issue before advancing.
If the apply fails, review the fresh plan in the recovery draft PR opened by tfaction rather than replaying the stale saved plan.

The [Pub/Sub bootstrap stage](tier3-bootstrap/README.md#pubsub-recovery-foundation) adds the namespace/KSA, installer and job RBAC, lifecycle access and zero idle quotas after the GCP foundation is verified.
The [bootstrap apply](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35485438834) succeeded with an empty refreshed plan; separate reads verified the Pub/Sub identity, observed zero quotas, empty inventory and runner access.
The common helper now inspects all five namespaces, and the [idle Operator configuration](tier3-operator/README.md) adds `tier3-pubsub` to its watch set.
The initial foundation deferred Pub/Sub topic/subscription grants; the lifecycle authority below follows the concrete application and owned resource design.
The initial eight-resource stage introduced no Pub/Sub data or resource-administration permissions.
Application/image publication, run admission, fault injection, ownership-aware service cleanup and separately approved numeric execution ceilings remain later steps.
This configuration does not create topics/subscriptions or start Kubernetes workloads, and it does not establish live Pub/Sub recovery.

### Pub/Sub lifecycle authority

The [IAM preparation](https://github.com/flink-gcp/flink-connector-gcp/issues/1391) adds five GCP resource instances: two lifecycle custom roles and their project bindings, plus one unbound consumer role.
The runner role contains topic/subscription creation, metadata reads, policy reads/writes, deletion and topic attachment.
The supervisor role contains topic/subscription metadata reads, policy reads and deletion, with no creation or policy writes.
Neither role directly includes publishing or consumption, but the runner's policy authority allows it to grant those permissions on any project topic or subscription, to itself or other principals.
These are trusted project-wide control identities: the grants do not enforce the `t3-` prefix, nonce labels or run ownership.
The [resource helper](../kubernetes/apps/pubsub/README.md#permissions-and-remaining-integration) must enforce those boundaries under the shared environment lock.

The third role, `tier3PubSubConsumer`, contains subscription metadata reads and consumption and has no project binding.
At runtime the runner installs fixed policies on owned resources: input publication for itself, input consumption and output publication for the workload, and output consumption for the supervisor.
No workload Pub/Sub project grant, service-account key, live topic or subscription is created by this configuration.
The runner needs no project IAM write permission to install these resource policies; its custom role explicitly grants the Pub/Sub resource policy operations.

Review the saved GCP plan for these five additions and no unrelated changes before merge.
After the merge-triggered apply, verify the role permissions and bindings and an empty refreshed GCP plan before integrating runtime admission.
Per-resource grants and identity-specific effective-access checks remain inside the future approved lifecycle; an empty infrastructure plan alone cannot validate them.
This preparation starts no Kubernetes workload or Pub/Sub data operation.

### Operator installation follows the cluster

First verify the foundation's apply succeeded and the root has an empty plan.
Then install the Flink Kubernetes Operator through the [separate Helm root](tier3-operator/README.md).
Keeping that release out of the cluster-creation plan avoids asking its provider to connect to an endpoint that does not yet exist.
The Operator's tracked value is `replicas: 0`, with `webhook.create: false`; cert-manager is unnecessary.
The [bootstrap root](tier3-bootstrap/README.md) owns namespaces, CRDs, persistent workload ServiceAccounts/RBAC and idle quotas.
It adopts the initial access resources through import blocks and has separate state, PR plans and merge-triggered CI application.
The [CUE manifest module](../kubernetes/README.md) provides application definitions, static validation and delivery rendering.
The [ownership decision](../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md) separates these responsibilities.
The Helm root installs only the idle release and selects a published GAR digest.
Application execution remains a separate stage.

### Lifecycle foundation

The [lifecycle foundation runbook](tier3-bootstrap/README.md#lifecycle-foundation) describes the identities, permissions and storage prepared for [issue #1310](https://github.com/flink-gcp/flink-connector-gcp/issues/1310).
The GCP root adds `tier3-runner` and `tier3-supervisor` service accounts and the regional STANDARD bucket `flink-gcp-tier3-evidence`.
Run evidence under `runs/` becomes deletion-eligible after 30 days, with soft delete and versioning disabled.
Runtime identities can create and read that evidence but cannot overwrite or delete it.
Mutable coordination records live under `_control/` and are excluded from automatic expiry.
Authorized control writers, including the plan identity for the environment lock, can still update or delete those records.
Admission must check durable run records and live inventory even when the lock is absent.
The lifecycle implementation must explicitly remove completed control records.
These resources carry no workload admission or execution by themselves.

### Run and cleanup contract

A separately approved run fixes image digests, Flink/Operator/GKE versions, namespace and queue names, workload identities, Pod requests/limits, Spot policy, input rate, repetitions, object prefixes, operation limits and expiry.
Use a quota and an independent expiry supervisor, and record the effective Pod resources after Autopilot admission.
Record eviction, rescheduling, CPU throttling and resource mutations alongside Flink checkpoint, GC and task observations.
An interrupted steady-state cell cannot provide a completed performance comparison.

Scale the Operator up only for a session.
At the end, keep it running until FlinkDeployment deletion and owned workload cleanup complete, then scale it back to zero.
Verify absence of owned deployments, Pods, PVCs, load balancers, queues and object prefixes, and verify the Operator has zero Pods.
Scaling the Operator down alone does not stop an existing Flink job.
The ClusterIP-only workload design requires no external load balancer.
Durable measurement evidence must be exported before cleanup; the temporary bucket's one-day lifecycle and disabled soft delete do not preserve published results.
Lifecycle expiry is an object fallback, not a workload or queue cleanup mechanism.

## Bootstrap (already done — recorded for reproducibility)

The backend bucket is managed by this configuration, which is circular on a
clean project. The order that resolves it, runnable by an owner with ADC and
no service account at all. Run these commands from the repository root:

1. Override the backend locally (uncommitted; `.gitignore` covers it):

   ```console
   $ cat > opentofu/flink-gcp/backend_override.tf <<'EOF'
   terraform {
     backend "local" {}
   }
   EOF
   $ mise x opentofu -- tofu -chdir=opentofu/flink-gcp init
   ```

2. `mise x opentofu -- tofu -chdir=opentofu/flink-gcp apply` — enables the APIs, creates the state bucket, the
   service accounts, WIF and all bindings, and adopts the pre-existing
   `flink-gcp` bucket and `flink_gcp_it` dataset via import blocks (removed
   from `it-resources.tf` once the state held them).

3. Move the state into the bucket that now exists:

   ```sh
   rm opentofu/flink-gcp/backend_override.tf
   mise x opentofu -- tofu -chdir=opentofu/flink-gcp init -migrate-state
   rm opentofu/flink-gcp/terraform.tfstate opentofu/flink-gcp/terraform.tfstate.backup
   ```

4. `mise x opentofu -- tofu -chdir=opentofu/flink-gcp plan` must report no changes. Commit `opentofu/flink-gcp/.terraform.lock.hcl`.

5. The first pull request's plan job is the live check that the read-only
   account's permissions suffice (impersonating it locally would need a
   `roles/iam.serviceAccountTokenCreator` grant, declined to keep personal
   identifiers out of this configuration). If a permission is missing, the
   plan job fails naming it; add it in `opentofu-sa.tf` and apply locally —
   pull-request CI can only plan, so fixing plan permissions never needs CI.

## Service agents (one-off, per service)

Enabling a service's API does **not** create its service agent
(`service-<project-number>@gcp-sa-<service>.iam.gserviceaccount.com`); agents
are provisioned lazily on first use, and granting a role to one that does not
exist yet is documented to fail. Before the first apply that grants to a new
service's agent, provision it once as the owner:

```sh
gcloud beta services identity create --service=<service>.googleapis.com --project=flink-gcp
```

Done for `pubsub.googleapis.com` on 2026-08-01 (the Pub/Sub agent performs
dead-letter forwarding,
[PR #170](https://github.com/flink-gcp/flink-connector-gcp/pull/170)).
The agent is permanent once created. For the record: the 403s that same PR's
applies actually hit were the apply workflow
authenticating as the read-only plan account (`TFACTION_IS_APPLY` was unset —
see the comment in `tofu-apply.yaml`), diagnosed only after the missing agent
had been blamed; check the authenticated principal in the workflow log before
theorising about the resource.

A state-changing operation after CI creates a pull request's saved plan leaves
that plan **stale**. A failed apply can bump the state serial, and an intentional
local apply can update the same state before the pull request merges. Do not
pre-apply a reviewed pull request locally; let the merge workflow apply its
saved plan. The recovery is a follow-up pull request whose fresh plan picks up
the current state; rerunning the old job can never succeed.
tfaction opens that follow-up pull request itself as a draft for the affected root module.
Its branch is `follow-up-<pr>-<target>-<timestamp>`, with `/` in the target replaced by `__`.
It is assigned to the merged pull request's author and to whoever merged it when those differ.
Review its plan alongside the apply error, complete it if the recovery needs more than the remainder, and merge it; a follow-up whose plan reports no change can simply be closed.
The generated commit touches `<root>/.tfaction/failed-prs` inside the affected root module, so the follow-up pull request selects that target and gets its own plan comment.

| Root module | Target in the branch name | Recovery record |
|---|---|---|
| `opentofu/flink-gcp` | `opentofu__flink-gcp` | `opentofu/flink-gcp/.tfaction/failed-prs` |
| `opentofu/tier3-bootstrap` | `opentofu__tier3-bootstrap` | `opentofu/tier3-bootstrap/.tfaction/failed-prs` |
| `opentofu/tier3-operator` | `opentofu__tier3-operator` | `opentofu/tier3-operator/.tfaction/failed-prs` |

The generated record can be removed when other changes in the recovery pull request still select the affected root.

The follow-up pull request was written by hand until
[#177](https://github.com/flink-gcp/flink-connector-gcp/issues/177) (ADR-0121),
and the reason is narrower than "tfaction could not do it": a `GITHUB_TOKEN`
granted `contents: write` could create the branch and the pull request, but a
push authenticated with it starts no workflow run, so the follow-up would arrive
with no plan on it — which is the whole point of opening one. A dispatch-triggered fresh-apply workflow was
built as an alternative on
[PR #176](https://github.com/flink-gcp/flink-connector-gcp/pull/176)
and withdrawn in the App's favour. If the credentials are ever absent the step
skips, and the hand-written recovery above is the fallback. The step is keyed on
the apply step itself failing rather than on the job failing, so a WIF or
state-lock flake before the apply does not open a follow-up for an apply that
never ran.
