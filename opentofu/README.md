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
  `push` / `schedule` / `workflow_dispatch` on `main`. Fork pull requests are
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
Routine E2E remains on MiniCluster; this rig is on demand and does not gate a release.

### Persistent foundation

The root creates the regional `flink-tier3` Autopilot cluster in `us-central1`, its private subnet, a dedicated node identity and the `flink-tier3` Artifact Registry Docker repository.
Nodes have private addresses, and operator access uses the IAM-authenticated DNS endpoint with IP endpoints disabled.
There is no Cloud NAT, bastion or IAP SSH firewall.
Mirror the pinned Operator and Flink images into `us-central1-docker.pkg.dev/flink-gcp/flink-tier3` before starting Pods; public registries are outside this network path.
The node identity has telemetry permissions and read access to that repository.
It has no connector data permissions.
Autopilot's required managed Prometheus collection stays enabled, with automatic workload monitoring disabled.
Optional application scraping and its ingestion cost belong to each run's CUE definition and budget.
System and workload logging also remain enabled; a run must bound application log volume and include its ingestion cost.

The separate `cloudtasks-benchmark` runtime account has project-wide `roles/cloudtasks.editor` and object read/write/delete access only to `flink-gcp-cloudtasks-benchmark`.
The Kubernetes identity `tier3-cloudtasks/cloudtasks-benchmark` may impersonate it through Workload Identity Federation for GKE; the workload ServiceAccount must carry the corresponding GCP account annotation.
Queue operations remain project-wide, so the run supervisor must enforce exact queue names.
Neither the node account nor the runtime account receives IAM administration permissions.
The apply identity gains `compute.networkAdmin`, `container.clusterAdmin`, `artifactregistry.admin` and permission to attach only the dedicated node account.

A merge applies this foundation and creates a cluster; it therefore requires resource and cost approval before merge.
The [GKE price list](https://cloud.google.com/kubernetes-engine/pricing) checked on 2026-09-11 charges $0.10 per cluster-hour.
The $74.40 monthly free-tier credit is shared by a billing account, and its availability for this project has not been verified.
Without that credit, a 744-hour month costs $74.40 in cluster management fees even with no workload Pods.
Image/object storage, application Pods and other metered services are additional.
No claim of zero idle cost follows from this configuration.
Cluster deletion protection prevents accidental removal; deliberate decommissioning requires a reviewed change disabling it before deletion.

### Operator installation follows the cluster

First verify the foundation's apply succeeded and the root has an empty plan.
Then install the Flink Kubernetes Operator through an OpenTofu-managed Helm release in a separate Kubernetes root.
Keeping that release out of the cluster-creation plan avoids asking its provider to connect to an endpoint that does not yet exist.
The Operator's tracked value is `replicas: 0`, with `webhook.create: false`; cert-manager is unnecessary.
The [bootstrap root](tier3-bootstrap/README.md) owns namespaces, CRDs, persistent workload ServiceAccounts/RBAC and idle quotas.
It adopts the initial access resources through import blocks and has separate state, PR plans and merge-triggered CI application.
The [CUE manifest module](../kubernetes/README.md) provides application definitions, static validation and delivery rendering.
The [ownership decision](../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md) separates these responsibilities.
Operator installation and application execution remain subsequent stages; the foundation alone does not install a runnable benchmark.

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
