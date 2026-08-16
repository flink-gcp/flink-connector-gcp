# OpenTofu persistent layer

The GCP resources behind the real-GCP integration tests
([#5](https://github.com/laughingman7743/flink-connector-gcp/issues/5)): the
`flink-gcp` project's service accounts, Workload Identity Federation, buckets
and the BigQuery dataset. *Persistent* means idle-cost-free resources applied
once and kept; the fine-grained resources (tables, topics, subscriptions,
queues) are created and deleted by the tests themselves.

## Layout

| Path | Contents |
|---|---|
| `flink-gcp/` | The single root module — one GCP project, one state |
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
| `flink-gcp/pubsub-e2e-iam.tf` | Service-agent and E2E-account IAM the Pub/Sub source real-GCP suite needs beyond `roles/pubsub.editor` |
| `flink-gcp/tfaction.yaml` | Marks the directory as a tfaction root module |
| `flink-gcp/.terraform.lock.hcl` | Committed provider release pin |
| `/tfaction-root.yaml` | Global tfaction configuration (repository root) |

CI: a pull request touching `opentofu/**` gets a plan comment from
`.github/workflows/tofu-plan.yaml`, which runs as a job of `ci.yaml` — so the
run that carries the plan file is `ci.yaml`'s; the merge to `main` applies that
reviewed plan file and comments the result (`tofu-apply.yaml`). Locally, `just
tofu <args>` runs OpenTofu in the root module and `just lint` checks
formatting.

## tfaction configuration decisions

[tfaction](https://suzuki-shunsuke.github.io/tfaction/) v2 drives the CI;
`/tfaction-root.yaml` holds the configuration. What is on, what is off, and
why:

| Setting | State | Why |
|---|---|---|
| `terraform_command: tofu` | on | This is an OpenTofu repository |
| Plan/apply as separate WIF service accounts | on | A pull request's plan job never holds write credentials |
| Plan file via GitHub Artifacts | on (built in) | The apply runs exactly the plan the PR reviewed; no extra storage |
| `plan_workflow_name` | `ci.yaml` | It names the workflow whose *run* owns the plan artifact, not the file the plan steps live in: the plan runs as a `workflow_call` child, whose artifacts belong to the caller's run. `ci.yaml` must therefore stay `pull_request`-only — the lookup takes the newest run on the head branch with no event filter ([#444](https://github.com/laughingman7743/flink-connector-gcp/issues/444)) |
| `dismiss_approval_before_plan` | on (default) | A re-plan dismisses stale approvals, so an approval always refers to the plan that will apply |
| `hide-comment` job in the plan workflow | on | Outdated plan comments are hidden; the visible comment is the one that would apply |
| GitHub App | exists, unused here | The org-owned `flink-gcp-bot` was adopted for CI push-back once the repository went public ([#177](https://github.com/flink-gcp/flink-connector-gcp/issues/177); ADR-0121), and pins actions today. No *tfaction* feature consumes it yet: plan, apply, comments and labels stay on plain `GITHUB_TOKEN`, which suffices for them. The rows below are what it would unlock |
| `test` action (auto-`fmt` commits, tflint, trivy) | off | Auto-fix commits pushed with `GITHUB_TOKEN` do not retrigger CI, leaving stale checks; `fmt` is checked (not fixed) in `just lint`, and `validate` is subsumed by the plan this workflow always runs. tflint/trivy can ride in later with an App |
| `drift_detection` | off (default) | Wants three more workflows and apply-job changes; a candidate follow-up now that the weekly E2E workflow ([#28](https://github.com/laughingman7743/flink-connector-gcp/issues/28)) has landed |

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

Then `just tofu plan`, `just tofu validate`, `just tofu state list`, etc.

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

## Bootstrap (already done — recorded for reproducibility)

The backend bucket is managed by this configuration, which is circular on a
clean project. The order that resolves it, runnable by an owner with ADC and
no service account at all:

1. Override the backend locally (uncommitted; `.gitignore` covers it):

   ```console
   $ cat > flink-gcp/backend_override.tf <<'EOF'
   terraform {
     backend "local" {}
   }
   EOF
   $ just tofu init
   ```

2. `just tofu apply` — enables the APIs, creates the state bucket, the
   service accounts, WIF and all bindings, and adopts the pre-existing
   `flink-gcp` bucket and `flink_gcp_it` dataset via import blocks (removed
   from `it-resources.tf` once the state held them).

3. Move the state into the bucket that now exists:

   ```sh
   rm flink-gcp/backend_override.tf
   just tofu init -migrate-state
   rm flink-gcp/terraform.tfstate flink-gcp/terraform.tfstate.backup
   ```

4. `just tofu plan` must report no changes. Commit `.terraform.lock.hcl`.

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
[PR #170](https://github.com/laughingman7743/flink-connector-gcp/pull/170)).
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
the current state; rerunning the old job can never succeed. tfaction can create
that follow-up pull request automatically, but only with a GitHub App token —
adopting the App is planned together with the dedicated org at go-public time
([#177](https://github.com/laughingman7743/flink-connector-gcp/issues/177)),
and until then the follow-up pull request is written by hand. A
dispatch-triggered fresh-apply workflow was built as an alternative on
[PR #176](https://github.com/laughingman7743/flink-connector-gcp/pull/176)
and withdrawn in the App's favour.
