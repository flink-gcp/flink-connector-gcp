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
| `flink-gcp/tfaction.yaml` | Marks the directory as a tfaction root module |
| `/tfaction-root.yaml` | Global tfaction configuration (repository root) |

CI: a pull request touching `opentofu/**` gets a plan comment from
`.github/workflows/tofu-plan.yaml`; the merge to `main` applies the reviewed
plan file and comments the result (`tofu-apply.yaml`). Locally, `just tofu
<args>` runs OpenTofu in the root module and `just lint` checks formatting.

## Security model

- **No service account keys, ever.** Every workflow credential is a
  short-lived token minted through WIF; local runs use your own ADC.
- The WIF provider condition pins the immutable repository and owner IDs, so
  a renamed or look-alike repository can never authenticate.
- Per-account bindings narrow further: the plan account is reachable from any
  event of this repository (and is read-only plus state-lock writes); the
  apply and E2E accounts only from `push` / `schedule` / `workflow_dispatch`
  on `main`. Fork pull requests are excluded outright — GitHub does not grant
  `id-token: write` to runs triggered from forks.

## Local use

Two variables in the repository-root `.env` (uncommitted, loaded by mise)
point everything at the dedicated gcloud configuration — two because they
have different readers. `CLOUDSDK_CONFIG` steers the `gcloud` CLI, but the
google provider's Go auth library does not read it (measured: with only
`CLOUDSDK_CONFIG` set, tofu silently fell back to the default
`~/.config/gcloud` ADC of a different account); the provider needs
`GOOGLE_APPLICATION_CREDENTIALS` naming the credentials file directly:

```
CLOUDSDK_CONFIG=/Users/<you>/.config/flink-gcp
GOOGLE_APPLICATION_CREDENTIALS=/Users/<you>/.config/flink-gcp/application_default_credentials.json
```

Then `just tofu plan`, `just tofu validate`, `just tofu state list`, etc.

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

   ```console
   $ rm flink-gcp/backend_override.tf
   $ just tofu init -migrate-state
   $ rm flink-gcp/terraform.tfstate flink-gcp/terraform.tfstate.backup
   ```

4. `just tofu plan` must report no changes. Commit `.terraform.lock.hcl`.

5. The first pull request's plan job is the live check that the read-only
   account's permissions suffice (impersonating it locally would need a
   `roles/iam.serviceAccountTokenCreator` grant, declined to keep personal
   identifiers out of this configuration). If a permission is missing, the
   plan job fails naming it; add it in `opentofu-sa.tf` and apply locally —
   pull-request CI can only plan, so fixing plan permissions never needs CI.
