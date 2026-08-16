# Copyright 2026 The flink-gcp authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Keyless authentication for GitHub Actions. No service account in this
# project has (or may ever have) an exported key; every workflow credential is
# a short-lived token minted through this pool, and the provider-level
# condition below rejects any workflow that is not this repository's.
#
# Fork safety rests on three layers: GitHub does not grant `id-token: write`
# to workflow runs triggered by pull requests from forks; the condition here
# pins the immutable repository and owner IDs; and the per-service-account
# bindings (opentofu-sa.tf, e2e-sa.tf) additionally restrict the event and
# ref where more than read-only-plus-state-lock power is at stake.
resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github"
  display_name              = "GitHub Actions"
  description               = "Workflows of flink-gcp/flink-connector-gcp"
}

resource "google_iam_workload_identity_pool_provider" "github_actions" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-actions"
  display_name                       = "GitHub Actions OIDC"

  attribute_mapping = {
    "google.subject"          = "assertion.sub"
    "attribute.repository_id" = "assertion.repository_id"
    # A principalSet member can match only one attribute value, so the
    # event/ref scoping the apply and E2E accounts need is a single
    # concatenated attribute rather than three conditions.
    "attribute.event_ref" = "assertion.repository_id + \":\" + assertion.event_name + \":\" + assertion.ref"
  }

  attribute_condition = "assertion.repository_id == \"${local.github_repository_id}\" && assertion.repository_owner_id == \"${local.github_repository_owner_id}\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

locals {
  # principalSet://iam.googleapis.com/projects/<number>/locations/global/
  # workloadIdentityPools/github — the prefix every WIF binding shares.
  wif_principal_set = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}"
}
