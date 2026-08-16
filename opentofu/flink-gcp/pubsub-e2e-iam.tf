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

# IAM the Pub/Sub source real-GCP suite (#82) needs beyond the E2E account's
# roles/pubsub.editor.

data "google_project" "this" {
  project_id = local.project_id
}

# Dead-letter forwarding is performed by the Pub/Sub service agent, not by the
# caller that configured the policy: the agent republishes a dead message to
# the dead-letter topic and acknowledges it on the source subscription, so it
# needs publisher and subscriber itself. Granted at project level because the
# suite's topics and subscriptions are created per run under random names.
# The agent's address is constructed from the project number rather than
# provisioned via google_project_service_identity, which would pull in the
# google-beta provider — but the agent is NOT created by enabling the API, as
# an earlier version of this comment claimed: service agents are provisioned
# lazily on first use. The pubsub agent was provisioned as a one-off
# (gcloud beta services identity create --service=pubsub.googleapis.com, run
# as the owner, recorded in ../README.md) and is permanent. Note the 403s
# this file's first applies hit were NOT the missing agent — they were the
# apply workflow authenticating as the read-only plan account (fixed in
# tofu-apply.yaml alongside this comment); a missing-agent grant failure
# remains a documented GCP behaviour, but this repository never actually
# observed it.
resource "google_project_iam_member" "pubsub_service_agent" {
  for_each = toset([
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
  ])

  project = local.project_id
  role    = each.value
  member  = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# An identity that can call the Pub/Sub API but is authorized for nothing in
# it, so the suite can assert the operator-facing messages the source's
# subscription admin produces when credentials lack a permission (describe,
# create and seek each wrap PERMISSION_DENIED differently). Deliberately
# granted no Pub/Sub role.
resource "google_service_account" "e2e_no_pubsub" {
  account_id   = "e2e-no-pubsub"
  display_name = "Real-GCP E2E permission-denied probe"
  description  = "Deliberately holds no Pub/Sub role; tests impersonate it to assert permission-denied messages"
}

# No key and no WIF binding: the weekly workflow reaches it only by
# impersonation from the E2E account. No grant for a local human here — the
# repository owner self-grants ad hoc when a local run needs it, which keeps
# personal identifiers out of source (the same call as the opentofu-sa
# runbook's fix-forward path).
resource "google_service_account_iam_member" "e2e_no_pubsub_impersonator" {
  service_account_id = google_service_account.e2e_no_pubsub.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = google_service_account.github_actions_e2e.member
}
