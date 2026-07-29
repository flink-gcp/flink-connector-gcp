# Copyright 2026 laughingman7743
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

# The account the nightly real-GCP E2E workflow runs as. Scoped to the fixed
# set of services the connectors touch; the fine-grained resources (tables,
# topics, subscriptions, queues) are created and deleted by the tests
# themselves, which is why the Pub/Sub and Cloud Tasks grants are
# create-capable. A new connector's E2E suite adds its grant here in the pull
# request that first needs it.

resource "google_service_account" "github_actions_e2e" {
  account_id   = "github-actions-e2e"
  display_name = "Real-GCP E2E tests"
  description  = "Runs the real-GCP integration test workflows"
}

resource "google_project_iam_member" "e2e" {
  for_each = toset([
    # Load and query jobs; table data access is granted on the dataset below.
    "roles/bigquery.jobUser",
    # Tests create and delete their own queues and tasks.
    "roles/cloudtasks.admin",
    # Tests create and delete their own topics and subscriptions.
    "roles/pubsub.editor",
  ])

  project = local.project_id
  role    = each.value
  member  = google_service_account.github_actions_e2e.member
}

resource "google_bigquery_dataset_iam_member" "e2e" {
  dataset_id = google_bigquery_dataset.it.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.github_actions_e2e.member
}

resource "google_storage_bucket_iam_member" "e2e" {
  bucket = google_storage_bucket.it.name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.github_actions_e2e.member
}

# Reachable only from this repository's push, schedule and workflow_dispatch
# events on main — the triggers the nightly E2E workflow will carry — never
# from a pull request.
resource "google_service_account_iam_member" "e2e_wif" {
  for_each = toset(["push", "schedule", "workflow_dispatch"])

  service_account_id = google_service_account.github_actions_e2e.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "${local.wif_principal_set}/attribute.event_ref/${local.github_repository_id}:${each.value}:refs/heads/main"
}
