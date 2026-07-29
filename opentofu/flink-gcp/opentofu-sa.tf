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

# The two accounts tfaction runs as: a read-only one for `tofu plan` on pull
# requests, and a managing one for `tofu apply` on pushes to main.

resource "google_service_account" "opentofu" {
  account_id   = "opentofu"
  display_name = "OpenTofu apply"
  description  = "Applies this configuration from the tofu-apply workflow on pushes to main"
}

resource "google_service_account" "opentofu_plan" {
  account_id   = "opentofu-plan"
  display_name = "OpenTofu plan"
  description  = "Read-only plan from the tofu-plan workflow on pull requests"
}

# Admin over exactly the resource kinds this configuration declares, not
# roles/owner or roles/editor. roles/resourcemanager.projectIamAdmin is the
# powerful edge — it can grant any role on the project — but managing the
# project-level bindings in this file requires it; the compensating control is
# that this account is reachable only from a push to main (binding below), so
# every change it applies was a reviewed pull request first.
resource "google_project_iam_member" "opentofu" {
  for_each = toset([
    "roles/bigquery.admin",
    "roles/iam.serviceAccountAdmin",
    "roles/iam.workloadIdentityPoolAdmin",
    "roles/resourcemanager.projectIamAdmin",
    "roles/serviceusage.serviceUsageAdmin",
    "roles/storage.admin",
  ])

  project = local.project_id
  role    = each.value
  member  = google_service_account.opentofu.member
}

# roles/viewer alone was measured insufficient on the first plan run: it
# lacks storage.buckets.getIamPolicy, which refreshing the bucket IAM members
# below needs. securityReviewer adds the *.getIamPolicy family and nothing
# writable, keeping the account read-only.
resource "google_project_iam_member" "opentofu_plan" {
  for_each = toset([
    "roles/iam.securityReviewer",
    "roles/viewer",
  ])

  project = local.project_id
  role    = each.value
  member  = google_service_account.opentofu_plan.member
}

# Plan takes the state lock, and a GCS lock is a written object — so the plan
# account is read-only on the project but read-write on the state bucket
# alone. The apply account needs no equivalent: roles/storage.admin covers it.
resource "google_storage_bucket_iam_member" "opentofu_plan_state" {
  bucket = google_storage_bucket.opentofu_state.name
  role   = "roles/storage.objectUser"
  member = google_service_account.opentofu_plan.member
}

# Any workflow of this repository may plan; only a push to main may apply.
resource "google_service_account_iam_member" "opentofu_plan_wif" {
  service_account_id = google_service_account.opentofu_plan.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "${local.wif_principal_set}/attribute.repository_id/${local.github_repository_id}"
}

resource "google_service_account_iam_member" "opentofu_wif" {
  service_account_id = google_service_account.opentofu.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "${local.wif_principal_set}/attribute.event_ref/${local.github_repository_id}:push:refs/heads/main"
}
