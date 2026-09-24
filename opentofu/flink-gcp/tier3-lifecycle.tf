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

resource "google_storage_bucket" "tier3_evidence" {
  name                        = "${local.project_id}-tier3-evidence"
  location                    = "us-central1"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  versioning {
    enabled = false
  }

  # Expiry applies only to run evidence. Authorized control writers can still
  # delete the lock; admission must also check run records and live resources.
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age            = 30
      matches_prefix = ["runs/"]
    }
  }
}

resource "google_service_account" "tier3_runner" {
  account_id   = "tier3-runner"
  display_name = "Tier-3 lifecycle runner"
  description  = "Starts and recovers approved Tier-3 runs from dedicated main workflows"
}

resource "google_service_account" "tier3_supervisor" {
  account_id   = "tier3-supervisor"
  display_name = "Tier-3 lifecycle supervisor"
  description  = "Exports run evidence and cleans temporary smoke state from the supervisor Pod"
}

resource "google_project_iam_member" "tier3_runner_cluster" {
  project = local.project_id
  role    = "roles/container.clusterViewer"
  member  = google_service_account.tier3_runner.member
}

resource "google_artifact_registry_repository_iam_member" "tier3_runner" {
  project    = local.project_id
  location   = google_artifact_registry_repository.tier3.location
  repository = google_artifact_registry_repository.tier3.repository_id
  role       = "roles/artifactregistry.reader"
  member     = google_service_account.tier3_runner.member
}

locals {
  tier3_lifecycle_members = {
    runner     = google_service_account.tier3_runner.member
    supervisor = google_service_account.tier3_supervisor.member
  }
  tier3_runner_workflows = toset([
    "workflow_dispatch:refs/heads/main:flink-gcp/flink-connector-gcp/.github/workflows/tier3-run.yaml@refs/heads/main",
    "workflow_dispatch:refs/heads/main:flink-gcp/flink-connector-gcp/.github/workflows/tier3-recover.yaml@refs/heads/main",
    "workflow_run:refs/heads/main:flink-gcp/flink-connector-gcp/.github/workflows/tier3-recover.yaml@refs/heads/main",
  ])
}

# Recovery verifies the originating workflow and saved run approval before
# making changes; this trust only identifies the recovery workflow itself.
resource "google_service_account_iam_member" "tier3_runner_wif" {
  for_each           = local.tier3_runner_workflows
  service_account_id = google_service_account.tier3_runner.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "${local.wif_principal_set}/attribute.lifecycle_runner/${local.github_repository_id}:${each.value}"
}

resource "google_service_account_iam_member" "tier3_supervisor_gke" {
  service_account_id = google_service_account.tier3_supervisor.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.project_id}.svc.id.goog[tier3-system/tier3-supervisor]"

  depends_on = [google_container_cluster.tier3]
}

# Receipts are immutable to runtime identities: use a new object for each
# observation and leave expiry to the bucket lifecycle policy.
resource "google_storage_bucket_iam_member" "tier3_evidence_reader" {
  for_each = local.tier3_lifecycle_members
  bucket   = google_storage_bucket.tier3_evidence.name
  role     = "roles/storage.objectViewer"
  member   = each.value
}

resource "google_storage_bucket_iam_member" "tier3_evidence_writer" {
  for_each = local.tier3_lifecycle_members
  bucket   = google_storage_bucket.tier3_evidence.name
  role     = "roles/storage.objectCreator"
  member   = each.value
  condition {
    title       = "run-evidence-only"
    description = "Create immutable run evidence without overwriting or deleting it"
    expression  = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.tier3_evidence.name}/objects/runs/')"
  }
}

resource "google_storage_bucket_iam_member" "tier3_control" {
  for_each = local.tier3_lifecycle_members
  bucket   = google_storage_bucket.tier3_evidence.name
  role     = "roles/storage.objectUser"
  member   = each.value
  condition {
    title       = "lifecycle-control-only"
    description = "Update the environment lock and run control records"
    expression  = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.tier3_evidence.name}/objects/_control/')"
  }
}

# Like the existing state lock, coordination requires a written object.
# Planning receives no mutation rights over run evidence or Kubernetes.
resource "google_storage_bucket_iam_member" "tier3_plan_lock" {
  bucket = google_storage_bucket.tier3_evidence.name
  role   = "roles/storage.objectUser"
  member = google_service_account.opentofu_plan.member
  condition {
    title       = "environment-lock-only"
    description = "Coordinate infrastructure plans with lifecycle execution"
    expression  = "resource.name == 'projects/_/buckets/${google_storage_bucket.tier3_evidence.name}/objects/_control/environment.json'"
  }
}

resource "google_storage_bucket_iam_member" "tier3_state_reader" {
  for_each = local.tier3_lifecycle_members
  bucket   = google_storage_bucket.tier3_smoke.name
  role     = "roles/storage.objectViewer"
  member   = each.value
}

resource "google_storage_bucket_iam_member" "tier3_state_cleanup" {
  for_each = local.tier3_lifecycle_members
  bucket   = google_storage_bucket.tier3_smoke.name
  role     = "roles/storage.objectUser"
  member   = each.value
  condition {
    title       = "smoke-run-state-only"
    description = "Clean state under smoke run prefixes without bucket administration"
    expression  = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.tier3_smoke.name}/objects/runs/')"
  }
}
