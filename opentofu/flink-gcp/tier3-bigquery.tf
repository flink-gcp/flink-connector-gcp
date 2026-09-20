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

# Persistent containers for separately approved Storage Write trials (#1312).
# The lifecycle runner owns temporary tables; OpenTofu owns the empty dataset.
resource "google_bigquery_dataset" "tier3_bigquery" {
  dataset_id                  = "flink_gcp_tier3_bigquery"
  location                    = "us-central1"
  description                 = "Temporary tables for separately approved Tier-3 BigQuery trials"
  default_table_expiration_ms = 86400000
  delete_contents_on_destroy  = false

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_service_account" "tier3_bigquery" {
  account_id   = "tier3-bigquery"
  display_name = "Tier-3 BigQuery workload"
  description  = "Writes to pre-created Tier-3 tables and stores Flink recovery state"
}

resource "google_service_account_iam_member" "tier3_bigquery_gke" {
  service_account_id = google_service_account.tier3_bigquery.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.project_id}.svc.id.goog[tier3-bigquery/bigquery]"

  depends_on = [google_container_cluster.tier3]
}

resource "google_storage_bucket" "tier3_bigquery" {
  name                        = "${local.project_id}-tier3-bigquery"
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

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age            = 1
      matches_prefix = ["runs/"]
    }
  }
}

locals {
  # Explicit identities prevent additions to the shared lifecycle registry
  # from inheriting access to BigQuery data or state.
  tier3_bigquery_lifecycle = {
    runner     = local.tier3_lifecycle_members.runner
    supervisor = local.tier3_lifecycle_members.supervisor
  }
  tier3_bigquery_observer_permissions = [
    "bigquery.datasets.get",
    "bigquery.tables.delete",
    "bigquery.tables.get",
    "bigquery.tables.getData",
    "bigquery.tables.list",
  ]
  tier3_bigquery_roles = {
    writer = {
      role_id     = "tier3BigQueryWriter"
      description = "Reads table metadata and appends rows to pre-created Tier-3 tables"
      member      = google_service_account.tier3_bigquery.member
      permissions = ["bigquery.tables.get", "bigquery.tables.updateData"]
    }
    runner = {
      role_id     = "tier3BigQueryRunner"
      description = "Creates, reads and deletes Tier-3 tables for admission and recovery"
      member      = local.tier3_bigquery_lifecycle.runner
      permissions = concat(local.tier3_bigquery_observer_permissions, ["bigquery.tables.create"])
    }
    supervisor = {
      role_id     = "tier3BigQuerySupervisor"
      description = "Reads and deletes Tier-3 tables without creating tables or appending rows"
      member      = local.tier3_bigquery_lifecycle.supervisor
      permissions = local.tier3_bigquery_observer_permissions
    }
  }
}

resource "google_project_iam_custom_role" "tier3_bigquery" {
  for_each    = local.tier3_bigquery_roles
  project     = local.project_id
  role_id     = each.value.role_id
  title       = "Tier-3 BigQuery ${each.key}"
  description = each.value.description
  permissions = each.value.permissions
  stage       = "GA"

  depends_on = [google_project_iam_member.opentofu["roles/iam.roleAdmin"]]
}

# Creating a project custom role does not grant it project-wide. These three
# bindings constrain data access to the dedicated dataset, across all its tables.
resource "google_bigquery_dataset_iam_member" "tier3_bigquery" {
  for_each   = local.tier3_bigquery_roles
  project    = local.project_id
  dataset_id = google_bigquery_dataset.tier3_bigquery.dataset_id
  role       = google_project_iam_custom_role.tier3_bigquery[each.key].name
  member     = each.value.member
}

# Query jobs are project resources. Both actors can finish cleanup after the
# other's failure; runtime ownership checks must constrain get/cancel to the
# exact query IDs recorded for the approved run.
resource "google_project_iam_custom_role" "tier3_bigquery_jobs" {
  project     = local.project_id
  role_id     = "tier3BigQueryJobs"
  title       = "Tier-3 BigQuery query lifecycle"
  description = "Creates, inspects and cancels query jobs for Tier-3 data validation"
  permissions = ["bigquery.jobs.create", "bigquery.jobs.get", "bigquery.jobs.update"]
  stage       = "GA"

  depends_on = [google_project_iam_member.opentofu["roles/iam.roleAdmin"]]
}

resource "google_project_iam_member" "tier3_bigquery_jobs" {
  for_each = local.tier3_bigquery_lifecycle
  project  = local.project_id
  role     = google_project_iam_custom_role.tier3_bigquery_jobs.name
  member   = each.value
}

resource "google_storage_bucket_iam_member" "tier3_bigquery_reader" {
  for_each = merge(local.tier3_bigquery_lifecycle, {
    writer = google_service_account.tier3_bigquery.member
  })
  bucket = google_storage_bucket.tier3_bigquery.name
  role   = "roles/storage.objectViewer"
  member = each.value
}

resource "google_storage_bucket_iam_member" "tier3_bigquery_state" {
  for_each = merge(local.tier3_bigquery_lifecycle, {
    writer = google_service_account.tier3_bigquery.member
  })
  bucket = google_storage_bucket.tier3_bigquery.name
  role   = "roles/storage.objectUser"
  member = each.value
  condition {
    title       = "bigquery-run-state-only"
    description = "Manage Flink state under run prefixes without bucket administration"
    expression  = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.tier3_bigquery.name}/objects/runs/')"
  }
}
