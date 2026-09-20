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

resource "google_service_account" "tier3_pubsub" {
  account_id   = "tier3-pubsub"
  display_name = "Tier-3 Pub/Sub recovery workload"
  description  = "Stores Pub/Sub recovery checkpoints, savepoints and HA metadata"
}

resource "google_storage_bucket" "tier3_pubsub" {
  name                        = "${local.project_id}-tier3-pubsub"
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
      age = 1
    }
  }
}

resource "google_storage_bucket_iam_member" "tier3_pubsub_workload" {
  bucket = google_storage_bucket.tier3_pubsub.name
  role   = "roles/storage.objectUser"
  member = google_service_account.tier3_pubsub.member
}

resource "google_service_account_iam_member" "tier3_pubsub_gke" {
  service_account_id = google_service_account.tier3_pubsub.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.project_id}.svc.id.goog[tier3-pubsub/pubsub]"

  depends_on = [google_container_cluster.tier3]
}

# List permission is checked on the bucket, separately from object mutations.
resource "google_storage_bucket_iam_member" "tier3_pubsub_state_reader" {
  for_each = local.tier3_lifecycle_members
  bucket   = google_storage_bucket.tier3_pubsub.name
  role     = "roles/storage.objectViewer"
  member   = each.value
}

resource "google_storage_bucket_iam_member" "tier3_pubsub_state_cleanup" {
  for_each = local.tier3_lifecycle_members
  bucket   = google_storage_bucket.tier3_pubsub.name
  role     = "roles/storage.objectUser"
  member   = each.value
  condition {
    title       = "pubsub-run-state-only"
    description = "Clean state under Pub/Sub run prefixes without bucket administration"
    expression  = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.tier3_pubsub.name}/objects/runs/')"
  }
}
