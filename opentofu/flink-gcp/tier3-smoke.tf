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

resource "google_service_account" "tier3_smoke" {
  account_id   = "tier3-smoke"
  display_name = "Tier-3 generic smoke workload"
  description  = "Stores generic smoke checkpoints, savepoints and HA metadata"
}

resource "google_storage_bucket" "tier3_smoke" {
  name                        = "${local.project_id}-tier3-smoke"
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

# Object Viewer/Creator cannot delete obsolete checkpoints. Object Admin also
# grants object IAM and retention changes, which this workload does not need.
resource "google_storage_bucket_iam_member" "tier3_smoke" {
  bucket = google_storage_bucket.tier3_smoke.name
  role   = "roles/storage.objectUser"
  member = google_service_account.tier3_smoke.member
}

resource "google_service_account_iam_member" "tier3_smoke_gke" {
  service_account_id = google_service_account.tier3_smoke.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.project_id}.svc.id.goog[tier3-smoke/smoke]"

  depends_on = [google_container_cluster.tier3]
}
