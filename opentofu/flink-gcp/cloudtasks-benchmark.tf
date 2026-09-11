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

# Issue #1246 runs separately approved workloads on the Tier-3 GKE cluster.
resource "google_service_account" "cloudtasks_benchmark" {
  account_id   = "cloudtasks-benchmark"
  display_name = "Cloud Tasks benchmark workload"
  description  = "Creates and observes temporary benchmark queues; no VM management or IAM grants"
}

resource "google_project_iam_member" "cloudtasks_benchmark" {
  project = local.project_id
  role    = "roles/cloudtasks.editor"
  member  = google_service_account.cloudtasks_benchmark.member
}

# A separate bucket keeps the worker's checkpoint/artifact access away from
# both the shared integration-test data and the OpenTofu state.
resource "google_storage_bucket" "cloudtasks_benchmark" {
  name                        = "${local.project_id}-cloudtasks-benchmark"
  location                    = "us-central1"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # An explicit zero avoids retaining deleted experiment objects for the
  # service's default soft-delete interval after verified logical cleanup.
  soft_delete_policy {
    retention_duration_seconds = 0
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

resource "google_storage_bucket_iam_member" "cloudtasks_benchmark" {
  bucket = google_storage_bucket.cloudtasks_benchmark.name
  role   = "roles/storage.objectUser"
  member = google_service_account.cloudtasks_benchmark.member
}

resource "google_service_account_iam_member" "cloudtasks_benchmark_gke" {
  service_account_id = google_service_account.cloudtasks_benchmark.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.project_id}.svc.id.goog[tier3-cloudtasks/cloudtasks-benchmark]"

  depends_on = [google_container_cluster.tier3]
}
