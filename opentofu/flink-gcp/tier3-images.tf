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

resource "google_service_account" "tier3_image_publisher" {
  account_id   = "tier3-image-publisher"
  display_name = "Tier-3 image publisher"
  description  = "Publishes reviewed AMD64 images to the existing Tier-3 repository"
}

resource "google_artifact_registry_repository_iam_member" "tier3_image_publisher" {
  project    = local.project_id
  location   = google_artifact_registry_repository.tier3.location
  repository = google_artifact_registry_repository.tier3.repository_id
  role       = "roles/artifactregistry.writer"
  member     = google_service_account.tier3_image_publisher.member
}

# Match the workflow as well as the repository, event and branch. Other main
# dispatch workflows must not inherit publication access.
resource "google_service_account_iam_member" "tier3_image_publisher_wif" {
  service_account_id = google_service_account.tier3_image_publisher.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "${local.wif_principal_set}/attribute.image_publisher/${local.github_repository_id}:workflow_dispatch:refs/heads/main:flink-gcp/flink-connector-gcp/.github/workflows/tier3-images.yaml@refs/heads/main"
}
