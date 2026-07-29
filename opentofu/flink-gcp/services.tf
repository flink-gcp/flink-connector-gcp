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

# Only the APIs this project's function needs are managed; the assortment a
# fresh project ships enabled (logging, monitoring, ...) is left alone. A new
# connector's E2E suite adds its API here in the pull request that first needs
# it (Bigtable and Spanner are the known candidates), not in advance.
resource "google_project_service" "this" {
  for_each = toset([
    # Workload Identity Federation and IAM management.
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "sts.googleapis.com",
    # Connector E2E targets.
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "cloudtasks.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com",
  ])

  service = each.value
  # Removing an entry from this list must not switch a service off under
  # resources that still use it; disabling stays a deliberate manual act.
  disable_on_destroy = false
}
