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

# The two resources the integration tests share, created by hand before this
# configuration existed and adopted with `tofu import` during the bootstrap
# (the import blocks were removed once the state held them).

# FILE_LOADS staging. Objects are transient, so anything a test leaves behind
# is swept after a day.
resource "google_storage_bucket" "it" {
  name          = "flink-gcp"
  location      = "us-central1"
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  # Raised from the hand-created bucket's "inherited": nothing here is public.
  public_access_prevention = "enforced"

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 1
    }
  }
}

# Tables created by integration tests expire after a day.
resource "google_bigquery_dataset" "it" {
  dataset_id                  = "flink_gcp_it"
  location                    = "us-central1"
  default_table_expiration_ms = 86400000
}
