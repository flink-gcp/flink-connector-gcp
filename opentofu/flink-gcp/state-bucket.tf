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

# The bucket behind backend.tf. Managing the backend's own bucket is the one
# circularity in this configuration; ../README.md documents the bootstrap
# order that resolves it.
resource "google_storage_bucket" "opentofu_state" {
  name          = "flink-gcp-opentofu"
  location      = "us-central1"
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # State history: every write keeps the previous state as a noncurrent
  # version for 30 days, which is the recovery path from a corrupted or
  # mistakenly-written state.
  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      days_since_noncurrent_time = 30
    }
  }
}
