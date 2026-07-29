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

provider "google" {
  project = local.project_id
}

locals {
  project_id = "flink-gcp"

  # Immutable numeric GitHub identifiers, pinned by the WIF provider condition
  # (wif.tf). Numbers rather than names, so renaming an account or repository
  # can never redirect the trust they anchor.
  github_repository_id       = "1305440656" # laughingman7743/flink-connector-gcp
  github_repository_owner_id = "3115686"    # laughingman7743
}
