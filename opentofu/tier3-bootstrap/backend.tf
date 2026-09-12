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

terraform {
  backend "gcs" {
    bucket = "flink-gcp-opentofu"
    prefix = "tier3-bootstrap"
  }
}

# KUBE_CONFIG_PATH is supplied at execution time; no token or runner path is saved.
provider "kubernetes" {
  config_context = "gke_flink-gcp_us-central1_flink-tier3"
}
