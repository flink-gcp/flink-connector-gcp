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

locals {
  namespaces = toset(["tier3-system", "tier3-smoke"])
  labels = {
    "app.kubernetes.io/part-of"    = "flink-tier3"
    "app.kubernetes.io/managed-by" = "opentofu"
  }
}

resource "kubernetes_namespace_v1" "tier3" {
  for_each = local.namespaces
  metadata {
    name   = each.key
    labels = local.labels
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "kubernetes_resource_quota_v1" "idle" {
  for_each = local.namespaces
  metadata {
    name      = "tier3-idle"
    namespace = kubernetes_namespace_v1.tier3[each.key].metadata[0].name
    labels    = local.labels
  }
  spec {
    hard = {
      pods                   = "0"
      persistentvolumeclaims = "0"
    }
  }
  lifecycle {
    prevent_destroy = true
  }
}
