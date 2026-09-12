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

# The generic smoke identity has no GSA annotation or GCP data grants.
resource "kubernetes_service_account_v1" "smoke" {
  metadata {
    name      = "smoke"
    namespace = kubernetes_namespace_v1.tier3["tier3-smoke"].metadata[0].name
    labels    = local.labels
  }
  depends_on = [kubernetes_manifest.crd]
}

resource "kubernetes_role_v1" "smoke" {
  metadata {
    name      = "tier3-smoke-job"
    namespace = kubernetes_namespace_v1.tier3["tier3-smoke"].metadata[0].name
    labels    = local.labels
  }
  rule {
    api_groups = [""]
    resources  = ["pods", "configmaps"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }
  rule {
    api_groups = ["apps"]
    resources  = ["deployments", "deployments/finalizers"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }
}

resource "kubernetes_role_binding_v1" "smoke" {
  lifecycle {
    prevent_destroy = true
  }
  metadata {
    name      = "tier3-smoke-job"
    namespace = kubernetes_namespace_v1.tier3["tier3-smoke"].metadata[0].name
    labels    = local.labels
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.smoke.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account_v1.smoke.metadata[0].name
    namespace = kubernetes_service_account_v1.smoke.metadata[0].namespace
  }
}
