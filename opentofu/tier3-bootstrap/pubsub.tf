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

# The GCP root owns this workload's GSA, bucket grant and KSA impersonation trust.
resource "kubernetes_service_account_v1" "pubsub" {
  metadata {
    name      = "pubsub"
    namespace = kubernetes_namespace_v1.tier3["tier3-pubsub"].metadata[0].name
    labels    = local.labels
    annotations = {
      "iam.gke.io/gcp-service-account" = "tier3-pubsub@flink-gcp.iam.gserviceaccount.com"
    }
  }
  depends_on = [kubernetes_manifest.crd]
}

resource "kubernetes_role_v1" "pubsub" {
  metadata {
    name      = "tier3-pubsub-job"
    namespace = kubernetes_namespace_v1.tier3["tier3-pubsub"].metadata[0].name
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

resource "kubernetes_role_binding_v1" "pubsub" {
  lifecycle {
    prevent_destroy = true
  }
  metadata {
    name      = "tier3-pubsub-job"
    namespace = kubernetes_namespace_v1.tier3["tier3-pubsub"].metadata[0].name
    labels    = local.labels
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.pubsub.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account_v1.pubsub.metadata[0].name
    namespace = kubernetes_service_account_v1.pubsub.metadata[0].namespace
  }
}

# The runner alone creates the Pub/Sub admission's workload access probe: a
# Pod that runs as the `pubsub` service account above, so that the workload
# identity's own effective access is observed before the application exists.
# The runner could already reach that identity through a FlinkDeployment the
# Operator turns into Pods, so this adds no authority the runner lacked.
resource "kubernetes_role_v1" "pubsub_probe" {
  metadata {
    name      = "tier3-lifecycle-probe"
    namespace = kubernetes_namespace_v1.tier3["tier3-pubsub"].metadata[0].name
    labels    = local.labels
  }
  rule {
    api_groups = [""]
    resources  = ["pods"]
    verbs      = ["create"]
  }
  depends_on = [kubernetes_role_v1.installer]
}

resource "kubernetes_role_binding_v1" "pubsub_probe" {
  metadata {
    name      = "tier3-lifecycle-probe"
    namespace = kubernetes_namespace_v1.tier3["tier3-pubsub"].metadata[0].name
    labels    = local.labels
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.pubsub_probe.metadata[0].name
  }
  subject {
    api_group = "rbac.authorization.k8s.io"
    kind      = "User"
    name      = "tier3-runner@flink-gcp.iam.gserviceaccount.com"
    namespace = ""
  }
}
