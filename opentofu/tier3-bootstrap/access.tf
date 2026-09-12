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
  read_verbs  = ["get", "list", "watch"]
  write_verbs = ["get", "list", "watch", "create", "update", "patch", "delete"]
  inventory_rules = [
    { api_groups = [""], resources = ["pods", "services", "serviceaccounts", "configmaps", "resourcequotas", "persistentvolumeclaims"], verbs = local.read_verbs },
    { api_groups = ["apps"], resources = ["deployments", "replicasets", "statefulsets"], verbs = local.read_verbs },
    { api_groups = ["batch"], resources = ["jobs", "cronjobs"], verbs = local.read_verbs },
    { api_groups = ["rbac.authorization.k8s.io"], resources = ["roles", "rolebindings"], verbs = local.read_verbs },
    { api_groups = ["flink.apache.org"], resources = ["flinkdeployments", "flinksessionjobs", "flinkstatesnapshots", "flinkbluegreendeployments"], verbs = local.read_verbs }
  ]
  # Held before creating or binding the checksum-pinned chart Operator Roles.
  operator_rules = [
    { api_groups = [""], resources = ["pods", "services", "events", "configmaps", "secrets"], verbs = ["get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"] },
    { api_groups = ["apps"], resources = ["deployments", "deployments/finalizers", "replicasets"], verbs = local.write_verbs },
    { api_groups = ["apps"], resources = ["deployments/scale"], verbs = ["get", "update", "patch"] },
    { api_groups = ["extensions"], resources = ["deployments", "ingresses"], verbs = local.write_verbs },
    { api_groups = ["flink.apache.org"], resources = ["flinkbluegreendeployments", "flinkbluegreendeployments/finalizers", "flinkdeployments", "flinkdeployments/finalizers", "flinksessionjobs", "flinksessionjobs/finalizers", "flinkstatesnapshots", "flinkstatesnapshots/finalizers"], verbs = local.write_verbs },
    { api_groups = ["flink.apache.org"], resources = ["flinkbluegreendeployments/status", "flinkdeployments/status", "flinksessionjobs/status", "flinkstatesnapshots/status"], verbs = ["get", "update", "patch"] },
    { api_groups = ["networking.k8s.io"], resources = ["ingresses"], verbs = local.write_verbs },
    { api_groups = ["coordination.k8s.io"], resources = ["leases"], verbs = local.write_verbs }
  ]
  bootstrap_rules = [
    { api_groups = [""], resources = ["serviceaccounts", "resourcequotas"], verbs = local.write_verbs }
  ]
  role_writer = { api_groups = ["rbac.authorization.k8s.io"], resources = ["roles", "rolebindings"], verbs = local.write_verbs }
  installer_roles = {
    "tier3-smoke-plan" = {
      namespace = "tier3-smoke"
      name      = "tier3-helm-plan"
      principal = "opentofu-plan@flink-gcp.iam.gserviceaccount.com"
      rules     = local.inventory_rules
    }
    "tier3-smoke-apply" = {
      namespace = "tier3-smoke"
      name      = "tier3-helm-apply"
      principal = "opentofu@flink-gcp.iam.gserviceaccount.com"
      rules     = concat(local.inventory_rules, local.operator_rules, local.bootstrap_rules, [local.role_writer])
    }
    "tier3-system-plan" = {
      namespace = "tier3-system"
      name      = "tier3-helm-plan"
      principal = "opentofu-plan@flink-gcp.iam.gserviceaccount.com"
      rules = concat(local.inventory_rules, [
        { api_groups = [""], resources = ["secrets"], verbs = local.read_verbs }
      ])
    }
    "tier3-system-apply" = {
      namespace = "tier3-system"
      name      = "tier3-helm-apply"
      principal = "opentofu@flink-gcp.iam.gserviceaccount.com"
      rules = concat(local.inventory_rules, [
        { api_groups = ["rbac.authorization.k8s.io"], resources = ["roles", "rolebindings"], verbs = local.write_verbs },
        { api_groups = [""], resources = ["secrets", "configmaps", "serviceaccounts", "resourcequotas"], verbs = local.write_verbs },
        { api_groups = ["apps"], resources = ["deployments"], verbs = local.write_verbs },
        { api_groups = ["coordination.k8s.io"], resources = ["leases"], verbs = local.write_verbs }
      ])
    }
  }
}

resource "kubernetes_role_v1" "installer" {
  for_each = local.installer_roles
  metadata {
    name      = each.value.name
    namespace = kubernetes_namespace_v1.tier3[each.value.namespace].metadata[0].name
    labels    = local.labels
  }
  dynamic "rule" {
    for_each = each.value.rules
    content {
      api_groups = rule.value.api_groups
      resources  = rule.value.resources
      verbs      = rule.value.verbs
    }
  }
  depends_on = [kubernetes_resource_quota_v1.idle]
}

resource "kubernetes_role_binding_v1" "installer" {
  lifecycle {
    prevent_destroy = true
  }
  for_each = local.installer_roles
  metadata {
    name      = each.value.name
    namespace = each.value.namespace
    labels    = local.labels
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.installer[each.key].metadata[0].name
  }
  subject {
    api_group = "rbac.authorization.k8s.io"
    kind      = "User"
    name      = each.value.principal
    namespace = ""
  }
}

resource "kubernetes_cluster_role_v1" "reader" {
  # Kubernetes provider 3.2.1 lists CRDs while resolving manifest schemas,
  # including when the managed object is itself a CRD.
  rule {
    api_groups = ["apiextensions.k8s.io"]
    resources  = ["customresourcedefinitions"]
    verbs      = ["list"]
  }
  metadata {
    name   = "tier3-bootstrap-reader"
    labels = local.labels
  }
  dynamic "rule" {
    for_each = local.cluster_read_rules
    content {
      api_groups     = rule.value.api_groups
      resources      = rule.value.resources
      resource_names = rule.value.resource_names
      verbs          = ["get"]
    }
  }
}

resource "kubernetes_cluster_role_binding_v1" "reader" {
  lifecycle {
    prevent_destroy = true
  }
  metadata {
    name   = "tier3-bootstrap-reader"
    labels = local.labels
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role_v1.reader.metadata[0].name
  }
  dynamic "subject" {
    for_each = ["opentofu-plan@flink-gcp.iam.gserviceaccount.com", "opentofu@flink-gcp.iam.gserviceaccount.com"]
    content {
      api_group = "rbac.authorization.k8s.io"
      kind      = "User"
      name      = subject.value
      namespace = ""
    }
  }
  depends_on = [kubernetes_resource_quota_v1.idle]
}


locals {
  cluster_read_rules = [
    { api_groups = [""], resources = ["namespaces"], resource_names = sort(tolist(local.namespaces)) },
    { api_groups = ["apiextensions.k8s.io"], resources = ["customresourcedefinitions"], resource_names = keys(local.crds) },
    { api_groups = ["rbac.authorization.k8s.io"], resources = ["clusterroles", "clusterrolebindings"], resource_names = ["tier3-bootstrap-reader", "tier3-bootstrap-writer"] }
  ]
}

# Kubernetes cannot restrict create by resourceNames. Existing-object mutations
# are limited to this foundation's names; RBAC escalation checks remain enabled.
resource "kubernetes_cluster_role_v1" "writer" {
  metadata {
    name   = "tier3-bootstrap-writer"
    labels = local.labels
  }
  dynamic "rule" {
    for_each = local.cluster_read_rules
    content {
      api_groups     = rule.value.api_groups
      resources      = rule.value.resources
      resource_names = rule.value.resource_names
      verbs          = ["get", "update", "patch"]
    }
  }
  dynamic "rule" {
    for_each = local.cluster_read_rules
    content {
      api_groups = rule.value.api_groups
      resources  = rule.value.resources
      verbs      = ["create"]
    }
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "kubernetes_cluster_role_binding_v1" "writer" {
  metadata {
    name   = "tier3-bootstrap-writer"
    labels = local.labels
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role_v1.writer.metadata[0].name
  }
  subject {
    api_group = "rbac.authorization.k8s.io"
    kind      = "User"
    name      = "opentofu@flink-gcp.iam.gserviceaccount.com"
    namespace = ""
  }
  lifecycle {
    prevent_destroy = true
  }
}
