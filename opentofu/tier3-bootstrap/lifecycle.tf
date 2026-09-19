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

resource "kubernetes_service_account_v1" "supervisor" {
  metadata {
    name      = "tier3-supervisor"
    namespace = kubernetes_namespace_v1.tier3["tier3-system"].metadata[0].name
    labels    = local.labels
    annotations = {
      "iam.gke.io/gcp-service-account" = "tier3-supervisor@flink-gcp.iam.gserviceaccount.com"
    }
  }
}

locals {
  # These grants extend the installer's existing permission inventory. An
  # administrator must grant the additions before CI can delegate them.
  lifecycle_installer_application = [
    { api_groups = [""], resources = ["pods/log", "services/proxy"], verbs = ["get"] },
  ]
  lifecycle_installer_system = [
    { api_groups = ["batch"], resources = ["jobs"], verbs = local.write_verbs },
    { api_groups = [""], resources = ["pods"], verbs = ["delete"] },
    { api_groups = [""], resources = ["pods/log"], verbs = ["get"] },
    { api_groups = ["apps"], resources = ["deployments/scale"], verbs = ["get", "update", "patch"], resource_names = ["flink-kubernetes-operator"] },
  ]
  lifecycle_application_rules = concat(local.inventory_rules, [
    { api_groups = [""], resources = ["pods", "services", "configmaps"], verbs = ["delete"] },
    { api_groups = [""], resources = ["pods/log", "services/proxy"], verbs = ["get"] },
    { api_groups = [""], resources = ["events"], verbs = local.read_verbs },
    { api_groups = ["apps"], resources = ["deployments", "replicasets"], verbs = ["delete"] },
    { api_groups = ["flink.apache.org"], resources = ["flinkdeployments"], verbs = ["create", "update", "patch", "delete"] },
    { api_groups = ["flink.apache.org"], resources = ["flinkdeployments/finalizers"], verbs = ["update", "patch"] },
    { api_groups = [""], resources = ["resourcequotas"], verbs = ["update", "patch"], resource_names = ["tier3-idle"] },
  ])
  lifecycle_roles = {
    "tier3-smoke"      = local.lifecycle_application_rules
    "tier3-cloudtasks" = local.lifecycle_application_rules
    "tier3-bigquery"   = local.lifecycle_application_rules
    "tier3-system" = [
      { api_groups = [""], resources = ["pods", "services", "configmaps", "resourcequotas", "persistentvolumeclaims"], verbs = local.read_verbs },
      { api_groups = ["apps"], resources = ["deployments", "replicasets", "statefulsets"], verbs = local.read_verbs },
      { api_groups = ["batch"], resources = ["jobs", "cronjobs"], verbs = local.read_verbs },
      { api_groups = ["flink.apache.org"], resources = ["flinkdeployments", "flinksessionjobs", "flinkstatesnapshots", "flinkbluegreendeployments"], verbs = local.read_verbs },
      { api_groups = [""], resources = ["configmaps"], verbs = ["create", "update", "patch", "delete"] },
      { api_groups = ["batch"], resources = ["jobs"], verbs = local.write_verbs },
      { api_groups = [""], resources = ["pods"], verbs = ["delete"] },
      { api_groups = [""], resources = ["pods/log"], verbs = ["get"] },
      { api_groups = ["apps"], resources = ["deployments/scale"], verbs = ["get", "update", "patch"], resource_names = ["flink-kubernetes-operator"] },
      { api_groups = [""], resources = ["resourcequotas"], verbs = ["update", "patch"], resource_names = ["tier3-idle"] },
    ]
  }
}

resource "kubernetes_role_v1" "lifecycle" {
  for_each = local.lifecycle_roles
  metadata {
    name      = "tier3-lifecycle"
    namespace = kubernetes_namespace_v1.tier3[each.key].metadata[0].name
    labels    = local.labels
  }
  dynamic "rule" {
    for_each = each.value
    content {
      api_groups     = rule.value.api_groups
      resources      = rule.value.resources
      verbs          = rule.value.verbs
      resource_names = try(rule.value.resource_names, null)
    }
  }
  depends_on = [kubernetes_role_v1.installer]
}

resource "kubernetes_role_binding_v1" "lifecycle" {
  for_each = local.lifecycle_roles
  metadata {
    name      = "tier3-lifecycle"
    namespace = each.key
    labels    = local.labels
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.lifecycle[each.key].metadata[0].name
  }
  subject {
    api_group = "rbac.authorization.k8s.io"
    kind      = "User"
    name      = "tier3-runner@flink-gcp.iam.gserviceaccount.com"
    namespace = ""
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account_v1.supervisor.metadata[0].name
    namespace = kubernetes_service_account_v1.supervisor.metadata[0].namespace
  }
  lifecycle {
    prevent_destroy = true
  }
}
