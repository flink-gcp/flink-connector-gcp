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

# Adopt the 16 prerequisites created during the initial administrator bootstrap.
import {
  for_each = local.namespaces
  to       = kubernetes_namespace_v1.tier3[each.key]
  id       = each.key
}
import {
  for_each = local.namespaces
  to       = kubernetes_resource_quota_v1.idle[each.key]
  id       = "${each.key}/tier3-idle"
}
import {
  for_each = local.installer_roles
  to       = kubernetes_role_v1.installer[each.key]
  id       = "${each.value.namespace}/${each.value.name}"
}
import {
  for_each = local.installer_roles
  to       = kubernetes_role_binding_v1.installer[each.key]
  id       = "${each.value.namespace}/${each.value.name}"
}
import {
  to = kubernetes_cluster_role_v1.reader
  id = "tier3-bootstrap-reader"
}
import {
  to = kubernetes_cluster_role_binding_v1.reader
  id = "tier3-bootstrap-reader"
}

import {
  to = kubernetes_cluster_role_v1.writer
  id = "tier3-bootstrap-writer"
}
import {
  to = kubernetes_cluster_role_binding_v1.writer
  id = "tier3-bootstrap-writer"
}
