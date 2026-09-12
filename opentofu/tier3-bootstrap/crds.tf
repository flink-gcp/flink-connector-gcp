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
  crds = { for filename in fileset("${path.module}/crds", "*.yaml") :
    trimsuffix(filename, ".yaml") => yamldecode(file("${path.module}/crds/${filename}"))
  }
}

resource "kubernetes_manifest" "crd" {
  for_each = local.crds
  manifest = each.value
  # The API omits zero printer priorities from its response. Keep the chart
  # payload intact and accept the returned value only at those leaf fields.
  computed_fields = concat(
    ["metadata.labels", "metadata.annotations"],
    flatten([
      for version_index, version in each.value.spec.versions : [
        for column_index, column in try(version.additionalPrinterColumns, []) :
        "spec.versions[${version_index}].additionalPrinterColumns[${column_index}].priority"
        if try(column.priority == 0, false)
      ]
    ])
  )
  field_manager {
    name            = "tier3-bootstrap"
    force_conflicts = false
  }
  wait {
    condition {
      type   = "Established"
      status = "True"
    }
  }
  timeouts {
    create = "2m"
    update = "2m"
  }
  lifecycle {
    prevent_destroy = true
  }
}
