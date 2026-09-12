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
  upstream = yamldecode(file("${path.module}/upstream.yaml"))
}

resource "helm_release" "operator" {
  name      = "flink-kubernetes-operator"
  namespace = "tier3-system"
  chart     = "${path.module}/.terraform/operator-chart.tgz"
  version   = local.upstream.version
  values    = [file("${path.module}/values.yaml")]

  skip_crds        = true
  create_namespace = false
  wait             = true
  timeout          = 300
  max_history      = 5

  lifecycle {
    precondition {
      condition     = filesha512("${path.module}/.terraform/operator-chart.tgz") == local.upstream.sha512
      error_message = "Prepare the checksum-pinned Operator chart with the Tier-3 helper before planning."
    }
  }
}
