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
  pubsub_cleanup_permissions = [
    "pubsub.subscriptions.delete",
    "pubsub.subscriptions.get",
    "pubsub.subscriptions.getIamPolicy",
    "pubsub.topics.delete",
    "pubsub.topics.get",
    "pubsub.topics.getIamPolicy",
  ]
  pubsub_lifecycle_roles = {
    runner = {
      member      = local.tier3_lifecycle_members.runner
      role_id     = "tier3PubSubRunner"
      description = "Creates, inspects, grants access to and deletes Pub/Sub resources throughout the project"
      permissions = concat(local.pubsub_cleanup_permissions, [
        "pubsub.subscriptions.create",
        "pubsub.subscriptions.setIamPolicy",
        "pubsub.topics.attachSubscription",
        "pubsub.topics.create",
        "pubsub.topics.setIamPolicy",
      ])
    }
    supervisor = {
      member      = local.tier3_lifecycle_members.supervisor
      role_id     = "tier3PubSubSupervisor"
      description = "Inspects and deletes Pub/Sub resources throughout the project; no creation or IAM writes"
      permissions = local.pubsub_cleanup_permissions
    }
  }
}

resource "google_project_iam_custom_role" "pubsub_lifecycle" {
  for_each    = local.pubsub_lifecycle_roles
  project     = local.project_id
  role_id     = each.value.role_id
  title       = "Tier-3 Pub/Sub ${each.key}"
  description = each.value.description
  permissions = each.value.permissions
  stage       = "GA"

  depends_on = [google_project_iam_member.opentofu["roles/iam.roleAdmin"]]
}

# These project bindings enforce neither the t3- prefix nor run ownership.
# The trusted runner can change any topic/subscription policy, including granting
# itself data access. The reviewed lifecycle must restrict its operations.
resource "google_project_iam_member" "pubsub_lifecycle" {
  for_each = local.pubsub_lifecycle_roles
  project  = local.project_id
  role     = google_project_iam_custom_role.pubsub_lifecycle[each.key].name
  member   = each.value.member
}

# Defined here, but granted only on exact run-owned subscriptions by the runner.
# Keep role_id aligned with ResourcePlan.grants in flink_tier3/pubsub.py.
resource "google_project_iam_custom_role" "pubsub_consumer" {
  project     = local.project_id
  role_id     = "tier3PubSubConsumer"
  title       = "Tier-3 Pub/Sub subscription consumer"
  description = "Reads subscription settings and consumes messages"
  permissions = ["pubsub.subscriptions.consume", "pubsub.subscriptions.get"]
  stage       = "GA"

  depends_on = [google_project_iam_member.opentofu["roles/iam.roleAdmin"]]
}
