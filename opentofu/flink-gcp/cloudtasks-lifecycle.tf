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
  # The supervisor can observe and stop an owned run, but cannot admit a queue.
  cloudtasks_cleanup_permissions = [
    "cloudtasks.queues.delete",
    "cloudtasks.queues.get",
    "cloudtasks.queues.pause",
    "cloudtasks.tasks.get",
    "cloudtasks.tasks.list",
  ]
  # Keep Cloud Tasks grants limited to these two identities if the shared
  # lifecycle registry later gains an identity with a different responsibility.
  cloudtasks_lifecycle_roles = {
    runner = {
      member      = local.tier3_lifecycle_members.runner
      role_id     = "tier3CloudTasksRunner"
      description = "Creates, pauses, observes and deletes benchmark queues; no task creation, dispatch or IAM changes"
      permissions = concat(local.cloudtasks_cleanup_permissions, ["cloudtasks.queues.create"])
    }
    supervisor = {
      member      = local.tier3_lifecycle_members.supervisor
      role_id     = "tier3CloudTasksSupervisor"
      description = "Pauses, observes and deletes benchmark queues; no queue admission, task creation, dispatch or IAM changes"
      permissions = local.cloudtasks_cleanup_permissions
    }
  }
}

resource "google_project_iam_custom_role" "cloudtasks_lifecycle" {
  for_each    = local.cloudtasks_lifecycle_roles
  project     = local.project_id
  role_id     = each.value.role_id
  title       = "Tier-3 Cloud Tasks ${each.key}"
  description = each.value.description
  permissions = each.value.permissions
  stage       = "GA"

  depends_on = [google_project_iam_member.opentofu["roles/iam.roleAdmin"]]
}

# Queue ownership and the ct1246- prefix must be checked by the reviewed
# lifecycle runtime; these project bindings do not enforce a name prefix.
resource "google_project_iam_member" "cloudtasks_lifecycle" {
  for_each = local.cloudtasks_lifecycle_roles
  project  = local.project_id
  role     = google_project_iam_custom_role.cloudtasks_lifecycle[each.key].name
  member   = each.value.member
}

resource "google_storage_bucket_iam_member" "cloudtasks_lifecycle_reader" {
  for_each = local.cloudtasks_lifecycle_roles
  bucket   = google_storage_bucket.cloudtasks_benchmark.name
  role     = "roles/storage.objectViewer"
  member   = each.value.member
}

resource "google_storage_bucket_iam_member" "cloudtasks_lifecycle_cleanup" {
  for_each = local.cloudtasks_lifecycle_roles
  bucket   = google_storage_bucket.cloudtasks_benchmark.name
  role     = "roles/storage.objectUser"
  member   = each.value.member
  condition {
    title       = "cloudtasks-run-state-only"
    description = "Clean checkpoint state under benchmark run prefixes without bucket administration"
    expression  = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.cloudtasks_benchmark.name}/objects/runs/')"
  }
}
