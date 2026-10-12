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

# Queue access for the Cloud Tasks recovery trial (#1362). The runner creates
# the run's queue and pauses it before admitting the job; the workload creates
# tasks in it; the supervisor reads the queue, lists its tasks for the oracle
# and deletes it at cleanup.
locals {
  # Explicit identities prevent additions to the shared lifecycle registry
  # from inheriting queue control or state access.
  tier3_cloudtasks_lifecycle = {
    runner     = local.tier3_lifecycle_members.runner
    supervisor = local.tier3_lifecycle_members.supervisor
  }
  # Both read the queue back: the runner to admit only a paused queue with the
  # retention the staged sink requires, the supervisor to check it is still
  # paused and, after deletion, that it is gone. tasks.list returns task names
  # in the basic view, which is all the oracle compares; no fullView.
  tier3_cloudtasks_roles = {
    # The staged sink creates tasks, and its committer reads the queue's
    # tombstoneTtl back before the first create. roles/cloudtasks.enqueuer
    # lacks that read, and every predefined role holding both adds what the
    # paused-queue trial must not have: roles/cloudtasks.editor also resumes,
    # purges and deletes queues and runs tasks, and tasks.run dispatches a task
    # even from a paused queue.
    workload = {
      role_id     = "tier3CloudTasksRecoveryWorkload"
      description = "Creates tasks in a queue and reads the queue; no queue control or dispatch"
      member      = google_service_account.cloudtasks_benchmark.member
      permissions = ["cloudtasks.queues.get", "cloudtasks.tasks.create"]
    }
    runner = {
      role_id     = "tier3CloudTasksRecoveryRunner"
      description = "Creates, pauses and reads Tier-3 recovery queues; no resume, deletion or task access"
      member      = local.tier3_cloudtasks_lifecycle.runner
      permissions = ["cloudtasks.queues.create", "cloudtasks.queues.get", "cloudtasks.queues.pause"]
    }
    supervisor = {
      role_id     = "tier3CloudTasksRecoverySupervisor"
      description = "Reads and deletes Tier-3 recovery queues and lists their tasks; no creation, resume or dispatch"
      member      = local.tier3_cloudtasks_lifecycle.supervisor
      permissions = ["cloudtasks.queues.delete", "cloudtasks.queues.get", "cloudtasks.tasks.list"]
    }
  }
}

resource "google_project_iam_custom_role" "tier3_cloudtasks" {
  for_each    = local.tier3_cloudtasks_roles
  project     = local.project_id
  role_id     = each.value.role_id
  title       = "Tier-3 Cloud Tasks recovery ${each.key}"
  description = each.value.description
  permissions = each.value.permissions
  stage       = "GA"

  depends_on = [google_project_iam_member.opentofu["roles/iam.roleAdmin"]]
}

# Cloud Tasks takes no resource-name IAM condition, so these bindings are
# project-wide; the trial acts only on the queue its run's intent names.
resource "google_project_iam_member" "tier3_cloudtasks" {
  for_each = local.tier3_cloudtasks_roles
  project  = local.project_id
  role     = google_project_iam_custom_role.tier3_cloudtasks[each.key].name
  member   = each.value.member
}

# The shared cleanup lists a run's state prefixes and deletes what remains, as
# either actor, as it does for the BigQuery and Pub/Sub state buckets.
resource "google_storage_bucket_iam_member" "tier3_cloudtasks_reader" {
  for_each = local.tier3_cloudtasks_lifecycle
  bucket   = google_storage_bucket.cloudtasks_benchmark.name
  role     = "roles/storage.objectViewer"
  member   = each.value
}

resource "google_storage_bucket_iam_member" "tier3_cloudtasks_state" {
  for_each = local.tier3_cloudtasks_lifecycle
  bucket   = google_storage_bucket.cloudtasks_benchmark.name
  role     = "roles/storage.objectUser"
  member   = each.value
  condition {
    title       = "cloudtasks-run-state-only"
    description = "Clean Flink state and its staged uploads under run prefixes"
    # Flink's GCS writer stages every object as .inprogress/<bucket>/<object>/
    # in the same bucket before composing it into place.
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.cloudtasks_benchmark.name}/objects/runs/') || resource.name.startsWith('projects/_/buckets/${google_storage_bucket.cloudtasks_benchmark.name}/objects/.inprogress/${google_storage_bucket.cloudtasks_benchmark.name}/runs/')"
  }
}
