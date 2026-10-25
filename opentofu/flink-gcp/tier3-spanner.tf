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

# The workload identity, state bucket and instance access of the Spanner
# recovery trials (#1360, #1872). Each run's lifecycle creates one instance
# named sp1360-<run-id> and its recovery database, and deletes it again.
resource "google_service_account" "tier3_spanner" {
  account_id   = "tier3-spanner"
  display_name = "Tier-3 Spanner recovery workload"
  description  = "Writes and reads one run's Spanner database and stores recovery state; no instance or database administration"
}

resource "google_storage_bucket" "tier3_spanner" {
  name                        = "${local.project_id}-tier3-spanner"
  location                    = "us-central1"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  versioning {
    enabled = false
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 1
    }
  }
}

resource "google_storage_bucket_iam_member" "tier3_spanner_workload" {
  bucket = google_storage_bucket.tier3_spanner.name
  role   = "roles/storage.objectUser"
  member = google_service_account.tier3_spanner.member
}

resource "google_service_account_iam_member" "tier3_spanner_gke" {
  service_account_id = google_service_account.tier3_spanner.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.project_id}.svc.id.goog[tier3-spanner/spanner]"

  depends_on = [google_container_cluster.tier3]
}

locals {
  # Explicit identities prevent additions to the shared lifecycle registry
  # from inheriting instance control or state access.
  tier3_spanner_lifecycle = {
    runner     = local.tier3_lifecycle_members.runner
    supervisor = local.tier3_lifecycle_members.supervisor
  }
  # Every run's instance, database and sessions sit under this name.
  tier3_spanner_instances = "projects/${local.project_id}/instances/sp1360-"
  # Every Spanner data call runs in a session, so each role holds
  # sessions.create, which the REST reference checks on the database. The
  # workload's pinned client creates a multiplexed session per database or
  # batch client and never deletes or gets it, so the workload holds neither
  # sessions.delete nor sessions.get, and the contract below keeps the runner
  # and the supervisor to the same. The reference checks the data calls'
  # permissions on the session's name, a resource type the IAM conditions
  # documentation does not list for Spanner. That the condition still applies
  # to them is not documented. An unmet premise fails the pilot trial's first
  # data call with PERMISSION_DENIED before any input runs, and the
  # preregistration (#1878) records which.
  #
  # The runner and the supervisor are not written yet (#1877), so their roles
  # are a contract their implementation must keep. Each uses one multiplexed
  # session, writes rows with Commit in a single-use read-write transaction,
  # which needs only databases.write, or with BatchWrite, and reads the oracles
  # with single-use queries, which need only databases.select, never through
  # the read API or a multi-use snapshot. Each waits for the instance and the
  # database by reading them back rather than polling their long-running
  # operations, as the Bigtable rig does, so no role holds instanceOperations
  # or databaseOperations permissions.
  tier3_spanner_roles = {
    # The production sink, Change Streams source and batch source. The sink's
    # INFORMATION_SCHEMA read of its tables' cell weights, the source's
    # INFORMATION_SCHEMA read of the change stream's metadata and the change
    # stream's read function are queries, which need databases.select. The
    # batch read plans its partitions with PartitionRead inside a read-only
    # transaction it begins, and reads each one with the read API, which needs
    # databases.read. The sink writes through BatchWrite, which needs
    # databases.write.
    workload = {
      role_id     = "tier3SpannerRecoveryWorkload"
      description = "Writes, reads and streams changes of Tier-3 recovery databases; no instance or database administration"
      member      = google_service_account.tier3_spanner.member
      permissions = ["spanner.databases.beginReadOnlyTransaction", "spanner.databases.partitionRead", "spanner.databases.read", "spanner.databases.select", "spanner.databases.write", "spanner.sessions.create"]
    }
    # Reads the instance the creator role below made, creates the recovery
    # database with its DDL, reads the database and its DDL back, adds split
    # points to the seed table and writes the seed rows before admission.
    # The REST reference requires only databases.create on the instance for a
    # CreateDatabase, whose extra DDL statements run atomically with the
    # creation, so the role holds no databases.updateDdl. That DDL includes the
    # ALTER DATABASE ... SET OPTIONS (version_retention_period = ...) statement
    # the application README requires. Should the service check it separately,
    # the runner's database create fails with PERMISSION_DENIED before any
    # input runs, and the preregistration (#1878) records which.
    runner = {
      role_id     = "tier3SpannerRecoveryRunner"
      description = "Creates and seeds Tier-3 recovery databases and reads their metadata; no table reads or deletion"
      member      = local.tier3_spanner_lifecycle.runner
      permissions = ["spanner.databases.addSplitPoints", "spanner.databases.create", "spanner.databases.get", "spanner.databases.getDdl", "spanner.databases.write", "spanner.instances.get", "spanner.sessions.create"]
    }
    # Reads the tables for the oracles, writes the late seed rows after the
    # batch read has planned its snapshot, deletes the instance and reads its
    # absence.
    supervisor = {
      role_id     = "tier3SpannerRecoverySupervisor"
      description = "Reads Tier-3 recovery tables, writes late seed rows and deletes the instance; no creation"
      member      = local.tier3_spanner_lifecycle.supervisor
      permissions = ["spanner.databases.select", "spanner.databases.write", "spanner.instances.delete", "spanner.instances.get", "spanner.sessions.create"]
    }
  }
}

resource "google_project_iam_custom_role" "tier3_spanner" {
  for_each    = local.tier3_spanner_roles
  project     = local.project_id
  role_id     = each.value.role_id
  title       = "Tier-3 Spanner recovery ${each.key}"
  description = each.value.description
  permissions = each.value.permissions
  stage       = "GA"

  depends_on = [google_project_iam_member.opentofu["roles/iam.roleAdmin"]]
}

# Spanner tests resource names in IAM conditions for instances and databases,
# so these bindings reach only the sp1360- instances: every run's, and any
# other instance whose ID starts with that prefix.
resource "google_project_iam_member" "tier3_spanner" {
  for_each = local.tier3_spanner_roles
  project  = local.project_id
  role     = google_project_iam_custom_role.tier3_spanner[each.key].name
  member   = each.value.member
  condition {
    title       = "spanner-recovery-instances-only"
    description = "Only Tier-3 Spanner recovery instances and what they contain"
    expression  = "resource.name.startsWith('${local.tier3_spanner_instances}')"
  }
}

# The REST reference checks instances.create on the parent, the project, and
# no instance condition matches the project's name, so this one role is
# unconditional. It lets the runner create an instance of any ID, configuration
# and capacity; only the runner's code bounds what it creates. The runner names
# every instance it creates sp1360-<run-id> and records the intent before the
# call; the supervisor deletes only recorded intents.
resource "google_project_iam_custom_role" "tier3_spanner_creator" {
  project     = local.project_id
  role_id     = "tier3SpannerRecoveryInstanceCreator"
  title       = "Tier-3 Spanner recovery instance creator"
  description = "Creates a Tier-3 recovery instance; nothing else"
  permissions = ["spanner.instances.create"]
  stage       = "GA"

  depends_on = [google_project_iam_member.opentofu["roles/iam.roleAdmin"]]
}

resource "google_project_iam_member" "tier3_spanner_creator" {
  project = local.project_id
  role    = google_project_iam_custom_role.tier3_spanner_creator.name
  member  = local.tier3_spanner_lifecycle.runner
}

# The pinned client enables its built-in client metrics unless
# SPANNER_DISABLE_BUILTIN_METRICS is set, and the connector does not disable
# them, so the clients export their own figures to the project's Cloud
# Monitoring, where the trial can read them outside the job JVM. The export is
# checked against the project, so this role is unconditional; without it every
# export is refused.
resource "google_project_iam_custom_role" "tier3_spanner_metrics" {
  project     = local.project_id
  role_id     = "tier3SpannerRecoveryClientMetrics"
  title       = "Tier-3 Spanner recovery client metrics"
  description = "Writes the Spanner client's built-in metrics; no reads or other metrics administration"
  permissions = ["monitoring.timeSeries.create"]
  stage       = "GA"

  depends_on = [google_project_iam_member.opentofu["roles/iam.roleAdmin"]]
}

resource "google_project_iam_member" "tier3_spanner_metrics" {
  project = local.project_id
  role    = google_project_iam_custom_role.tier3_spanner_metrics.name
  member  = google_service_account.tier3_spanner.member
}

# The shared cleanup lists a run's state prefixes and deletes what remains, as
# either actor, as it does for the other recovery state buckets.
resource "google_storage_bucket_iam_member" "tier3_spanner_state_reader" {
  for_each = local.tier3_spanner_lifecycle
  bucket   = google_storage_bucket.tier3_spanner.name
  role     = "roles/storage.objectViewer"
  member   = each.value
}

resource "google_storage_bucket_iam_member" "tier3_spanner_state_cleanup" {
  for_each = local.tier3_spanner_lifecycle
  bucket   = google_storage_bucket.tier3_spanner.name
  role     = "roles/storage.objectUser"
  member   = each.value
  condition {
    title       = "spanner-run-state-only"
    description = "Clean Flink state and its staged uploads under run prefixes"
    # Flink's GCS writer stages every object as .inprogress/<bucket>/<object>/
    # in the same bucket before composing it into place.
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.tier3_spanner.name}/objects/runs/') || resource.name.startsWith('projects/_/buckets/${google_storage_bucket.tier3_spanner.name}/objects/.inprogress/${google_storage_bucket.tier3_spanner.name}/runs/')"
  }
}
