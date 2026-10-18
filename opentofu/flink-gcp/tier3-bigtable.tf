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

# The workload identity, state bucket and instance access of the Bigtable
# recovery trials (#1316, #1317). Each run's lifecycle creates one instance
# named bt1316-<run-id>, its tables and its app profile, and deletes it again.
resource "google_service_account" "tier3_bigtable" {
  account_id   = "tier3-bigtable"
  display_name = "Tier-3 Bigtable recovery workload"
  description  = "Writes and reads one run's Bigtable instance and stores recovery state; no instance or table administration"
}

resource "google_storage_bucket" "tier3_bigtable" {
  name                        = "${local.project_id}-tier3-bigtable"
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

resource "google_storage_bucket_iam_member" "tier3_bigtable_workload" {
  bucket = google_storage_bucket.tier3_bigtable.name
  role   = "roles/storage.objectUser"
  member = google_service_account.tier3_bigtable.member
}

resource "google_service_account_iam_member" "tier3_bigtable_gke" {
  service_account_id = google_service_account.tier3_bigtable.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.project_id}.svc.id.goog[tier3-bigtable/bigtable]"

  depends_on = [google_container_cluster.tier3]
}

locals {
  # Explicit identities prevent additions to the shared lifecycle registry
  # from inheriting instance control or state access.
  tier3_bigtable_lifecycle = {
    runner     = local.tier3_lifecycle_members.runner
    supervisor = local.tier3_lifecycle_members.supervisor
  }
  # Every run's instance, cluster, tables and app profile sit under this name.
  tier3_bigtable_instances = "projects/${local.project_id}/instances/bt1316-"
  tier3_bigtable_roles = {
    # The production sinks and the Change Streams source. MutateRows and the
    # staged commit's CheckAndMutateRow need mutateRows; ReadChangeStream and
    # GenerateInitialChangeStreamPartitions need readRows. The staged sink and
    # the source's coordinator read the app profile and the table (families and
    # change-stream retention) before they write or read, and the connector
    # keeps the client library's default of priming every channel with
    # PingAndWarm, which needs instances.ping.
    workload = {
      role_id     = "tier3BigtableRecoveryWorkload"
      description = "Writes, reads and streams changes of Tier-3 recovery tables; no instance or table administration"
      member      = google_service_account.tier3_bigtable.member
      permissions = ["bigtable.appProfiles.get", "bigtable.instances.ping", "bigtable.tables.get", "bigtable.tables.mutateRows", "bigtable.tables.readRows"]
    }
    # Creates the run's tables and app profile inside the instance the
    # creator role below made, and reads all three back before admission.
    runner = {
      role_id     = "tier3BigtableRecoveryRunner"
      description = "Creates and reads tables and app profiles of Tier-3 recovery instances; no data access or deletion"
      member      = local.tier3_bigtable_lifecycle.runner
      permissions = ["bigtable.appProfiles.create", "bigtable.appProfiles.get", "bigtable.instances.get", "bigtable.tables.create", "bigtable.tables.get"]
    }
    # Reads the tables back for the oracles, disables the change stream,
    # which an instance holding one refuses to delete without, deletes the
    # instance and reads its absence.
    supervisor = {
      role_id     = "tier3BigtableRecoverySupervisor"
      description = "Reads Tier-3 recovery tables, disables their change streams and deletes the instance; no writes or creation"
      member      = local.tier3_bigtable_lifecycle.supervisor
      permissions = ["bigtable.instances.delete", "bigtable.instances.get", "bigtable.tables.get", "bigtable.tables.readRows", "bigtable.tables.update"]
    }
  }
}

resource "google_project_iam_custom_role" "tier3_bigtable" {
  for_each    = local.tier3_bigtable_roles
  project     = local.project_id
  role_id     = each.value.role_id
  title       = "Tier-3 Bigtable recovery ${each.key}"
  description = each.value.description
  permissions = each.value.permissions
  stage       = "GA"

  depends_on = [google_project_iam_member.opentofu["roles/iam.roleAdmin"]]
}

# Bigtable tests resource names in IAM conditions for instances, clusters,
# tables and app profiles, so these bindings reach only the bt1316- instances.
resource "google_project_iam_member" "tier3_bigtable" {
  for_each = local.tier3_bigtable_roles
  project  = local.project_id
  role     = google_project_iam_custom_role.tier3_bigtable[each.key].name
  member   = each.value.member
  condition {
    title       = "bigtable-recovery-instances-only"
    description = "Only Tier-3 Bigtable recovery instances and what they contain"
    expression  = "resource.name.startsWith('${local.tier3_bigtable_instances}')"
  }
}

# A create request names the project as the instance's parent, and no
# instance condition matches the project's name, so this one role is
# unconditional; the lifecycle's probe measures that IAM checks the parent. The
# runner names every instance it creates bt1316-<run-id> and records the
# intent before the call; the supervisor deletes only recorded intents.
resource "google_project_iam_custom_role" "tier3_bigtable_creator" {
  project     = local.project_id
  role_id     = "tier3BigtableRecoveryInstanceCreator"
  title       = "Tier-3 Bigtable recovery instance creator"
  description = "Creates a Tier-3 recovery instance and its cluster; nothing else"
  permissions = ["bigtable.clusters.create", "bigtable.instances.create"]
  stage       = "GA"

  depends_on = [google_project_iam_member.opentofu["roles/iam.roleAdmin"]]
}

resource "google_project_iam_member" "tier3_bigtable_creator" {
  project = local.project_id
  role    = google_project_iam_custom_role.tier3_bigtable_creator.name
  member  = local.tier3_bigtable_lifecycle.runner
}

# The client library's default built-in metrics (attempt latencies, retries)
# are exported to the project's Cloud Monitoring, where the trial can read the
# clients' own figures outside the job JVM. The export is checked against the
# project, so this role is unconditional; without it each exporter warns once
# and drops every export.
resource "google_project_iam_custom_role" "tier3_bigtable_metrics" {
  project     = local.project_id
  role_id     = "tier3BigtableRecoveryClientMetrics"
  title       = "Tier-3 Bigtable recovery client metrics"
  description = "Writes the Bigtable client's built-in metrics; no reads or other metrics administration"
  permissions = ["monitoring.timeSeries.create"]
  stage       = "GA"

  depends_on = [google_project_iam_member.opentofu["roles/iam.roleAdmin"]]
}

resource "google_project_iam_member" "tier3_bigtable_metrics" {
  project = local.project_id
  role    = google_project_iam_custom_role.tier3_bigtable_metrics.name
  member  = google_service_account.tier3_bigtable.member
}

# The shared cleanup lists a run's state prefixes and deletes what remains, as
# either actor, as it does for the other recovery state buckets.
resource "google_storage_bucket_iam_member" "tier3_bigtable_state_reader" {
  for_each = local.tier3_bigtable_lifecycle
  bucket   = google_storage_bucket.tier3_bigtable.name
  role     = "roles/storage.objectViewer"
  member   = each.value
}

resource "google_storage_bucket_iam_member" "tier3_bigtable_state_cleanup" {
  for_each = local.tier3_bigtable_lifecycle
  bucket   = google_storage_bucket.tier3_bigtable.name
  role     = "roles/storage.objectUser"
  member   = each.value
  condition {
    title       = "bigtable-run-state-only"
    description = "Clean Flink state and its staged uploads under run prefixes"
    # Flink's GCS writer stages every object as .inprogress/<bucket>/<object>/
    # in the same bucket before composing it into place.
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.tier3_bigtable.name}/objects/runs/') || resource.name.startsWith('projects/_/buckets/${google_storage_bucket.tier3_bigtable.name}/objects/.inprogress/${google_storage_bucket.tier3_bigtable.name}/runs/')"
  }
}
