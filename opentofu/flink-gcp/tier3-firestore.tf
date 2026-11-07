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

# The workload identity, state bucket and database access of the Firestore
# recovery trials (#1879). Each run's lifecycle creates two databases, one in
# Native mode and one in Datastore mode, named fs1879-<label>, and deletes them
# again.
resource "google_service_account" "tier3_firestore" {
  account_id   = "tier3-firestore"
  display_name = "Tier-3 Firestore recovery workload"
  description  = "Writes and reads one run's Firestore databases and stores recovery state; no database administration"
}

resource "google_storage_bucket" "tier3_firestore" {
  name                        = "${local.project_id}-tier3-firestore"
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

resource "google_storage_bucket_iam_member" "tier3_firestore_workload" {
  bucket = google_storage_bucket.tier3_firestore.name
  role   = "roles/storage.objectUser"
  member = google_service_account.tier3_firestore.member
}

resource "google_service_account_iam_member" "tier3_firestore_gke" {
  service_account_id = google_service_account.tier3_firestore.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.project_id}.svc.id.goog[tier3-firestore/firestore]"

  depends_on = [google_container_cluster.tier3]
}

locals {
  # Explicit identities prevent additions to the shared lifecycle registry
  # from inheriting database control or state access.
  tier3_firestore_lifecycle = {
    runner     = local.tier3_lifecycle_members.runner
    supervisor = local.tier3_lifecycle_members.supervisor
  }
  # Every run's databases sit under this name. Firestore documents IAM
  # conditions on a database's name; that a Datastore-mode database's data
  # calls test the same name is a premise, and an unmet one fails the pilot
  # trial's first data call with PERMISSION_DENIED before any input runs, as
  # the preregistration (#2085) records.
  tier3_firestore_databases = "projects/${local.project_id}/databases/fs1879-"
  # Who reads and writes the run's databases: the workload's sinks and batch
  # sources, the runner's seed writes and read-back of each new database, and
  # the supervisor's oracle reads, late seed writes and absence check.
  tier3_firestore_data = {
    workload   = google_service_account.tier3_firestore.member
    runner     = local.tier3_firestore_lifecycle.runner
    supervisor = local.tier3_firestore_lifecycle.supervisor
  }
}

# The predefined user role rather than a custom one listing each call's
# permission (owner's decision, #2083): the condition already confines it to
# the run's databases, and a permission a custom role missed would surface
# only as PERMISSION_DENIED in a paid trial. Beyond what the trials call, it
# lets each identity delete entities and allocate ids in those databases.
resource "google_project_iam_member" "tier3_firestore_data" {
  for_each = local.tier3_firestore_data
  project  = local.project_id
  role     = "roles/datastore.user"
  member   = each.value
  condition {
    title       = "firestore-recovery-databases-only"
    description = "Only Tier-3 Firestore recovery databases and what they contain"
    expression  = "resource.name.startsWith('${local.tier3_firestore_databases}')"
  }
}

# Of the Datastore and Firestore roles only datastore.admin and
# datastore.owner carry datastore.databases.delete, and the clone and restore
# admin roles that carry the create carry more besides (e2e-sa.tf), so each
# lifecycle identity takes a custom role holding the one it calls.
#
# A create names the project as the database's parent, which no database
# condition matches; that the service checks the create there alone is a
# premise, so the creator binding is unconditional and lets the
# runner create a database of any ID, mode and location; only the runner's
# code bounds what it creates. The runner names every database it creates
# fs1879-<label> and records the intent before the call, and the supervisor
# deletes only recorded intents. Both wait for a database by reading it back
# rather than polling its long-running operation, so neither holds a
# datastore.operations permission.
resource "google_project_iam_custom_role" "tier3_firestore_creator" {
  project     = local.project_id
  role_id     = "tier3FirestoreRecoveryDatabaseCreator"
  title       = "Tier-3 Firestore recovery database creator"
  description = "Creates a Tier-3 recovery database; nothing else"
  permissions = ["datastore.databases.create"]
  stage       = "GA"

  depends_on = [google_project_iam_member.opentofu["roles/iam.roleAdmin"]]
}

resource "google_project_iam_member" "tier3_firestore_creator" {
  project = local.project_id
  role    = google_project_iam_custom_role.tier3_firestore_creator.name
  member  = local.tier3_firestore_lifecycle.runner
}

resource "google_project_iam_custom_role" "tier3_firestore_deleter" {
  project     = local.project_id
  role_id     = "tier3FirestoreRecoveryDatabaseDeleter"
  title       = "Tier-3 Firestore recovery database deleter"
  description = "Deletes a Tier-3 recovery database; nothing else"
  permissions = ["datastore.databases.delete"]
  stage       = "GA"

  depends_on = [google_project_iam_member.opentofu["roles/iam.roleAdmin"]]
}

resource "google_project_iam_member" "tier3_firestore_deleter" {
  project = local.project_id
  role    = google_project_iam_custom_role.tier3_firestore_deleter.name
  member  = local.tier3_firestore_lifecycle.supervisor
  condition {
    title       = "firestore-recovery-databases-only"
    description = "Only Tier-3 Firestore recovery databases"
    expression  = "resource.name.startsWith('${local.tier3_firestore_databases}')"
  }
}

# The shared cleanup lists a run's state prefixes and deletes what remains, as
# either actor, as it does for the other recovery state buckets.
resource "google_storage_bucket_iam_member" "tier3_firestore_state_reader" {
  for_each = local.tier3_firestore_lifecycle
  bucket   = google_storage_bucket.tier3_firestore.name
  role     = "roles/storage.objectViewer"
  member   = each.value
}

resource "google_storage_bucket_iam_member" "tier3_firestore_state_cleanup" {
  for_each = local.tier3_firestore_lifecycle
  bucket   = google_storage_bucket.tier3_firestore.name
  role     = "roles/storage.objectUser"
  member   = each.value
  condition {
    title       = "firestore-run-state-only"
    description = "Clean Flink state and its staged uploads under run prefixes"
    # Flink's GCS writer stages every object as .inprogress/<bucket>/<object>/
    # in the same bucket before composing it into place.
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.tier3_firestore.name}/objects/runs/') || resource.name.startsWith('projects/_/buckets/${google_storage_bucket.tier3_firestore.name}/objects/.inprogress/${google_storage_bucket.tier3_firestore.name}/runs/')"
  }
}
