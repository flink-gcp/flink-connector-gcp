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

# The account the nightly real-GCP E2E workflow runs as. Scoped to the fixed
# set of services the connectors touch; the fine-grained resources (tables,
# topics, subscriptions, queues, and the Bigtable and Spanner instances
# themselves) are created and deleted by the tests themselves, which is why the
# Pub/Sub, Cloud Tasks, Bigtable and Spanner grants are create-capable. A new
# connector's E2E suite adds its grant here in the pull request that first
# needs it.

resource "google_service_account" "github_actions_e2e" {
  account_id   = "github-actions-e2e"
  display_name = "Real-GCP E2E tests"
  description  = "Runs the real-GCP integration test workflows"
}

resource "google_project_iam_member" "e2e" {
  for_each = toset([
    # Load and query jobs; table data access is granted on the dataset below.
    "roles/bigquery.jobUser",
    # The BigQuery source's gated suite (#390) creates Storage Read API
    # sessions. bigquery.readsessions.create is a project-level permission, so
    # neither jobUser nor the dataset-scoped dataEditor below carries it, and
    # this is the narrowest predefined role that does. Reading the rows still
    # needs the dataset grant, so this binding alone opens no data.
    "roles/bigquery.readSessionUser",
    # Admin rather than roles/bigtable.user, and the widest grant in this list:
    # the Bigtable suite (#218) creates and deletes an ephemeral instance per
    # gated test class, and no *predefined* role narrower than admin can create
    # an instance (roles/bigtable.user is data access only). It also carries the
    # table admin and data access the tests need, so this is one binding rather
    # than two, and there is nothing persistent for it to reach: the instance a
    # run works in was created by that run. A custom role holding just the
    # instance/cluster/table permissions would be narrower still, and was not
    # taken — every other grant in this file is a predefined role too, and a
    # custom one is a definition to maintain against an API that adds
    # permissions.
    "roles/bigtable.admin",
    # Starting/stopping the fixed App Engine version requires
    # appengine.versions.update; listing its one instance requires get/list.
    # serviceAdmin is the narrowest predefined role carrying that combination.
    # It also carries service/version deletion, which the lifecycle wrapper
    # never calls; appAdmin/editor/admin add still wider application, memcache,
    # debug, or deployment privileges.
    "roles/appengine.serviceAdmin",
    # Tests create and delete their own queues and tasks.
    "roles/cloudtasks.admin",
    # Tests create and delete their own topics and subscriptions.
    "roles/pubsub.editor",
    # Editor, not admin, and unlike Bigtable above this connector does not need
    # the wider grant: the Spanner suite (#224) creates and deletes an ephemeral
    # instance per gated test class, and roles/spanner.editor carries
    # spanner.instances.create/delete along with database creation, DDL, reads,
    # writes and spanner.databases.useDataBoost — every permission the suite
    # uses, checked one by one against `gcloud iam roles describe`. What admin
    # adds over it is what a CI account should not hold: setIamPolicy on
    # instances, databases, backups and backup schedules, which is a
    # privilege-escalation surface, plus CMEK key-handle creation and tag
    # bindings. Nothing persistent exists for it to reach either way: the
    # instance a run works in was created by that run. A custom role was
    # declined for the reason written out above.
    "roles/spanner.editor",
  ])

  project = local.project_id
  role    = each.value
  member  = google_service_account.github_actions_e2e.member
}

resource "google_bigquery_dataset_iam_member" "e2e" {
  dataset_id = google_bigquery_dataset.it.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.github_actions_e2e.member
}

resource "google_storage_bucket_iam_member" "e2e" {
  bucket = google_storage_bucket.it.name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.github_actions_e2e.member
}

# Reachable only from this repository's push, schedule, workflow_dispatch and
# workflow_run events on main — the triggers the E2E workflow carries, plus
# the one the sweep uses to run when E2E finishes (issue #964) — never from a
# pull request. workflow_run widens which *event* may assume this account,
# not which repository, ref or roles: the sweep runs as the same service
# account on the same branch it already runs on hourly schedules.
resource "google_service_account_iam_member" "e2e_wif" {
  for_each = toset(["push", "schedule", "workflow_dispatch", "workflow_run"])

  service_account_id = google_service_account.github_actions_e2e.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "${local.wif_principal_set}/attribute.event_ref/${local.github_repository_id}:${each.value}:refs/heads/main"
}
