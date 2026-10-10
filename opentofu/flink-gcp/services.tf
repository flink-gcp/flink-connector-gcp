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

# Only the APIs this project's function needs are managed; the assortment a
# fresh project ships enabled (logging, monitoring, ...) is left alone. A new
# connector's E2E suite or deployment infrastructure adds its API here in
# the pull request that first needs it, not in advance.
resource "google_project_service" "this" {
  for_each = toset([
    # Workload Identity Federation and IAM management.
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "sts.googleapis.com",
    # Connector E2E targets.
    # App Engine Standard builds use Cloud Build and store their images in
    # Artifact Registry. A dedicated project-owned bucket keeps the fixture
    # source available for deployment (appengine-e2e.tf).
    "appengine.googleapis.com",
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    # Two services, because the Bigtable E2E suite (#218) spans both planes: it
    # creates and deletes an ephemeral instance through the admin API and then
    # writes and reads rows through the data one.
    "bigtable.googleapis.com",
    "bigtableadmin.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudtasks.googleapis.com",
    # Two services, because a Firestore database in Datastore mode is read and
    # written through a different API: datastore.googleapis.com serves that
    # mode's data plane, and firestore.googleapis.com serves Native mode's
    # together with the database administration of both. The E2E suite (#1546)
    # creates an ephemeral database of each mode and works in it. The Datastore
    # API was already on before this list named it, most likely enabled when
    # App Engine created the project's (default) Datastore-mode database, so
    # adding it here changes nothing in the project. The Firestore API was first
    # needed by a probe of the service's value checks (#1589).
    "datastore.googleapis.com",
    "firestore.googleapis.com",
    "pubsub.googleapis.com",
    # One service, not two as Bigtable needs: Spanner's instance and database
    # administration and its data plane all live behind this single API, so the
    # E2E suite (#224) creating and deleting an ephemeral instance and then
    # writing and reading through it needs nothing further enabled.
    "spanner.googleapis.com",
    "storage.googleapis.com",
    # Tier-3 deployment infrastructure: cluster and private node network.
    "compute.googleapis.com",
    "container.googleapis.com",
  ])

  service = each.value
  # Removing an entry from this list must not switch a service off under
  # resources that still use it; disabling stays a deliberate manual act.
  disable_on_destroy = false
}
