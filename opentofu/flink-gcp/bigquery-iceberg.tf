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

# What measuring the BigQuery sink against Iceberg managed tables needs
# (#2089, for #1994). BigQuery writes such a table's Parquet files and Iceberg
# metadata into a Cloud Storage bucket through a Cloud resource connection, so
# creating one names both. The tables themselves are test resources and are
# created by the tests, in the flink_gcp_it dataset.

# In the dataset's location, where Google's setup for these tables creates the
# connection.
resource "google_bigquery_connection" "iceberg" {
  connection_id = "flink_gcp_iceberg"
  location      = google_bigquery_dataset.it.location
  description   = "Iceberg managed tables created by the BigQuery sink's tests"

  cloud_resource {}

  depends_on = [google_project_service.this]
}

# A bucket of its own rather than the flink-gcp staging bucket, whose one-day
# lifecycle rule would delete files under a table still in use: Google warns
# that changing or deleting a table's files outside BigQuery loses data or
# makes the table unreadable. Single-region next to the dataset, uniform
# access, public access prevention and no versioning follow Google's bucket
# guidance for these tables; object versioning is unsupported for them.
resource "google_storage_bucket" "iceberg" {
  name          = "flink-gcp-iceberg"
  location      = google_bigquery_dataset.it.location
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Deleting a table does not garbage-collect its files, so something has to.
  # The rule counts from each object's creation and knows nothing of tables,
  # so it rests on an assumption: no test uses a table for three days. A test
  # creates its tables under a prefix of its own and deletes them when it
  # finishes, within minutes. Whether the dataset's one-day default table
  # expiration also reaches these tables is not relied on.
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 3
    }
  }

  # Zero, like the Tier-3 buckets in this root: the default seven days would
  # keep every deleted object billable for a week, including the files that
  # BigQuery's garbage collection removes after compaction. Google recommends
  # keeping the default as protection against accidental deletion; these
  # tables hold test data only.
  soft_delete_policy {
    retention_duration_seconds = 0
  }
}

# The two roles Google names for the connection's service account: object
# read and write, and the bucket metadata read that objectUser lacks.
resource "google_storage_bucket_iam_member" "iceberg_connection" {
  for_each = toset([
    "roles/storage.objectUser",
    "roles/storage.legacyBucketReader",
  ])

  bucket = google_storage_bucket.iceberg.name
  role   = each.value
  member = "serviceAccount:${google_bigquery_connection.iceberg.cloud_resource[0].service_account_id}"
}

# Creating an Iceberg managed table through the connection needs
# bigquery.connections.delegate on it. Checked against `gcloud iam roles
# describe` over every roles/bigquery* role, connectionAdmin is the narrowest
# carrying it (the others are bigquery.admin, bigquery.studioAdmin and a
# service agent); roles/bigquery.connectionUser carries use, not delegate. It is
# granted on this connection alone, not on the project, so what it adds
# beyond delegate (update, delete, updateTag and setIamPolicy) reaches nothing
# but this connection. A custom role was declined for the reason written out in
# e2e-sa.tf.
resource "google_bigquery_connection_iam_member" "iceberg_e2e" {
  location      = google_bigquery_connection.iceberg.location
  connection_id = google_bigquery_connection.iceberg.connection_id
  role          = "roles/bigquery.connectionAdmin"
  member        = google_service_account.github_actions_e2e.member
}
