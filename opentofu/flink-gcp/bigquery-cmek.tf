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

# What measuring the BigQuery connector's customer-managed encryption needs
# (#2101, for #1995): one Cloud KMS key that the gated tests configure on the
# tables, temporary tables and query results the connector creates, so each
# test can read the key back from the created resource.

# Neither half can be removed once created: Cloud KMS has no delete for a key
# ring, and a key can only have its versions destroyed. The pair is therefore
# created once and kept; no test creates or destroys either.
#
# In the dataset's location, because BigQuery accepts only a key in the
# location of the dataset it encrypts a table in.
resource "google_kms_key_ring" "it" {
  name     = "flink-gcp-it"
  location = google_bigquery_dataset.it.location

  # A destroy would only drop the ring from state and leave the name taken.
  lifecycle {
    prevent_destroy = true
  }

  # The apply account receives cloudkms.admin in the same apply; without the
  # edge the ring can be created before the grant and be refused. The key and
  # its grant follow the ring.
  depends_on = [
    google_project_service.this,
    google_project_iam_member.opentofu["roles/cloudkms.admin"],
  ]
}

# No rotation_period: the key protects one-day test tables, and each rotation
# would add a billable key version that nothing needs.
resource "google_kms_crypto_key" "it_cmek" {
  name     = "flink-gcp-it-cmek"
  key_ring = google_kms_key_ring.it.id
  purpose  = "ENCRYPT_DECRYPT"

  # Destroying the resource would only schedule the versions for destruction
  # and leave the name taken, so a plan that tries is a mistake to stop at.
  lifecycle {
    prevent_destroy = true
  }
}

# BigQuery encrypts and decrypts with its per-project encryption service
# account (bq-<project number>@bigquery-encryption.iam.gserviceaccount.com),
# not with the caller's identity. The data source reads that account through
# projects.getServiceAccount, which also provisions it if it does not exist
# yet, so the grant never names an account that is missing; an email built
# from the project number would. It carries no depends_on: the BigQuery API is
# already on, and a dependency on the pending API change would defer the read
# to apply, hiding from the pull-request plan whether the read-only plan
# account may make it. The grant is on this key alone.
data "google_bigquery_default_service_account" "this" {}

resource "google_kms_crypto_key_iam_member" "it_cmek_bigquery" {
  crypto_key_id = google_kms_crypto_key.it_cmek.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = data.google_bigquery_default_service_account.this.member
}

# No grant to the Cloud Storage service agent: #1995 leaves FILE_LOADS staging
# objects to the staging bucket's default key, so nothing writes an object
# with this key, and setting it as the default key of gs://flink-gcp would
# reach every other test that stages there.
