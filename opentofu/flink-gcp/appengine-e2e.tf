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
  # scripts/appengine-e2e-fixture.sh reads these two values rather than keeping
  # another copy. They are stable routing identifiers; E2E start/stop state is
  # deliberately outside OpenTofu ownership.
  cloudtasks_appengine_e2e_service = "default"
  cloudtasks_appengine_e2e_version = "flink-e2e"
}

# The application is the one permanent part of this fixture: Google does not
# let a project change or delete its App Engine location. App Engine calls this
# location us-central while Cloud Tasks calls the corresponding queue location
# us-central1.
resource "google_app_engine_application" "e2e" {
  project     = local.project_id
  location_id = "us-central"

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    google_project_iam_member.opentofu["roles/appengine.appCreator"],
    google_project_service.this["appengine.googleapis.com"],
  ]
}

# The handler has no project-level role and no Google Cloud client. Its only
# resource access is read-only access to the App Engine-owned code bucket below
# so the platform can load this deployment.
resource "google_service_account" "appengine_e2e_runtime" {
  account_id   = "appengine-e2e-runtime"
  display_name = "Cloud Tasks App Engine E2E runtime"
  description  = "Runs the no-project-access Cloud Tasks App Engine E2E handler"
}

resource "google_storage_bucket_object" "appengine_e2e_source" {
  name         = "cloudtasks-appengine-e2e/main.py"
  bucket       = google_app_engine_application.e2e.code_bucket
  source       = "${path.module}/appengine-e2e/main.py"
  content_type = "text/x-python"
}

resource "google_storage_bucket_iam_member" "appengine_e2e_source" {
  bucket = google_app_engine_application.e2e.code_bucket
  role   = "roles/storage.objectViewer"
  member = google_service_account.appengine_e2e_runtime.member
}

# OpenTofu may impersonate this one runtime identity when it creates a version;
# the grant is on the identity itself, not the project.
resource "google_service_account_iam_member" "appengine_e2e_deployer" {
  service_account_id = google_service_account.appengine_e2e_runtime.name
  role               = "roles/iam.serviceAccountUser"
  member             = google_service_account.opentofu.member
}

# Apply persistent version changes through the merge workflow from the reviewed
# plan. A local state-changing apply after CI plans the change would invalidate
# that saved plan and require a follow-up root-module pull request.
#
# Four things about this resource, all of them learned by getting them wrong.
#
# **Any edit to appengine-e2e/main.py redeploys the version**, because sha1_sum
# below is that file's hash. Edits with no behavioural content count: rewriting
# the licence header across the repository (#754) produced a plan updating this
# version and nothing else.
#
# **sha1_sum is write-only.** The provider never reads it back from the Admin
# API, so state is whatever the last apply *recorded*, not what is running.
# Measured: with the version serving one file and state naming another, both
# `plan` and `plan -refresh-only` answer "No changes". Nothing in OpenTofu will
# ever report this divergence.
#
# **So a failed apply is not self-healing, and does not look like a failure
# afterwards.** The apply for #754 died ten seconds into "Still modifying..."
# with `Error code 13, message: an internal error has occurred` — a server-side
# failure, the bucket object in the same apply having already succeeded — and
# still wrote the new hash into state. The version went on serving the previous
# file with every subsequent plan clean. When an apply over this resource fails,
# read the deployment back from the Admin API rather than trusting the next plan.
#
# **Repair is an in-place update, not a replacement.** `-replace` cannot work:
# App Engine refuses to delete the final version of a service ("Cannot delete
# the final version of a service (module)"), and this service has exactly one.
# Changing the file's content is the only lever, which is why /revision exists
# in main.py — it serves that same hash, so `curl` against a started version
# compared with `sha1sum` of the file is the check that does notice.
#
# Allow **at least fifteen minutes** for an apply here. A successful update takes
# around ten; the ten-second failure above is what a *failure* looks like. A
# client-side timeout shorter than that kills the wait while the deployment
# succeeds server-side, which produces the divergence in the opposite direction —
# observed, and repaired by re-applying.
resource "google_app_engine_standard_app_version" "e2e" {
  project         = local.project_id
  service         = local.cloudtasks_appengine_e2e_service
  version_id      = local.cloudtasks_appengine_e2e_version
  runtime         = "python312"
  instance_class  = "B1"
  service_account = google_service_account.appengine_e2e_runtime.email

  deployment {
    files {
      name     = "main.py"
      sha1_sum = sha1(file("${path.module}/appengine-e2e/main.py"))
      source_url = format(
        "https://storage.googleapis.com/%s/%s",
        google_storage_bucket_object.appengine_e2e_source.bucket,
        google_storage_bucket_object.appengine_e2e_source.name,
      )
    }
  }

  entrypoint {
    shell = "python3 main.py"
  }

  manual_scaling {
    instances = 1
  }

  lifecycle {
    # scripts/appengine-e2e-fixture.sh starts and stops the version at runtime.
    # The provider explicitly requires this exclusion when the Admin API owns
    # the live manual-scaling count, otherwise every E2E run creates drift.
    ignore_changes = [manual_scaling[0].instances]
  }

  depends_on = [
    google_project_iam_member.opentofu["roles/appengine.deployer"],
    google_project_iam_member.opentofu["roles/appengine.serviceAdmin"],
    google_project_iam_member.opentofu["roles/cloudbuild.builds.editor"],
    google_project_service.this["artifactregistry.googleapis.com"],
    google_project_service.this["cloudbuild.googleapis.com"],
    google_service_account_iam_member.appengine_e2e_deployer,
    google_storage_bucket_iam_member.appengine_e2e_source,
  ]
}
