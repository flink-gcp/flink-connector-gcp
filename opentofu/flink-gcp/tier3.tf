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

# Shared deployment environment for issue #38. Workloads and the Operator
# installation follow separately after this cluster's reviewed apply.
resource "google_compute_network" "tier3" {
  name                    = "flink-tier3"
  auto_create_subnetworks = false

  depends_on = [
    google_project_service.this["compute.googleapis.com"],
    google_project_iam_member.opentofu["roles/compute.networkAdmin"],
  ]
}

resource "google_compute_subnetwork" "tier3" {
  name                     = "flink-tier3-us-central1"
  region                   = "us-central1"
  ip_cidr_range            = "10.38.0.0/24"
  network                  = google_compute_network.tier3.id
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.40.0.0/16"
  }
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.41.0.0/20"
  }
}

resource "google_service_account" "tier3_nodes" {
  account_id   = "flink-tier3-nodes"
  display_name = "Flink Tier-3 GKE nodes"
  description  = "Node telemetry and image retrieval; connector access uses workload identities"
}

resource "google_project_iam_member" "tier3_nodes" {
  project = local.project_id
  role    = "roles/container.defaultNodeServiceAccount"
  member  = google_service_account.tier3_nodes.member
}

# Only the apply identity may attach this account when creating the cluster.
resource "google_service_account_iam_member" "tier3_nodes_attach" {
  service_account_id = google_service_account.tier3_nodes.name
  role               = "roles/iam.serviceAccountUser"
  member             = google_service_account.opentofu.member
}

resource "google_container_cluster" "tier3" {
  name                = "flink-tier3"
  location            = "us-central1"
  enable_autopilot    = true
  deletion_protection = true
  network             = google_compute_network.tier3.id
  subnetwork          = google_compute_subnetwork.tier3.id
  resource_labels = {
    purpose = "tier3"
  }

  release_channel {
    channel = "REGULAR"
  }

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  private_cluster_config {
    enable_private_nodes = true
  }

  # Authenticated clients, including GitHub-hosted plan runners, reach the
  # DNS endpoint without a public control-plane IP or a bastion.
  control_plane_endpoints_config {
    dns_endpoint_config {
      allow_external_traffic    = true
      enable_k8s_tokens_via_dns = false
      enable_k8s_certs_via_dns  = false
    }
    ip_endpoints_config {
      enabled = false
    }
  }

  cluster_autoscaling {
    auto_provisioning_defaults {
      service_account = google_service_account.tier3_nodes.email
      oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    }
  }

  workload_identity_config {
    workload_pool = "${local.project_id}.svc.id.goog"
  }

  # Autopilot retains system and workload logging; run manifests bound the
  # application's log volume alongside its other metered observations.
  logging_config {
    enable_components = ["SYSTEM_COMPONENTS", "WORKLOADS"]
  }
  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS"]
    managed_prometheus {
      # Autopilot requires managed collection. Optional application scraping
      # is configured and priced by the subsequent CUE workload definitions.
      enabled = true
      auto_monitoring_config {
        scope = "NONE"
      }
    }
  }

  depends_on = [
    google_project_service.this["container.googleapis.com"],
    google_project_iam_member.opentofu["roles/container.clusterAdmin"],
    google_project_iam_member.tier3_nodes,
    google_service_account_iam_member.tier3_nodes_attach,
  ]
}

# Mirror pinned Operator and Flink images here before starting any Pod.
# Private nodes have no Cloud NAT route to Docker Hub or GHCR.
resource "google_artifact_registry_repository" "tier3" {
  location      = "us-central1"
  repository_id = "flink-tier3"
  description   = "Digest-pinned Flink Tier-3 runtime images"
  format        = "DOCKER"

  depends_on = [
    google_project_service.this["artifactregistry.googleapis.com"],
    google_project_iam_member.opentofu["roles/artifactregistry.admin"],
  ]
}

resource "google_artifact_registry_repository_iam_member" "tier3_nodes" {
  project    = local.project_id
  location   = google_artifact_registry_repository.tier3.location
  repository = google_artifact_registry_repository.tier3.repository_id
  role       = "roles/artifactregistry.reader"
  member     = google_service_account.tier3_nodes.member
}
