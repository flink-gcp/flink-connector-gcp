// Copyright 2026 The flink-gcp authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package images

// Published by https://github.com/flink-gcp/flink-connector-gcp/actions/runs/34760632704.
// Recheck availability: every GAR version becomes deletion-eligible after seven days.
flink: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/flink@sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd"

// BigQuery trial publication: https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35869627208.
lifecycleTools: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/lifecycle-tools@sha256:107e3096d0a4d09f625b8ce61eeae1f961297db2848a5650957739af538940ca"

// BigQuery trial publication: https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35869627208.
smoke: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/smoke@sha256:8e4803c7330b68a80326fdb2542721f40859ee85bcba0450560ad5ccb99ffe45"
