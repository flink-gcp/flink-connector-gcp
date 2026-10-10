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

// First published by https://github.com/flink-gcp/flink-connector-gcp/actions/runs/34760632704; the current
// GAR version was recreated by https://github.com/flink-gcp/flink-connector-gcp/actions/runs/37956928238.
// Recheck availability: every GAR version becomes deletion-eligible after seven days.
flink: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/flink@sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd"

// BigQuery FILE_LOADS trial publication: https://github.com/flink-gcp/flink-connector-gcp/actions/runs/37956928238.
lifecycleTools: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/lifecycle-tools@sha256:69d4424cc81dae202ee9aa0d1ae68969c7af48622c3b5f545d58b272c5fec71f"

// BigQuery FILE_LOADS trial publication: https://github.com/flink-gcp/flink-connector-gcp/actions/runs/37956928238.
smoke: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/smoke@sha256:8e1d7e18fe882b36abb6ceea78240ee4482b2ba4ff708fc3b463f17662fa1f63"
