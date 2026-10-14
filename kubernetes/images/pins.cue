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
// GAR version was recreated by https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38561068834.
// Recheck availability: every GAR version becomes deletion-eligible after seven days.
flink: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/flink@sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd"

// BigQuery fault trial publication: https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38561068834.
lifecycleTools: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/lifecycle-tools@sha256:4f5ec406125a5eccddf8e95bd5f1a6cc355d7609f7322fdb4539434e5a003c2c"

// BigQuery fault trial publication: https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38561068834.
smoke: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/smoke@sha256:e4cfa8e10332ad603e9464c5acb6642649820544d128f1a6e3fe20383a54d7a2"
