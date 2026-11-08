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
// GAR version was recreated by https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42057831971.
// Recheck availability: every GAR version becomes deletion-eligible after seven days.
flink: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/flink@sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd"

// Firestore recovery publication: https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42484046881.
lifecycleTools: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/lifecycle-tools@sha256:3ad53a9c6e2c5edac17228bc3b284ab43957db4e7287eef85fb77add89ee07db"

// Firestore recovery publication: https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42484046881.
smoke: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/smoke@sha256:f8292e22306331c4c0ac38f44886c8d40690521729a09f2d217d57f853c73ed3"
