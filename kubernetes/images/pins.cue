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
// GAR version was recreated by https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39449756339.
// Recheck availability: every GAR version becomes deletion-eligible after seven days.
flink: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/flink@sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd"

// Bigtable rescale-in rerun publication: https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39449756339.
lifecycleTools: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/lifecycle-tools@sha256:2298529af059233d04adfc17ca7c2ad21f63113737c496a589c3f4c522ef91fc"

// Bigtable rescale-in rerun publication: https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39449756339.
smoke: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/smoke@sha256:774b98d2f0fab8ae960fcb44126437443f82f8448d45dfe602b9c515a3b188dc"
