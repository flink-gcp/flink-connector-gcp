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
flink:          "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/flink@sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd"
lifecycleTools: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/lifecycle-tools@sha256:fd12afa55a4b1f23703e64f2b039d7d75203df25cb881293fc7923e52762e31c"

// Smoke publication: https://github.com/flink-gcp/flink-connector-gcp/actions/runs/34768916308.
smoke: "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/smoke@sha256:29cc0533b2e1a984343a51315cdfe4110aa028a6c040d2275a103c3cfa591a3d"
