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

package tier3

import "github.com/flink-gcp/flink-connector-gcp/kubernetes/schemas"

cluster: {
	project: "flink-gcp"
	region:  "us-central1"
	name:    "flink-tier3"
	context: "gke_flink-gcp_us-central1_flink-tier3"
}

// Smaller values render first; resource keys break ties deterministically.
delivery: order: {
	ConfigMap:       30
	Service:         70
	Deployment:      80
	Job:             80
	FlinkDeployment: 90
}

// A leaf adds named resources; its ancestors supply constraints.
delivery: resources: [string]: schemas.#Object & {
	metadata: labels: {
		"app.kubernetes.io/part-of":    "flink-tier3"
		"app.kubernetes.io/managed-by": "cue"
	}
}
