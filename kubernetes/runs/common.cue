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

import (
	"strings"
	"time"
)

run: {
	id:        string & =~"^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$" @tag(run_id)
	expiresAt: time.Time                                         @tag(expires_at)
	namespace: *"tier3-smoke" | "tier3-cloudtasks" | "tier3-bigquery" | "tier3-pubsub"
	image:     string @tag(image)
}

let flinkPodPolicy = {
	metadata: labels: "flink-gcp.io/run-id": run.id
	spec: nodeSelector: {
		"kubernetes.io/arch":        "amd64"
		"cloud.google.com/gke-spot": "true"
	}
}

delivery: resources: [string]: {
	// Foundation resources belong to OpenTofu, never to an expiring run.
	kind: "ConfigMap" | "Deployment" | "FlinkDeployment" | "Job" | "Service"
	metadata: {
		// Resolve the run default before unifying a package namespace.
		namespace: "\(run.namespace)"
		labels: "flink-gcp.io/run-id":          run.id
		annotations: "flink-gcp.io/expires-at": run.expiresAt
	}
	if kind == "FlinkDeployment" {
		spec: {
			image:           =~"^us-central1-docker[.]pkg[.]dev/flink-gcp/flink-tier3/[a-z0-9/_-]+@sha256:[0-9a-f]{64}$"
			serviceAccount!: string & strings.MinRunes(1)
			job!: {
				parallelism!:          int
				allowNonRestoredState: false
			}
			// Each namespace admits one runtime line set and one parallelism class set.
			if run.namespace == "tier3-smoke" {
				flinkVersion: "v2_2"
				job: parallelism: >=1 & <=2
			}
			if run.namespace == "tier3-pubsub" {
				flinkVersion:   "v2_2"
				serviceAccount: "pubsub"
				job: parallelism: 1 | 2
			}
			if run.namespace == "tier3-cloudtasks" {
				flinkVersion:   "v1_20" | "v2_2"
				serviceAccount: "cloudtasks-benchmark"
				job: parallelism: 1 | 4 | 16
			}
			if run.namespace == "tier3-bigquery" {
				flinkVersion:   "v2_2"
				serviceAccount: "bigquery"
				image:          =~"^us-central1-docker[.]pkg[.]dev/flink-gcp/flink-tier3/bigquery-recovery@sha256:[0-9a-f]{64}$"
				job: parallelism: 2
			}
			podTemplate: flinkPodPolicy
			jobManager?: podTemplate?:  flinkPodPolicy
			taskManager?: podTemplate?: flinkPodPolicy
		}
	}
	if kind == "Service" {
		spec: type: "ClusterIP"
	}
}
