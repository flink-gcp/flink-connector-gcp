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
	"encoding/json"
	"time"
	"github.com/flink-gcp/flink-connector-gcp/kubernetes/images"
	smoke "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/smoke"
	runPolicy "github.com/flink-gcp/flink-connector-gcp/kubernetes/runs:tier3"
)

runID:         string & =~"^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$" @tag(run_id)
nonce:         string & =~"^[0-9a-f]{32}$"                       @tag(nonce)
expires:       time.Time                                         @tag(expires_at)
approval:      *"{}" | string                                    @tag(approval)
activeSeconds: int & >0 & <=3420                                 @tag(active_seconds,type=int)
// Supplied as JSON by flink-tier3 render or the lifecycle runner.
packageSources: {
	"__init__.py"!:              string
	"__main__.py"!:              string
	"runtime.py"!:               string
	"policy.toml"!:              string
	[=~"^[a-z_]+[.](py|toml)$"]: string
}

// Use the same namespace, image, Spot and schema constraints as ordinary runs.
application: (runPolicy & {
	run: {id: runID, expiresAt: expires, image: images.smoke}
	delivery: resources: app: (smoke.#Application & {run: {id: runID, image: images.smoke}}).resource & {
		metadata: annotations: "flink-gcp.io/approval": nonce
	}
}).delivery.resources.app

let sharedMetadata = {
	namespace: "tier3-system"
	labels: "flink-gcp.io/run-id": runID
	annotations: {
		"flink-gcp.io/approval":   nonce
		"flink-gcp.io/expires-at": expires
	}
}

delivery: resources: {
	config: {
		apiVersion: "v1"
		kind:       "ConfigMap"
		metadata: sharedMetadata & {name: "lifecycle-\(runID)"}
		immutable: true
		data: {
			for name, content in packageSources {
				"flink_tier3_\(name)": content
			}
			"approval.json":    approval
			"application.json": json.Marshal(application)
		}
	}
	supervisor: {
		apiVersion: "batch/v1"
		kind:       "Job"
		metadata: sharedMetadata & {name: "lifecycle-\(runID)"}
		spec: {
			parallelism:           1
			completions:           1
			backoffLimit:          0
			activeDeadlineSeconds: activeSeconds
			template: {
				metadata: sharedMetadata
				spec: {
					serviceAccountName:            "tier3-supervisor"
					restartPolicy:                 "Never"
					terminationGracePeriodSeconds: 120
					nodeSelector: "kubernetes.io/arch": "amd64"
					affinity: nodeAffinity: requiredDuringSchedulingIgnoredDuringExecution: nodeSelectorTerms: [{
						matchExpressions: [{key: "cloud.google.com/gke-spot", operator: "NotIn", values: ["true"]}]
					}]
					securityContext: {
						runAsNonRoot: true
						runAsUser:    65532
						runAsGroup:   65532
						seccompProfile: type: "RuntimeDefault"
					}
					containers: [{
						name:  "supervisor"
						image: images.lifecycleTools
						command: ["python3", "-m", "flink_tier3", "supervisor"]
						workingDir: "/lifecycle"
						env: [{name: "POD_UID", valueFrom: fieldRef: fieldPath: "metadata.uid"}]
						resources: {
							requests: {cpu: "250m", memory: "512Mi", "ephemeral-storage": "128Mi"}
							limits: {cpu: "250m", memory: "512Mi", "ephemeral-storage": "128Mi"}
						}
						securityContext: {
							allowPrivilegeEscalation: false
							readOnlyRootFilesystem:   true
							capabilities: drop: ["ALL"]
						}
						volumeMounts: [{name: "source", mountPath: "/lifecycle", readOnly: true}]
					}]
					volumes: [{name: "source", configMap: {name: "lifecycle-\(runID)", items: [
						for name, _ in packageSources {
							key:  "flink_tier3_\(name)"
							path: "flink_tier3/\(name)"
						},
						{key: "approval.json", path: "approval.json"},
						{key: "application.json", path: "application.json"},
					]}}]
				}
			}
		}
	}
}
