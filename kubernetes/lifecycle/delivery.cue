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
)

runID:            string & =~"^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$"                                      @tag(run_id)
nonce:            string & =~"^[0-9a-f]{32}$"                                                            @tag(nonce)
expires:          time.Time                                                                              @tag(expires_at)
approval:         *"{}" | string                                                                         @tag(approval)
activeSeconds:    int & >0                                                                               @tag(active_seconds,type=int)
scenario:         *"smoke" | "generic-recovery" | "cloudtasks" | "bigquery-recovery" | "pubsub-recovery" @tag(scenario)
applicationImage: *"" | string                                                                           @tag(application_image)
// Shared offline proposal document for BigQuery and Pub/Sub.
proposal: *"{}" | string @tag(proposal)
// Supplied as JSON by flink-tier3 render or the lifecycle runner.
packageSources: {
	"__init__.py"!:                 string
	"__main__.py"!:                 string
	"cli.py"!:                      string
	"runtime.py"!:                  string
	"policy.toml"!:                 string
	[=~"^[a-z0-9_]+[.](py|toml)$"]: string
}

// A smoke run is bounded by one 60-minute approval; a measurement session by
// its plan plus cleanup, at most 300 minutes.
if scenario == "smoke" || scenario == "generic-recovery" || scenario == "pubsub-recovery" {activeSeconds: <=3420}
if scenario == "bigquery-recovery" {activeSeconds: <=5220}
if scenario == "cloudtasks" {activeSeconds: <=17820}

upgradeApplication: _

let sharedMetadata = {
	namespace: "tier3-system"
	labels: "flink-gcp.io/run-id": runID
	annotations: {
		"flink-gcp.io/approval":   nonce
		"flink-gcp.io/expires-at": expires
		"flink-gcp.io/scenario":   scenario
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
			"approval.json": approval
			// application.json is declared by the selected scenario file.
			if scenario == "generic-recovery" || scenario == "bigquery-recovery" || scenario == "pubsub-recovery" {"upgrade-application.json": json.Marshal(upgradeApplication)}
		}
	}
	supervisor: {
		apiVersion: "batch/v1"
		kind:       "Job"
		metadata: sharedMetadata & {name: "lifecycle-\(runID)"}
		spec: {
			parallelism: 1
			completions: 1
			// The infrastructure can take the supervisor Pod away; kube-dns has
			// preempted it within its first half minute. A disrupted Pod is
			// replaced, twice at most and only once it is terminal; any other
			// failure ends the Job. The control record's claim fences the Pod
			// a replacement displaces.
			backoffLimit: 2
			podFailurePolicy: rules: [
				{action: "Count", onPodConditions: [{type: "DisruptionTarget", status: "True"}]},
				{action: "FailJob", onExitCodes: {containerName: "supervisor", operator: "NotIn", values: [0]}},
			]
			podReplacementPolicy:  "Failed"
			activeDeadlineSeconds: activeSeconds
			template: {
				metadata: sharedMetadata & {
					annotations: "cluster-autoscaler.kubernetes.io/safe-to-evict": "false"
				}
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
							requests: {cpu: "1", memory: "2Gi", "ephemeral-storage": "128Mi"}
							limits: {cpu: "1", memory: "2Gi", "ephemeral-storage": "128Mi"}
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
						if scenario == "bigquery-recovery" || scenario == "pubsub-recovery" {{key: "proposal.json", path: "proposal.json"}},
						if scenario == "generic-recovery" || scenario == "bigquery-recovery" || scenario == "pubsub-recovery" {{key: "upgrade-application.json", path: "upgrade-application.json"}},
					]}}]
				}
			}
		}
	}
}
