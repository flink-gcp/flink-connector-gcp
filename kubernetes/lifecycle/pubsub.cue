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
	"github.com/flink-gcp/flink-connector-gcp/kubernetes/images"
	pubsub "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/pubsub"
	runPolicy "github.com/flink-gcp/flink-connector-gcp/kubernetes/runs:tier3"
)

// Offline inputs only; serialized approval still refuses Pub/Sub execution.
pubsubTrial:      *"jm-replacement" | "tm-replacement" | "rescale-out" | "rescale-in" @tag(pubsub_trial)
pubsubRecords:    *1000 | (int & >=2 & <=10000)                                       @tag(pubsub_records,type=int)
pubsubEntryPoint: *"datastream" | "table"                                             @tag(pubsub_entry_point)
// The workload access probe's test plan and program, from `pubsub_plan.prepare`,
// which checks the rendered Pod against both.
pubsubProbe:       *"{}" | string @tag(pubsub_probe)
pubsubProbeSource: *"" | string   @tag(pubsub_probe_source)

if scenario == "pubsub-recovery" {
	_applications: [for recovery in [false, true] {
		(runPolicy & {
			run: {id: runID, expiresAt: expires, namespace: "tier3-pubsub", image: applicationImage}
			delivery: resources: app: (pubsub.#Application & {
				run: {
					id: runID, image: applicationImage, recordsPerSubscription: pubsubRecords, entryPoint: pubsubEntryPoint
					if pubsubTrial == "rescale-out" && !recovery || pubsubTrial == "rescale-in" && recovery {parallelism: 1}
					if pubsubTrial == "rescale-out" && recovery || pubsubTrial == "rescale-in" && !recovery || pubsubTrial == "jm-replacement" || pubsubTrial == "tm-replacement" {parallelism: 2}
					if recovery && (pubsubTrial == "rescale-out" || pubsubTrial == "rescale-in") {phase: "upgrade"}
					if !recovery || pubsubTrial == "jm-replacement" || pubsubTrial == "tm-replacement" {phase: "initial"}
				}
			}).resource & {
				metadata: annotations: "flink-gcp.io/approval": nonce
				metadata: annotations: "flink-gcp.io/scenario": scenario
			}
		}).delivery.resources.app
	}]
	application:        _applications[0]
	upgradeApplication: _applications[1]
	delivery: resources: config: data: {
		"application.json": json.Marshal(application)
		"proposal.json":    proposal
	}
	// Runs once, as the workload's service account, before the application
	// exists; the runner creates it, reads its log and deletes it.
	delivery: resources: probe: {
		apiVersion: "v1"
		kind:       "Pod"
		metadata: {
			name:      "\(runID)-access-probe"
			namespace: "tier3-pubsub"
			labels: "flink-gcp.io/run-id": runID
			annotations: {
				"flink-gcp.io/approval":   nonce
				"flink-gcp.io/expires-at": expires
				"flink-gcp.io/scenario":   scenario
			}
		}
		spec: {
			serviceAccountName:            "pubsub"
			restartPolicy:                 "Never"
			activeDeadlineSeconds:         900
			terminationGracePeriodSeconds: 10
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
				name:  "probe"
				image: images.lifecycleTools
				command: ["python3", "-I", "-c", pubsubProbeSource]
				args: [pubsubProbe]
				resources: {
					requests: {cpu: "1", memory: "2Gi", "ephemeral-storage": "128Mi"}
					limits: {cpu: "1", memory: "2Gi", "ephemeral-storage": "128Mi"}
				}
				securityContext: {
					allowPrivilegeEscalation: false
					readOnlyRootFilesystem:   true
					capabilities: drop: ["ALL"]
				}
			}]
		}
	}
}
