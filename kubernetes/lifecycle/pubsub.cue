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

// The reviewed trial's inputs, which dispatch and the offline renderer supply.
// The AdaptiveScheduler kinds only this scenario runs (#2055, reworked by #2126), as
// `flink_tier3/pubsub/verdict.py` declares them in its `ADAPTIVE` table, which a test holds this
// to: whether the scheduler runs in reactive mode, the job vertex's parallelism before and after
// the fault, and how long the relay holds a checkpoint's notification. Their per-record delay and
// flow-control limit are the same module's constants.
pubsubAdaptiveKinds: {
	"reactive-scale-out": {scheduler: "reactive", initial: 2, recovery: 3, holdMillis: 30000}
	"reactive-tm-kill": {scheduler: "reactive", initial: 3, recovery: 3, holdMillis: 90000}
	"adaptive-scale-in": {scheduler: "adaptive", initial: 3, recovery: 2, holdMillis: 30000}
}
pubsubAdaptiveReplayDelayMillis: 100
pubsubAdaptiveMaxOutstanding:    100000
pubsubTrial: *defaultFault | or([for kind, _ in faultKinds {kind}, for kind, _ in pubsubAdaptiveKinds {kind}]) @tag(pubsub_trial)
pubsubRecords:              *1000 | (int & >=2 & <=10000) @tag(pubsub_records,type=int)
pubsubEntryPoint:           *"datastream" | "table"       @tag(pubsub_entry_point)
pubsubRestoreDeduplication: *false | bool                 @tag(pubsub_restore_deduplication,type=bool)
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
					id:                   runID, image: applicationImage, recordsPerSubscription: pubsubRecords, entryPoint: pubsubEntryPoint
					restoreDeduplication: pubsubRestoreDeduplication
					// An unknown kind takes this branch, whose index refuses it.
					if pubsubAdaptiveKinds[pubsubTrial] == _|_ {
						// The recovery manifest takes the kind's upgrade size, and only a rescale's runs the upgrade phase.
						parallelism: faultKinds[pubsubTrial][[if recovery {"upgrade"}, "initial"][0]]
						phase: [if recovery && faultKinds[pubsubTrial].rescale != null {"upgrade"}, "initial"][0]
					}
					if pubsubAdaptiveKinds[pubsubTrial] != _|_ {
						// Both manifests run the same job. A reactive kind's recovery manifest
						// changes only the TaskManagers, and the adaptive kind's only the job
						// vertex's parallelism override; the Operator applies either in place.
						if pubsubAdaptiveKinds[pubsubTrial].scheduler == "reactive" {
							reactive:     true
							taskManagers: pubsubAdaptiveKinds[pubsubTrial][[if recovery {"recovery"}, "initial"][0]]
						}
						if pubsubAdaptiveKinds[pubsubTrial].scheduler == "adaptive" {
							adaptive: true
							if recovery {
								parallelismOverride: pubsubAdaptiveKinds[pubsubTrial].recovery
							}
						}

						// Each of these kinds shows restore deduplication suppressing its case, so
						// it runs only with the setting on, and only the DataStream entry point
						// chains the holding observer to the source; a delivery saying
						// otherwise fails here.
						restoreDeduplication:      true
						entryPoint:                "datastream"
						parallelism:               pubsubAdaptiveKinds[pubsubTrial].initial
						replayDelayMillis:         pubsubAdaptiveReplayDelayMillis
						maxOutstandingMessages:    pubsubAdaptiveMaxOutstanding
						holdAfterCheckpointMillis: pubsubAdaptiveKinds[pubsubTrial].holdMillis
					}
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
