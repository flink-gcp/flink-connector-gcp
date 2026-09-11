// Generated from the Apache Flink Kubernetes Operator 1.15.0 chart.
// Regenerate with just tier3-schemas refresh; do not edit by hand.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1beta1

#FlinkSessionJob: {
	_embeddedResource
	spec?: {
		deploymentName?: string
		flinkConfiguration?: null | bool | number | string | [...] | {
			...
		}
		job?: {
			allowNonRestoredState?: bool
			args?: [...string]
			autoscalerResetNonce?:   int
			checkpointTriggerNonce?: int
			entryClass?:             string
			initialSavepointPath?:   string
			jarURI?:                 string
			parallelism?:            int
			savepointRedeployNonce?: int
			savepointTriggerNonce?:  int
			state?:                  "running" | "suspended"
			upgradeMode?:            "last-state" | "savepoint" | "stateless"
		}
		restartNonce?: int
	}
	status?: {
		error?: string
		jobStatus?: {
			checkpointInfo?: {
				formatType?: "FULL" | "INCREMENTAL" | "UNKNOWN"
				lastCheckpoint?: {
					formatType?:   "FULL" | "INCREMENTAL" | "UNKNOWN"
					timeStamp?:    int
					triggerNonce?: int
					triggerType?:  "MANUAL" | "PERIODIC" | "UNKNOWN" | "UPGRADE"
				}
				lastPeriodicCheckpointTimestamp?: int
				triggerId?:                       string
				triggerTimestamp?:                int
				triggerType?:                     "MANUAL" | "PERIODIC" | "UNKNOWN" | "UPGRADE"
			}
			jobId?:   string
			jobName?: string
			savepointInfo?: {
				formatType?:                     "CANONICAL" | "NATIVE" | "UNKNOWN"
				lastPeriodicSavepointTimestamp?: int
				lastSavepoint?: {
					formatType?:   "CANONICAL" | "NATIVE" | "UNKNOWN"
					location?:     string
					timeStamp?:    int
					triggerNonce?: int
					triggerType?:  "MANUAL" | "PERIODIC" | "UNKNOWN" | "UPGRADE"
				}
				savepointHistory?: [...{
					formatType?:   "CANONICAL" | "NATIVE" | "UNKNOWN"
					location?:     string
					timeStamp?:    int
					triggerNonce?: int
					triggerType?:  "MANUAL" | "PERIODIC" | "UNKNOWN" | "UPGRADE"
				}]
				triggerId?:        string
				triggerTimestamp?: int
				triggerType?:      "MANUAL" | "PERIODIC" | "UNKNOWN" | "UPGRADE"
			}
			startTime?:            string
			state?:                "CANCELED" | "CANCELLING" | "CREATED" | "FAILED" | "FAILING" | "FINISHED" | "INITIALIZING" | "RECONCILING" | "RESTARTING" | "RUNNING" | "SUSPENDED"
			updateTime?:           string
			upgradeSavepointPath?: string
		}
		lifecycleState?:     "CREATED" | "DELETED" | "DELETING" | "DEPLOYED" | "FAILED" | "ROLLED_BACK" | "ROLLING_BACK" | "STABLE" | "SUSPENDED" | "UPGRADING"
		observedGeneration?: int
		reconciliationStatus?: {
			lastReconciledSpec?:      string
			lastStableSpec?:          string
			reconciliationTimestamp?: int
			state?:                   "DEPLOYED" | "ROLLED_BACK" | "ROLLING_BACK" | "UPGRADING"
		}
	}

	_embeddedResource: {
		apiVersion!: string
		kind!:       string
		metadata?: {
			...
		}
	}
	apiVersion: "flink.apache.org/v1beta1"
	kind:       "FlinkSessionJob"
	metadata!: {
		name!:      string
		namespace!: string
		labels?: [string]:      string
		annotations?: [string]: string
		...
	}
}
