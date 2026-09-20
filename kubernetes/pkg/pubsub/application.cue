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

package pubsub

import flink "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/flink"

#Application: {
	run: {
		id:                     string & =~"^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$"
		image:                  string & =~"^us-central1-docker[.]pkg[.]dev/flink-gcp/flink-tier3/pubsub-recovery@sha256:[0-9a-f]{64}$"
		phase:                  *"initial" | "upgrade"
		recordsPerSubscription: *1000 | (int & >=1 & <=10000)
		parallelism:            *1 | 2
	}
	let storage = "gs://flink-gcp-tier3-pubsub/runs/\(run.id)"
	let containerResources = {
		requests: {cpu: "1", memory: "2Gi", "ephemeral-storage": "1Gi"}
		limits: {cpu: "1", memory: "2Gi", "ephemeral-storage": "1Gi"}
	}
	resource: flink.#Application & {
		metadata: {name: run.id, namespace: "tier3-pubsub"}
		spec: {
			image:          run.image
			flinkVersion:   "v2_2"
			serviceAccount: "pubsub"
			mode:           "native"
			flinkConfiguration: {
				"taskmanager.numberOfTaskSlots":                             "1"
				"state.backend.type":                                        "hashmap"
				"execution.checkpointing.storage":                           "filesystem"
				"execution.checkpointing.interval":                          "30 s"
				"execution.checkpointing.timeout":                           "120 s"
				"execution.checkpointing.max-concurrent-checkpoints":        "1"
				"execution.checkpointing.num-retained":                      "2"
				"execution.checkpointing.externalized-checkpoint-retention": "RETAIN_ON_CANCELLATION"
				"execution.checkpointing.dir":                               "\(storage)/checkpoints"
				"execution.checkpointing.savepoint-dir":                     "\(storage)/savepoints"
				"high-availability.type":                                    "kubernetes"
				"high-availability.storageDir":                              "\(storage)/ha"
				"restart-strategy.type":                                     "fixed-delay"
				"restart-strategy.fixed-delay.attempts":                     "3"
				"restart-strategy.fixed-delay.delay":                        "10 s"
			}
			jobManager: replicas:  1
			taskManager: replicas: run.parallelism
			for manager in ["jobManager", "taskManager"] {
				"\(manager)": {
					resource: {cpu: 1, memory: "2Gi"}
					podTemplate: spec: containers: [{
						name:      "flink-main-container"
						resources: containerResources
					}]
				}
			}
			podTemplate: spec: containers: [{
				name:      "flink-main-container"
				resources: containerResources
			}]
			job: {
				jarURI:                "local:///opt/flink/usrlib/pubsub-recovery.jar"
				entryClass:            "io.github.flink.gcp.connector.tier3.pubsub.PubSubRecoveryJob"
				parallelism:           run.parallelism
				upgradeMode:           "savepoint"
				allowNonRestoredState: false
				args: ["--run-id=\(run.id)", "--phase=\(run.phase)",
					"--records-per-subscription=\(run.recordsPerSubscription)",
					"--parallelism=\(run.parallelism)",
					"--require-restored=\(run.phase == "upgrade")"]
			}
		}
	}
}
