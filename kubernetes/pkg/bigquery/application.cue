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

package bigquery

import flink "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/flink"

// Each delivery supplies concrete run inputs and a published BigQuery image digest.
#Application: {
	run: {
		id:           string & =~"^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$"
		image:        string & =~"^us-central1-docker[.]pkg[.]dev/flink-gcp/flink-tier3/bigquery-recovery@sha256:[0-9a-f]{64}$"
		phase:        *"initial" | "upgrade"
		mode:         "ALO" | "EO"
		destinations: 10 | 50
		records:      int & >=(2 * destinations)
		if mode == "ALO" {records: *28800 | (int & <=32768)}
		if mode == "EO" {records: *1843200 | (int & <=2097152)}
	}
	let storage = "gs://flink-gcp-tier3-bigquery/runs/\(run.id)"
	resource: flink.#Application & {
		metadata: {name: run.id, namespace: "tier3-bigquery"}
		spec: {
			image:          run.image
			flinkVersion:   "v2_2"
			serviceAccount: "bigquery"
			mode:           "native"
			flinkConfiguration: {
				"kubernetes.operator.job.upgrade.last-state-fallback.enabled": "false"
				"kubernetes.operator.snapshot.resource.enabled":               "false"
				"job.autoscaler.enabled":                                      "false"
				"taskmanager.numberOfTaskSlots":                               "1"
				"state.backend.type":                                          "hashmap"
				"execution.checkpointing.storage":                             "filesystem"
				"execution.checkpointing.interval":                            "30 s"
				"execution.checkpointing.timeout":                             "120 s"
				"execution.checkpointing.max-concurrent-checkpoints":          "1"
				"execution.checkpointing.num-retained":                        "2"
				"execution.checkpointing.dir":                                 "\(storage)/checkpoints"
				"execution.checkpointing.savepoint-dir":                       "\(storage)/savepoints"
				"high-availability.type":                                      "kubernetes"
				"high-availability.storageDir":                                "\(storage)/ha"
				"restart-strategy.type":                                       "fixed-delay"
				"restart-strategy.fixed-delay.attempts":                       "3"
				"restart-strategy.fixed-delay.delay":                          "10 s"
			}
			jobManager: {
				replicas: 1
				resource: {cpu: 1, memory: "2Gi"}
				podTemplate: spec: containers: [{
					name: "flink-main-container"
					resources: {
						requests: {cpu: "1", memory: "2Gi", "ephemeral-storage": "1Gi"}
						limits: {cpu: "1", memory: "2Gi", "ephemeral-storage": "1Gi"}
					}
				}]
			}
			taskManager: {
				replicas: 2
				resource: {cpu: 1, memory: "2Gi"}
				podTemplate: spec: containers: [{
					name: "flink-main-container"
					resources: {
						requests: {cpu: "1", memory: "2Gi", "ephemeral-storage": "1Gi"}
						limits: {cpu: "1", memory: "2Gi", "ephemeral-storage": "1Gi"}
					}
				}]
			}
			job: {
				jarURI:      "local:///opt/flink/usrlib/bigquery-recovery.jar"
				entryClass:  "io.github.flink.gcp.connector.tier3.bigquery.BigQueryRecoveryJob"
				parallelism: 2
				upgradeMode: "savepoint"
				args: ["--run-id", run.id, "--phase", run.phase,
					"--mode", run.mode, "--destinations", "\(run.destinations)",
					"--records", "\(run.records)", "--bytes-per-second", "1048576",
					"--require-restored", "\(run.phase == "upgrade")"]
			}
		}
	}
}
