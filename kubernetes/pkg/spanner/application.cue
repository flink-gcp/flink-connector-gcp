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

package spanner

import (
	"time"
	flink "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/flink"
)

// Each delivery supplies concrete run inputs and a published Spanner recovery image digest.
#Application: {
	run: {
		// The instance ID is sp1360- and the run ID, which a full 40-character run label keeps
		// within Spanner's 64 characters.
		id:    string & =~"^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$"
		image: string & =~"^us-central1-docker[.]pkg[.]dev/flink-gcp/flink-tier3/spanner-recovery@sha256:[0-9a-f]{64}$"
		phase: *"initial" | "upgrade"
		// The application's own bounds, which it checks again when the job starts.
		recordsPerSecond: (*100 | int) & >=1 & <=100
		// At most 30 minutes of input, as the other trials in the same 90-minute window have. The
		// bounds apply to the default too, which a lower rate makes too long.
		records: (*180000 | int) & >=2 & <=360000 & <=(recordsPerSecond * 1800)
		// The seed copy's pace per reading subtask, which keeps the batch read's partitions
		// unfinished through both recoveries; the application's bounds.
		seedRowsPerSecond: (*50 | int) & >=1 & <=1000
		// Every vertex but the input, one subtask per single-slot TaskManager; a rescale trial's
		// phases differ. No default: the Flink package's own parallelism default would contradict
		// one.
		parallelism: 1 | 2
		// Where the Change Streams source stops, so the job can finish; the lifecycle chooses it
		// after the input's last commit. UTC, which the application parses as an instant.
		changeStreamEnd: time.Time & =~"Z$"
	}
	// The instance the lifecycle creates for this run, named from the run.
	let instance = "sp1360-\(run.id)"
	let storage = "gs://flink-gcp-tier3-spanner/runs/\(run.id)"
	let containerResources = {
		requests: {cpu: "1", memory: "2Gi", "ephemeral-storage": "1Gi"}
		limits: {cpu: "1", memory: "2Gi", "ephemeral-storage": "1Gi"}
	}

	resource: flink.#Application & {
		metadata: {name: run.id, namespace: "tier3-spanner"}
		spec: {
			image:          run.image
			flinkVersion:   "v2_2"
			serviceAccount: "spanner"
			mode:           "native"
			// The interval is the one the application enables, which takes precedence.
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
				"execution.checkpointing.tolerable-failed-checkpoints":        "2"
				"execution.checkpointing.dir":                                 "\(storage)/checkpoints"
				"execution.checkpointing.savepoint-dir":                       "\(storage)/savepoints"
				"high-availability.type":                                      "kubernetes"
				"high-availability.storageDir":                                "\(storage)/ha"
				"restart-strategy.type":                                       "fixed-delay"
				"restart-strategy.fixed-delay.attempts":                       "3"
				"restart-strategy.fixed-delay.delay":                          "10 s"
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
			job: {
				jarURI:      "local:///opt/flink/usrlib/spanner-recovery.jar"
				entryClass:  "io.github.flink.gcp.connector.tier3.spanner.SpannerRecoveryJob"
				parallelism: run.parallelism
				upgradeMode: "savepoint"
				args: ["--run-id", run.id, "--instance", instance, "--phase", run.phase,
					"--records", "\(run.records)", "--records-per-second", "\(run.recordsPerSecond)",
					"--change-stream-end", run.changeStreamEnd,
					"--seed-rows-per-second", "\(run.seedRowsPerSecond)",
					"--require-restored", "\(run.phase == "upgrade")",
					"--parallelism", "\(run.parallelism)"]
			}
		}
	}
}
