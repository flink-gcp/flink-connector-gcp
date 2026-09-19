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

package cloudtasks

import (
	"strconv"
	flink "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/flink"
)

// One measurement cell of a reviewed session file; the vocabulary matches the
// application's argument grammar in kubernetes/apps/cloudtasks/README.md.
#Cell: {
	id:                   string & =~"^[a-z0-9][a-z0-9-]{0,39}$"
	arm:                  "UNNAMED" | "NAMED_HASH" | "NAMED_RANDOM_CONTROL" | "STAGED_HASH" | "STAGED_RANDOM"
	body_bytes:           1024 | 65536
	parallelism:          1 | 4 | 16
	concurrency:          1 | 4 | 16
	checkpoint_seconds:   1 | 10 | 60
	channel_pool_size:    1 | 4 | 8
	distribution:         "even" | "skew"
	offered_rate:         int & >=1 & <=10000
	warmup_seconds:       int & >=1 & <=120
	observation_seconds:  int & >=1 & <=600
	record_limit:         int & >=1 & <=10000000
	attempt_limit:        int & >=1
	control_delay_millis: 0 | 100
	emit_attempts:        bool
}

// Each runtime line has its own published GAR package.
let images = {
	"2.2.1":  "^us-central1-docker[.]pkg[.]dev/flink-gcp/flink-tier3/cloudtasks-measurement@sha256:[0-9a-f]{64}$"
	"1.20.4": "^us-central1-docker[.]pkg[.]dev/flink-gcp/flink-tier3/cloudtasks-measurement-flink120@sha256:[0-9a-f]{64}$"
}

let flinkVersions = {
	"2.2.1":  "v2_2"
	"1.20.4": "v1_20"
}

// Container quantities per manager; requests equal limits, and the Operator's
// numeric resource block mirrors cpu and memory. The TaskManager shape follows
// the cell's parallelism class.
let jobManagerShape = {cpu: "1", memory: "2Gi", "ephemeral-storage": "1Gi"}
let taskManagerShapes = {
	"1": {cpu: "1", memory: "4Gi", "ephemeral-storage": "1Gi"}
	"4": {cpu: "2", memory: "8Gi", "ephemeral-storage": "2Gi"}
	"16": {cpu: "4", memory: "16Gi", "ephemeral-storage": "4Gi"}
}

// One FlinkDeployment per cell; the delivery supplies the run inputs and the
// published application digest for the selected line.
#Application: {
	run: {
		id:     string & =~"^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$"
		line:   "2.2.1" | "1.20.4"
		image:  string & =~images[line]
		queue:  string & =~"^projects/flink-gcp/locations/us-central1/queues/ct1246-[a-z0-9-]+$"
		target: string & =~"^https://"
	}
	cell: #Cell
	let storage = "gs://flink-gcp-cloudtasks-benchmark/runs/\(run.id)/cells/\(cell.id)/state"
	let taskManagerShape = taskManagerShapes["\(cell.parallelism)"]
	resource: flink.#Application & {
		metadata: {
			name: cell.id
			annotations: "flink-gcp.io/cell": cell.id
		}
		spec: {
			image:          run.image
			flinkVersion:   flinkVersions[run.line]
			serviceAccount: "cloudtasks-benchmark"
			mode:           "native"
			flinkConfiguration: {
				"taskmanager.numberOfTaskSlots":                             "\(cell.parallelism)"
				"state.backend.type":                                        "hashmap"
				"execution.checkpointing.storage":                           "filesystem"
				"execution.checkpointing.interval":                          "\(cell.checkpoint_seconds) s"
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
			// JobManager and TaskManager shapes differ, so each manager carries its
			// own container resources; runs/common.cue still applies the shared
			// Spot/AMD64 policy to both templates.
			jobManager: {
				replicas: 1
				resource: {cpu: strconv.Atoi(jobManagerShape.cpu), memory: jobManagerShape.memory}
				podTemplate: spec: containers: [{
					name: "flink-main-container"
					resources: {requests: jobManagerShape, limits: jobManagerShape}
				}]
			}
			taskManager: {
				replicas: 1
				resource: {cpu: strconv.Atoi(taskManagerShape.cpu), memory: taskManagerShape.memory}
				podTemplate: spec: containers: [{
					name: "flink-main-container"
					resources: {requests: taskManagerShape, limits: taskManagerShape}
				}]
			}
			job: {
				jarURI:                "local:///opt/flink/usrlib/cloudtasks-measurement.jar"
				entryClass:            "io.github.flink.gcp.connector.tier3.cloudtasks.CloudTasksMeasurementJob"
				parallelism:           cell.parallelism
				upgradeMode:           "stateless"
				allowNonRestoredState: false
				args: [
					"--run-id", run.id,
					"--cell-id", cell.id,
					"--queue", run.queue,
					"--target", run.target,
					"--arm", cell.arm,
					"--body-bytes", "\(cell.body_bytes)",
					"--parallelism", "\(cell.parallelism)",
					"--concurrency", "\(cell.concurrency)",
					"--checkpoint-seconds", "\(cell.checkpoint_seconds)",
					"--channel-pool-size", "\(cell.channel_pool_size)",
					"--distribution", cell.distribution,
					"--offered-rate", "\(cell.offered_rate)",
					"--warmup-seconds", "\(cell.warmup_seconds)",
					"--observation-seconds", "\(cell.observation_seconds)",
					"--record-limit", "\(cell.record_limit)",
					"--attempt-limit", "\(cell.attempt_limit)",
					"--control-delay-millis", "\(cell.control_delay_millis)",
					"--emit-attempts", "\(cell.emit_attempts)",
				]
			}
		}
	}
}
