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

// The id of the DataStream entry point's one job vertex, which Flink derives from the source's
// uid; `PubSubRecoveryJob.DATASTREAM_VERTEX_ID` pins the same value, and a relay test holds it to
// the job graph.
#DataStreamVertexID: "1fc1e910bda7a08a74f8b1a434d3e9b9"

#Application: {
	run: {
		id:                     string & =~"^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$"
		image:                  string & =~"^us-central1-docker[.]pkg[.]dev/flink-gcp/flink-tier3/pubsub-recovery@sha256:[0-9a-f]{64}$"
		phase:                  *"initial" | "upgrade"
		recordsPerSubscription: *1000 | (int & >=1 & <=10000)
		parallelism:            *1 | 2 | 3
		entryPoint:             *"datastream" | "table"
		// The reactive kinds (#2055, #2126) run in the Operator's standalone mode under Flink's
		// reactive scheduler, whose parallelism follows the TaskManagers rather than the job's,
		// so a change to `taskManagers` alone rescales the job in place from its latest
		// checkpoint. Every other kind runs in native mode at `parallelism` TaskManagers.
		reactive:     *false | bool
		taskManagers: int & >=1 & <=3
		if !reactive {
			taskManagers: parallelism
		}

		// The adaptive kind (#2126) runs in native mode under Flink's AdaptiveScheduler without
		// reactive mode, which accepts a change to the job's resource requirements; the Operator
		// applies a change to `parallelismOverride` alone through that API, in place, and the
		// scheduler rescales when its next checkpoint completes.
		adaptive: *false | bool
		if reactive {
			adaptive: false
		}

		// The upper bound on the DataStream job vertex's parallelism, or null for none.
		parallelismOverride:    *null | (int & >=1 & <=3)
		restoreDeduplication:   *false | bool
		replayDelayMillis:      *0 | (int & >=0 & <=1000)
		maxOutstandingMessages: *1000 | (int & >=1 & <=100000)
		// How long the relay holds the first replay-cohort record after each snapshot of an
		// attempt that restored nothing, which keeps that checkpoint's completion notification
		// queued on the task thread (#2126).
		holdAfterCheckpointMillis: *0 | (int & >=0 & <=600000)
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
			mode: [if run.reactive {"standalone"}, "native"][0]
			flinkConfiguration: {
				if run.adaptive {
					"jobmanager.scheduler": "adaptive"
					// As for reactive mode below: a TaskManager that registered after the job
					// started at a lower parallelism would rescale it before the fault, and that
					// restore would disable the relay's hold for the rest of the trial.
					"jobmanager.adaptive-scheduler.submission.resource-stabilization-timeout": "120 s"
				}
				if run.parallelismOverride != null {
					"pipeline.jobvertex-parallelism-overrides": "\(#DataStreamVertexID):\(run.parallelismOverride)"
				}
				if run.reactive {
					"scheduler-mode": "reactive"
					// Reactive mode wants every TaskManager it can get, so it starts the job after
					// this wait rather than when the initial ones have registered. A TaskManager
					// that came after the first checkpoint would restart the job before the fault.
					"jobmanager.adaptive-scheduler.submission.resource-stabilization-timeout": "120 s"
				}
				"taskmanager.numberOfTaskSlots":                               "1"
				"kubernetes.operator.job.upgrade.last-state-fallback.enabled": "false"
				"kubernetes.operator.snapshot.resource.enabled":               "false"
				"state.backend.type":                                          "hashmap"
				"execution.checkpointing.storage":                             "filesystem"
				"execution.checkpointing.interval":                            "120 s"
				"execution.checkpointing.timeout":                             "120 s"
				"execution.checkpointing.max-concurrent-checkpoints":          "1"
				"execution.checkpointing.num-retained":                        "2"
				"execution.checkpointing.tolerable-failed-checkpoints":        "2"
				"execution.checkpointing.externalized-checkpoint-retention":   "RETAIN_ON_CANCELLATION"
				"execution.checkpointing.dir":                                 "\(storage)/checkpoints"
				"execution.checkpointing.savepoint-dir":                       "\(storage)/savepoints"
				"high-availability.type":                                      "kubernetes"
				"high-availability.storageDir":                                "\(storage)/ha"
				"restart-strategy.type":                                       "fixed-delay"
				"restart-strategy.fixed-delay.attempts":                       "3"
				"restart-strategy.fixed-delay.delay":                          "10 s"
			}
			jobManager: replicas:  1
			taskManager: replicas: run.taskManagers
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
			if run.reactive {
				// In standalone application mode the Operator (1.15.0 and 1.16.1) mounts an empty
				// volume over /opt/flink/usrlib whenever pipeline.classpaths is set, hiding the relay's
				// JAR and dependencies the image installs there, so the JobManager cannot load the entry
				// class. Its UserLibMountDecorator leaves a container alone that already mounts a
				// path under /opt/flink/usrlib, which this empty mount is for.
				jobManager: podTemplate: spec: {
					volumes: [{name: "usrlib-guard", emptyDir: {}}]
					containers: [{
						name: "flink-main-container"
						volumeMounts: [{name: "usrlib-guard", mountPath: "/opt/flink/usrlib/.operator-guard"}]
					}]
				}
			}
			job: {
				jarURI:                "local:///opt/flink/usrlib/pubsub-recovery.jar"
				entryClass:            "io.github.flink.gcp.connector.tier3.pubsub.PubSubRecoveryJob"
				parallelism:           run.parallelism
				upgradeMode:           "savepoint"
				allowNonRestoredState: false
				args: ["--run-id=\(run.id)", "--phase=\(run.phase)",
					"--records-per-subscription=\(run.recordsPerSubscription)",
					"--parallelism=\(run.parallelism)",
					"--require-restored=\(run.phase == "upgrade")",
					"--entry-point=\(run.entryPoint)",
					"--restore-deduplication=\(run.restoreDeduplication)",
					"--replay-delay-millis=\(run.replayDelayMillis)",
					"--max-outstanding-messages=\(run.maxOutstandingMessages)",
					"--hold-after-checkpoint-millis=\(run.holdAfterCheckpointMillis)"]
			}
		}
	}
}
