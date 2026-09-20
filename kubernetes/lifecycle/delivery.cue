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
	"list"
	"time"
	"github.com/flink-gcp/flink-connector-gcp/kubernetes/images"
	cloudtasks "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/cloudtasks"
	smoke "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/smoke"
	runPolicy "github.com/flink-gcp/flink-connector-gcp/kubernetes/runs:tier3"
)

runID:         string & =~"^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$" @tag(run_id)
nonce:         string & =~"^[0-9a-f]{32}$"                       @tag(nonce)
expires:       time.Time                                         @tag(expires_at)
approval:      *"{}" | string                                    @tag(approval)
activeSeconds: int & >0                                          @tag(active_seconds,type=int)
scenario:      *"smoke" | "generic-recovery" | "cloudtasks"      @tag(scenario)
// Cloud Tasks only: a JSON array of session cells, the runtime line, the
// published application digest for that line and the synthetic HTTPS target.
cells:            *"[]" | string      @tag(cells)
flinkVersion:     *"2.2.1" | "1.20.4" @tag(flink_version)
applicationImage: *"" | string        @tag(application_image)
targetURL:        *"" | string        @tag(target)
// Supplied as JSON by flink-tier3 render or the lifecycle runner.
packageSources: {
	"__init__.py"!:                 string
	"__main__.py"!:                 string
	"runtime.py"!:                  string
	"policy.toml"!:                 string
	[=~"^[a-z0-9_]+[.](py|toml)$"]: string
}

// A smoke run is bounded by one 60-minute approval; a measurement session by
// its plan plus cleanup, at most 300 minutes.
if scenario != "cloudtasks" {activeSeconds: <=3420}
if scenario == "cloudtasks" {activeSeconds: <=17820}

// Use the same namespace, image, Spot and schema constraints as ordinary runs.
// Each scenario declares its application expression and its application.json
// entry inside one body: a field declared in a comprehension body is visible
// only to references inside that body.
if scenario != "cloudtasks" {
	application: (runPolicy & {
		run: {id: runID, expiresAt: expires, image: images.smoke}
		delivery: resources: app: (smoke.#Application & {run: {
			id: runID, image: images.smoke
			if scenario == "generic-recovery" {records: 12000}
		}}).resource & {
			metadata: annotations: "flink-gcp.io/approval": nonce
			metadata: annotations: "flink-gcp.io/scenario": scenario
			if scenario == "generic-recovery" {
				spec: flinkConfiguration: "kubernetes.operator.job.upgrade.last-state-fallback.enabled": "false"
				spec: flinkConfiguration: "kubernetes.operator.snapshot.resource.enabled":               "false"
			}
		}
	}).delivery.resources.app
	delivery: resources: config: data: "application.json": json.Marshal(application)
}

upgradeApplication: _
if scenario == "generic-recovery" {
	upgradeApplication: (runPolicy & {
		run: {id: runID, expiresAt: expires, image: images.smoke}
		delivery: resources: app: (smoke.#Application & {
			run: {id: runID, image: images.smoke, phase: "upgrade", records: 12000}
		}).resource & {
			metadata: annotations: "flink-gcp.io/approval":                                          nonce
			metadata: annotations: "flink-gcp.io/scenario":                                          scenario
			spec: flinkConfiguration: "kubernetes.operator.job.upgrade.last-state-fallback.enabled": "false"
			spec: flinkConfiguration: "kubernetes.operator.snapshot.resource.enabled":               "false"
		}
	}).delivery.resources.app
}

// One FlinkDeployment per session cell, in session order. Every cell passes the
// same run policy as a smoke application, in the tier3-cloudtasks namespace.
if scenario == "cloudtasks" {
	_cellList: [...cloudtasks.#Cell] & json.Unmarshal(cells)
	// The ConfigMap's kind disjunction evaluates this body where CUE 0.17
	// reports only errors on the exported value path: a closed-struct violation,
	// a false comprehension guard or a conflicting hidden field that the
	// manifests do not read still renders. Both session checks therefore feed
	// the manifests. Cells are keyed by ID with their session position, so a
	// duplicate ID conflicts on the position the ordering reads. Every #Cell
	// field is required, so a cell with more fields than #Cell carries an
	// unknown key, and each manifest is selected by that field count.
	_cellsByID: {for i, c in _cellList {(c.id): {position: i, cell: c}}}
	_orderedCells: list.Sort([for _, entry in _cellsByID {entry}], {
		x: {position: int, ...}
		y: {position: int, ...}
		less: x.position < y.position
	})
	_cellFieldCount: len([for k, _ in cloudtasks.#Cell {k}])
	// An empty session would admit paid infrastructure for nothing.
	cellManifests: list.MinItems(1) & [
		for entry in _orderedCells let c = entry.cell {
			{
				"\(_cellFieldCount)": (runPolicy & {
					run: {id: runID, expiresAt: expires, namespace: "tier3-cloudtasks", image: applicationImage}
					delivery: resources: app: (cloudtasks.#Application & {
						run: {
							id:     runID
							line:   flinkVersion
							image:  applicationImage
							queue:  "projects/flink-gcp/locations/us-central1/queues/ct1246-\(runID)"
							target: targetURL
						}
						cell: c
					}).resource & {
						metadata: annotations: "flink-gcp.io/approval": nonce
						metadata: annotations: "flink-gcp.io/scenario": scenario
					}
				}).delivery.resources.app
			}["\(len(c))"]
		},
	]
	delivery: resources: config: data: "application.json": json.Marshal(cellManifests)
}

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
			// application.json is declared by the selected scenario's body above.
			if scenario == "generic-recovery" {"upgrade-application.json": json.Marshal(upgradeApplication)}
		}
	}
	supervisor: {
		apiVersion: "batch/v1"
		kind:       "Job"
		metadata: sharedMetadata & {name: "lifecycle-\(runID)"}
		spec: {
			parallelism: 1
			completions: 1
			// The infrastructure can take the supervisor Pod away before it
			// admits anything; two replacements cover that without letting a
			// crash loop spend the approved window.
			backoffLimit:          2
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
						if scenario == "generic-recovery" {{key: "upgrade-application.json", path: "upgrade-application.json"}},
					]}}]
				}
			}
		}
	}
}
