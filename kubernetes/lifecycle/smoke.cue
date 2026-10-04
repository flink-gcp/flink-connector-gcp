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
	smoke "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/smoke"
	runPolicy "github.com/flink-gcp/flink-connector-gcp/kubernetes/runs:tier3"
)

// Use the same namespace, image, Spot and schema constraints as ordinary runs.
// Each scenario declares its application expression and its application.json
// entry inside one body: a field declared in a comprehension body is visible
// only to references inside that body.
if scenario == "smoke" || scenario == "generic-recovery" {
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
