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
	bigquery "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/bigquery"
	runPolicy "github.com/flink-gcp/flink-connector-gcp/kubernetes/runs:tier3"
)

// Offline BigQuery application inputs; these do not extend lifecycle admission.
bigqueryMode:         *"EO" | "ALO" | "FILE_LOADS" @tag(bigquery_mode)
bigqueryDestinations: *10 | 50                     @tag(bigquery_destinations,type=int)

// Both BigQuery phases come from the same input identity and fixed package.
// approval.json remains empty in offline proposals; runtime validation refuses it.
if scenario == "bigquery-recovery" {
	_applications: [for trialPhase in ["initial", "upgrade"] {
		(runPolicy & {
			run: {id: runID, expiresAt: expires, namespace: "tier3-bigquery", image: applicationImage}
			delivery: resources: app: (bigquery.#Application & {
				run: {id: runID, image: applicationImage, phase: trialPhase
					mode: bigqueryMode, destinations: bigqueryDestinations
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
}
