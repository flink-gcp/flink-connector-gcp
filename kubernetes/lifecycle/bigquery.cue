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

// The trial's inputs, which dispatch and the offline renderer supply.
bigqueryMode:         *"EO" | "ALO" | "FILE_LOADS" @tag(bigquery_mode)
bigqueryDestinations: *10 | 50                     @tag(bigquery_destinations,type=int)
// The fault a trial runs in place of one of the two recoveries every trial has run, or none.
bigqueryFault: *"none" | or([for kind, _ in faultKinds if kind != defaultFault {kind}]) @tag(bigquery_fault)

// The writers' parallelism of each phase, the fault kind's; a trial without a fault keeps the
// default kind's. Indexed, because with the package sources on standard input, as every render
// supplies them, the tag's disjunction alone refused no fault in an export of the applications.
_bigqueryFaults: {
	"none": faultKinds[defaultFault]
	for kind, sizes in faultKinds if kind != defaultFault {(kind): sizes}
}
_bigqueryParallelism: _bigqueryFaults[bigqueryFault]

// Both BigQuery phases come from the same input identity and fixed package.
// approval.json stays empty in a proposal; dispatch embeds the approval when it
// binds the delivery.
if scenario == "bigquery-recovery" {
	_applications: [for trialPhase in ["initial", "upgrade"] {
		(runPolicy & {
			run: {id: runID, expiresAt: expires, namespace: "tier3-bigquery", image: applicationImage}
			delivery: resources: app: (bigquery.#Application & {
				run: {id: runID, image: applicationImage, phase: trialPhase
					mode: bigqueryMode, destinations: bigqueryDestinations, parallelism: _bigqueryParallelism[trialPhase]
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
