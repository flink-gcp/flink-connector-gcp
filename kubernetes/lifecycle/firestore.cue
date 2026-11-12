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
	firestore "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/firestore"
	runPolicy "github.com/flink-gcp/flink-connector-gcp/kubernetes/runs:tier3"
)

// The trial's inputs, which dispatch and the offline renderer supply; the package owns their
// defaults and bounds.
firestoreRecords:              int @tag(firestore_records,type=int)
firestoreRecordsPerSecond:     int @tag(firestore_records_per_second,type=int)
firestoreSeedRecordsPerSecond: int @tag(firestore_seed_records_per_second,type=int)
// A single-fault trial runs a savepoint upgrade and then deletes a Pod. Its kind names the fault
// that makes it a trial of its own: the JobManager deleted, a TaskManager deleted in its place, or
// an upgrade that rescales every vertex but the input from one subtask to two or from two to one.
// The `sequence` trial (ADR-0204) runs every fault in one run, starting and upgrading at two; the
// exercise patches its rescales from the upgrade.
_firestoreKinds: {
	faultKinds
	sequence: faultKinds[defaultFault]
}
firestoreTrial: *defaultFault | or([for kind, _ in _firestoreKinds {kind}]) @tag(firestore_trial)

// Both Firestore phases come from the same input identity and fixed package.
// approval.json stays empty in a proposal; dispatch embeds the approval when it
// binds the delivery.
if scenario == "firestore-recovery" {
	_applications: [for trialPhase in ["initial", "upgrade"] {
		(runPolicy & {
			run: {id: runID, expiresAt: expires, namespace: "tier3-firestore", image: applicationImage}
			delivery: resources: app: (firestore.#Application & {
				run: {id: runID, image: applicationImage, phase: trialPhase
					records:              firestoreRecords, recordsPerSecond: firestoreRecordsPerSecond
					seedRecordsPerSecond: firestoreSeedRecordsPerSecond
					parallelism:          _firestoreKinds[firestoreTrial][trialPhase]
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
