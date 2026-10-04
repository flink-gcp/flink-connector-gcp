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
	cloudtasks "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/cloudtasks"
	runPolicy "github.com/flink-gcp/flink-connector-gcp/kubernetes/runs:tier3"
)

// Cloud Tasks only: the session cell array, runtime line and synthetic HTTPS target.
cells:        *"[]" | string      @tag(cells)
flinkVersion: *"2.2.1" | "1.20.4" @tag(flink_version)
targetURL:    *"" | string        @tag(target)

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
