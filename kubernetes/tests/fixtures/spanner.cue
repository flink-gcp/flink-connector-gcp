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

import spanner "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/spanner"

let inputs = run
let application = spanner.#Application & {
	run: {id: inputs.id, image: inputs.image, phase: inputs.phase}
	if inputs.changeStreamEnd != _|_ {run: changeStreamEnd: inputs.changeStreamEnd}
	if inputs.records != _|_ {run: records: inputs.records}
	if inputs.recordsPerSecond != _|_ {run: recordsPerSecond: inputs.recordsPerSecond}
	if inputs.seedRowsPerSecond != _|_ {run: seedRowsPerSecond: inputs.seedRowsPerSecond}
	if inputs.parallelism != _|_ {run: parallelism: inputs.parallelism}
}

delivery: resources: app: application.resource
